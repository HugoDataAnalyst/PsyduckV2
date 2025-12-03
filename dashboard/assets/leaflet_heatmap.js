(function() {
    'use strict';

    var mapInstance = null;
    var markersLayer = null;
    var heatLayer = null;
    var gridLayer = null;
    var legendControl = null;

    // HELPER FUNCTIONS

    // Get color for Grid mode (0.0 to 1.0)
    function getColor(value) {
        var h = (1.0 - value) * 240;
        return "hsl(" + h + ", 100%, 50%)";
    }

    // Dynamic Legend Control
    function updateLegend(min, max, mode) {
        if (legendControl) {
            legendControl.remove();
            legendControl = null;
        }

        // Only show legend for Density or Grid
        if (mode === 'markers') return;

        legendControl = L.control({position: 'bottomright'});

        legendControl.onAdd = function (map) {
            var div = L.DomUtil.create('div', 'info legend');
            // Basic styling for the legend box
            div.style.backgroundColor = "rgba(255,255,255,0.9)";
            div.style.padding = "8px 12px";
            div.style.borderRadius = "5px";
            div.style.boxShadow = "0 0 15px rgba(0,0,0,0.2)";
            div.style.color = "#333";
            div.style.fontFamily = "sans-serif";

            var title = (mode === 'grid') ? 'Spawns per Cell' : 'Heatmap Intensity';
            var maxLabel = (mode === 'grid') ? Math.round(max) : 'High';
            var minLabel = (mode === 'grid') ? min : 'Low';

            div.innerHTML = `<strong>${title}</strong><br>`;

            // Gradient Bar Visual
            div.innerHTML += `
                <div style="
                    background: linear-gradient(to right, blue, cyan, lime, yellow, red);
                    width: 150px;
                    height: 15px;
                    margin-top: 5px;
                    margin-bottom: 5px;
                    border: 1px solid #ccc;">
                </div>
                <div style="display: flex; justify-content: space-between; font-size: 12px;">
                    <span>${minLabel}</span>
                    <span>${maxLabel}</span>
                </div>
            `;
            return div;
        };

        legendControl.addTo(mapInstance);
    }

    // MAIN RENDER FUNCTION

    function renderHeatmap(data, blocklist, renderMode) {
        var container = document.getElementById('heatmap-map-container');
        if (!container) return;

        // Reset Logic
        if (mapInstance) {
            // Clear layers but try to keep map instance if possible?
            // For stability with switching modes, it's often safer to clear layers.
            if (markersLayer) {
                markersLayer.clearLayers();
                // If it's a cluster group, also remove it from the map
                if (typeof markersLayer.clearLayers === 'function') {
                    mapInstance.removeLayer(markersLayer);
                }
            }
            if (heatLayer) mapInstance.removeLayer(heatLayer);
            if (gridLayer) mapInstance.removeLayer(gridLayer);
            if (legendControl) legendControl.remove();
        }

        // Validate Data
        if (!Array.isArray(data) || data.length === 0) {
            if(mapInstance) mapInstance.remove(); // Clean up if no data
            mapInstance = null;
            container.innerHTML = '<div style="color: #aaa; text-align: center; padding: 20px;">No data points available.</div>';
            return;
        }

        try {
            // Filter Data based on Blocklist
            let filteredData = data;
            if (blocklist && blocklist.length > 0) {
                const blockedSet = new Set(blocklist);
                filteredData = data.filter(function(p) {
                    const key = p.pokemon_id + ":" + p.form;
                    return !blockedSet.has(key);
                });
            }

            if (filteredData.length === 0) {
                if(mapInstance) mapInstance.remove();
                mapInstance = null;
                container.innerHTML = '<div style="color: #aaa; text-align: center; padding: 20px;">All points filtered out.</div>';
                return;
            }

            // MAP INITIALIZATION

            // Calculate bounds
            var minLat = 90, maxLat = -90, minLon = 180, maxLon = -180;
            for (var i = 0; i < filteredData.length; i++) {
                var lat = filteredData[i].latitude;
                var lon = filteredData[i].longitude;
                if (lat < minLat) minLat = lat;
                if (lat > maxLat) maxLat = lat;
                if (lon < minLon) minLon = lon;
                if (lon > maxLon) maxLon = lon;
            }

            if (!mapInstance) {
                mapInstance = L.map('heatmap-map-container', {
                    center: [(minLat + maxLat) / 2, (minLon + maxLon) / 2],
                    zoom: 13,
                    preferCanvas: true
                });

                L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                    maxZoom: 19,
                    attribution: '© OpenStreetMap'
                }).addTo(mapInstance);
            }

            // Always fit bounds on new data render
            mapInstance.fitBounds([[minLat, minLon], [maxLat, maxLon]], { padding: [20, 20] });


            // RENDER MODES

            if (renderMode === 'density') {
                // DENSITY HEATMAP
                if (typeof L.heatLayer !== 'function') {
                    console.error("Leaflet.heat plugin not loaded!");
                    return;
                }

                // Normalize based on the busiest single spawnpoint
                var maxSingleCount = 0;
                for (var i = 0; i < filteredData.length; i++) {
                    if (filteredData[i].count > maxSingleCount) maxSingleCount = filteredData[i].count;
                }
                if (maxSingleCount === 0) maxSingleCount = 1;

                // Map points: intensity = count / maxSingleCount
                var heatPoints = filteredData.map(p => {
                    return [p.latitude, p.longitude, p.count / maxSingleCount];
                });

                // Dynamic Saturation Threshold
                // Raise max saturation cap as dataset grows to prevent BIAS visual
                var datasetSize = filteredData.length;
                var saturationThreshold = 1.0;

                if (datasetSize > 15000) saturationThreshold = 8.0;
                else if (datasetSize > 5000) saturationThreshold = 5.0;
                else if (datasetSize > 1000) saturationThreshold = 3.0;
                else if (datasetSize > 200) saturationThreshold = 1.5;

                // Adjust radius slightly for density
                var radius = datasetSize > 5000 ? 15 : 25;

                heatLayer = L.heatLayer(heatPoints, {
                    radius: radius,
                    blur: radius * 0.7,
                    maxZoom: 17,
                    max: saturationThreshold, // Dynamic scaling
                    minOpacity: 0.3,
                    gradient: {0.0: 'blue', 0.4: 'cyan', 0.6: 'lime', 0.8: 'orange', 1.0: 'red'}
                }).addTo(mapInstance);

                updateLegend(0, "High", 'density');

            } else if (renderMode === 'grid') {
                // GRID
                gridLayer = L.layerGroup().addTo(mapInstance);

                // Grid size approx 50-60 meters
                var step = 0.0005;
                var grid = {};
                var maxGridCount = 0;

                // Binning - also collect Pokemon info per cell
                filteredData.forEach(p => {
                    var latKey = Math.floor(p.latitude / step);
                    var lonKey = Math.floor(p.longitude / step);
                    var key = latKey + "_" + lonKey;

                    if (!grid[key]) {
                        grid[key] = {
                            count: 0,
                            bounds: [[latKey * step, lonKey * step], [(latKey+1) * step, (lonKey+1) * step]],
                            pokemon: {}  // Track Pokemon in this cell
                        };
                    }
                    grid[key].count += p.count;
                    if (grid[key].count > maxGridCount) maxGridCount = grid[key].count;

                    // Aggregate Pokemon data per cell
                    var monKey = p.pokemon_id + ":" + p.form;
                    if (!grid[key].pokemon[monKey]) {
                        grid[key].pokemon[monKey] = {
                            pokemon_id: p.pokemon_id,
                            form: p.form,
                            species_name: p.species_name || 'Pokemon #' + p.pokemon_id,
                            form_name: p.form_name || '',
                            icon_url: p.icon_url || ('https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main/pokemon/' + p.pokemon_id + '.webp'),
                            count: 0
                        };
                    }
                    grid[key].pokemon[monKey].count += p.count;
                });

                // Drawing
                Object.values(grid).forEach(cell => {
                    var intensity = cell.count / maxGridCount;
                    var hue = (1 - intensity) * 240;
                    var colorStr = `hsla(${hue}, 100%, 50%, 0.75)`;

                    // Build rich tooltip HTML with Pokemon images
                    var tooltipHtml = '<div style="text-align:center; min-width: 120px;">';
                    tooltipHtml += '<strong style="font-size: 14px;">Total: ' + cell.count + '</strong><hr style="margin: 5px 0;">';

                    // Sort Pokemon by count descending
                    var pokemonList = Object.values(cell.pokemon).sort((a, b) => b.count - a.count);

                    // Show top 5 Pokemon max to keep tooltip manageable
                    pokemonList.slice(0, 5).forEach(mon => {
                        var iconUrl = mon.icon_url;

                        tooltipHtml += '<div style="display: flex; align-items: center; margin: 4px 0; gap: 8px;">';
                        tooltipHtml += '<img src="' + iconUrl + '" style="width: 32px; height: 32px;" onerror="this.style.display=\'none\'">';
                        tooltipHtml += '<div style="text-align: left;">';
                        tooltipHtml += '<div style="font-weight: bold; font-size: 12px;">' + mon.species_name + '</div>';
                        if (mon.form_name) {
                            tooltipHtml += '<div style="font-size: 10px; color: #666;">' + mon.form_name + '</div>';
                        }
                        tooltipHtml += '<div style="font-size: 11px; color: #333;">Count: ' + mon.count + '</div>';
                        tooltipHtml += '</div></div>';
                    });

                    if (pokemonList.length > 5) {
                        tooltipHtml += '<div style="font-size: 10px; color: #888; margin-top: 4px;">+' + (pokemonList.length - 5) + ' more...</div>';
                    }
                    tooltipHtml += '</div>';

                    L.rectangle(cell.bounds, {
                        color: "transparent",
                        weight: 1,
                        fillColor: colorStr,
                        fillOpacity: 0.75,
                    })
                    .bindTooltip(tooltipHtml, {direction: 'top', sticky: true, className: 'pokemon-grid-tooltip'})
                    .addTo(gridLayer);
                });

                updateLegend(1, maxGridCount, 'grid');

            } else {
                // MARKERS MODE
                // Use MarkerClusterGroup if available, otherwise fallback to regular LayerGroup
                if (typeof L.markerClusterGroup === 'function') {
                    markersLayer = L.markerClusterGroup({
                        // Much more aggressive clustering for large datasets
                        maxClusterRadius: function(zoom) {
                            // Larger radius at lower zoom = more clustering
                            return (zoom < 13) ? 120 : (zoom < 15) ? 80 : 60;
                        },
                        disableClusteringAtZoom: 17, // Show individual markers only when very zoomed in
                        spiderfyOnMaxZoom: false, // Disable spiderfy, just zoom in instead
                        showCoverageOnHover: false,
                        zoomToBoundsOnClick: true,
                        chunkedLoading: true, // Better performance for large datasets
                        chunkInterval: 200,
                        chunkDelay: 50,
                        iconCreateFunction: function(cluster) {
                            var count = cluster.getChildCount();
                            // Larger clusters for visibility
                            var size = Math.min(70, Math.max(40, 40 + Math.log10(count + 1) * 12));

                            // Color based on count for better visual feedback
                            var color = 'rgba(110, 204, 57, 0.85)'; // Green
                            if (count > 100) color = 'rgba(255, 165, 0, 0.85)'; // Orange
                            if (count > 500) color = 'rgba(255, 69, 0, 0.85)'; // Red

                            return L.divIcon({
                                html: '<div style="background-color: ' + color + '; border-radius: 50%; width: ' + size + 'px; height: ' + size + 'px; display: flex; align-items: center; justify-content: center; color: white; font-weight: bold; font-size: ' + Math.min(16, Math.max(11, size/3)) + 'px; border: 3px solid rgba(255,255,255,0.6); box-shadow: 0 0 10px rgba(0,0,0,0.3);">' + count + '</div>',
                                className: 'marker-cluster-custom',
                                iconSize: L.point(size, size)
                            });
                        }
                    }).addTo(mapInstance);
                } else {
                    markersLayer = L.layerGroup().addTo(mapInstance);
                }

                // Group by precise location
                var grouped = {};
                filteredData.forEach(function(p) {
                    var key = p.spawnpoint ? String(p.spawnpoint) : (p.latitude + "," + p.longitude);
                    if (!grouped[key]) grouped[key] = { lat: p.latitude, lon: p.longitude, spawnpoint: p.spawnpoint, mons: [], totalCount: 0 };
                    grouped[key].mons.push(p);
                    grouped[key].totalCount += p.count;
                });

                var locations = Object.values(grouped);
                const uniqueSpecies = new Set();
                filteredData.forEach(p => uniqueSpecies.add(p.pokemon_id + ":" + p.form));
                const useIcons = (uniqueSpecies.size === 1);

                locations.forEach(function(loc, idx) {
                    // Build rich popup HTML with Pokemon images and names
                    var popupHtml = '<div style="text-align:center; max-height: 250px; overflow-y: auto; min-width: 150px;">';

                    if (loc.spawnpoint) {
                        popupHtml += '<div style="font-size: 11px; color: #666; margin-bottom: 5px;">Spawnpoint: ' + loc.spawnpoint + '</div>';
                    }

                    popupHtml += '<div style="font-weight: bold; font-size: 14px; margin-bottom: 8px; padding-bottom: 5px; border-bottom: 1px solid #ddd;">Total: ' + loc.totalCount + '</div>';

                    // Sort Pokemon by count descending
                    loc.mons.sort((a, b) => b.count - a.count);

                    loc.mons.forEach(function(m) {
                        var iconUrl = m.icon_url || ('https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main/pokemon/' + m.pokemon_id + '.webp');
                        var speciesName = m.species_name || 'Pokemon #' + m.pokemon_id;
                        var formName = m.form_name || '';

                        popupHtml += '<div style="display: flex; align-items: center; margin: 8px 0; padding: 5px; background: #f8f8f8; border-radius: 8px; gap: 10px;">';
                        popupHtml += '<img src="' + iconUrl + '" style="width: 48px; height: 48px; flex-shrink: 0;" onerror="this.src=\'https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main/pokemon/0.webp\'">';
                        popupHtml += '<div style="text-align: left; flex-grow: 1;">';
                        popupHtml += '<div style="font-weight: bold; font-size: 13px; color: #333;">' + speciesName + '</div>';
                        if (formName) {
                            popupHtml += '<div style="font-size: 11px; color: #666;">' + formName + '</div>';
                        }
                        popupHtml += '<div style="font-size: 12px; color: #007bff; font-weight: bold;">Count: ' + m.count + '</div>';
                        popupHtml += '</div></div>';
                    });

                    popupHtml += '</div>';

                    if (useIcons) {
                        // Single Pokemon Mode - Use Image
                        var primaryMon = loc.mons[0];
                        var iconUrl = primaryMon.icon_url || ('https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main/pokemon/' + primaryMon.pokemon_id + '.webp');
                        var iconSize = Math.min(50, Math.max(30, 30 + Math.log10(loc.totalCount + 1) * 10));

                        L.marker([loc.lat, loc.lon], {
                            icon: L.icon({iconUrl: iconUrl, iconSize: [iconSize, iconSize], className: 'pokemon-marker-icon'}),
                            zIndexOffset: idx
                        }).bindPopup(popupHtml, {maxWidth: 300}).addTo(markersLayer);
                    } else {
                        // Multi Pokemon Mode - Use Colored Dots
                        var size = Math.min(50, Math.max(15, 15 + Math.log10(loc.totalCount + 1) * 15));
                        var colorClass = (loc.totalCount > 10) ? 'high' : (loc.totalCount > 3) ? 'medium' : 'low'; // Simplified thresholds

                        var divHtml = '<div class="heatmap-dot ' + colorClass + '" style="width:' + size + 'px; height:' + size + 'px; line-height:' + size + 'px; font-size:' + Math.max(10, size/3) + 'px;">' + loc.totalCount + '</div>';

                        L.marker([loc.lat, loc.lon], {
                            icon: L.divIcon({className: 'custom-div-icon', html: divHtml, iconSize: [size, size], iconAnchor: [size/2, size/2]}),
                            zIndexOffset: idx
                        }).bindPopup(popupHtml, {maxWidth: 300}).addTo(markersLayer);
                    }
                });
            }

        } catch (err) {
            console.error("Error rendering heatmap:", err);
            container.innerHTML = '<div style="color: red; text-align: center;">Error rendering map. Check console.</div>';
        }
    }

    window.renderPokemonHeatmap = renderHeatmap;
})();
