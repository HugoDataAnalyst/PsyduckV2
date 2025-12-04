// Invasion Heatmap Renderer for Pokestops
(function() {
    var mapInstance = null;
    var markersLayer = null;
    var heatLayer = null;
    var gridLayer = null;
    var legendControl = null;

    // Grunt type colors based on type
    function getGruntTypeColor(charId) {
        // Leaders & Giovanni
        if (charId === 41) return "#d32f2f";  // Cliff - Red
        if (charId === 42) return "#f57c00";  // Arlo - Orange
        if (charId === 43) return "#7b1fa2";  // Sierra - Purple
        if (charId === 44) return "#212121";  // Giovanni - Black

        // Type-based grunts (approximate groupings)
        var id = parseInt(charId);

        // Bug (6, 7, 55, 56)
        if ([6, 7, 55, 56].includes(id)) return "#9ccc65";
        // Dark (10, 11, 57, 58) and Darkness (8, 9)
        if ([8, 9, 10, 11, 57, 58].includes(id)) return "#5d4037";
        // Dragon (12, 13, 59, 60)
        if ([12, 13, 59, 60].includes(id)) return "#7c4dff";
        // Fairy (14, 15, 61, 62)
        if ([14, 15, 61, 62].includes(id)) return "#f48fb1";
        // Fighting (16, 17, 63, 64)
        if ([16, 17, 63, 64].includes(id)) return "#c62828";
        // Fire (18, 19, 65, 66)
        if ([18, 19, 65, 66].includes(id)) return "#ff5722";
        // Flying (20, 21, 67, 68)
        if ([20, 21, 67, 68].includes(id)) return "#90caf9";
        // Grass (22, 23, 69, 70)
        if ([22, 23, 69, 70].includes(id)) return "#4caf50";
        // Ground (24, 25, 71, 72)
        if ([24, 25, 71, 72].includes(id)) return "#8d6e63";
        // Ice (26, 27, 73, 74)
        if ([26, 27, 73, 74].includes(id)) return "#4dd0e1";
        // Metal/Steel (28, 29, 75, 76)
        if ([28, 29, 75, 76].includes(id)) return "#78909c";
        // Normal (30, 31, 77, 78)
        if ([30, 31, 77, 78].includes(id)) return "#bdbdbd";
        // Poison (32, 33, 79, 80)
        if ([32, 33, 79, 80].includes(id)) return "#9c27b0";
        // Psychic (34, 35, 81, 82)
        if ([34, 35, 81, 82].includes(id)) return "#e91e63";
        // Rock (36, 37, 83, 84)
        if ([36, 37, 83, 84].includes(id)) return "#795548";
        // Water (38, 39, 85, 86)
        if ([38, 39, 85, 86].includes(id)) return "#2196f3";
        // Ghost (47, 48, 87, 88)
        if ([47, 48, 87, 88].includes(id)) return "#673ab7";
        // Electric (49, 50, 89, 90)
        if ([49, 50, 89, 90].includes(id)) return "#ffc107";

        // Generic/Unknown grunts
        return "#607d8b";
    }

    // Dynamic Legend Control
    function updateLegend(min, max, mode) {
        if (legendControl) {
            legendControl.remove();
            legendControl = null;
        }

        if (mode === 'markers') return;

        legendControl = L.control({position: 'bottomright'});

        legendControl.onAdd = function (map) {
            var div = L.DomUtil.create('div', 'info legend');
            div.style.backgroundColor = "rgba(255,255,255,0.9)";
            div.style.padding = "8px 12px";
            div.style.borderRadius = "5px";
            div.style.boxShadow = "0 0 15px rgba(0,0,0,0.2)";
            div.style.color = "#333";

            var minLabel = mode === 'density' ? 'Low' : min;
            var maxLabel = mode === 'density' ? 'High' : max;

            div.innerHTML = '<strong>Invasion Density</strong><br>';
            div.innerHTML += '<div style="background: linear-gradient(to right, blue, cyan, lime, yellow, red); width: 150px; height: 15px; margin-top: 5px; margin-bottom: 5px; border: 1px solid #ccc;"></div>';
            div.innerHTML += '<div style="display: flex; justify-content: space-between; font-size: 12px;"><span>' + minLabel + '</span><span>' + maxLabel + '</span></div>';
            return div;
        };

        legendControl.addTo(mapInstance);
    }

    // MAIN RENDER FUNCTION
    function renderInvasionHeatmap(data, blocklist, renderMode) {
        var container = document.getElementById('invasions-heatmap-map-container');
        if (!container) return;

        // Reset Logic
        if (mapInstance) {
            if (markersLayer) {
                markersLayer.clearLayers();
                if (mapInstance.hasLayer(markersLayer)) mapInstance.removeLayer(markersLayer);
            }
            if (heatLayer) {
                mapInstance.removeLayer(heatLayer);
                heatLayer = null;
            }
            if (gridLayer) {
                mapInstance.removeLayer(gridLayer);
                gridLayer = null;
            }
        }

        try {
            if (!data || !Array.isArray(data) || data.length === 0) {
                if (mapInstance) mapInstance.remove();
                mapInstance = null;
                container.innerHTML = '<div style="color: #aaa; text-align: center; padding: 20px;">No invasion data available for this selection.</div>';
                return;
            }

            // Filter by blocklist (hidden grunts)
            var filteredData = data;
            if (blocklist && blocklist.length > 0) {
                var blockedSet = new Set(blocklist);
                filteredData = data.filter(function(p) {
                    var key = String(p.character || 0);
                    return !blockedSet.has(key);
                });
            }

            if (filteredData.length === 0) {
                if (mapInstance) mapInstance.remove();
                mapInstance = null;
                container.innerHTML = '<div style="color: #aaa; text-align: center; padding: 20px;">All grunt types filtered out. Use the filter to show some grunts.</div>';
                return;
            }

            // MAP INITIALIZATION
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
                mapInstance = L.map('invasions-heatmap-map-container', {
                    center: [(minLat + maxLat) / 2, (minLon + maxLon) / 2],
                    zoom: 13,
                    preferCanvas: true
                });

                L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
                    attribution: '&copy; OpenStreetMap contributors &copy; CARTO',
                    subdomains: 'abcd',
                    maxZoom: 19
                }).addTo(mapInstance);
            }

            mapInstance.fitBounds([[minLat, minLon], [maxLat, maxLon]], {padding: [20, 20]});

            // RENDER MODES
            if (renderMode === 'density') {
                // DENSITY HEATMAP
                if (typeof L.heatLayer !== 'function') {
                    console.error("Leaflet.heat plugin not loaded!");
                    return;
                }

                var maxSingleCount = 0;
                for (var i = 0; i < filteredData.length; i++) {
                    if (filteredData[i].count > maxSingleCount) maxSingleCount = filteredData[i].count;
                }
                if (maxSingleCount === 0) maxSingleCount = 1;

                var heatPoints = filteredData.map(function(p) {
                    return [p.latitude, p.longitude, p.count / maxSingleCount];
                });

                var datasetSize = filteredData.length;
                var saturationThreshold = 1.0;
                if (datasetSize > 15000) saturationThreshold = 8.0;
                else if (datasetSize > 5000) saturationThreshold = 5.0;
                else if (datasetSize > 1000) saturationThreshold = 3.0;
                else if (datasetSize > 200) saturationThreshold = 1.5;

                var radius = datasetSize > 5000 ? 15 : 25;

                heatLayer = L.heatLayer(heatPoints, {
                    radius: radius,
                    blur: radius * 0.7,
                    maxZoom: 17,
                    max: saturationThreshold,
                    minOpacity: 0.3,
                    gradient: {0.0: 'blue', 0.4: 'cyan', 0.6: 'lime', 0.8: 'orange', 1.0: 'red'}
                }).addTo(mapInstance);

                updateLegend(0, "High", 'density');

            } else if (renderMode === 'grid') {
                // GRID
                gridLayer = L.layerGroup().addTo(mapInstance);

                var step = 0.0005;
                var grid = {};
                var maxGridCount = 0;

                filteredData.forEach(function(p) {
                    var latKey = Math.floor(p.latitude / step);
                    var lonKey = Math.floor(p.longitude / step);
                    var key = latKey + "_" + lonKey;

                    if (!grid[key]) {
                        grid[key] = {
                            count: 0,
                            bounds: [[latKey * step, lonKey * step], [(latKey+1) * step, (lonKey+1) * step]],
                            grunts: {}
                        };
                    }
                    grid[key].count += p.count;
                    if (grid[key].count > maxGridCount) maxGridCount = grid[key].count;

                    // Track grunts
                    var gruntKey = String(p.character || 0);
                    if (!grid[key].grunts[gruntKey]) {
                        grid[key].grunts[gruntKey] = {
                            character: p.character,
                            icon_url: p.icon_url,
                            count: 0
                        };
                    }
                    grid[key].grunts[gruntKey].count += p.count;
                });

                if (maxGridCount === 0) maxGridCount = 1;

                Object.keys(grid).forEach(function(key) {
                    var cell = grid[key];
                    var intensity = cell.count / maxGridCount;
                    var hue = 240 - (intensity * 240);
                    var color = 'hsl(' + hue + ', 100%, 50%)';

                    var rect = L.rectangle(cell.bounds, {
                        color: color,
                        weight: 1,
                        fillColor: color,
                        fillOpacity: 0.5
                    }).addTo(gridLayer);

                    // Tooltip with grunt breakdown
                    var gruntList = Object.values(cell.grunts).sort(function(a, b) { return b.count - a.count; });
                    var tooltipHtml = '<div style="text-align: center; min-width: 150px;">';
                    tooltipHtml += '<strong>Total Invasions: ' + cell.count + '</strong><br><hr style="margin: 5px 0;">';

                    var shown = 0;
                    gruntList.forEach(function(grunt) {
                        if (shown < 5) {
                            var iconUrl = grunt.icon_url || 'https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main/invasion/0.webp';
                            tooltipHtml += '<div style="display: flex; align-items: center; gap: 5px; margin: 3px 0;">';
                            tooltipHtml += '<img src="' + iconUrl + '" style="width: 24px; height: 24px;">';
                            tooltipHtml += '<span>x' + grunt.count + '</span>';
                            tooltipHtml += '</div>';
                            shown++;
                        }
                    });

                    if (gruntList.length > 5) {
                        tooltipHtml += '<div style="color: #aaa; font-size: 11px;">+' + (gruntList.length - 5) + ' more...</div>';
                    }
                    tooltipHtml += '</div>';

                    rect.bindTooltip(tooltipHtml, {sticky: true, className: 'custom-tooltip'});
                });

                updateLegend(0, maxGridCount, 'grid');

            } else {
                // MARKERS MODE (Pokestops)
                updateLegend(0, 0, 'markers');

                if (typeof L.markerClusterGroup === 'function' && filteredData.length > 100) {
                    markersLayer = L.markerClusterGroup({
                        maxClusterRadius: function(zoom) {
                            if (zoom <= 12) return 120;
                            if (zoom <= 14) return 80;
                            return 60;
                        },
                        spiderfyOnMaxZoom: true,
                        showCoverageOnHover: false,
                        zoomToBoundsOnClick: true,
                        disableClusteringAtZoom: 17,
                        iconCreateFunction: function(cluster) {
                            var markers = cluster.getAllChildMarkers();
                            var count = 0;
                            markers.forEach(function(m) {
                                count += m.options.totalCount || 1;
                            });

                            var size = 40;
                            if (count > 500) size = 60;
                            else if (count > 100) size = 50;

                            var color = '#4CAF50';
                            if (count > 500) color = '#f44336';
                            else if (count > 100) color = '#ff9800';

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

                // Group by pokestop
                var grouped = {};
                filteredData.forEach(function(p) {
                    var key = p.pokestop_name || (p.latitude + "," + p.longitude);
                    if (!grouped[key]) {
                        grouped[key] = {
                            lat: p.latitude,
                            lon: p.longitude,
                            pokestop_name: p.pokestop_name || "Unknown Pokestop",
                            invasions: [],
                            totalCount: 0
                        };
                    }
                    grouped[key].invasions.push(p);
                    grouped[key].totalCount += p.count;
                });

                var pokestops = Object.values(grouped);

                pokestops.forEach(function(stop, idx) {
                    var popupHtml = '<div style="text-align:center; max-height: 300px; overflow-y: auto; min-width: 180px;">';
                    popupHtml += '<div style="font-weight: bold; font-size: 14px; margin-bottom: 5px; color: #333;">' + stop.pokestop_name + '</div>';
                    popupHtml += '<div style="font-size: 13px; margin-bottom: 8px; padding-bottom: 5px; border-bottom: 1px solid #ddd; color: #e91e63; font-weight: bold;">Total Invasions: ' + stop.totalCount + '</div>';

                    // Sort invasions by count
                    stop.invasions.sort(function(a, b) { return b.count - a.count; });

                    stop.invasions.forEach(function(invasion) {
                        var iconUrl = invasion.icon_url || 'https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main/invasion/' + invasion.character + '.webp';
                        var gruntColor = getGruntTypeColor(invasion.character);

                        popupHtml += '<div style="display: flex; align-items: center; margin: 8px 0; padding: 5px; background: #f8f8f8; border-radius: 8px; gap: 10px;">';
                        popupHtml += '<img src="' + iconUrl + '" style="width: 48px; height: 48px; flex-shrink: 0;" onerror="this.src=\'https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main/invasion/0.webp\'">';
                        popupHtml += '<div style="flex-grow: 1; text-align: left;">';
                        popupHtml += '<div style="font-weight: bold; color: #333;">ID: ' + invasion.character + '</div>';
                        popupHtml += '<div style="font-size: 12px; color: ' + gruntColor + '; font-weight: bold;">×' + invasion.count + '</div>';
                        popupHtml += '</div></div>';
                    });

                    popupHtml += '</div>';

                    // Create marker
                    var totalCount = stop.totalCount;
                    var markerSize = 24;
                    if (totalCount > 50) markerSize = 36;
                    else if (totalCount > 20) markerSize = 32;
                    else if (totalCount > 10) markerSize = 28;

                    var markerColor = '#e91e63';
                    if (totalCount > 50) markerColor = '#d32f2f';
                    else if (totalCount > 20) markerColor = '#f44336';

                    var markerHtml = '<div style="background: linear-gradient(135deg, ' + markerColor + ' 0%, #ad1457 100%); border-radius: 50%; width: ' + markerSize + 'px; height: ' + markerSize + 'px; display: flex; align-items: center; justify-content: center; color: white; font-weight: bold; font-size: 11px; border: 2px solid rgba(255,255,255,0.8); box-shadow: 0 2px 6px rgba(0,0,0,0.4);">' + totalCount + '</div>';

                    var icon = L.divIcon({
                        html: markerHtml,
                        className: 'custom-invasion-marker',
                        iconSize: [markerSize, markerSize],
                        iconAnchor: [markerSize/2, markerSize/2]
                    });

                    var marker = L.marker([stop.lat, stop.lon], {
                        icon: icon,
                        totalCount: totalCount
                    });

                    marker.bindPopup(popupHtml, {maxWidth: 300, maxHeight: 350});
                    markersLayer.addLayer(marker);
                });
            }

        } catch (e) {
            console.error("Error rendering invasion heatmap:", e);
            container.innerHTML = '<div style="color: #ff5555; text-align: center; padding: 20px;">Error rendering map: ' + e.message + '</div>';
        }
    }

    // Expose globally
    window.renderInvasionHeatmap = renderInvasionHeatmap;
})();
