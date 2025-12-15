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

    // Get raid level color
    function getRaidLevelColor(level) {
        var lvl = String(level);
        if (lvl === "1") return "#f0ad4e";  // Orange (more visible than gray)
        if (lvl === "3") return "#f0ad4e";  // Orange
        if (lvl === "5") return "#dc3545";  // Red (Legendary)
        if (lvl === "6") return "#a020f0";  // Purple (Mega)
        if (lvl === "7") return "#7fce83";  // Greenish (Mega 5)
        if (lvl === "8") return "#e881f1";  // Pinkish (Ultra Beast)
        if (lvl === "9") return "#ce2c2c";  // Dark Red (Extended Egg)
        if (lvl === "10") return "#ad5b2c"; // Brown/Orange (Primal)
        if (lvl === "11") return "#0a0a0a"; // Shadow Level 1
        if (lvl === "12") return "#0a0a0a"; // Shadow Level 2
        if (lvl === "13") return "#0a0a0a"; // Shadow Level 3
        if (lvl === "14") return "#0a0a0a"; // Shadow Level 4
        if (lvl === "15") return "#0a0a0a"; // Shadow Level 5
        // Legacy shadow raids (1xxx, 2xxx, 3xxx)
        if (lvl.length === 4) {
            var base = lvl.charAt(0);
            if (base === "1") return "#4a3070";
            if (base === "2") return "#5a2080";
            if (base === "3") return "#6a1090";
        }
        return "#888888";
    }

    // Get raid level label
    function getRaidLevelLabel(level) {
        var lvl = String(level);
        if (lvl === "1") return "★";
        if (lvl === "3") return "★★★";
        if (lvl === "5") return "★★★★★";
        if (lvl === "6") return "Mega";
        if (lvl === "7") return "Mega 5★";
        if (lvl === "8") return "Ultra Beast";
        if (lvl === "9") return "Extended";
        if (lvl === "10") return "Primal";
        if (lvl === "11") return "Shadow ★";
        if (lvl === "12") return "Shadow ★★";
        if (lvl === "13") return "Shadow ★★★";
        if (lvl === "14") return "Shadow ★★★★";
        if (lvl === "15") return "Shadow ★★★★★";
        // Legacy shadow raids (1xxx, 2xxx, 3xxx)
        if (lvl.length === 4) {
            var base = lvl.charAt(0);
            if (base === "1") return "Shadow ★";
            if (base === "2") return "Shadow ★★★";
            if (base === "3") return "Shadow ★★★★★";
        }
        return "Lvl " + lvl;
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
            div.style.fontFamily = "sans-serif";

            var title = (mode === 'grid') ? 'Raids per Cell' : 'Raid Density';
            var maxLabel = (mode === 'grid') ? Math.round(max) : 'High';
            var minLabel = (mode === 'grid') ? min : 'Low';

            div.innerHTML = '<strong>' + title + '</strong><br>';
            div.innerHTML += '<div style="background: linear-gradient(to right, blue, cyan, lime, yellow, red); width: 150px; height: 15px; margin-top: 5px; margin-bottom: 5px; border: 1px solid #ccc;"></div>';
            div.innerHTML += '<div style="display: flex; justify-content: space-between; font-size: 12px;"><span>' + minLabel + '</span><span>' + maxLabel + '</span></div>';
            return div;
        };

        legendControl.addTo(mapInstance);
    }

    // MAIN RENDER FUNCTION
    function renderRaidHeatmap(data, blocklist, renderMode) {
        var container = document.getElementById('raids-heatmap-map-container');
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
                container.innerHTML = '<div style="color: #aaa; text-align: center; padding: 20px;">No raid data available for this selection.</div>';
                return;
            }

            // Filter by blocklist (hidden Pokemon)
            var filteredData = data;
            if (blocklist && blocklist.length > 0) {
                var blockedSet = new Set(blocklist);
                filteredData = data.filter(function(p) {
                    var key = p.raid_pokemon + ":" + (p.raid_form || 0);
                    return !blockedSet.has(key);
                });
            }

            if (filteredData.length === 0) {
                if (mapInstance) mapInstance.remove();
                mapInstance = null;
                container.innerHTML = '<div style="color: #aaa; text-align: center; padding: 20px;">All raid bosses filtered out. Use the filter to show some Pokemon.</div>';
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
                mapInstance = L.map('raids-heatmap-map-container', {
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

            mapInstance.fitBounds([[minLat, minLon], [maxLat, maxLon]], { padding: [20, 20] });

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
                            raids: {}
                        };
                    }
                    grid[key].count += p.count;
                    if (grid[key].count > maxGridCount) maxGridCount = grid[key].count;

                    var raidKey = p.raid_pokemon + ":" + p.raid_form;
                    if (!grid[key].raids[raidKey]) {
                        grid[key].raids[raidKey] = {
                            raid_pokemon: p.raid_pokemon,
                            raid_form: p.raid_form,
                            raid_level: p.raid_level,
                            icon_url: p.icon_url,
                            count: 0
                        };
                    }
                    grid[key].raids[raidKey].count += p.count;
                });

                Object.values(grid).forEach(function(cell) {
                    var intensity = cell.count / maxGridCount;
                    var hue = (1 - intensity) * 240;
                    var colorStr = 'hsla(' + hue + ', 100%, 50%, 0.75)';

                    var tooltipHtml = '<div style="text-align:center; min-width: 120px;">';
                    tooltipHtml += '<strong style="font-size: 14px;">Total Raids: ' + cell.count + '</strong><hr style="margin: 5px 0;">';

                    var raidList = Object.values(cell.raids).sort(function(a, b) { return b.count - a.count; });

                    raidList.slice(0, 5).forEach(function(raid) {
                        var iconUrl = raid.icon_url || 'https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main/pokemon/' + raid.raid_pokemon + '.webp';
                        var levelColor = getRaidLevelColor(raid.raid_level);
                        var levelLabel = getRaidLevelLabel(raid.raid_level);

                        tooltipHtml += '<div style="display: flex; align-items: center; margin: 4px 0; gap: 8px;">';
                        tooltipHtml += '<img src="' + iconUrl + '" style="width: 32px; height: 32px;" onerror="this.style.display=\'none\'">';
                        tooltipHtml += '<div style="text-align: left;">';
                        tooltipHtml += '<div style="font-size: 10px; color: ' + levelColor + '; font-weight: bold;">' + levelLabel + '</div>';
                        tooltipHtml += '<div style="font-size: 11px; color: #333;">Count: ' + raid.count + '</div>';
                        tooltipHtml += '</div></div>';
                    });

                    if (raidList.length > 5) {
                        tooltipHtml += '<div style="font-size: 10px; color: #888; margin-top: 4px;">+' + (raidList.length - 5) + ' more...</div>';
                    }
                    tooltipHtml += '</div>';

                    L.rectangle(cell.bounds, {
                        color: "transparent",
                        weight: 1,
                        fillColor: colorStr,
                        fillOpacity: 0.75,
                    })
                    .bindTooltip(tooltipHtml, {direction: 'top', sticky: true, className: 'raid-grid-tooltip'})
                    .addTo(gridLayer);
                });

                updateLegend(1, maxGridCount, 'grid');

            } else {
                // MARKERS MODE (Gyms)
                if (typeof L.markerClusterGroup === 'function') {
                    markersLayer = L.markerClusterGroup({
                        maxClusterRadius: function(zoom) {
                            return (zoom < 13) ? 120 : (zoom < 15) ? 80 : 60;
                        },
                        disableClusteringAtZoom: 17,
                        spiderfyOnMaxZoom: false,
                        showCoverageOnHover: false,
                        zoomToBoundsOnClick: true,
                        chunkedLoading: true,
                        chunkInterval: 200,
                        chunkDelay: 50,
                        iconCreateFunction: function(cluster) {
                            var count = cluster.getChildCount();
                            var size = Math.min(70, Math.max(40, 40 + Math.log10(count + 1) * 12));

                            var color = 'rgba(110, 204, 57, 0.85)';
                            if (count > 100) color = 'rgba(255, 165, 0, 0.85)';
                            if (count > 500) color = 'rgba(255, 69, 0, 0.85)';

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

                // Group by gym
                var grouped = {};
                filteredData.forEach(function(p) {
                    var key = p.gym_name || (p.latitude + "," + p.longitude);
                    if (!grouped[key]) {
                        grouped[key] = {
                            lat: p.latitude,
                            lon: p.longitude,
                            gym_name: p.gym_name || "Unknown Gym",
                            raids: [],
                            totalCount: 0
                        };
                    }
                    grouped[key].raids.push(p);
                    grouped[key].totalCount += p.count;
                });

                var gyms = Object.values(grouped);

                gyms.forEach(function(gym, idx) {
                    var popupHtml = '<div style="text-align:center; max-height: 300px; overflow-y: auto; min-width: 180px;">';
                    popupHtml += '<div style="font-weight: bold; font-size: 14px; margin-bottom: 5px; color: #333;">' + gym.gym_name + '</div>';
                    popupHtml += '<div style="font-size: 13px; margin-bottom: 8px; padding-bottom: 5px; border-bottom: 1px solid #ddd; color: #007bff; font-weight: bold;">Total Raids: ' + gym.totalCount + '</div>';

                    // Sort raids by count
                    gym.raids.sort(function(a, b) { return b.count - a.count; });

                    gym.raids.forEach(function(raid) {
                        var iconUrl = raid.icon_url || 'https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main/pokemon/' + raid.raid_pokemon + '.webp';
                        var levelColor = getRaidLevelColor(raid.raid_level);
                        var levelLabel = getRaidLevelLabel(raid.raid_level);

                        popupHtml += '<div style="display: flex; align-items: center; margin: 8px 0; padding: 5px; background: #f8f8f8; border-radius: 8px; gap: 10px;">';
                        popupHtml += '<img src="' + iconUrl + '" style="width: 48px; height: 48px; flex-shrink: 0;" onerror="this.src=\'https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main/pokemon/0.webp\'">';
                        popupHtml += '<div style="text-align: left; flex-grow: 1;">';
                        popupHtml += '<div style="font-size: 11px; color: ' + levelColor + '; font-weight: bold;">' + levelLabel + '</div>';
                        popupHtml += '<div style="font-size: 12px; color: #333;">Raids: ' + raid.count + '</div>';
                        popupHtml += '</div></div>';
                    });

                    popupHtml += '</div>';

                    // Use raid egg icon for gym marker
                    var size = Math.min(50, Math.max(25, 25 + Math.log10(gym.totalCount + 1) * 12));
                    var divHtml = '<div class="heatmap-dot" style="width:' + size + 'px; height:' + size + 'px; line-height:' + size + 'px; font-size:' + Math.max(10, size/3) + 'px; background: linear-gradient(135deg, #dc3545, #f0ad4e);">' + gym.totalCount + '</div>';

                    L.marker([gym.lat, gym.lon], {
                        icon: L.divIcon({
                            className: 'custom-div-icon',
                            html: divHtml,
                            iconSize: [size, size],
                            iconAnchor: [size/2, size/2]
                        }),
                        zIndexOffset: idx
                    }).bindPopup(popupHtml, {maxWidth: 350}).addTo(markersLayer);
                });
            }

        } catch (err) {
            console.error("Error rendering raid heatmap:", err);
            container.innerHTML = '<div style="color: red; text-align: center;">Error rendering map. Check console.</div>';
        }
    }

    window.renderRaidHeatmap = renderRaidHeatmap;
})();
