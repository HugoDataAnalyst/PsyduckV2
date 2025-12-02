(function() {
    'use strict';

    var mapInstance = null;
    var markersLayer = null;
    var heatLayer = null;

    // markers or density
    function renderHeatmap(data, blocklist, renderMode) {
        var container = document.getElementById('heatmap-map-container');
        if (!container) return;

        // Validate Data
        if (!Array.isArray(data)) {
            container.innerHTML = '<div style="color: #aaa; text-align: center; padding: 20px;">No data points available.</div>';
            return;
        }

        // Reset Map Wrapper
        if (mapInstance) {
            mapInstance.remove();
            mapInstance = null;
            markersLayer = null;
            heatLayer = null;
        }

        if (data.length === 0) {
            container.innerHTML = '<div style="color: #aaa; text-align: center; padding: 20px;">No data points found for this selection.</div>';
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
                container.innerHTML = '<div style="color: #aaa; text-align: center; padding: 20px;">All points filtered out.</div>';
                return;
            }

            // MAP INIT
            // Calculate bounds iteratively to avoid Maximum call stack size exceeded with spread operator
            var minLat = 90, maxLat = -90;
            var minLon = 180, maxLon = -180;

            for (var i = 0; i < filteredData.length; i++) {
                var lat = filteredData[i].latitude;
                var lon = filteredData[i].longitude;
                if (lat < minLat) minLat = lat;
                if (lat > maxLat) maxLat = lat;
                if (lon < minLon) minLon = lon;
                if (lon > maxLon) maxLon = lon;
            }

            mapInstance = L.map('heatmap-map-container', {
                center: [(minLat + maxLat) / 2, (minLon + maxLon) / 2],
                zoom: 13,
                preferCanvas: true
            });

            L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                maxZoom: 19,
                attribution: '© OpenStreetMap'
            }).addTo(mapInstance);

            mapInstance.fitBounds([[minLat, minLon], [maxLat, maxLon]], { padding: [20, 20] });


            // RENDER BASED ON MODE

            if (renderMode === 'density') {
                // DENSITY HEATMAP MODE L.heatLayer
                if (typeof L.heatLayer !== 'function') {
                    console.error("Leaflet.heat plugin not loaded!");
                } else {
                    var heatPoints = filteredData.map(p => [p.latitude, p.longitude, p.count]);

                    heatLayer = L.heatLayer(heatPoints, {
                        radius: 25,
                        blur: 15,
                        maxZoom: 17,
                        gradient: {0.4: 'blue', 0.65: 'lime', 1: 'red'}
                    }).addTo(mapInstance);
                }

            } else {
                // MARKERS MODE
                markersLayer = L.layerGroup().addTo(mapInstance);

                var grouped = {};
                filteredData.forEach(function(p) {
                    var key = p.spawnpoint ? String(p.spawnpoint) : (p.latitude + "," + p.longitude);
                    if (!grouped[key]) {
                        grouped[key] = {
                            lat: p.latitude, lon: p.longitude, spawnpoint: p.spawnpoint, mons: []
                        };
                    }
                    grouped[key].mons.push(p);
                });
                var locations = Object.values(grouped);

                const uniqueSpecies = new Set();
                filteredData.forEach(p => uniqueSpecies.add(p.pokemon_id + ":" + p.form));
                const useIcons = (uniqueSpecies.size === 1);

                // If too many locations, maybe force simple markers or warn?
                // For now, we just render them all.

                locations.forEach(function(loc) {
                    var popupHtml = `<div style="text-align:center; max-height: 200px; overflow-y: auto;">`;
                    if (loc.spawnpoint) popupHtml += `<b>Spawnpoint:</b> ${loc.spawnpoint}<br><hr style="margin:5px 0;">`;

                    loc.mons.forEach(function(m) {
                        var formSuffix = (m.form && m.form > 0) ? '_f' + m.form : '';
                        var iconUrl = 'https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main/pokemon/' + m.pokemon_id + formSuffix + '.webp';
                        popupHtml += `
                            <div style="display: flex; align-items: center; margin-bottom: 6px; text-align: left;">
                                <img src="${iconUrl}" width="40" height="40" style="margin-right: 8px;"
                                     onerror="this.src='https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main/pokemon/${m.pokemon_id}.webp'"/>
                                <div><b>ID:</b> ${m.pokemon_id} <small>(Form: ${m.form || 0})</small><br><b>Count:</b> ${m.count}</div>
                            </div>`;
                    });
                    popupHtml += `<small style="color:#888;">${loc.lat.toFixed(5)}, ${loc.lon.toFixed(5)}</small></div>`;

                    if (useIcons) {
                        var primaryMon = loc.mons[0];
                        var formSuffix = (primaryMon.form && primaryMon.form > 0) ? '_f' + primaryMon.form : '';
                        var iconUrl = 'https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main/pokemon/' + primaryMon.pokemon_id + formSuffix + '.webp';
                        var customIcon = L.icon({ iconUrl: iconUrl, iconSize: [35, 35], className: 'pokemon-marker-icon' });
                        L.marker([loc.lat, loc.lon], { icon: customIcon }).bindPopup(popupHtml).addTo(markersLayer);
                    } else {
                        var totalCount = loc.mons.reduce((acc, m) => acc + m.count, 0);
                        var size = Math.min(40, 20 + (totalCount * 1.5));

                        var colorClass = 'low';
                        if (totalCount > 10) colorClass = 'high';
                        else if (totalCount > 5) colorClass = 'medium';

                        var divHtml = `<div class="heatmap-dot ${colorClass}" style="width:${size}px; height:${size}px; line-height:${size}px;">${totalCount > 1 ? totalCount : ''}</div>`;
                        var numberedIcon = L.divIcon({ className: 'custom-div-icon', html: divHtml, iconSize: [size, size], iconAnchor: [size/2, size/2] });
                        L.marker([loc.lat, loc.lon], { icon: numberedIcon }).bindPopup(popupHtml).addTo(markersLayer);
                    }
                });
            }

        } catch (err) {
            console.error("Error rendering heatmap:", err);
            container.innerHTML = '<div style="color: red; text-align: center;">Error rendering map.</div>';
        }
    }

    window.renderPokemonHeatmap = renderHeatmap;
})();
