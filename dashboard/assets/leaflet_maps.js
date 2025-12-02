// Leaflet map initialization for area geofences
(function() {
    'use strict';

    var mapInstances = {};
    var debounceTimer = null;

    // Safely destroy a specific map instance
    function destroyMap(mapId) {
        if (mapInstances[mapId]) {
            try {
                // Remove the map to clean up event listeners and DOM
                mapInstances[mapId].off();
                mapInstances[mapId].remove();
            } catch (e) {
                // Ignore _leaflet_pos errors during destruction of detached nodes
            }
            delete mapInstances[mapId];
        }
    }

    // Destroy all maps (used when modal closes)
    function destroyAllMaps() {
        Object.keys(mapInstances).forEach(destroyMap);
    }

    // Main function to reconcile the DOM with Leaflet instances
    function manageMaps() {
        // Destroy maps that no longer exist in the DOM
        Object.keys(mapInstances).forEach(function(mapId) {
            var element = document.getElementById(mapId);
            // If element is gone, or if the element exists but doesn't have the leaflet class
            // (meaning Dash replaced the div with a fresh one), destroy the map.
            if (!element || !element.classList.contains('leaflet-container')) {
                destroyMap(mapId);
            }
        });

        // Initialize new maps
        var mapContainers = document.querySelectorAll('[data-map-geofence]');

        mapContainers.forEach(function(container) {
            var mapId = container.id;

            // Skip if already initialized and valid
            if (mapInstances[mapId]) {
                try {
                    // Check if we need to resize existing maps
                    mapInstances[mapId].invalidateSize();
                } catch(e) {}
                return;
            }

            try {
                var geofenceData = JSON.parse(container.getAttribute('data-map-geofence'));

                if (!geofenceData || !geofenceData.coordinates) return;

                // Coordinate Parsing Logic
                var coords = geofenceData.coordinates;
                var ring = coords;
                if (coords[0] && Array.isArray(coords[0]) && !Array.isArray(coords[0][0])) {
                    ring = coords;
                } else if (coords[0] && Array.isArray(coords[0][0])) {
                    ring = coords[0];
                }

                // Swap lon,lat to lat,lon
                var latLngs = ring.map(function(coord) {
                    return [coord[1], coord[0]];
                });

                // Center/Zoom Calculation
                var lats = latLngs.map(c => c[0]);
                var lons = latLngs.map(c => c[1]);
                var centerLat = lats.reduce((a, b) => a + b, 0) / lats.length;
                var centerLon = lons.reduce((a, b) => a + b, 0) / lons.length;

                var maxRange = Math.max(
                    Math.max.apply(null, lats) - Math.min.apply(null, lats),
                    Math.max.apply(null, lons) - Math.min.apply(null, lons)
                );

                var zoom = 12;
                if (maxRange > 1) zoom = 8;
                else if (maxRange > 0.5) zoom = 9;
                else if (maxRange > 0.1) zoom = 10;
                else if (maxRange > 0.05) zoom = 11;

                // Map Creation
                var map = L.map(mapId, {
                    center: [centerLat, centerLon],
                    zoom: zoom,
                    zoomControl: false,
                    dragging: false,
                    scrollWheelZoom: false,
                    doubleClickZoom: false,
                    boxZoom: false,
                    keyboard: false,
                    attributionControl: false,
                    fadeAnimation: false, // Disable animations to prevent _leaflet_pos errors
                    zoomAnimation: false
                });

                L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                    maxZoom: 19
                }).addTo(map);

                var polygon = L.polygon(latLngs, {
                    color: '#007bff',
                    fillColor: '#007bff',
                    fillOpacity: 0.3,
                    weight: 3
                }).addTo(map);

                map.fitBounds(polygon.getBounds(), { padding: [10, 10], animate: false });

                mapInstances[mapId] = map;

            } catch (error) {
                console.warn('Error initializing map ' + mapId, error);
            }
        });
    }

    // Debounce function to prevent looping execution
    function debouncedManageMaps() {
        if (debounceTimer) clearTimeout(debounceTimer);
        debounceTimer = setTimeout(function() {
            manageMaps();
        }, 150); // Wait 150ms for Dash to finish DOM updates
    }

    // Observer setup
    function setupObserver() {
        // Observe the body for added/removed nodes (general Dash updates)
        var observer = new MutationObserver(function(mutations) {
            var shouldUpdate = false;
            mutations.forEach(function(mutation) {
                // Only trigger if nodes are added/removed
                if (mutation.type === 'childList') {
                    shouldUpdate = true;
                }
            });
            if (shouldUpdate) {
                debouncedManageMaps();
            }
        });

        observer.observe(document.body, {
            childList: true,
            subtree: true
        });

        // Specific watcher for the Modal opening
        var modalInterval = setInterval(function() {
            var modal = document.getElementById('area-modal');
            if (modal) {
                var modalObserver = new MutationObserver(function(mutations) {
                    mutations.forEach(function(mutation) {
                        if (mutation.attributeName === 'class' || mutation.attributeName === 'style') {
                            // Check if visible
                            var isVisible = (modal.style.display && modal.style.display !== 'none') ||
                                            modal.classList.contains('show');

                            if (isVisible) {
                                // Destroy old ones to be safe, then init
                                setTimeout(function(){
                                    destroyAllMaps();
                                    manageMaps();
                                }, 200);
                            } else {
                                destroyAllMaps();
                            }
                        }
                    });
                });
                modalObserver.observe(modal, { attributes: true, attributeFilter: ['style', 'class'] });
                clearInterval(modalInterval);
            }
        }, 500);
    }

    // Start
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', setupObserver);
    } else {
        setupObserver();
    }

    // Expose for debugging or manual triggering
    window.leafletMapsHandler = {
        refresh: debouncedManageMaps,
        destroy: destroyAllMaps
    };

})();
