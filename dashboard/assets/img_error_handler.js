// Enhanced image error handler with retry logic and dynamic content support
(function() {
    'use strict';

    // Track retry attempts per image
    var retryCount = new WeakMap();
    var maxRetries = 2; // Reduced from 3 to minimize interference with loading

    // Fallback to simple colored square if all else fails
    var errorPlaceholder = 'data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" width="40" height="40"%3E%3Crect width="40" height="40" fill="%23444"/%3E%3Ctext x="50%25" y="50%25" font-size="20" fill="%23999" text-anchor="middle" dy=".3em"%3E?%3C/text%3E%3C/svg%3E';

    function handleImageError(img) {
        if (!img || img.tagName !== 'IMG') return;

        // Don't retry if image has already loaded successfully
        if (img.complete && img.naturalWidth > 0) {
            return; // Image loaded successfully, nothing to do
        }

        var src = img.src;
        var attempts = retryCount.get(img) || 0;

        // Check if it's a Pokemon icon from the repo
        if (src.indexOf('wwm-uicons-webp') === -1) return;

        // Mark that we're handling this error to prevent duplicate processing
        if (img.getAttribute('data-error-processing') === 'true') {
            return;
        }
        img.setAttribute('data-error-processing', 'true');

        // If we haven't exceeded retry limit, try fallback strategies
        if (attempts < maxRetries) {
            retryCount.set(img, attempts + 1);

            var newSrc = null;

            // If it's a form image (_f), try base form
            if (src.indexOf('_f') !== -1 && !img.getAttribute('data-tried-base')) {
                var parts = src.split('/');
                var filename = parts.pop();
                var id = filename.split('_f')[0];
                newSrc = parts.join('/') + '/' + id + '.webp';
                img.setAttribute('data-tried-base', 'true');
            }
            // For non-form images that failed, show placeholder immediately
            else {
                // Don't retry, just show error
                newSrc = null;
            }

            if (newSrc) {
                // Clear processing flag before retry
                img.removeAttribute('data-error-processing');

                // Immediate retry (no delay) to avoid race conditions
                img.src = newSrc;
                return;
            }
        }

        // All retries exhausted, use error placeholder
        if (!img.getAttribute('data-error-handled')) {
            img.setAttribute('data-error-handled', 'true');
            img.src = errorPlaceholder;
            img.style.opacity = '0.5';
            img.title = 'Image not available';
        }

        img.removeAttribute('data-error-processing');
    }

    // Global error event listener
    window.addEventListener('error', function(event) {
        if (event.target && event.target.tagName === 'IMG') {
            handleImageError(event.target);
        }
    }, true);

    // Also handle images that are added to DOM dynamically
    // This helps with table sorting and re-renders
    function setupImageObserver() {
        var observer = new MutationObserver(function(mutations) {
            mutations.forEach(function(mutation) {
                // Check for newly added nodes
                mutation.addedNodes.forEach(function(node) {
                    if (node.nodeType === 1) { // Element node
                        // Check if the node itself is an image
                        if (node.tagName === 'IMG' && node.classList.contains('pokemon-icon')) {
                            ensureImageLoads(node);
                        }
                        // Check for images within the node
                        var images = node.querySelectorAll ? node.querySelectorAll('img.pokemon-icon') : [];
                        images.forEach(ensureImageLoads);
                    }
                });
            });
        });

        // Observe the entire document body for changes
        observer.observe(document.body, {
            childList: true,
            subtree: true
        });

        // Also handle existing images on page load
        window.addEventListener('DOMContentLoaded', function() {
            document.querySelectorAll('img.pokemon-icon').forEach(ensureImageLoads);
        });
    }

    function ensureImageLoads(img) {
        // Check if already successfully loaded
        if (img.complete && img.naturalWidth > 0) {
            img.style.opacity = '1';
            return; // Already loaded successfully
        }

        // Check if already failed and handled
        if (img.getAttribute('data-error-handled') === 'true') {
            return; // Already handled
        }

        if (!img.complete) {
            // Image is still loading - add listeners
            var loadHandler = function() {
                // Cancel any error handling since load succeeded
                img.style.opacity = '1';
                img.removeAttribute('data-error-processing');
            };

            var errorHandler = function() {
                // Only handle error if image actually failed
                if (!img.complete || img.naturalWidth === 0) {
                    handleImageError(img);
                }
            };

            img.addEventListener('load', loadHandler, { once: true });
            img.addEventListener('error', errorHandler, { once: true });
        } else if (img.naturalWidth === 0) {
            // Image failed to load (completed but no width)
            handleImageError(img);
        }
    }

    // Initialize observer when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', setupImageObserver);
    } else {
        setupImageObserver();
    }

    // Export for debugging
    window.pokemonImageHandler = {
        retryCount: retryCount,
        handleImageError: handleImageError
    };
})();
