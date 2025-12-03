window.dash_clientside = window.dash_clientside || {};
window.dash_clientside.clientside = window.dash_clientside.clientside || {};

window.dash_clientside.clientside = Object.assign({}, window.dash_clientside.clientside, {

    // Existing scroll function
    scrollToSelected: function(isOpen) {
        if (isOpen) {
            setTimeout(function() {
                const element = document.getElementById("selected-area-card");
                if (element) {
                    element.scrollIntoView({behavior: "smooth", block: "center"});
                }
            }, 300);
        }
        return window.dash_clientside.no_update;
    },

    // Fixed renderer trigger
    triggerHeatmapRenderer: function(data, blocklist, renderMode) {
        // Safety check if the renderer function is loaded
        if (!window.renderPokemonHeatmap) {
            console.warn("renderPokemonHeatmap not found on window");
            return window.dash_clientside.no_update;
        }

        // Safety check for data
        if (!data || !Array.isArray(data)) {
            // Render empty if no data
            window.renderPokemonHeatmap([], [], renderMode);
            return window.dash_clientside.no_update;
        }

        // Pass data directly to the renderer (filtering happens there now)
        window.renderPokemonHeatmap(data, blocklist || [], renderMode);
        return window.dash_clientside.no_update;
    }
});
