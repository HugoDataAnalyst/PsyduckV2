window.dash_clientside = Object.assign({}, window.dash_clientside, {
    clientside: {

        // existing scroll function
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

        triggerHeatmapRenderer: function(data, blocklist) {
            if (!window.renderPokemonHeatmap) return window.dash_clientside.no_update;

            // Safety check
            if (!data || !Array.isArray(data)) {
                window.renderPokemonHeatmap([], false);
                return window.dash_clientside.no_update;
            }

            // Filter Data based on Blocklist
            let filteredData = data;
            if (blocklist && blocklist.length > 0) {
                const blockedSet = new Set(blocklist);
                filteredData = data.filter(function(p) {
                    // Ensure consistent key generation matching Python
                    const key = p.pokemon_id + ":" + p.form;
                    return !blockedSet.has(key);
                });
            }

            // Analyze Data for Rendering Mode
            // Create a Set of unique ID:Form combinations
            const uniqueSpecies = new Set();
            filteredData.forEach(p => uniqueSpecies.add(p.pokemon_id + ":" + p.form));

            // If exactly one unique species remains, show Icons. Otherwise, show dots.
            const useIcons = (uniqueSpecies.size === 1);

            // Render
            window.renderPokemonHeatmap(filteredData, useIcons);
            return window.dash_clientside.no_update;
        },
    }
});
