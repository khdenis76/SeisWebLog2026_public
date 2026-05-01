// ======================================================
// ROV Deployment / Recovery Lazy Map Loader
// ======================================================

export function initROVDepRecLoad(options = {}) {

    const selector = options.selector || ".lazy-bokeh-map-tab";
    const debug = options.debug || false;

    const loadedMaps = new Set();

    function log(...args) {
        if (debug) console.log("[ROVDepRecLoad]", ...args);
    }

    async function loadMap(tabButton) {

        const url = tabButton.dataset.mapUrl;
        const targetId = tabButton.dataset.mapTarget;

        if (!url || !targetId) {
            log("Missing attributes:", tabButton);
            return;
        }

        if (loadedMaps.has(targetId)) {
            log("Already loaded:", targetId);
            return;
        }

        const target = document.getElementById(targetId);

        if (!target) {
            log("Target not found:", targetId);
            return;
        }

        // Loading UI
        target.innerHTML = `
            <div class="d-flex align-items-center justify-content-center h-100 text-muted">
                <div>
                    <div class="spinner-border spinner-border-sm me-2"></div>
                    Loading map...
                </div>
            </div>
        `;

        try {
            log("Fetching:", url);

            const response = await fetch(url, {
                method: "GET",
                headers: {
                    "X-Requested-With": "XMLHttpRequest"
                }
            });

            const data = await response.json();

            if (!response.ok || !data.ok) {
                throw new Error(data.error || "Map loading failed");
            }

            target.innerHTML = "";

            log("Embedding Bokeh:", targetId);

            await Bokeh.embed.embed_item(data.item, targetId);

            loadedMaps.add(targetId);

            // Fix Bokeh rendering inside hidden tabs
            setTimeout(() => {
                window.dispatchEvent(new Event("resize"));
            }, 300);

        } catch (err) {
            console.error("[ROVDepRecLoad ERROR]", err);

            target.innerHTML = `
                <div class="alert alert-danger m-3">
                    Failed to load map: ${err.message}
                </div>
            `;
        }
    }

    function bindTabs() {
        document.querySelectorAll(selector).forEach(tab => {
            tab.addEventListener("shown.bs.tab", function () {
                loadMap(tab);
            });
        });
    }

    function autoLoadActive() {
        const activeTab = document.querySelector(`${selector}.active`);
        if (activeTab) {
            loadMap(activeTab);
        }
    }

    // Init
    bindTabs();
    autoLoadActive();

    log("Initialized");
}