// rov/static/rov/js/initRECDBHistogram.js

export function initRECDBHistogramCharts() {
    const binsInput = document.getElementById("recdbHistBins");
    const maxOffsetInput = document.getElementById("recdbHistMaxOffset");

    const preplotPane = document.getElementById("recdb-preplot-pane");
    const primaryPane = document.getElementById("recdb-primary-pane");

    const btnPreplot = document.getElementById("btnLoadRecdbPreplotHist");
    const btnPrimary = document.getElementById("btnLoadRecdbPrimaryHist");

    const preplotTab = document.getElementById("recdb-preplot-tab");
    const primaryTab = document.getElementById("recdb-primary-tab");

    if (!preplotPane && !primaryPane) {
        return;
    }

    function getNumberValue(input, fallbackValue) {
        if (!input) return fallbackValue;

        const value = parseFloat(input.value);
        return Number.isFinite(value) ? value : fallbackValue;
    }

    async function loadBokehItem(url, containerId, forceReload = false) {
        const container = document.getElementById(containerId);

        if (!container || !url) {
            return;
        }

        if (!forceReload && container.dataset.loaded === "1") {
            return;
        }

        const bins = getNumberValue(binsInput, 60);
        const maxOffset = getNumberValue(maxOffsetInput, 10);

        const finalUrl = new URL(url, window.location.origin);
        finalUrl.searchParams.set("bins", String(bins));
        finalUrl.searchParams.set("max_offset", String(maxOffset));

        container.dataset.loaded = "0";
        container.innerHTML = `
            <div class="d-flex align-items-center gap-2 text-muted py-3">
                <div class="spinner-border spinner-border-sm" role="status"></div>
                <span>Loading REC_DB histogram...</span>
            </div>
        `;

        try {
            const response = await fetch(finalUrl.toString(), {
                method: "GET",
                headers: {
                    "X-Requested-With": "XMLHttpRequest"
                }
            });

            const data = await response.json();

            if (!response.ok || !data.ok) {
                throw new Error(data.error || "Failed to load REC_DB histogram.");
            }

            if (!window.Bokeh || !window.Bokeh.embed) {
                throw new Error("BokehJS is not loaded on this page.");
            }

            container.innerHTML = "";
            await window.Bokeh.embed.embed_item(data.item, containerId);

            container.dataset.loaded = "1";

        } catch (err) {
            container.dataset.loaded = "0";
            container.innerHTML = `
                <div class="alert alert-danger mb-0">
                    <strong>REC_DB histogram error:</strong> ${err.message}
                </div>
            `;
        }
    }

    function loadPreplot(forceReload = false) {
        if (!preplotPane) return;

        loadBokehItem(
            preplotPane.dataset.url,
            "recdb-preplot-hist-container",
            forceReload
        );
    }

    function loadPrimary(forceReload = false) {
        if (!primaryPane) return;

        loadBokehItem(
            primaryPane.dataset.url,
            "recdb-primary-hist-container",
            forceReload
        );
    }

    if (btnPreplot) {
        btnPreplot.addEventListener("click", function () {
            loadPreplot(true);
        });
    }

    if (btnPrimary) {
        btnPrimary.addEventListener("click", function () {
            loadPrimary(true);
        });
    }

    if (preplotTab) {
        preplotTab.addEventListener("shown.bs.tab", function () {
            loadPreplot(false);
        });
    }

    if (primaryTab) {
        primaryTab.addEventListener("shown.bs.tab", function () {
            loadPrimary(false);
        });
    }

    // Auto-load active tab
    if (preplotPane && preplotPane.classList.contains("active")) {
        loadPreplot(false);
    }

    if (primaryPane && primaryPane.classList.contains("active")) {
        loadPrimary(false);
    }
}