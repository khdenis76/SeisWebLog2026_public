import { getCSRFToken } from "../../baseproject/js/csrf.js";
export function initDSRHistogram() {

    const button = document.getElementById("load-dsr-hist-button");
    const container = document.getElementById("hist-container");

    if (!button || !container) return;

    // inputs (optional if not found)
    const binsEl = document.getElementById("hist-bins");
    const maxOffsetEl = document.getElementById("hist-max-offset");
    const kdeEl = document.getElementById("hist-kde");
    const stdEl = document.getElementById("hist-std");
    const showEl = document.getElementById("hist-show");

    const readInt = (el, fallback) => {
        if (!el) return fallback;
        const v = parseInt(el.value, 10);
        return Number.isFinite(v) ? v : fallback;
    };

    const readBool = (el, fallback) => {
        if (!el) return fallback;
        return !!el.checked;
    };

    button.addEventListener("click", async () => {

        const url = button.dataset.url;

        // build payload
        const payload = {
            bins: readInt(binsEl, 40),
            max_offset: readInt(maxOffsetEl, 150),
            kde: readBool(kdeEl, true),
            std: readBool(stdEl, true),
            is_show: readBool(showEl, true),
        };

        let originalHTML = button.innerHTML;

        try {
            // UI loading state
            button.disabled = true;
            button.innerHTML = `
                <span class="spinner-border spinner-border-sm me-2"></span>
                Loading...
            `;

            container.innerHTML = `
                <div class="d-flex justify-content-center align-items-center h-100">
                    <div class="text-muted">
                        <div class="spinner-border me-2"></div>
                        Loading histograms...
                    </div>
                </div>
            `;

            const response = await fetch(url, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "X-CSRFToken": getCSRFToken(),
                    "X-Requested-With": "XMLHttpRequest",
                },
                body: JSON.stringify(payload),
            });

            if (!response.ok) {
                const txt = await response.text().catch(() => "");
                throw new Error(`HTTP ${response.status}: ${txt}`);
            }

            const data = await response.json();

            // clear container and embed
            container.innerHTML = "";
            Bokeh.embed.embed_item(data.hist, container);

            // restore button
            button.innerHTML = originalHTML;
            button.disabled = false;

        } catch (err) {

            console.error("DSR histogram load error:", err);

            container.innerHTML = `
                <div class="alert alert-danger m-3">
                    Failed to load histograms.
                </div>
            `;

            button.innerHTML = originalHTML;
            button.disabled = false;
        }
    });
}