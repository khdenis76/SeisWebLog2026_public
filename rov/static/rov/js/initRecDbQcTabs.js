export function initRecDbQcTabs() {

    const tabs = document.querySelectorAll(
        "#recdb-qc-tabs button[data-plot-url]"
    );

    if (!tabs.length) return;

    async function loadPlot(btn) {

        const url = btn.dataset.plotUrl;
        const targetId = btn.dataset.plotTarget;

        const target = document.getElementById(targetId);

        if (!url || !target) return;

        // already loaded
        if (target.dataset.loaded === "1") {
            return;
        }

        target.innerHTML = `
        <div class="d-flex align-items-center justify-content-center h-100">
            <div class="spinner-border text-primary me-2"></div>
            <span class="text-muted">
                Loading REC_DB QC...
            </span>
        </div>
        `;

        try {

            const response = await fetch(url, {
                headers: {
                    "X-Requested-With": "XMLHttpRequest"
                }
            });

            const data = await response.json();

            if (!response.ok || !data.ok) {
                throw new Error(
                    data.error || "Failed loading plot"
                );
            }

            target.innerHTML = "";

            await Bokeh.embed.embed_item(
                data.plot,
                targetId
            );

            target.dataset.loaded = "1";

        }
        catch(err){

            console.error(
                "REC_DB plot error:",
                err
            );

            target.innerHTML=`
            <div class="alert alert-danger m-2">
                <i class="fa-solid fa-circle-exclamation me-2"></i>
                ${err.message}
            </div>
            `;
        }

    }

    tabs.forEach(btn=>{

        btn.addEventListener(
            "shown.bs.tab",
            ()=>loadPlot(btn)
        );

    });

    // auto load active tab

    const activeTab=document.querySelector(
        "#recdb-qc-tabs button.active[data-plot-url]"
    );

    if(activeTab){
        loadPlot(activeTab);
    }

    const lineTab = document.getElementById("tab-recdb-line-offsets");
    const lineTarget = document.getElementById("plot-recdb-line-offsets");
    const lineLabel = document.getElementById("recdb-line-offset-label");
    let selectedLine = null;
    let lineController = null;

    async function loadLineOffsets(line) {
        if (!lineTarget || !line) return;

        const baseUrl = lineTarget.dataset.plotUrl;
        const url = new URL(baseUrl, window.location.origin);
        url.searchParams.set("line", line);

        if (lineController) lineController.abort();
        lineController = new AbortController();
        lineTarget.dataset.loadingLine = String(line);

        lineTarget.innerHTML = `
            <div class="d-flex align-items-center justify-content-center h-100">
                <div class="spinner-border text-primary me-2"></div>
                <span class="text-muted">Loading line offsets...</span>
            </div>`;

        try {
            const response = await fetch(url, {
                headers: {"X-Requested-With": "XMLHttpRequest"},
                signal: lineController.signal
            });
            const data = await response.json();
            if (!response.ok || !data.ok) {
                throw new Error(data.error || "Failed loading line offsets");
            }

            lineTarget.innerHTML = "";
            await Bokeh.embed.embed_item(data.plot, lineTarget.id);
            lineTarget.dataset.line = String(line);
            delete lineTarget.dataset.loadingLine;
        } catch (err) {
            if (err?.name === "AbortError") return;
            delete lineTarget.dataset.loadingLine;
            console.error("REC_DB line-offset plot error:", err);
            lineTarget.innerHTML = `
                <div class="alert alert-danger m-2">
                    <i class="fa-solid fa-circle-exclamation me-2"></i>
                    ${err.message}
                </div>`;
        }
    }

    if (lineTab) {
        lineTab.addEventListener("shown.bs.tab", () => {
            if (selectedLine &&
                lineTarget.dataset.line !== String(selectedLine) &&
                lineTarget.dataset.loadingLine !== String(selectedLine)) {
                loadLineOffsets(selectedLine);
            }
        });
    }

    document.addEventListener("click", (event) => {
        const row = event.target.closest("tr.dsr-line[data-line]");
        if (!row) return;

        selectedLine = row.dataset.line;
        if (!selectedLine) return;

        if (lineLabel) lineLabel.textContent = selectedLine;

        // Preload with the other line QC plots; it is ready when REC DB is opened.
        loadLineOffsets(selectedLine);
    });

}
