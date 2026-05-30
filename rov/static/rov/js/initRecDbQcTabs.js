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

}