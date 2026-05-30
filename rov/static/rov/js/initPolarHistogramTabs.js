// rov/static/rov/js/initPolarHistogramTabs.js

export function initPolarHistogramTabs() {
    const tabs = document.querySelectorAll(".lazy-plotly-polar-tab");

    function executeScripts(container) {
        container.querySelectorAll("script").forEach((oldScript) => {
            const newScript = document.createElement("script");

            for (const attr of oldScript.attributes) {
                newScript.setAttribute(attr.name, attr.value);
            }

            newScript.textContent = oldScript.textContent;
            oldScript.parentNode.replaceChild(newScript, oldScript);
        });
    }

    function resizePlotly(container) {
        if (!window.Plotly) return;

        container.querySelectorAll(".js-plotly-plot").forEach((plot) => {
            window.Plotly.Plots.resize(plot);
        });
    }

    tabs.forEach((tab) => {
        tab.addEventListener("shown.bs.tab", async () => {
            if (tab.dataset.loaded === "1") {
                resizePlotly(document.getElementById(tab.dataset.target));
                return;
            }

            const url = tab.dataset.url;
            const target = document.getElementById(tab.dataset.target);

            if (!url || !target) {
                console.warn("Polar histogram tab missing data-url or data-target.", tab);
                return;
            }

            target.innerHTML = `
                <div class="d-flex align-items-center gap-2 text-muted p-3">
                    <div class="spinner-border spinner-border-sm" role="status"></div>
                    <span>Loading polar histograms...</span>
                </div>
            `;

            try {
                const response = await fetch(url, {
                    method: "GET",
                    headers: {
                        "X-Requested-With": "XMLHttpRequest",
                    },
                });

                const data = await response.json();

                if (!response.ok || !data.ok) {
                    throw new Error(data.error || "Failed to load polar histograms.");
                }

                target.innerHTML = data.html || "";

                executeScripts(target);

                tab.dataset.loaded = "1";

                setTimeout(() => resizePlotly(target), 300);
                setTimeout(() => resizePlotly(target), 900);

            } catch (error) {
                console.error(error);

                target.innerHTML = `
                    <div class="alert alert-danger m-2">
                        <b>Polar histogram failed</b><br>
                        ${error.message}
                    </div>
                `;
            }
        });
    });
}