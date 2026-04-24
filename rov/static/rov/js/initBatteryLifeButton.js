import { getCSRFToken } from "../../baseproject/js/csrf.js"; // adjust path if needed
export function initBatteryLifeMap() {
    const button = document.getElementById("bl-load-button");
    const container = document.getElementById("bl-map-container");

    if (!button || !container) return;

    button.addEventListener("click", async () => {
        const url = button.dataset.url;

        try {
            button.disabled = true;
            button.innerHTML = `
                <span class="spinner-border spinner-border-sm me-2"></span>
                Loading...
            `;

            container.innerHTML =
                "<div class='text-center p-5'>Loading map...</div>";

            const response = await fetch(url, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "X-CSRFToken": getCSRFToken(),   // 👈 your existing function
                    "X-Requested-With": "XMLHttpRequest"
                },
                body: JSON.stringify({
                    // send params if needed
                    // example:
                    // rov: "ROV1"
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP error ${response.status}`);
            }

            const data = await response.json();

            container.innerHTML = "";

            // Render Bokeh plot
            Bokeh.embed.embed_item(data.map, container);

        } catch (error) {
            console.error("Battery Life map load error:", error);
            container.innerHTML =
                "<div class='text-danger p-3'>Failed to load map</div>";
        } finally {
            button.disabled = false;
            button.innerHTML =
                `<i class="fas fa-image me-2"></i> Load map`;
        }
    });
}
export function initBatteryLifeRestMap() {
    const button = document.getElementById("bl-load-button2");
    const container = document.getElementById("bl2-map-container");
    const maxDaysInput = document.getElementById("max-days-in-water");
    const binsNumberInput = document.getElementById("bins-number");

    if (!button || !container || !maxDaysInput || !binsNumberInput) return;

    button.addEventListener("click", async () => {
        const url = button.dataset.url;
        const maxDays = parseInt(maxDaysInput.value, 10);
        const binsNumber = parseInt(binsNumberInput.value, 10);

        if (!Number.isFinite(maxDays) || maxDays < 1) {
            container.innerHTML = "<div class='text-danger p-3'>Invalid MAX DAYS value</div>";
            return;
        }

        try {
            button.disabled = true;
            button.innerHTML = `<span class="spinner-border spinner-border-sm me-2"></span> Loading...`;
            container.innerHTML = "<div class='text-center p-5'>Loading map...</div>";

            const response = await fetch(url, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "X-CSRFToken": getCSRFToken(),              // your existing function
                    "X-Requested-With": "XMLHttpRequest",
                },
                body: JSON.stringify({
                    max_days_in_water: maxDays,
                    bins_number: binsNumber,
                }),
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            const data = await response.json();

            container.innerHTML = "";

            // If your view returns JsonResponse(json_item(p))
            // data is directly the json_item dict
            Bokeh.embed.embed_item(data.map, container);

            // If your view returns {"success":true,"plot": json_item(p)} use:
            // Bokeh.embed.embed_item(data.plot, container);

        } catch (err) {
            console.error("Battery Life Rest map load error:", err);
            container.innerHTML = "<div class='text-danger p-3'>Failed to load map</div>";
        } finally {
            button.disabled = false;
            button.innerHTML = `<i class="fas fa-image me-2"></i> Load map`;
        }
    });
}