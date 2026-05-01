export function initDSRSpeedHeadingMap() {
    const tab = document.getElementById("dsr-speed-heading-tab");
    const container = document.getElementById("dsr-speed-heading-map");

    if (!tab || !container) return;

    let loaded = false;

    async function loadMap() {
        if (loaded) return;

        container.innerHTML = `
            <div class="text-muted small p-2">
                Loading DSR speed / heading map...
            </div>
        `;

        try {
            const url = "/project/rov/api/dsr-speed-heading-map/";

            const response = await fetch(url, {
                headers: { "X-Requested-With": "XMLHttpRequest" }
            });

            const contentType = response.headers.get("content-type") || "";

            if (!response.ok) {
                const text = await response.text();
                throw new Error(`HTTP ${response.status}: ${text.slice(0, 300)}`);
            }

            if (!contentType.includes("application/json")) {
                const text = await response.text();
                throw new Error(
                    `Expected JSON but got ${contentType}. Response starts with: ${text.slice(0, 300)}`
                );
            }

            const item = await response.json();

            container.innerHTML = "";
            await Bokeh.embed.embed_item(item, "dsr-speed-heading-map");

            loaded = true;

        } catch (err) {
            console.error("DSR speed heading map error:", err);
            container.innerHTML = `
                <div class="text-danger p-2">
                    Failed to load map<br>
                    <small>${err.message}</small>
                </div>
            `;
        }
    }

    tab.addEventListener("shown.bs.tab", loadMap);
}