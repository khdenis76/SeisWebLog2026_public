export function initDSRSpeedHeadingMap() {
    const mapConfigs = [
        {
            tabId: "dsr-speed-heading-tab",
            containerId: "dsr-speed-heading-map",
            url: "/project/rov/api/dsr-speed-heading-map/",
            label: "deployment",
        },
        {
            tabId: "dsr-recovery-speed-heading-tab",
            containerId: "dsr-recovery-speed-heading-map",
            url: "/project/rov/api/dsr-recovery-speed-heading-map/",
            label: "recovery",
        },
    ];

    mapConfigs.forEach(({ tabId, containerId, url, label }) => {
      const tab = document.getElementById(tabId);
      const container = document.getElementById(containerId);
      if (!tab || !container) return;

      let loaded = false;

      async function loadMap() {
        if (loaded) return;

        container.innerHTML = `
            <div class="text-muted small p-2">
                Loading DSR ${label} speed / heading map...
            </div>
        `;

        try {
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
            await Bokeh.embed.embed_item(item, containerId);

            loaded = true;

        } catch (err) {
            console.error(`DSR ${label} speed heading map error:`, err);
            container.innerHTML = `
                <div class="text-danger p-2">
                    Failed to load map<br>
                    <small>${err.message}</small>
                </div>
            `;
        }
      }

      tab.addEventListener("shown.bs.tab", loadMap);
    });
}
