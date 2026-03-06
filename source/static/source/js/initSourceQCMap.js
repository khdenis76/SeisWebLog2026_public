export function initSourceQCMap() {

  const tabBtn = document.getElementById("progress-map-tab");
  const targetDivId = "source-progress-map";
  const msgEl = document.getElementById("source-map-msg");

  if (!tabBtn) return;

  let loaded = false;
  let loading = false;

  function setMsg(text, type="muted") {
    if (!msgEl) return;
    msgEl.className = "small mb-2 text-" + type;
    msgEl.textContent = text || "";
  }

  async function loadMap() {

    if (loaded || loading) return;

    loading = true;
    setMsg("Loading map…", "muted");

    try {

      const res = await fetch("/source/qc/progress-map-json/", {
        headers: { "X-Requested-With": "XMLHttpRequest" },
        credentials: "same-origin",
      });

      if (!res.ok) {
        throw new Error("HTTP " + res.status);
      }

      const item = await res.json();

      Bokeh.embed.embed_item(item, targetDivId);

      loaded = true;
      setMsg("");

    } catch (err) {

      console.error(err);
      setMsg("Failed to load map.", "danger");

      const div = document.getElementById(targetDivId);

      if (div) {
        div.innerHTML = `
          <div class="alert alert-danger">
            Map loading error
          </div>
          <button class="btn btn-sm btn-primary" id="retry-source-map">
            Retry
          </button>
        `;

        const btn = document.getElementById("retry-source-map");
        if (btn) {
          btn.onclick = () => {
            loading = false;
            loadMap();
          };
        }
      }

    } finally {
      loading = false;
    }
  }

  // load when tab opened
  tabBtn.addEventListener("shown.bs.tab", loadMap);

  // if already active on page load
  if (tabBtn.classList.contains("active")) {
    loadMap();
  }

}