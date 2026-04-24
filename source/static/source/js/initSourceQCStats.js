export function initSourceQCStats() {

  const tabBtn = document.getElementById("statistics-tab");
  const targetDivId = "source-sunburst";
  const msgEl = document.getElementById("source-stats-msg");

  if (!tabBtn) return;

  let loaded = false;
  let loading = false;

  function setMsg(text, type="muted") {
    if (!msgEl) return;
    msgEl.className = "small mb-2 text-" + type;
    msgEl.textContent = text || "";
  }

  function detectTheme() {
    // Bootstrap 5.3 often uses data-bs-theme="dark|light" on <html> or <body>
    const htmlTheme = document.documentElement.getAttribute("data-bs-theme");
    const bodyTheme = document.body ? document.body.getAttribute("data-bs-theme") : null;
    const t = (htmlTheme || bodyTheme || "light").toLowerCase();
    return (t === "dark") ? "dark" : "light";
  }

  async function loadSunburst() {

    if (loaded || loading) return;

    loading = true;
    setMsg("Loading statistics…", "muted");

    const div = document.getElementById(targetDivId);
    if (div) {
      div.innerHTML = `<div class="text-muted text-center p-5">Loading…</div>`;
    }

    try {
      const theme = detectTheme();

      const res = await fetch(`/source/qc/sunburst-json/?theme=${encodeURIComponent(theme)}`, {
        headers: { "X-Requested-With": "XMLHttpRequest" },
        credentials: "same-origin",
      });

      if (!res.ok) throw new Error("HTTP " + res.status);

      const data = await res.json();

      if (!data || data.ok !== true || !data.figure) {
        // your error helper returns html in data.html
        const html = (data && data.html) ? data.html : `<div class="alert alert-danger">Sunburst error</div>`;
        if (div) div.innerHTML = html;
        setMsg("Failed to load statistics.", "danger");
        return;
      }

      // Render Plotly
      if (typeof Plotly === "undefined") {
        throw new Error("Plotly is not loaded. Add plotly.min.js in your base template.");
      }

      if (div) div.innerHTML = "";
      Plotly.react(targetDivId, data.figure.data, data.figure.layout, { responsive: true });

      loaded = true;
      setMsg("");

    } catch (err) {
      console.error(err);
      setMsg("Failed to load statistics.", "danger");

      if (div) {
        div.innerHTML = `
          <div class="alert alert-danger mb-2">Statistics loading error</div>
          <button class="btn btn-sm btn-primary" id="retry-source-stats">Retry</button>
        `;

        const btn = document.getElementById("retry-source-stats");
        if (btn) {
          btn.onclick = () => {
            loaded = false;
            loading = false;
            loadSunburst();
          };
        }
      }
    } finally {
      loading = false;
    }
  }

  // load when tab opened
  tabBtn.addEventListener("shown.bs.tab", loadSunburst);

  // if already active on page load
  if (tabBtn.classList.contains("active")) {
    loadSunburst();
  }
}