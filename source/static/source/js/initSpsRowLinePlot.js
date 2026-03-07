export function initSpsRowLinePlot() {
  const plotEl = document.getElementById("source-line-map-plot");
  if (!plotEl) return;

  const url = plotEl.dataset.url;
  if (!url) {
    console.error("source-line-map-plot is missing data-url");
    return;
  }

  let currentLine = null;
  let currentRequestId = 0;

  function setPlotMessage(html) {
    plotEl.innerHTML = `
      <div class="d-flex align-items-center justify-content-center h-100 text-muted small">
        ${html}
      </div>
    `;
  }

  function clearSelectedRows() {
    document.querySelectorAll("tr.sps-row-active").forEach(tr => {
      tr.classList.remove("sps-row-active");
    });
  }

  function markRowSelected(tr) {
    clearSelectedRows();
    tr.classList.add("sps-row-active");
  }

  function shouldIgnoreClick(event) {
    return !!event.target.closest(
      "input, button, a, label, select, textarea, option"
    );
  }

  async function loadLinePlot(line, rowEl, forceReload = false) {
    line = parseInt(line, 10);
    if (!Number.isFinite(line) || line <= 0) {
      setPlotMessage("Invalid line");
      return;
    }

    if (!forceReload && currentLine === line) {
      markRowSelected(rowEl);
      return;
    }

    currentLine = line;
    currentRequestId += 1;
    const requestId = currentRequestId;

    markRowSelected(rowEl);
    setPlotMessage(`
      <div class="text-center">
        <div class="spinner-border spinner-border-sm me-2" role="status"></div>
        Loading line ${line}...
      </div>
    `);

    try {
      const qs = new URLSearchParams({
        line: String(line)
      });

      const resp = await fetch(`${url}?${qs.toString()}`, {
        method: "GET",
        headers: {
          "X-Requested-With": "XMLHttpRequest"
        }
      });

      const data = await resp.json();

      if (requestId !== currentRequestId) return;

      if (!resp.ok || !data.ok) {
        throw new Error(data.error || `HTTP ${resp.status}`);
      }

      if (!window.Bokeh || !window.Bokeh.embed || !window.Bokeh.embed.embed_item) {
        throw new Error("Bokeh JS is not loaded");
      }

      plotEl.innerHTML = "";
      window.Bokeh.embed.embed_item(data.item, plotEl);

    } catch (err) {
      if (requestId !== currentRequestId) return;

      console.error("Failed to load line plot:", err);
      setPlotMessage(`
        <div class="text-danger text-center px-3">
          Failed to load plot<br>
          <span class="small">${err.message || err}</span>
        </div>
      `);
    }
  }

  document.addEventListener("click", function (event) {
    const tr = event.target.closest("tr[id^='sps-row-']");
    if (!tr) return;

    if (shouldIgnoreClick(event)) return;

    const line = tr.dataset.line;
    loadLinePlot(line, tr);
  });

  // optional: expose reload helper
  window.reloadCurrentSourceLinePlot = function () {
    const activeRow = document.querySelector("tr.sps-row-active");
    if (!activeRow) return;
    loadLinePlot(activeRow.dataset.line, activeRow, true);
  };
}