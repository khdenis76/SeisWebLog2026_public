// initDSRLineQCTabs.js
import { getCSRFToken } from "../../baseproject/js/csrf.js";
import { renderBokehInto } from "../../baseproject/js/renderBokeh.js"; // your helper

export function initDSRLineQCTabs() {
  const qcRoot = document.getElementById("dsr-qc-root");
  const lineTabPill =document.getElementById("pills-line-tab")
  if (!qcRoot) return;

  const plotUrl = qcRoot.dataset.plotItemUrl;
  if (!plotUrl) return;

  const TAB_TO_PLOT = {
    "pane-water":  { key: "water",   divId: "plot-water" },
    "pane-primsec":{ key: "primsec", divId: "plot-primsec" },
    "pane-ellipse":{ key: "ellipse", divId: "plot-ellipse" },
    "pane-deplpre":{ key: "deplpre", divId: "plot-deplpre" },
    "pane-lineinfo":{ key: "lineinfo", divId: "dsr-table-body" },
    "pane-map":    { key: "map",     divId: "plot-map" },
    "pane-delta":  { key: "delta",   divId: "plot-delta" },
    "pane-xline":  { key: "xline",   divId: "plot-xline" },
    "pane-timing": { key: "timing",  divId: "plot-timing" },
    "pane-3d":     { key: "3d",      divId: "plot-3d" },
  };

  let selectedLine = null;
  let controller = null;
  const loaded = new Map(); // line -> Set(plot_key)

  function setMsg(divId, msg) {
    const el = document.getElementById(divId);
    if (el) el.innerHTML = `<div class="text-muted p-2">${msg}</div>`;
  }

  function resetPanels() {
    Object.values(TAB_TO_PLOT).forEach(({ key, divId }) => {
      setMsg(divId, `Open tab to load… (${key})`);
    });
  }

  async function loadPane(paneId, force=false) {
    const cfg = TAB_TO_PLOT[paneId];
    if (!cfg) return;

    if (!selectedLine) {
      setMsg(cfg.divId, "Select a line…");
      return;
    }
    lineTabPill.innerHTML = `<i class="fas fa-arrow-down-up-across-line me-2"></i>LINE:${selectedLine}`;
    const lineKey = String(selectedLine);
    if (!loaded.has(lineKey)) loaded.set(lineKey, new Set());
    const set = loaded.get(lineKey);

    if (!force && set.has(cfg.key)) return;

    if (controller) controller.abort();
    controller = new AbortController();

    setMsg(cfg.divId, `Loading ${cfg.key}…`);

    try {
      const resp = await fetch(plotUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCSRFToken(),
        },
        body: JSON.stringify({ line: selectedLine, plot_key: cfg.key }),
        signal: controller.signal,
      });

      const data = await resp.json();

      if (!resp.ok || !data.ok) {
        setMsg(cfg.divId, data.error || "Failed to load");
        return;
      }

      // bokeh
      if (data.item) {
        renderBokehInto(cfg.divId, data.item);
        set.add(cfg.key);
        return;
      }

      // html fallback
      if (typeof data.html === "string") {
        document.getElementById(cfg.divId).innerHTML = data.html;
        set.add(cfg.key);
        return;
      }

      setMsg(cfg.divId, "No content returned");

    } catch (err) {
      if (err?.name === "AbortError") return;
      console.error(err);
      setMsg(cfg.divId, "Error loading");
    }
  }

  // 1) click on DSR line row
  document.addEventListener("click", (e) => {
    const tr = e.target.closest("tr.dsr-line[data-line]");
    if (!tr) return;

    selectedLine = tr.dataset.line;

    // highlight (optional)
    const tbody = tr.parentElement;
    tbody?.querySelectorAll("tr.dsr-line.is-active").forEach(r => r.classList.remove("is-active"));
    tr.classList.add("is-active");

    resetPanels();

    // load currently active pane immediately
    const activePane = document.querySelector("#qcTabsContent .tab-pane.active");
    if (activePane) loadPane(activePane.id, true);
  });

  // 2) tab open triggers load
  const tabs = document.getElementById("qcTabs");
  if (tabs) {
    tabs.addEventListener("shown.bs.tab", (event) => {
      const target = event.target.getAttribute("data-bs-target"); // #pane-water
      const paneId = (target || "").replace("#", "");
      if (paneId) loadPane(paneId, false);
    });
  }

  // initial message
  resetPanels();
}