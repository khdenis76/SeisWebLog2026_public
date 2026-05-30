import { getCSRFToken } from "../../baseproject/js/csrf.js";
import { renderBokehInto } from "../../baseproject/js/renderBokeh.js";

export function initDSRLineQCTabs() {
  const qcRoot = document.getElementById("dsr-qc-root");
  const lineTabPill = document.getElementById("pills-line-tab");

  if (!qcRoot) return;

  const plotUrl = qcRoot.dataset.plotItemUrl;
  if (!plotUrl) return;

  const TAB_TO_PLOT = {
    "pane-water": { key: "water", divId: "plot-water" },
    "pane-primsec": { key: "primsec", divId: "plot-primsec" },
    "pane-ellipse": { key: "ellipse", divId: "plot-ellipse" },
    "pane-deplpre": { key: "deplpre", divId: "plot-deplpre" },
    "pane-lineinfo": { key: "lineinfo", divId: "dsr-table-body" },
    "pane-map": { key: "map", divId: "plot-map" },
    "pane-delta": { key: "delta", divId: "plot-delta" },
    "pane-xline": { key: "xline", divId: "plot-xline" },
    "pane-recovery": { key: "noderec", divId: "plot-noderec" },

    // REC_DB QC tabs loaded by selected DSR line
    "pane-recdbvsdep": { key: "recdbvsdep", divId: "plot-recdbvsdep" },
    "pane-recdbvsrec": { key: "recdbvsrec", divId: "plot-recdbvsrec" },

    // two target divs for one backend key
    "pane-timing": {
      key: "timing",
      divIds: ["plot-timing", "plot-rec-timing"],
    },

    "pane-3d": { key: "3d", divId: "plot-3d" },
  };

  let selectedLine = null;
  let controller = null;

  // loaded cache:
  // line -> Set(plot_key)
  const loaded = new Map();

  function getTargetIds(cfg) {
    if (!cfg) return [];
    if (Array.isArray(cfg.divIds)) return cfg.divIds;
    if (cfg.divId) return [cfg.divId];
    return [];
  }

  function getActivePane() {
    return document.querySelector("#qcTabsContent .tab-pane.active");
  }

  function getLoadedSet(lineKey) {
    if (!loaded.has(lineKey)) {
      loaded.set(lineKey, new Set());
    }
    return loaded.get(lineKey);
  }

  function setMsg(target, msg, extraClass = "text-muted") {
    const ids = Array.isArray(target) ? target : [target];

    ids.forEach((divId) => {
      const el = document.getElementById(divId);
      if (!el) return;

      el.innerHTML = `
        <div class="${extraClass} p-2">
          ${msg}
        </div>
      `;
    });
  }

  function setLoading(target, msg) {
    const ids = Array.isArray(target) ? target : [target];

    ids.forEach((divId) => {
      const el = document.getElementById(divId);
      if (!el) return;

      el.innerHTML = `
        <div class="d-flex align-items-center justify-content-center h-100 text-muted">
          <div class="spinner-border spinner-border-sm text-primary me-2"></div>
          <span>${msg}</span>
        </div>
      `;
    });
  }

  function clearTargets(target) {
    const ids = Array.isArray(target) ? target : [target];

    ids.forEach((divId) => {
      const el = document.getElementById(divId);
      if (el) el.innerHTML = "";
    });
  }

  function resetPanels() {
    Object.values(TAB_TO_PLOT).forEach((cfg) => {
      setMsg(getTargetIds(cfg), `Open tab to load… (${cfg.key})`);
    });
  }

  function resetLoadedForLine(lineKey) {
    loaded.delete(String(lineKey));
  }

  function renderHtmlInto(divId, html) {
    const el = document.getElementById(divId);
    if (el) el.innerHTML = html;
  }

  function markSelectedLine(tr) {
    const tbody = tr.parentElement;

    tbody?.querySelectorAll("tr.dsr-line.is-active").forEach((r) => {
      r.classList.remove("is-active");
    });

    tr.classList.add("is-active");

    const icon = tr.querySelector("i.dsr-line-click");
    if (icon) icon.classList.remove("d-none");
  }

  async function loadPane(paneId, force = false) {
    const cfg = TAB_TO_PLOT[paneId];
    if (!cfg) return;

    const targetIds = getTargetIds(cfg);

    if (!selectedLine) {
      setMsg(targetIds, "Select a line…");
      return;
    }

    if (lineTabPill) {
      lineTabPill.innerHTML = `
        <i class="fas fa-arrow-down-up-across-line me-2"></i>
        LINE:${selectedLine}
      `;
    }

    const lineKey = String(selectedLine);
    const set = getLoadedSet(lineKey);

    if (!force && set.has(cfg.key)) return;

    if (controller) controller.abort();
    controller = new AbortController();

    setLoading(targetIds, `Loading ${cfg.key} for line ${selectedLine}…`);

    try {
      const resp = await fetch(plotUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCSRFToken(),
          "X-Requested-With": "XMLHttpRequest",
        },
        body: JSON.stringify({
          line: selectedLine,
          plot_key: cfg.key,
        }),
        signal: controller.signal,
      });

      let data = {};
      try {
        data = await resp.json();
      } catch (jsonErr) {
        setMsg(targetIds, "Invalid JSON response from server.", "text-danger");
        return;
      }

      if (!resp.ok || !data.ok) {
        setMsg(targetIds, data.error || "Failed to load.", "text-danger");
        return;
      }

      // multi-bokeh response: { items: [item1, item2, ...] }
      if (Array.isArray(data.items) && data.items.length) {
        targetIds.forEach((divId, i) => {
          if (data.items[i]) {
            clearTargets(divId);
            renderBokehInto(divId, data.items[i]);
          } else {
            setMsg(divId, "No content returned.");
          }
        });

        set.add(cfg.key);
        return;
      }

      // single-bokeh response: { item: {...} }
      if (data.item) {
        if (!cfg.divId) {
          setMsg(targetIds, "No target div configured.", "text-danger");
          return;
        }

        clearTargets(cfg.divId);
        renderBokehInto(cfg.divId, data.item);
        set.add(cfg.key);
        return;
      }

      // single html response: { html: "..." }
      if (typeof data.html === "string") {
        if (!cfg.divId) {
          setMsg(targetIds, "No target div configured.", "text-danger");
          return;
        }

        renderHtmlInto(cfg.divId, data.html);
        set.add(cfg.key);
        return;
      }

      // multi-html response: { htmls: ["...", "..."] }
      if (Array.isArray(data.htmls) && data.htmls.length) {
        targetIds.forEach((divId, i) => {
          if (typeof data.htmls[i] === "string") {
            renderHtmlInto(divId, data.htmls[i]);
          } else {
            setMsg(divId, "No content returned.");
          }
        });

        set.add(cfg.key);
        return;
      }

      setMsg(targetIds, "No content returned.");

    } catch (err) {
      if (err?.name === "AbortError") return;

      console.error("DSR QC tab load error:", err);
      setMsg(targetIds, `Error loading: ${err.message || err}`, "text-danger");
    }
  }

  // ---------------------------------------------------------
  // Click on DSR line row
  // ---------------------------------------------------------
  document.addEventListener("click", (e) => {
    const tr = e.target.closest("tr.dsr-line[data-line]");
    if (!tr) return;

    selectedLine = tr.dataset.line;

    markSelectedLine(tr);
    resetLoadedForLine(selectedLine);
    resetPanels();

    const activePane = getActivePane();
    if (activePane) {
      loadPane(activePane.id, true);
    }
  });

  // ---------------------------------------------------------
  // Bootstrap tab open triggers lazy load
  // ---------------------------------------------------------
  const tabs = document.getElementById("qcTabs");

  if (tabs) {
    tabs.addEventListener("shown.bs.tab", (event) => {
      const target = event.target.getAttribute("data-bs-target");
      const paneId = (target || "").replace("#", "");

      if (paneId) {
        loadPane(paneId, false);
      }
    });
  }

  resetPanels();
}