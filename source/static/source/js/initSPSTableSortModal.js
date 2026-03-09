export function initSpsTableSortModal(options = {}) {
  const tableId = options.tableId || "sps-table";
  const tbodyId = options.tbodyId || "sps-table-tbody";
  const modalId = options.modalId || "spsSortModal";
  const labelId = options.labelId || "sps-sort-label";

  const table = document.getElementById(tableId);
  const tbody = document.getElementById(tbodyId);
  const modalEl = document.getElementById(modalId);
  const labelEl = document.getElementById(labelId);

  if (!table || !tbody || !modalEl) return;

  const keys = [
    document.getElementById("spss-key-1"),
    document.getElementById("spss-key-2"),
    document.getElementById("spss-key-3"),
    document.getElementById("spss-key-4"),
  ];
  const dirs = [
    document.getElementById("spss-dir-1"),
    document.getElementById("spss-dir-2"),
    document.getElementById("spss-dir-3"),
    document.getElementById("spss-dir-4"),
  ];
  const btnApply = document.getElementById("spss-apply");
  const btnReset = document.getElementById("spss-reset");

  if (keys.some(x => !x) || dirs.some(x => !x) || !btnApply || !btnReset) return;

  function getRows() {
    return Array.from(tbody.querySelectorAll("tr"));
  }

  function numOrText(v) {
    const s = (v ?? "").toString().trim();
    if (s === "") return { type: "text", v: "" };
    const n = Number(s);
    if (Number.isFinite(n)) return { type: "num", v: n };
    return { type: "text", v: s.toLowerCase() };
  }

  // Attempt priority: ASC => R first, L second, others after (alphabetical)
  function attemptRank(val) {
    const s = (val ?? "").toString().trim().toUpperCase();
    if (s === "R") return 0;
    if (s === "L") return 1;
    if (s === "") return 9999;
    return 100 + s.charCodeAt(0);
  }

  function getValue(tr, key) {
    switch (key) {
      case "sailline":   return tr.dataset.sailline || "";
      case "line":       return tr.dataset.line || "";
      case "seq":        return tr.dataset.seq || "";
      case "attempt":    return tr.dataset.attempt || "";
      case "vesselName": return tr.dataset.vesselName || "";
      case "purpose":    return tr.dataset.purpose || "";
      case "maxspi":     return tr.dataset.maxspi || "";
      case "wd":         return tr.dataset.wd || "";
      case "gd":         return tr.dataset.gd || "";
      default:           return "";
    }
  }

  function buildSortStack() {
    const stack = [];
    for (let i = 0; i < 4; i++) {
      const key = (keys[i].value || "").trim();
      if (!key) continue;
      const dir = (dirs[i].value || "asc").toLowerCase() === "desc" ? -1 : 1;
      stack.push({ key, dir });
    }
    return stack;
  }

  function compare(a, b, stack) {
    for (const s of stack) {
      const key = s.key;
      const dir = s.dir;

      const va = getValue(a, key);
      const vb = getValue(b, key);

      // Special rule for Attempt
      if (key === "attempt") {
        const ra = attemptRank(va);
        const rb = attemptRank(vb);
        if (ra < rb) return -1 * dir;
        if (ra > rb) return 1 * dir;
        continue;
      }

      // Numeric sort fields
      if (key === "line" || key === "seq" || key === "maxspi") {
        const na = Number(va);
        const nb = Number(vb);
        const aNum = Number.isFinite(na) ? na : 0;
        const bNum = Number.isFinite(nb) ? nb : 0;
        if (aNum < bNum) return -1 * dir;
        if (aNum > bNum) return 1 * dir;
        continue;
      }

      // Generic: number if possible else text
      const pa = numOrText(va);
      const pb = numOrText(vb);

      if (pa.type === "num" && pb.type === "num") {
        if (pa.v < pb.v) return -1 * dir;
        if (pa.v > pb.v) return 1 * dir;
      } else {
        if (pa.v < pb.v) return -1 * dir;
        if (pa.v > pb.v) return 1 * dir;
      }
    }
    return 0;
  }

  function applySort() {
    const stack = buildSortStack();
    const rows = getRows();

    if (stack.length === 0) {
      if (labelEl) labelEl.textContent = "";
      return;
    }

    rows.sort((a, b) => compare(a, b, stack));
    rows.forEach(r => tbody.appendChild(r));

    if (labelEl) {
      const nice = stack.map(s => `${s.key}:${s.dir === 1 ? "ASC" : "DESC"}`).join("  |  ");
      labelEl.textContent = nice;
    }
  }

  function resetSort() {
    keys.forEach(k => (k.value = ""));
    dirs.forEach(d => (d.value = "asc"));
    if (labelEl) labelEl.textContent = "";
  }

  btnApply.addEventListener("click", () => {
    applySort();
  });

  btnReset.addEventListener("click", () => {
    resetSort();
    applySort();
  });

  modalEl.addEventListener("shown.bs.modal", () => {
    // nothing required
  });
}
export function initSpsTableAdvancedSort(options = {}) {
  const tableId = options.tableId || "sps-table";
  const theadId = options.theadId || "sps-table-thead";
  const tbodyId = options.tbodyId || "sps-table-tbody";
  const labelId = options.labelId || "sps-sort-label";
  const storageKey = options.storageKey || "seisweblog:sps-table-sort";
  const maxLevels = Number.isInteger(options.maxLevels) ? options.maxLevels : 4;

  const table = document.getElementById(tableId);
  const thead = document.getElementById(theadId);
  const tbody = document.getElementById(tbodyId);
  const labelEl = document.getElementById(labelId);

  if (!table || !thead || !tbody) return null;

  let sortStack = loadSortState();
  let suppressClick = false;

  const NUMERIC_KEYS = new Set([
    "line", "seq", "fsp", "fgsp", "lgsp", "lsp",
    "linelength", "pplength", "percentline", "percentseq",
    "production", "nonproduction", "kill",
    "gdmin", "gdmax", "wdmin", "wdmax",
    "seqlenpercentage", "maxspi",
    "startx", "starty", "endx", "endy"
  ]);

  const DATE_KEYS = new Set([
    "start_time", "prodstart", "prodend", "end_time"
  ]);

  const TEXT_KEYS = new Set([
    "sailline", "attempt", "vesselname", "purpose", "wd", "gd"
  ]);

  function normalizeKey(key) {
    return String(key || "").trim().toLowerCase();
  }

  function getRows() {
    return Array.from(tbody.querySelectorAll("tr"));
  }

  function attemptRank(val) {
    const s = String(val || "").trim().toUpperCase();
    if (s === "R") return 0;
    if (s === "L") return 1;
    if (!s) return 9999;
    return 100 + s.charCodeAt(0);
  }

  function readRaw(tr, key) {
    switch (key) {
      case "sailline": return tr.dataset.sailline || "";
      case "line": return tr.dataset.line || "";
      case "seq": return tr.dataset.seq || "";
      case "attempt": return tr.dataset.attempt || "";
      case "vesselname": return tr.dataset.vesselName || "";
      case "purpose": return tr.dataset.purpose || "";
      case "fsp": return tr.dataset.fsp || "";
      case "fgsp": return tr.dataset.fgsp || "";
      case "lgsp": return tr.dataset.lgsp || "";
      case "lsp": return tr.dataset.lsp || "";
      case "linelength": return tr.dataset.linelength || "";
      case "pplength": return tr.dataset.pplength || "";
      case "percentline": return tr.dataset.percentline || "";
      case "percentseq": return tr.dataset.percentseq || "";
      case "production": return tr.dataset.production || "";
      case "nonproduction": return tr.dataset.nonproduction || "";
      case "kill": return tr.dataset.kill || "";
      case "gdmin": return tr.dataset.gdmin || "";
      case "gdmax": return tr.dataset.gdmax || "";
      case "wdmin": return tr.dataset.wdmin || "";
      case "wdmax": return tr.dataset.wdmax || "";
      case "seqlenpercentage": return tr.dataset.seqlenpercentage || "";
      case "maxspi": return tr.dataset.maxspi || "";
      case "startx": return tr.dataset.startx || "";
      case "starty": return tr.dataset.starty || "";
      case "endx": return tr.dataset.endx || "";
      case "endy": return tr.dataset.endy || "";
      case "start_time": return tr.dataset.startTime || "";
      case "prodstart": return tr.dataset.prodstart || "";
      case "prodend": return tr.dataset.prodend || "";
      case "end_time": return tr.dataset.endTime || "";
      case "wd": return tr.dataset.wd || "";
      case "gd": return tr.dataset.gd || "";
      default: return "";
    }
  }

  function autoParse(key, raw) {
    const s = String(raw ?? "").trim();

    if (key === "attempt") {
      return { type: "attempt", value: attemptRank(s), raw: s };
    }

    if (NUMERIC_KEYS.has(key)) {
      const n = Number(s);
      return { type: "num", value: Number.isFinite(n) ? n : Number.NEGATIVE_INFINITY, raw: s };
    }

    if (DATE_KEYS.has(key)) {
      const t = Date.parse(s);
      if (Number.isFinite(t)) {
        return { type: "date", value: t, raw: s };
      }
      return { type: "text", value: s.toLowerCase(), raw: s };
    }

    if (TEXT_KEYS.has(key)) {
      return { type: "text", value: s.toLowerCase(), raw: s };
    }

    const n = Number(s);
    if (s !== "" && Number.isFinite(n)) {
      return { type: "num", value: n, raw: s };
    }

    const t = Date.parse(s);
    if (s !== "" && Number.isFinite(t)) {
      return { type: "date", value: t, raw: s };
    }

    return { type: "text", value: s.toLowerCase(), raw: s };
  }

  function compareParsed(a, b, dir) {
    if (a.value < b.value) return -1 * dir;
    if (a.value > b.value) return 1 * dir;
    return 0;
  }

  function buildDecoratedRows(rows) {
    return rows.map((tr, index) => {
      const cache = {};
      for (const item of sortStack) {
        const key = normalizeKey(item.key);
        const raw = readRaw(tr, key);
        cache[key] = autoParse(key, raw);
      }
      return { tr, index, cache };
    });
  }

  function sortRows() {
    const rows = getRows();
    if (!rows.length) {
      updateHeaderIndicators();
      updateLabel();
      saveSortState();
      return;
    }

    if (!sortStack.length) {
      updateHeaderIndicators();
      updateLabel();
      saveSortState();
      return;
    }

    const decorated = buildDecoratedRows(rows);

    decorated.sort((a, b) => {
      for (const item of sortStack) {
        const key = normalizeKey(item.key);
        const dir = item.dir === "desc" ? -1 : 1;
        const cmp = compareParsed(a.cache[key], b.cache[key], dir);
        if (cmp !== 0) return cmp;
      }
      return a.index - b.index;
    });

    const frag = document.createDocumentFragment();
    for (const item of decorated) {
      frag.appendChild(item.tr);
    }
    tbody.appendChild(frag);

    updateHeaderIndicators();
    updateLabel();
    saveSortState();
  }

  function cycleDir(current) {
    if (current === "asc") return "desc";
    if (current === "desc") return null;
    return "asc";
  }

  function onHeaderClick(ev) {
  if (suppressClick) return;

  const th = ev.target.closest("th[data-sort-key]");
  if (!th || !thead.contains(th)) return;

  const key = normalizeKey(th.dataset.sortKey);
  if (!key) return;

  const isMulti = ev.shiftKey;

  // for normal click, detect current direction from full sortStack
  // for shift+click, detect it from the working copy
  let next = isMulti ? [...sortStack] : [];
  const source = isMulti ? next : sortStack;

  const existingIndex = source.findIndex(x => normalizeKey(x.key) === key);
  const currentDir = existingIndex >= 0 ? source[existingIndex].dir : null;
  const newDir = cycleDir(currentDir);

  if (isMulti) {
    const idxInNext = next.findIndex(x => normalizeKey(x.key) === key);
    if (idxInNext >= 0) {
      next.splice(idxInNext, 1);
    }
  }

  if (newDir) {
    next.unshift({ key, dir: newDir });
  }

  sortStack = next.slice(0, maxLevels);
  sortRows();
}

  function updateHeaderIndicators() {
    thead.querySelectorAll("th[data-sort-key]").forEach(th => {
      th.classList.remove("sort-asc", "sort-desc", "sort-active");
      th.removeAttribute("data-sort-order");
      th.removeAttribute("data-sort-priority");

      const key = normalizeKey(th.dataset.sortKey);
      const idx = sortStack.findIndex(x => normalizeKey(x.key) === key);
      if (idx >= 0) {
        const item = sortStack[idx];
        th.classList.add("sort-active");
        th.classList.add(item.dir === "desc" ? "sort-desc" : "sort-asc");
        th.setAttribute("data-sort-order", item.dir);
        th.setAttribute("data-sort-priority", String(idx + 1));
      }
    });
  }

  function updateLabel() {
    if (!labelEl) return;
    if (!sortStack.length) {
      labelEl.textContent = "";
      return;
    }
    labelEl.textContent = sortStack
      .map((s, i) => `${i + 1}. ${s.key} ${String(s.dir).toUpperCase()}`)
      .join("  |  ");
  }

  function saveSortState() {
    try {
      localStorage.setItem(storageKey, JSON.stringify(sortStack));
    } catch (_) {}
  }

  function loadSortState() {
    try {
      const raw = localStorage.getItem(storageKey);
      if (!raw) return [];
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return [];
      return parsed
        .map(x => ({
          key: normalizeKey(x?.key),
          dir: x?.dir === "desc" ? "desc" : "asc",
        }))
        .filter(x => x.key);
    } catch (_) {
      return [];
    }
  }

  function clearSort() {
    sortStack = [];
    saveSortState();
    updateHeaderIndicators();
    updateLabel();
  }

  function setSort(newStack) {
    sortStack = Array.isArray(newStack)
      ? newStack
          .map(x => ({
            key: normalizeKey(x?.key),
            dir: x?.dir === "desc" ? "desc" : "asc",
          }))
          .filter(x => x.key)
          .slice(0, maxLevels)
      : [];
    sortRows();
  }

  function reapply() {
    sortRows();
  }

  function refreshAfterAjax() {
    sortRows();
  }

  thead.addEventListener("click", onHeaderClick);

  // expose helpers globally so your AJAX update can call them
  window.spsTableSort = {
    clear: clearSort,
    set: setSort,
    reapply: reapply,
    refreshAfterAjax: refreshAfterAjax,
    getState: () => [...sortStack],
  };

  sortRows();

  return window.spsTableSort;
}