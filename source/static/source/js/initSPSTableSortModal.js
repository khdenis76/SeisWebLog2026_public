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