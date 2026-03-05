export function initShotLineSort(opts = {}) {
  const tbodyId = opts.tbodyId || "shot-line-summary-tbody"; // <-- set your real tbody id
  const tbody = document.getElementById(tbodyId);
  if (!tbody) return;

  const modalEl = document.getElementById("shotSortModal");
  const keyEl   = document.getElementById("shot-sort-key");
  const applyBtn = document.getElementById("shot-sort-apply");
  const resetBtn = document.getElementById("shot-sort-reset");

  if (!modalEl || !keyEl || !applyBtn || !resetBtn) return;

  // Keep original order for Reset
  const originalIds = Array.from(tbody.querySelectorAll("tr")).map(tr => tr.id);

  function getDir() {
    const el = document.querySelector('input[name="shot-sort-dir"]:checked');
    return el ? el.value : "asc";
  }

  function asNumber(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  }

  function asText(v) {
    return (v ?? "").toString().trim().toUpperCase();
  }

  function keyValue(tr, key) {
    const d = tr.dataset;

    switch (key) {
      case "line_code": return asText(d.lineCode);
      case "line":      return asNumber(d.line);
      case "attempt":   return asNumber(d.attempt);
      case "seq":       return asNumber(d.seq);
      case "shotcount": return asNumber(d.shotcount);
      case "diffsum":   return asNumber(d.diffsumValue || d.diffsum || 0); // see note below
      case "in_sl":     return asNumber(d.inSl);
      case "qc_all":    return asNumber(d.qcAllmatch || d.qcAllMatch || 0);
      case "qc_any":    return asNumber(d.qcAnymatch || d.qcAnyMatch || 0);
      default:          return asText(d.lineCode);
    }
  }

  function compare(aTr, bTr, key, dir) {
    const av = keyValue(aTr, key);
    const bv = keyValue(bTr, key);

    let out = 0;

    if (typeof av === "number" && typeof bv === "number") {
      out = av - bv;
    } else {
      out = av.localeCompare(bv);
    }

    // stable tie-breakers (always same)
    if (out === 0) out = asText(aTr.dataset.lineCode).localeCompare(asText(bTr.dataset.lineCode));
    if (out === 0) out = asNumber(aTr.dataset.attempt) - asNumber(bTr.dataset.attempt);
    if (out === 0) out = asNumber(aTr.dataset.seq) - asNumber(bTr.dataset.seq);

    return dir === "desc" ? -out : out;
  }

  function applySort() {
    const key = keyEl.value;
    const dir = getDir();

    const rows = Array.from(tbody.querySelectorAll("tr"));

    rows.sort((a, b) => compare(a, b, key, dir));

    const frag = document.createDocumentFragment();
    for (const tr of rows) frag.appendChild(tr);
    tbody.appendChild(frag);

    // close modal
    const modal = bootstrap.Modal.getInstance(modalEl) || new bootstrap.Modal(modalEl);
    modal.hide();
  }

  function resetSort() {
    const map = new Map();
    for (const tr of tbody.querySelectorAll("tr")) map.set(tr.id, tr);

    const frag = document.createDocumentFragment();
    for (const id of originalIds) {
      const tr = map.get(id);
      if (tr) frag.appendChild(tr);
    }
    tbody.appendChild(frag);
  }

  applyBtn.addEventListener("click", applySort);
  resetBtn.addEventListener("click", resetSort);
}