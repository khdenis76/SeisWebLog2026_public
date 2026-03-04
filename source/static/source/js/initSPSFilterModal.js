// static/source/js/initSPSFilterModal.js
export function initSpsTableFilterModal(options = {}) {
  const tbodyId = options.tbodyId || "sps-table-tbody";
  const tbody = document.getElementById(tbodyId);
  if (!tbody) return;

  // Optional external controls
  const countEl = document.getElementById(options.countId || "sps-filter-count");
  const quickClear = document.getElementById(options.quickClearId || "sps-filter-clear-quick");

  // Modal inputs
  const elSearch   = document.getElementById("spsf-search");
  const elVessel   = document.getElementById("spsf-vessel");
  const elPurpose  = document.getElementById("spsf-purpose");
  const elSailLine = document.getElementById("spsf-sailline");
  const elAttempt  = document.getElementById("spsf-attempt");

  const elLineFrom = document.getElementById("spsf-line-from");
  const elLineTo   = document.getElementById("spsf-line-to");
  const elSeqFrom  = document.getElementById("spsf-seq-from");
  const elSeqTo    = document.getElementById("spsf-seq-to");

  const elWD       = document.getElementById("spsf-wd"); // Ok/Error
  const elGD       = document.getElementById("spsf-gd"); // Ok/Error

  const elOnlyChk  = document.getElementById("spsf-only-checked");

  const btnApply = document.getElementById("spsf-apply");
  const btnReset = document.getElementById("spsf-reset");

  const required = [
    elSearch, elVessel, elPurpose, elSailLine, elAttempt,
    elLineFrom, elLineTo, elSeqFrom, elSeqTo,
    elWD, elGD,
    elOnlyChk, btnApply, btnReset
  ];
  if (required.some(x => !x)) return;

  function getRows() {
    // IMPORTANT: do NOT cache rows, because tbody can be replaced by AJAX
    return Array.from(tbody.querySelectorAll("tr"));
  }

  function clearSelectKeepFirst(selectEl) {
    selectEl.querySelectorAll("option:not(:first-child)").forEach(o => o.remove());
  }

  function buildOptionsFromRows() {
    const rows = getRows();

    const vesselMap  = new Map(); // id -> name
    const purposeSet = new Set();
    const sailSet    = new Set();
    const attemptSet = new Set();

    for (const tr of rows) {
      const vId = (tr.dataset.vessel || "").trim();
      const vNm = (tr.dataset.vesselName || "").trim();
      if (vId) vesselMap.set(vId, vNm || vId);

      const p = (tr.dataset.purpose || "").trim();
      if (p) purposeSet.add(p);

      // NOTE: your attribute is data-sailline => dataset.sailline (NOT sailLine)
      const sl = (tr.dataset.sailline || "").trim();
      if (sl) sailSet.add(sl);

      const a = (tr.dataset.attempt || "").trim();
      if (a) attemptSet.add(a);
    }

    clearSelectKeepFirst(elVessel);
    clearSelectKeepFirst(elPurpose);
    clearSelectKeepFirst(elSailLine);
    clearSelectKeepFirst(elAttempt);

    [...vesselMap.entries()]
      .sort((a, b) => (a[1] || "").localeCompare(b[1] || ""))
      .forEach(([id, name]) => elVessel.appendChild(new Option(name, id)));

    [...purposeSet].sort().forEach(p => elPurpose.appendChild(new Option(p, p)));
    [...sailSet].sort().forEach(sl => elSailLine.appendChild(new Option(sl, sl)));
    [...attemptSet].sort().forEach(a => elAttempt.appendChild(new Option(a, a)));
  }

  function parseNumOrNull(val) {
    const s = (val ?? "").toString().trim();
    if (!s) return null;
    const n = Number(s);
    return Number.isFinite(n) ? n : null;
  }

  function inRange(valueNum, fromNum, toNum) {
    if (valueNum == null) return false;
    if (fromNum != null && valueNum < fromNum) return false;
    if (toNum != null && valueNum > toNum) return false;
    return true;
  }

  function rowMatches(tr) {
    const q = (elSearch.value || "").trim().toLowerCase();
    if (q) {
      const hay = (tr.innerText || "").toLowerCase();
      if (!hay.includes(q)) return false;
    }

    const vesselVal = (elVessel.value || "").trim();
    if (vesselVal && (tr.dataset.vessel || "").trim() !== vesselVal) return false;

    const purposeVal = (elPurpose.value || "").trim();
    if (purposeVal && (tr.dataset.purpose || "").trim() !== purposeVal) return false;

    const sailVal = (elSailLine.value || "").trim();
    if (sailVal && (tr.dataset.sailline || "").trim() !== sailVal) return false;

    const attemptVal = (elAttempt.value || "").trim();
    if (attemptVal && (tr.dataset.attempt || "").trim() !== attemptVal) return false;

    // Line range
    const lineNum = parseNumOrNull(tr.dataset.line);
    const lineFrom = parseNumOrNull(elLineFrom.value);
    const lineTo = parseNumOrNull(elLineTo.value);
    if (lineFrom != null || lineTo != null) {
      if (!inRange(lineNum, lineFrom, lineTo)) return false;
    }

    // Seq range
    const seqNum = parseNumOrNull(tr.dataset.seq);
    const seqFrom = parseNumOrNull(elSeqFrom.value);
    const seqTo = parseNumOrNull(elSeqTo.value);
    if (seqFrom != null || seqTo != null) {
      if (!inRange(seqNum, seqFrom, seqTo)) return false;
    }

    // WD QC
    const wdVal = (elWD.value || "").trim();
    if (wdVal && (tr.dataset.wd || "").trim() !== wdVal) return false;

    // GD QC
    const gdVal = (elGD.value || "").trim();
    if (gdVal && (tr.dataset.gd || "").trim() !== gdVal) return false;

    // Only checked
    if (elOnlyChk.checked) {
      const cb = tr.querySelector("input.row-check[type='checkbox']");
      if (!cb || !cb.checked) return false;
    }

    return true;
  }

  function applyFilters() {
    const rows = getRows();
    let shown = 0;

    for (const tr of rows) {
      const ok = rowMatches(tr);

      // ✅ IMPORTANT: DO NOT overwrite className
      // Only add/remove d-none
      tr.classList.toggle("d-none", !ok);
      if (ok) shown++;
    }

    if (countEl) countEl.textContent = `${shown} / ${rows.length} rows`;

    // notify selection module
    tbody.dispatchEvent(new CustomEvent("sps:filtered", { bubbles: true }));
  }

  function resetFilters() {
    elSearch.value = "";
    elVessel.value = "";
    elPurpose.value = "";
    elSailLine.value = "";
    elAttempt.value = "";

    elLineFrom.value = "";
    elLineTo.value = "";
    elSeqFrom.value = "";
    elSeqTo.value = "";

    elWD.value = "";
    elGD.value = "";

    elOnlyChk.checked = false;
  }

  // Events
  btnApply.addEventListener("click", applyFilters);

  btnReset.addEventListener("click", () => {
    resetFilters();
    applyFilters();
  });

  if (quickClear) {
    quickClear.addEventListener("click", () => {
      resetFilters();
      applyFilters();
    });
  }

  // If checkboxes change while "only checked" enabled
  tbody.addEventListener("change", (e) => {
    if (e.target && e.target.matches("input.row-check[type='checkbox']")) {
      if (elOnlyChk.checked) applyFilters();
    }
  });

  // Init
  buildOptionsFromRows();
  applyFilters();
}