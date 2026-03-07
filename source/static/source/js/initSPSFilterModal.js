// static/source/js/initSPSFilterModal.js
export function initSpsTableFilterModal(options = {}) {
  const tbodyId = options.tbodyId || "sps-table-tbody";
  const endpoint = options.endpoint || "/source/sps/table-data/";
  const tbody = document.getElementById(tbodyId);
  if (!tbody) return;

  const countEl = document.getElementById(options.countId || "sps-filter-count");
  const quickClear = document.getElementById(options.quickClearId || "sps-filter-clear-quick");

  // modal
  const modalEl = document.getElementById(options.modalId || "spsFilterModal");
  const modalInstance = modalEl && window.bootstrap ? bootstrap.Modal.getOrCreateInstance(modalEl) : null;

  // modal inputs
  const elSearch   = document.getElementById("spsf-search");
  const elVessel   = document.getElementById("spsf-vessel");
  const elPurpose  = document.getElementById("spsf-purpose");
  const elSailLine = document.getElementById("spsf-sailline");
  const elAttempt  = document.getElementById("spsf-attempt");

  const elLineFrom = document.getElementById("spsf-line-from");
  const elLineTo   = document.getElementById("spsf-line-to");
  const elSeqFrom  = document.getElementById("spsf-seq-from");
  const elSeqTo    = document.getElementById("spsf-seq-to");

  const elWD       = document.getElementById("spsf-wd");
  const elGD       = document.getElementById("spsf-gd");

  const elOnlyChk  = document.getElementById("spsf-only-checked");

  // optional sort controls
  const elSortBy   = document.getElementById("spsf-sort-by");
  const elSortDir  = document.getElementById("spsf-sort-dir");

  const btnApply = document.getElementById("spsf-apply");
  const btnReset = document.getElementById("spsf-reset");

  const required = [
    elSearch, elVessel, elPurpose, elSailLine, elAttempt,
    elLineFrom, elLineTo, elSeqFrom, elSeqTo,
    elWD, elGD, elOnlyChk, btnApply, btnReset
  ];
  if (required.some(x => !x)) return;

  let isLoading = false;
  let originalApplyHtml = btnApply.innerHTML;
  let originalResetHtml = btnReset.innerHTML;
  let originalQuickClearHtml = quickClear ? quickClear.innerHTML : "";

  function getTbody() {
    return document.getElementById(tbodyId);
  }

  function getRows() {
    const currentTbody = getTbody();
    if (!currentTbody) return [];
    return Array.from(currentTbody.querySelectorAll("tr"));
  }

  function clearSelectKeepFirst(selectEl) {
    if (!selectEl) return;
    selectEl.querySelectorAll("option:not(:first-child)").forEach(o => o.remove());
  }

  function ensureTableLoader() {
    let loader = document.getElementById("sps-table-loader");
    if (loader) return loader;

    const currentTbody = getTbody();
    if (!currentTbody) return null;

    loader = document.createElement("div");
    loader.id = "sps-table-loader";
    loader.className = "d-none position-absolute top-50 start-50 translate-middle text-center px-3 py-2 rounded shadow-sm border bg-body";
    loader.style.zIndex = "30";
    loader.innerHTML = `
      <div class="d-flex align-items-center gap-2">
        <div class="spinner-border spinner-border-sm text-primary" role="status" aria-hidden="true"></div>
        <span class="small">Loading data...</span>
      </div>
    `;

    const wrapper =
      currentTbody.closest(".table-responsive, .card-body, .position-relative") ||
      currentTbody.parentElement;

    if (wrapper) {
      const wrapperStyle = window.getComputedStyle(wrapper);
      if (wrapperStyle.position === "static") {
        wrapper.style.position = "relative";
      }
      wrapper.appendChild(loader);
    }

    return loader;
  }

  function ensureModalLoader() {
    if (!modalEl) return null;

    let loader = modalEl.querySelector(".sps-modal-loader");
    if (loader) return loader;

    const modalBody = modalEl.querySelector(".modal-body");
    if (!modalBody) return null;

    const bodyStyle = window.getComputedStyle(modalBody);
    if (bodyStyle.position === "static") {
      modalBody.style.position = "relative";
    }

    loader = document.createElement("div");
    loader.className = "sps-modal-loader d-none position-absolute top-0 start-0 w-100 h-100";
    loader.style.zIndex = "20";
    loader.style.background = "rgba(255,255,255,0.45)";
    loader.innerHTML = `
      <div class="position-absolute top-50 start-50 translate-middle text-center px-3 py-2 rounded shadow-sm border bg-body">
        <div class="d-flex align-items-center gap-2">
          <div class="spinner-border spinner-border-sm text-primary" role="status" aria-hidden="true"></div>
          <span class="small">Applying filter...</span>
        </div>
      </div>
    `;
    modalBody.appendChild(loader);
    return loader;
  }

  function setButtonsLoading(state) {
    if (state) {
      btnApply.innerHTML = `
        <span class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>
        Loading...
      `;
      btnReset.innerHTML = `
        <span class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>
        Please wait
      `;
      if (quickClear) {
        quickClear.innerHTML = `
          <span class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>
          Please wait
        `;
      }
    } else {
      btnApply.innerHTML = originalApplyHtml;
      btnReset.innerHTML = originalResetHtml;
      if (quickClear) {
        quickClear.innerHTML = originalQuickClearHtml;
      }
    }
  }

  function setLoading(state) {
    isLoading = !!state;

    const tableLoader = ensureTableLoader();
    const modalLoader = ensureModalLoader();

    if (tableLoader) tableLoader.classList.toggle("d-none", !isLoading);
    if (modalLoader) modalLoader.classList.toggle("d-none", !isLoading);

    btnApply.disabled = isLoading;
    btnReset.disabled = isLoading;
    if (quickClear) quickClear.disabled = isLoading;

    [
      elSearch, elVessel, elPurpose, elSailLine, elAttempt,
      elLineFrom, elLineTo, elSeqFrom, elSeqTo,
      elWD, elGD, elOnlyChk, elSortBy, elSortDir
    ].forEach(el => {
      if (el) el.disabled = isLoading;
    });

    setButtonsLoading(isLoading);
  }

  function buildOptionsFromRows() {
    const rows = getRows();

    const vesselMap  = new Map();
    const purposeSet = new Set();
    const sailSet    = new Set();
    const attemptSet = new Set();

    for (const tr of rows) {
      const vId = (tr.dataset.vessel || "").trim();
      const vNm = (tr.dataset.vesselName || "").trim();
      if (vId) vesselMap.set(vId, vNm || vId);

      const p = (tr.dataset.purpose || "").trim();
      if (p) purposeSet.add(p);

      const sl = (tr.dataset.sailline || "").trim();
      if (sl) sailSet.add(sl);

      const a = (tr.dataset.attempt || "").trim();
      if (a) attemptSet.add(a);
    }

    const keepVessel   = elVessel.value;
    const keepPurpose  = elPurpose.value;
    const keepSailLine = elSailLine.value;
    const keepAttempt  = elAttempt.value;

    clearSelectKeepFirst(elVessel);
    clearSelectKeepFirst(elPurpose);
    clearSelectKeepFirst(elSailLine);
    clearSelectKeepFirst(elAttempt);

    Array.from(vesselMap.entries())
      .sort((a, b) => (a[1] || "").localeCompare(b[1] || ""))
      .forEach(([id, name]) => {
        elVessel.appendChild(new Option(name, id));
      });

    Array.from(purposeSet)
      .sort((a, b) => String(a).localeCompare(String(b)))
      .forEach(v => elPurpose.appendChild(new Option(v, v)));

    Array.from(sailSet)
      .sort((a, b) => String(a).localeCompare(String(b), undefined, { numeric: true }))
      .forEach(v => elSailLine.appendChild(new Option(v, v)));

    Array.from(attemptSet)
      .sort((a, b) => Number(a) - Number(b))
      .forEach(v => elAttempt.appendChild(new Option(v, v)));

    if ([...elVessel.options].some(o => o.value === keepVessel)) elVessel.value = keepVessel;
    if ([...elPurpose.options].some(o => o.value === keepPurpose)) elPurpose.value = keepPurpose;
    if ([...elSailLine.options].some(o => o.value === keepSailLine)) elSailLine.value = keepSailLine;
    if ([...elAttempt.options].some(o => o.value === keepAttempt)) elAttempt.value = keepAttempt;
  }

  function setCount(shown, total) {
    if (countEl) {
      countEl.textContent = `${shown} / ${total} rows`;
    }
  }

  function applyCheckedOnlyFilter() {
    const rows = getRows();
    let shown = 0;

    for (const tr of rows) {
      let ok = true;

      if (elOnlyChk.checked) {
        const cb = tr.querySelector("input.row-check[type='checkbox']");
        ok = !!(cb && cb.checked);
      }

      tr.classList.toggle("d-none", !ok);
      if (ok) shown++;
    }

    setCount(shown, rows.length);

    const currentTbody = getTbody();
    if (currentTbody) {
      currentTbody.dispatchEvent(new CustomEvent("sps:filtered", { bubbles: true }));
    }
  }

  function buildParams() {
    const params = new URLSearchParams();

    const put = (key, value) => {
      const s = (value ?? "").toString().trim();
      if (s !== "") params.set(key, s);
    };

    put("search", elSearch.value);
    put("vessel", elVessel.value);
    put("purpose", elPurpose.value);
    put("sailline", elSailLine.value);
    put("attempt", elAttempt.value);

    put("line_from", elLineFrom.value);
    put("line_to", elLineTo.value);
    put("seq_from", elSeqFrom.value);
    put("seq_to", elSeqTo.value);

    put("wd", elWD.value);
    put("gd", elGD.value);

    if (elSortBy) put("sort_by", elSortBy.value || "seq");
    if (elSortDir) put("sort_dir", elSortDir.value || "asc");

    return params;
  }

  async function loadFromBackend({ closeModalAfterSuccess = false } = {}) {
    if (isLoading) return false;

    const currentTbody = getTbody();
    if (!currentTbody) return false;

    const params = buildParams();
    const url = `${endpoint}?${params.toString()}`;

    try {
      setLoading(true);

      const resp = await fetch(url, {
        method: "GET",
        headers: {
          "X-Requested-With": "XMLHttpRequest"
        }
      });

      let data = null;
      try {
        data = await resp.json();
      } catch (e) {
        console.error("Invalid JSON response:", e);
        return false;
      }

      if (!resp.ok || !data?.ok) {
        console.error("SPS table load failed:", data?.error || resp.statusText);
        return false;
      }

      const freshTbody = getTbody();
      if (!freshTbody) return false;

      freshTbody.innerHTML = data.sps_summary || "";

      buildOptionsFromRows();
      applyCheckedOnlyFilter();

      if (!elOnlyChk.checked && typeof data.count === "number") {
        setCount(data.count, data.count);
      }

      if (closeModalAfterSuccess && modalInstance) {
        modalInstance.hide();
      }

      return true;
    } catch (err) {
      console.error("Failed to reload SPS table:", err);
      return false;
    } finally {
      setLoading(false);
    }
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

    if (elSortBy) elSortBy.value = "seq";
    if (elSortDir) elSortDir.value = "asc";

    elOnlyChk.checked = false;
  }

  btnApply.addEventListener("click", async (e) => {
    if (isLoading) return;
    e.preventDefault();
    await loadFromBackend({ closeModalAfterSuccess: true });
  });

  btnReset.addEventListener("click", async (e) => {
    if (isLoading) return;
    e.preventDefault();
    resetFilters();
    await loadFromBackend({ closeModalAfterSuccess: true });
  });

  if (quickClear) {
    quickClear.addEventListener("click", async (e) => {
      if (isLoading) return;
      e.preventDefault();
      resetFilters();
      await loadFromBackend({ closeModalAfterSuccess: false });
    });
  }

  elOnlyChk.addEventListener("change", () => {
    if (isLoading) return;
    applyCheckedOnlyFilter();
  });

  document.addEventListener("change", (e) => {
    const currentTbody = getTbody();
    if (!currentTbody) return;
    if (!currentTbody.contains(e.target)) return;

    if (e.target.matches("input.row-check[type='checkbox']")) {
      if (elOnlyChk.checked && !isLoading) {
        applyCheckedOnlyFilter();
      }
    }
  });

  buildOptionsFromRows();
  applyCheckedOnlyFilter();
  ensureTableLoader();
  ensureModalLoader();
}