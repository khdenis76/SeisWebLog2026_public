export function initDSRLineFilters() {
  const modalEl = document.getElementById("dsrFilterModal");
  const openBtn = document.getElementById("btnOpenDsrFilter");
  if (!modalEl || !openBtn) return;

  const dayEl = document.getElementById("dsrFilterDay");
  const tFromEl = document.getElementById("dsrFilterTimeFrom");
  const tToEl = document.getElementById("dsrFilterTimeTo");
  const lineMinEl = document.getElementById("dsrFilterLineMin");
  const lineMaxEl = document.getElementById("dsrFilterLineMax");

  const applyBtn = document.getElementById("dsrFilterApplyBtn");
  const resetBtn = document.getElementById("dsrFilterResetBtn");

  const tbodyId = "dsr-line-table-body";

  const hardResetModals = () => {
    document.querySelectorAll(".modal-backdrop").forEach((b) => b.remove());
    document.body.classList.remove("modal-open");
    document.body.style.removeProperty("padding-right");
  };

  const getTbody = () => document.getElementById(tbodyId);

  const getRows = () => {
    const tb = getTbody();
    if (!tb) return [];
    return Array.from(tb.querySelectorAll("tr[data-line]"));
  };

  const hhmmToMinutes = (hhmm) => {
    if (!hhmm) return null;
    const [h, m] = hhmm.split(":").map(Number);
    if (Number.isNaN(h) || Number.isNaN(m)) return null;
    return h * 60 + m;
  };

  // expects "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DDTHH:MM:SS"
  const timestampToMinutes = (ts) => {
    if (!ts) return null;
    const s = String(ts).trim();
    if (!s) return null;
    const parts = s.includes("T") ? s.split("T") : s.split(" ");
    if (parts.length < 2) return null;
    const time = parts[1].slice(0, 5); // HH:MM
    return hhmmToMinutes(time);
  };

  const applyFilters = () => {
    const day = (dayEl && dayEl.value ? dayEl.value.trim() : "");
    const tFrom = hhmmToMinutes(tFromEl && tFromEl.value ? tFromEl.value : "");
    const tTo = hhmmToMinutes(tToEl && tToEl.value ? tToEl.value : "");

    const lineMin = lineMinEl && lineMinEl.value !== "" ? Number(lineMinEl.value) : null;
    const lineMax = lineMaxEl && lineMaxEl.value !== "" ? Number(lineMaxEl.value) : null;

    getRows().forEach((tr) => {
      const line = Number(tr.dataset.line);
      const rDay = (tr.dataset.day || "").trim();
      const depStart = (tr.dataset.depstart || "").trim();
      const depStartMin = timestampToMinutes(depStart);

      let ok = true;

      if (day) ok = ok && rDay === day;

      if (tFrom !== null || tTo !== null) {
        if (depStartMin === null) {
          ok = false;
        } else {
          if (tFrom !== null) ok = ok && depStartMin >= tFrom;
          if (tTo !== null) ok = ok && depStartMin <= tTo;
        }
      }

      if (lineMin !== null && !Number.isNaN(lineMin)) ok = ok && line >= lineMin;
      if (lineMax !== null && !Number.isNaN(lineMax)) ok = ok && line <= lineMax;

      tr.classList.toggle("d-none", !ok);
    });
  };

  const resetFilters = () => {
    if (dayEl) dayEl.value = "";
    if (tFromEl) tFromEl.value = "";
    if (tToEl) tToEl.value = "";
    if (lineMinEl) lineMinEl.value = "";
    if (lineMaxEl) lineMaxEl.value = "";

    getRows().forEach((tr) => tr.classList.remove("d-none"));
  };

  const modal = bootstrap.Modal.getOrCreateInstance(modalEl, { backdrop: true, keyboard: true });

  // open modal (do NOT rely on data-bs-toggle to avoid "black canvas only" bug)
  openBtn.addEventListener("click", () => {
    hardResetModals();
    modal.show();
  });

  // safety: cleanup after hide
  modalEl.addEventListener("hidden.bs.modal", () => {
    hardResetModals();
  });

  if (applyBtn) {
    applyBtn.addEventListener("click", () => {
      applyFilters();
      modal.hide();
    });
  }

  if (resetBtn) {
    resetBtn.addEventListener("click", () => {
      resetFilters();
    });
  }

  // Apply on Enter inside modal
  modalEl.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      if (applyBtn) applyBtn.click();
    }
  });
}

