export function initDSRLineFilters() {
  const modalEl = document.getElementById("dsrFilterModal");
  const openBtn = document.getElementById("btnOpenDsrFilter");
  if (!modalEl || !openBtn) return;

  const dayEl = document.getElementById("dsrFilterDay");
  const tFromEl = document.getElementById("dsrFilterTimeFrom");
  const tToEl = document.getElementById("dsrFilterTimeTo");

  const lineMinEl = document.getElementById("dsrFilterLineMin");
  const lineMaxEl = document.getElementById("dsrFilterLineMax");
  const lineMinSelEl = document.getElementById("dsrFilterLineMinSelect");
  const lineMaxSelEl = document.getElementById("dsrFilterLineMaxSelect");

  const vesselEl = document.getElementById("dsrFilterVessel");

  const applyBtn = document.getElementById("dsrFilterApplyBtn");
  const resetBtn = document.getElementById("dsrFilterResetBtn");

  const tbodyId = "dsr-line-table-body";

  const getTbody = () => document.getElementById(tbodyId);
  const getRows = () => {
    const tb = getTbody();
    if (!tb) return [];
    return Array.from(tb.querySelectorAll("tr[data-line]"));
  };

  const parseNumberOrNull = (v) => {
    if (!v) return null;
    const n = Number(v);
    return Number.isNaN(n) ? null : n;
  };

  const hhmmToMinutes = (hhmm) => {
    if (!hhmm) return null;
    const [h, m] = hhmm.split(":").map(Number);
    if (Number.isNaN(h) || Number.isNaN(m)) return null;
    return h * 60 + m;
  };

  const timestampToMinutes = (ts) => {
    if (!ts) return null;
    const s = String(ts).trim();
    if (!s) return null;
    const parts = s.includes("T") ? s.split("T") : s.split(" ");
    if (parts.length < 2) return null;
    const time = parts[1].slice(0, 5);
    return hhmmToMinutes(time);
  };

  // -------------------------
  // Fill vessel dropdown dynamically
  // -------------------------
  const fillVesselSelect = () => {
    if (!vesselEl) return;

    const set = new Set();
    getRows().forEach((tr) => {
      const v = (tr.dataset.vessel || "").trim();
      if (v) set.add(v);
    });

    const vessels = Array.from(set).sort((a, b) =>
      a.localeCompare(b)
    );

    vesselEl.innerHTML = `<option value="">— any vessel —</option>`;

    vessels.forEach((v) => {
      const opt = document.createElement("option");
      opt.value = v;
      opt.textContent = v;
      vesselEl.appendChild(opt);
    });
  };

  const applyFilters = () => {
    const day = dayEl?.value?.trim() || "";
    const tFrom = hhmmToMinutes(tFromEl?.value);
    const tTo = hhmmToMinutes(tToEl?.value);

    const lineMin =
      parseNumberOrNull(lineMinEl?.value) ??
      parseNumberOrNull(lineMinSelEl?.value);

    const lineMax =
      parseNumberOrNull(lineMaxEl?.value) ??
      parseNumberOrNull(lineMaxSelEl?.value);

    const vesselFilter = vesselEl?.value?.trim().toLowerCase() || "";

    getRows().forEach((tr) => {
      const line = Number(tr.dataset.line);
      const rDay = (tr.dataset.day || "").trim();
      const depStart = (tr.dataset.depstart || "").trim();
      const depStartMin = timestampToMinutes(depStart);
      const vessel = (tr.dataset.vessel || "").trim().toLowerCase();

      let ok = true;

      // Vessel filter
      if (vesselFilter) {
        ok = ok && vessel === vesselFilter;
      }

      // Day
      if (day) ok = ok && rDay === day;

      // Time
      if (tFrom !== null || tTo !== null) {
        if (depStartMin === null) {
          ok = false;
        } else {
          if (tFrom !== null) ok = ok && depStartMin >= tFrom;
          if (tTo !== null) ok = ok && depStartMin <= tTo;
        }
      }

      // Line range
      if (lineMin !== null) ok = ok && line >= lineMin;
      if (lineMax !== null) ok = ok && line <= lineMax;

      tr.classList.toggle("d-none", !ok);
    });
  };

  const resetFilters = () => {
    if (dayEl) dayEl.value = "";
    if (tFromEl) tFromEl.value = "";
    if (tToEl) tToEl.value = "";

    if (lineMinEl) lineMinEl.value = "";
    if (lineMaxEl) lineMaxEl.value = "";
    if (lineMinSelEl) lineMinSelEl.value = "";
    if (lineMaxSelEl) lineMaxSelEl.value = "";

    if (vesselEl) vesselEl.value = "";

    getRows().forEach((tr) => tr.classList.remove("d-none"));
  };

  const modal = bootstrap.Modal.getOrCreateInstance(modalEl);

  openBtn.addEventListener("click", () => {
    fillVesselSelect();
    modal.show();
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
}