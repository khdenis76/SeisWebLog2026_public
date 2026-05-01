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
  const modal = bootstrap.Modal.getOrCreateInstance(modalEl);

  const getTbody = () => document.getElementById(tbodyId);

  const getRows = () => {
    const tb = getTbody();
    if (!tb) return [];
    return Array.from(tb.querySelectorAll("tr[data-line]"));
  };

  const parseNumberOrNull = (v) => {
    if (v === null || v === undefined) return null;
    const s = String(v).trim();
    if (!s) return null;
    const n = Number(s);
    return Number.isNaN(n) ? null : n;
  };

  const hhmmToMinutes = (hhmm) => {
    if (!hhmm) return null;
    const parts = String(hhmm).trim().split(":");
    if (parts.length < 2) return null;

    const h = Number(parts[0]);
    const m = Number(parts[1]);

    if (Number.isNaN(h) || Number.isNaN(m)) return null;
    return h * 60 + m;
  };

  const timestampToMinutes = (ts) => {
    if (!ts) return null;
    const s = String(ts).trim();
    if (!s) return null;

    // supports "YYYY-MM-DD HH:MM:SS" and "YYYY-MM-DDTHH:MM:SS"
    const parts = s.includes("T") ? s.split("T") : s.split(" ");
    if (parts.length < 2) return null;

    const time = parts[1].slice(0, 5);
    return hhmmToMinutes(time);
  };

  const datePart = (ts) => String(ts || "").trim().slice(0, 10);

  const getTriGroup = (name) =>
    modalEl.querySelector(`.dsr-tri[data-filter="${name}"]`);

  const getTriValue = (name) => {
    const group = getTriGroup(name);
    if (!group) return "any";

    const checked = group.querySelector(".btn-check:checked");
    return checked?.value || "any";
  };

  const setTriValue = (name, value = "any") => {
    const group = getTriGroup(name);
    if (!group) return;

    const input = group.querySelector(`.btn-check[value="${value}"]`);
    if (input) input.checked = true;
  };

  const fillVesselSelect = () => {
    if (!vesselEl) return;

    const current = vesselEl.value || "";

    const vessels = Array.from(
      new Set(
        getRows()
          .map((tr) => (tr.dataset.vessel || "").trim())
          .filter(Boolean)
      )
    ).sort((a, b) =>
      a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" })
    );

    vesselEl.innerHTML = `<option value="">— any vessel —</option>`;

    vessels.forEach((v) => {
      const opt = document.createElement("option");
      opt.value = v;
      opt.textContent = v;
      vesselEl.appendChild(opt);
    });

    if (current && vessels.includes(current)) {
      vesselEl.value = current;
    }
  };

  const fillLineSelects = () => {
    const currentMin = lineMinSelEl?.value || "";
    const currentMax = lineMaxSelEl?.value || "";

    const lines = Array.from(
      new Set(
        getRows()
          .map((tr) => parseNumberOrNull(tr.dataset.line))
          .filter((v) => v !== null)
      )
    ).sort((a, b) => a - b);

    const fillOne = (selectEl, currentValue) => {
      if (!selectEl) return;

      selectEl.innerHTML = `<option value="">— select —</option>`;

      lines.forEach((line) => {
        const opt = document.createElement("option");
        opt.value = String(line);
        opt.textContent = String(line);
        selectEl.appendChild(opt);
      });

      if (currentValue && lines.includes(Number(currentValue))) {
        selectEl.value = String(currentValue);
      }
    };

    fillOne(lineMinSelEl, currentMin);
    fillOne(lineMaxSelEl, currentMax);
  };

  const applyFilters = () => {
    const day = (dayEl?.value || "").trim(); // native date input => YYYY-MM-DD
    const tFrom = hhmmToMinutes(tFromEl?.value);
    const tTo = hhmmToMinutes(tToEl?.value);

    const lineMin =
      parseNumberOrNull(lineMinEl?.value) ??
      parseNumberOrNull(lineMinSelEl?.value);

    const lineMax =
      parseNumberOrNull(lineMaxEl?.value) ??
      parseNumberOrNull(lineMaxSelEl?.value);

    const vesselFilter = (vesselEl?.value || "").trim().toLowerCase();

    const deployedFilter = getTriValue("deployed");
    const recoveredFilter = getTriValue("recovered");
    const smFilter = getTriValue("sm");
    const smrFilter = getTriValue("smr");

    getRows().forEach((tr) => {
      const line = parseNumberOrNull(tr.dataset.line);
      const rDay = (tr.dataset.day || "").trim();
      const depStart = (tr.dataset.depstart || "").trim();
      const depStartMin = timestampToMinutes(depStart);
      const vessel = (tr.dataset.vessel || "").trim().toLowerCase();

      const isDeployed = (tr.dataset.completelyDeployed || "").trim().toLowerCase();
      const retrievedCount = Number(tr.dataset.retrievedCount || 0);
      const isRecovered = retrievedCount > 0 ? "1" : "0";
      const isSmLoaded = (tr.dataset.smLoaded || "").trim().toLowerCase();
      const isSmrLoaded = (tr.dataset.smrLoaded || "").trim().toLowerCase();

      let ok = true;

      if (vesselFilter) {
        ok = ok && vessel === vesselFilter;
      }

      if (day) {
        const rowDate = (rDay || "").slice(0, 10);  // "2026-03-08"
        ok = ok && rowDate === day;                 // "2026-03-08"
      }

      if (tFrom !== null || tTo !== null) {
        if (depStartMin === null) {
          ok = false;
        } else {
          if (tFrom !== null) ok = ok && depStartMin >= tFrom;
          if (tTo !== null) ok = ok && depStartMin <= tTo;
        }
      }

      if (lineMin !== null) {
        ok = ok && line !== null && line >= lineMin;
      }

      if (lineMax !== null) {
        ok = ok && line !== null && line <= lineMax;
      }

      if (deployedFilter !== "any") {
        ok = ok && isDeployed === deployedFilter;
      }

      if (recoveredFilter !== "any") {
        ok = ok && isRecovered === recoveredFilter;
      }

      if (smFilter !== "any") {
        ok = ok && isSmLoaded === smFilter;
      }

      if (smrFilter !== "any") {
        ok = ok && isSmrLoaded === smrFilter;
      }

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

    setTriValue("deployed", "any");
    setTriValue("recovered", "any");
    setTriValue("sm", "any");
    setTriValue("smr", "any");

    getRows().forEach((tr) => tr.classList.remove("d-none"));
  };

  const syncSelectToInput = (selectEl, inputEl) => {
    if (!selectEl || !inputEl) return;

    if (selectEl.dataset.boundToInput === "1") return;
    selectEl.dataset.boundToInput = "1";

    selectEl.addEventListener("change", () => {
      inputEl.value = selectEl.value || "";
    });
  };

  if (openBtn.dataset.dsrFilterBound !== "1") {
    openBtn.dataset.dsrFilterBound = "1";

    openBtn.addEventListener("click", () => {
      fillVesselSelect();
      fillLineSelects();
      modal.show();
    });
  }

  if (applyBtn && applyBtn.dataset.dsrFilterBound !== "1") {
    applyBtn.dataset.dsrFilterBound = "1";

    applyBtn.addEventListener("click", () => {
      applyFilters();
      modal.hide();
    });
  }

  if (resetBtn && resetBtn.dataset.dsrFilterBound !== "1") {
    resetBtn.dataset.dsrFilterBound = "1";

    resetBtn.addEventListener("click", () => {
      resetFilters();
    });
  }

  syncSelectToInput(lineMinSelEl, lineMinEl);
  syncSelectToInput(lineMaxSelEl, lineMaxEl);
}