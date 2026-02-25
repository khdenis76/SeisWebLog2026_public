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

  const parseNumberOrNull = (v) => {
    if (v === null || v === undefined) return null;
    const s = String(v).trim();
    if (!s) return null;
    const n = Number(s);
    return Number.isNaN(n) ? null : n;
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

  // -------------------------
  // Tri-state helpers
  // -------------------------
  const getTriValue = (key) => {
    const group = modalEl.querySelector(`.dsr-tri[data-filter="${key}"]`);
    if (!group) return "any";
    const active = group.querySelector("button.active");
    return active ? active.dataset.value : "any";
  };

  const triMatch = (rowVal, triVal) => {
    // triVal: "any" | "1" | "0"
    if (triVal === "any") return true;
    return String(rowVal ?? "") === String(triVal);
  };

  const initTriButtons = () => {
    modalEl.querySelectorAll(".dsr-tri").forEach((group) => {
      const buttons = Array.from(group.querySelectorAll("button"));
      buttons.forEach((btn) => {
        btn.addEventListener("click", () => {
          buttons.forEach((b) => b.classList.remove("active"));
          btn.classList.add("active");
        });
      });
    });
  };

  const resetTriButtons = () => {
    modalEl.querySelectorAll(".dsr-tri").forEach((group) => {
      const buttons = Array.from(group.querySelectorAll("button"));
      const anyBtn = group.querySelector('button[data-value="any"]');
      buttons.forEach((b) => b.classList.remove("active"));
      if (anyBtn) anyBtn.classList.add("active");
    });
  };

  // -------------------------
  // Line dropdown fill
  // -------------------------
  const getUniqueLinesSorted = () => {
    const set = new Set();
    getRows().forEach((tr) => {
      const v = Number(tr.dataset.line);
      if (!Number.isNaN(v)) set.add(v);
    });
    return Array.from(set).sort((a, b) => a - b);
  };

  const fillLineSelects = () => {
    if (!lineMinSelEl || !lineMaxSelEl) return;

    const lines = getUniqueLinesSorted();

    const build = (sel) => {
      sel.innerHTML = `<option value="">— select —</option>`;
      lines.forEach((ln) => {
        const opt = document.createElement("option");
        opt.value = String(ln);
        opt.textContent = String(ln);
        sel.appendChild(opt);
      });
    };

    build(lineMinSelEl);
    build(lineMaxSelEl);
  };

  // Select -> autofill input (nice UX)
  if (lineMinSelEl && lineMinEl) {
    lineMinSelEl.addEventListener("change", () => {
      if (lineMinSelEl.value !== "") lineMinEl.value = lineMinSelEl.value;
    });
  }
  if (lineMaxSelEl && lineMaxEl) {
    lineMaxSelEl.addEventListener("change", () => {
      if (lineMaxSelEl.value !== "") lineMaxEl.value = lineMaxSelEl.value;
    });
  }

  const applyFilters = () => {
    const day = (dayEl && dayEl.value ? dayEl.value.trim() : "");
    const tFrom = hhmmToMinutes(tFromEl && tFromEl.value ? tFromEl.value : "");
    const tTo = hhmmToMinutes(tToEl && tToEl.value ? tToEl.value : "");

    // priority: manual input > select
    const lineMin = parseNumberOrNull(lineMinEl?.value) ?? parseNumberOrNull(lineMinSelEl?.value);
    const lineMax = parseNumberOrNull(lineMaxEl?.value) ?? parseNumberOrNull(lineMaxSelEl?.value);

    // tri-state filters
    const fDeployed = getTriValue("deployed");
    const fRecovered = getTriValue("recovered");
    const fSm = getTriValue("sm");
    const fSmr = getTriValue("smr");

    getRows().forEach((tr) => {
      const line = Number(tr.dataset.line);
      const rDay = (tr.dataset.day || "").trim();
      const depStart = (tr.dataset.depstart || "").trim();
      const depStartMin = timestampToMinutes(depStart);

      // IMPORTANT: these must exist on <tr> as dataset attrs
      // data-completely-deployed="1/0"
      // data-completely-recovered="1/0"
      // data-sm-loaded="1/0"
      // data-smr-loaded="1/0"
      const deployed = tr.dataset.completelyDeployed;
      const recovered = tr.dataset.completelyRecovered;
      const smLoaded = tr.dataset.smLoaded;
      const smrLoaded = tr.dataset.smrLoaded;

      let ok = true;

      // status filters (AND)
      ok = ok && triMatch(deployed, fDeployed);
      ok = ok && triMatch(recovered, fRecovered);
      ok = ok && triMatch(smLoaded, fSm);
      ok = ok && triMatch(smrLoaded, fSmr);

      // day
      if (day) ok = ok && rDay === day;

      // time range
      if (tFrom !== null || tTo !== null) {
        if (depStartMin === null) {
          ok = false;
        } else {
          if (tFrom !== null) ok = ok && depStartMin >= tFrom;
          if (tTo !== null) ok = ok && depStartMin <= tTo;
        }
      }

      // line range
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

    resetTriButtons();
    getRows().forEach((tr) => tr.classList.remove("d-none"));
  };

  const modal = bootstrap.Modal.getOrCreateInstance(modalEl, { backdrop: true, keyboard: true });

  // init UI once
  initTriButtons();
  resetTriButtons();
  fillLineSelects();

  // open modal (do NOT rely on data-bs-toggle to avoid "black canvas only" bug)
  openBtn.addEventListener("click", () => {
    hardResetModals();
    // in case tbody changed since last time
    fillLineSelects();
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