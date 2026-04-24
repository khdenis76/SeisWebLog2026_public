import { getCSRFToken } from "../../baseproject/js/csrf.js";
import { initBBoxFileTable } from "./initBBoxFileTable.js";

export function initBBoxFilter() {
  const btn = document.getElementById("bbox-filter-button");
  const modalEl = document.getElementById("bboxFilterModal");
  const applyBtn = document.getElementById("bbox-filter-apply-btn");
  const resetBtn = document.getElementById("bbox-filter-reset-btn");
  const tbody = document.getElementById("bbox-list-tbody");

  const vesselInput = document.getElementById("bbox-filter-vessel");
  const startDayInput = document.getElementById("bbox-filter-start-day");
  const endDayInput = document.getElementById("bbox-filter-end-day");

  if (!btn || !modalEl || !applyBtn || !resetBtn || !tbody) {
    return;
  }

  const modal = bootstrap.Modal.getOrCreateInstance(modalEl);

  function showToast(message, type = "success") {
    if (window.showToast) {
      window.showToast(message, type);
    } else {
      console.log(type, message);
    }
  }

  function saveState() {
    const state = {
      vessel: vesselInput?.value || "",
      start_day: startDayInput?.value || "",
      end_day: endDayInput?.value || "",
    };
    sessionStorage.setItem("bboxFilterState", JSON.stringify(state));
  }

  function loadState() {
    try {
      const raw = sessionStorage.getItem("bboxFilterState");
      if (!raw) return;
      const state = JSON.parse(raw);

      if (vesselInput) vesselInput.value = state.vessel || "";
      if (startDayInput) startDayInput.value = state.start_day || "";
      if (endDayInput) endDayInput.value = state.end_day || "";
    } catch (err) {
      console.warn("Failed to restore bbox filter state", err);
    }
  }

  async function applyFilter(payload) {
    const url = btn.dataset.url;
    if (!url) {
      showToast("BBOX filter URL is missing.", "danger");
      return;
    }

    applyBtn.disabled = true;
    const originalHtml = applyBtn.innerHTML;
    applyBtn.innerHTML = `<span class="spinner-border spinner-border-sm me-2"></span>Applying...`;

    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCSRFToken(),
          "X-Requested-With": "XMLHttpRequest",
        },
        body: JSON.stringify(payload),
      });

      const data = await resp.json();

      if (!resp.ok || !data.ok) {
        throw new Error(data.error || "Failed to filter BBOX files.");
      }

      tbody.innerHTML = data.bbox_file_tbody || "";
      initBBoxFileTable();
      saveState();
      modal.hide();
      showToast("BBOX filter applied.", "success");
    } catch (err) {
      showToast(err.message || "Unexpected error.", "danger");
    } finally {
      applyBtn.disabled = false;
      applyBtn.innerHTML = originalHtml;
    }
  }

  btn.addEventListener("click", () => {
    loadState();
    modal.show();
  });

  applyBtn.addEventListener("click", async () => {
    const payload = {
      vessel: vesselInput?.value || "",
      start_day: startDayInput?.value || "",
      end_day: endDayInput?.value || "",
    };
    await applyFilter(payload);
  });

  resetBtn.addEventListener("click", async () => {
    if (vesselInput) vesselInput.value = "";
    if (startDayInput) startDayInput.value = "";
    if (endDayInput) endDayInput.value = "";

    sessionStorage.removeItem("bboxFilterState");

    await applyFilter({
      vessel: "",
      start_day: "",
      end_day: "",
    });
  });
}