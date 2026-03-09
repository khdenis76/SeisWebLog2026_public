import { getCSRFToken } from "../../baseproject/js/csrf.js";
import { showToast } from "./toast.js";
import { showConfirmModal } from "./confirmModal.js";

export function initDeleteDSRLines() {
  const menu = document.querySelector("#dsr-delete-btn + .dropdown-menu");
  if (!menu) return;

  menu.addEventListener("click", async (e) => {
    const btn = e.target.closest(".dropdown-item");
    if (!btn) return;

    e.preventDefault();

    const mode = btn.dataset.deleteMode || "all";
    const modeLabelMap = {
      all: "all DSR data",
      recdb: "REC_DB records",
      sm: "Survey Manager fields",
    };

    const lines = Array.from(document.querySelectorAll(".dsr-line-checkbox:checked")).map(
      (cb) => cb.value
    );

    if (!lines.length) {
      showToast({
        title: "Delete DSR lines",
        message: "No lines selected.",
        type: "warning",
      });
      return;
    }

    const confirmed = await showConfirmModal({
      title: "Delete DSR lines",
      message: `Apply delete mode "${modeLabelMap[mode] || mode}" to ${lines.length} line(s)?`,
      confirmText: "Yes",
      cancelText: "No",
      confirmClass: mode === "all" ? "btn-danger" : "btn-warning",
    });
    if (!confirmed) return;

    try {
      const resp = await fetch(btn.dataset.url || "dsr/delete_line", {
        method: "POST",
        headers: {
          "X-CSRFToken": getCSRFToken(),
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ lines, mode }),
      });

      const data = await resp.json().catch(() => ({}));
      if (!resp.ok) throw new Error(data.error || data.message || "Delete failed");

      showToast({
        title: "Delete DSR lines",
        message: data.toast?.message || data.success || "Delete completed.",
        type: "success",
      });

      window.location.reload();
    } catch (err) {
      console.error(err);
      showToast({
        title: "Delete DSR lines",
        message: err.message || "Delete failed.",
        type: "danger",
      });
    }
  });
}
