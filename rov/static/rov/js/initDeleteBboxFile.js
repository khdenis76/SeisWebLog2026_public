import { getCSRFToken } from "../../baseproject/js/csrf.js";
import { showToast } from "./toast.js";
import { showConfirmModal } from "./confirmModal.js";

export function initDeleteBboxFiles() {
  const btn = document.getElementById("deleteBboxFileBtn");
  const tbody = document.getElementById("bbox-list-tbody");
  if (!btn) return;

  btn.addEventListener("click", async () => {
    const checked = document.querySelectorAll(".bbox-file-checkbox:checked");

    if (!checked.length) {
      showToast({
        title: "Delete BBOX files",
        message: "No BBOX files selected.",
        type: "warning",
      });
      return;
    }

    const ids = Array.from(checked).map((cb) => cb.value);
    const confirmed = await showConfirmModal({
      title: "Delete BBOX files",
      message: `Delete ${ids.length} BBOX file(s)? This cannot be undone.`,
      confirmText: "Delete",
      cancelText: "Cancel",
      confirmClass: "btn-danger",
    });
    if (!confirmed) return;

    const url = btn.dataset.deleteUrl;

    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCSRFToken(),
        },
        body: JSON.stringify({ ids }),
      });

      const data = await resp.json().catch(() => ({}));
      if (!resp.ok || !data.ok) {
        throw new Error(data.error || "Delete failed");
      }

      if (tbody && data.bbox_file_tbody !== undefined) {
        tbody.innerHTML = data.bbox_file_tbody;
      }

      showToast({
        title: "Delete BBOX files",
        message: data.toast?.message || `Deleted ${data.deleted || ids.length} file(s).`,
        type: "success",
      });
    } catch (err) {
      console.error(err);
      showToast({
        title: "Delete BBOX files",
        message: err.message || "Delete failed.",
        type: "danger",
      });
    }
  });
}
