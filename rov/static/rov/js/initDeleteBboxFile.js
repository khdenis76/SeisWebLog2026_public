import { getCSRFToken } from "../../baseproject/js/csrf.js";
import { initBBoxFileTable } from "./initBBoxFileTable.js";

export function initDeleteBBoxFiles() {
  const btn = document.getElementById("btn-delete-bbox-files");
  const confirmBtn = document.getElementById("confirm-delete-bbox-files");
  const modalEl = document.getElementById("deleteBBoxFilesModal");
  const tbody = document.getElementById("bbox-list-tbody");
  const countText = document.getElementById("delete-bbox-files-count-text");

  if (!btn || !confirmBtn || !modalEl || !tbody) return;

  const modal = bootstrap.Modal.getOrCreateInstance(modalEl);

  function getSelectedIds() {
    return Array.from(tbody.querySelectorAll(".bbox-file-checkbox:checked"))
      .map(cb => parseInt(cb.value, 10))
      .filter(v => !Number.isNaN(v));
  }

  function showToast(message, type = "success") {
    if (window.showToast) {
      window.showToast(message, type);
    } else {
      console.log(type, message);
    }
  }

  btn.addEventListener("click", () => {
    const ids = getSelectedIds();
    if (!ids.length) {
      showToast("No BBOX files selected.", "warning");
      return;
    }
    countText.textContent = `Selected: ${ids.length} file(s).`;
    modal.show();
  });

  confirmBtn.addEventListener("click", async () => {
    const ids = getSelectedIds();
    const url = btn.dataset.url;

    if (!ids.length) {
      showToast("No BBOX files selected.", "warning");
      return;
    }
    if (!url) {
      showToast("Delete URL is missing.", "danger");
      return;
    }

    const originalHtml = confirmBtn.innerHTML;
    confirmBtn.disabled = true;
    btn.disabled = true;
    confirmBtn.innerHTML = `<span class="spinner-border spinner-border-sm me-2"></span>Deleting...`;

    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCSRFToken(),
        },
        body: JSON.stringify({ ids }),
      });

      const data = await resp.json();

      if (!resp.ok || !data.ok) {
        throw new Error(data.error || "Failed to delete BBOX files.");
      }

      tbody.innerHTML = data.bbox_file_tbody || "";
      modal.hide();
      initBBoxFileTable();
      showToast(`Deleted ${data.deleted || ids.length} file(s).`, "success");
    } catch (err) {
      showToast(err.message || "Unexpected error.", "danger");
    } finally {
      confirmBtn.disabled = false;
      btn.disabled = false;
      confirmBtn.innerHTML = originalHtml;
    }
  });
}