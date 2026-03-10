import { getCSRFToken } from "./csrf.js";
import { showAppToast } from "./toast.js";

export function initShapeFolderSearchButton() {
  const btn = document.getElementById("shape-folder-search-button");
  const input = document.getElementById("shape-folder-input");
  const tbody = document.getElementById("shape-folder-body");

  if (!btn || !input || !tbody) return;

  btn.addEventListener("click", async () => {
    const folderPath = input.value.trim();
    if (!folderPath) {
      showAppToast("Folder path is empty.", { title: "Shapes folder", variant: "warning" });
      return;
    }

    const url = btn.dataset.searchUrl;

    try {
      btn.disabled = true;

      const resp = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCSRFToken(),
        },
        body: JSON.stringify({ folder: folderPath }),
      });

      const data = await resp.json();

      if (!resp.ok) {
        showAppToast(data.error || "Failed to load shapes.", { title: "Shapes folder", variant: "danger" });
        return;
      }
      tbody.innerHTML = data.shapes_in_folder;
      showAppToast("Folder scanned successfully.", { title: "Shapes folder", variant: "success", delay: 3000 });
    } catch (err) {
      console.error(err);
      showAppToast("Network error while loading shapes.", { title: "Shapes folder", variant: "danger" });
    } finally {
      btn.disabled = false;
    }
  });
}
