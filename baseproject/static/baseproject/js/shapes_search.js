import { getCSRFToken } from "./csrf.js";

export function initShapeFolderSearchButton() {
  const btn = document.getElementById("shape-folder-search-button");
  const input = document.getElementById("shape-folder-input");
  const tbody = document.getElementById("shape-folder-body");

  if (!btn || !input || !tbody) return;

  btn.addEventListener("click", async () => {
    const folderPath = input.value.trim();
    if (!folderPath) {
      alert("Folder path is empty");
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
        alert(data.error || "Failed to load shapes");
        return;
      }

      // 🔥 Обновляем только tbody
      tbody.innerHTML = data.html;

    } catch (err) {
      console.error(err);
      alert("Network error");
    } finally {
      btn.disabled = false;
    }
  });
}
