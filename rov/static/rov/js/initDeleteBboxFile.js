import { getCSRFToken } from "../../baseproject/js/csrf.js"; // adjust path if needed

export function initDeleteBboxFiles() {
  const btn = document.getElementById("deleteBboxFileBtn");
  const tbody =document.getElementById('bbox-list-tbody')
  if (!btn) return;

  btn.addEventListener("click", async () => {
    const checked = document.querySelectorAll(".bbox-file-checkbox:checked");

    if (!checked.length) {
      alert("No BBOX files selected");
      return;
    }

    const ids = Array.from(checked).map(cb => cb.value);

    if (!confirm(`Delete ${ids.length} BBOX file(s)?`)) return;

    const url = btn.dataset.deleteUrl;

    const resp = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRFToken": getCSRFToken(),
      },
      body: JSON.stringify({ ids }),
    });

    const data = await resp.json();

    if (data.ok) {
      tbody.innerHTML=data.bbox_file_tbody
    } else {
      alert(data.error || "Delete failed");
    }
  });
}
