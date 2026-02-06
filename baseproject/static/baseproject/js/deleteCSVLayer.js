import { getCSRFToken } from "./csrf.js";

export function initDeleteLayersBtn() {
  const btn = document.getElementById("deleteLayersBtn");
  const tbody = document.getElementById("layersTbody");
  if (!btn || !tbody) return;

  btn.addEventListener("click", async () => {
    const checkboxes = tbody.querySelectorAll(".layers-checkbox:checked");
    if (checkboxes.length === 0) {
      alert("Select layers to delete");
      return;
    }

    const ids = Array.from(checkboxes).map(cb => cb.dataset.layerid);

    if (!confirm(`Delete ${ids.length} layer(s)?`)) return;

    const url = btn.dataset.deleteUrl;

    try {
      btn.disabled = true;

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
        alert(data.error || "Delete failed");
        return;
      }

      // ✅ remove rows from table
      ids.forEach(id => {
        const row = tbody.querySelector(`tr[data-id="${id}"]`);
        if (row) row.remove();
      });

      // optional: if table becomes empty, show "No data"
      if (tbody.querySelectorAll("tr").length === 0) {
        tbody.innerHTML = `
          <tr class="table-warning">
            <td colspan="6">No data</td>
          </tr>
        `;
      }

    } catch (err) {
      console.error(err);
      alert("Network error");
    } finally {
      btn.disabled = false;
    }
  });
}
