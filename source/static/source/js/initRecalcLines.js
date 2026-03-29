import { getCSRFToken } from "../../baseproject/js/csrf.js";

export function initRecalcLines() {
  const btn = document.getElementById("btn-recalc-lines");
  if (!btn) return;

  btn.addEventListener("click", async () => {
    const checked = document.querySelectorAll(
      '#shot-summary-tbody input[type="checkbox"]:checked'
    );

    if (!checked.length) {
      alert("No lines selected");
      return;
    }

    const lines = Array.from(checked).map(cb => cb.value);

    if (!confirm(`Recalculate ${lines.length} line(s)?`)) return;

    btn.disabled = true;

    try {
      const resp = await fetch("/source/api/recalc-lines/", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCSRFToken(),
        },
        body: JSON.stringify({ lines }),
      });

      const data = await resp.json();

      if (!resp.ok || !data.ok) {
        throw new Error(data.error || "Recalc failed");
      }

      // update table
      if (data.tbody_html) {
        const tbody = document.getElementById("shot-summary-tbody");
        if (tbody) tbody.innerHTML = data.tbody_html;
      }

      showToast(`Recalculated ${lines.length} line(s)`, "success");

    } catch (err) {
      showToast(err.message || "Error", "danger");
    } finally {
      btn.disabled = false;
    }
  });
}

function showToast(text, type = "success") {
  const el = document.createElement("div");
  el.className = `toast align-items-center text-bg-${type} border-0 position-fixed bottom-0 end-0 m-3`;
  el.innerHTML = `
    <div class="d-flex">
      <div class="toast-body">${text}</div>
      <button type="button" class="btn-close btn-close-white me-2 m-auto"></button>
    </div>
  `;
  document.body.appendChild(el);

  const toast = new bootstrap.Toast(el);
  toast.show();

  el.querySelector(".btn-close").onclick = () => el.remove();
}