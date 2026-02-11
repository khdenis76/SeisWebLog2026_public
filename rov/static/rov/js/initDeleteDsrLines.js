import { getCSRFToken } from "../../baseproject/js/csrf.js"; // adjust path if needed

export function initDeleteDSRLines() {
  const menu = document.querySelector("#dsr-delete-btn + .dropdown-menu");
  if (!menu) return;

  menu.addEventListener("click", async (e) => {
    const btn = e.target.closest(".dropdown-item");
    if (!btn) return;

    e.preventDefault();

    const mode = btn.dataset.deleteMode || "all";

    // collect checked lines
    const lines = Array.from(
      document.querySelectorAll(".dsr-line-checkbox:checked")
    ).map(cb => cb.value);

    if (!lines.length) {
      alert("No lines selected");
      return;
    }

    if (!confirm(`Delete ${lines.length} line(s)?`)) return;

    try {
      const resp = await fetch(btn.dataset.url || "dsr/delete_line", {
        method: "POST",
        headers: {
          "X-CSRFToken": getCSRFToken(),
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          lines: lines,
          mode: mode,
        }),
      });

      const data = await resp.json();
      if (!resp.ok) throw new Error(data.error || "Delete failed");

      alert(data.success || "Deleted");

      // optional: reload table or remove rows from DOM
      window.location.reload();

    } catch (err) {
      console.error(err);
      alert(err.message);
    }
  });
}
