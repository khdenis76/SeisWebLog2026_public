export function initDSRLineRowClick() {
  const table = document.getElementById("dsrLineTable");
  if (!table) return;

  const url = table.dataset.clickUrl;
  if (!url) return;

  table.addEventListener("click", async (e) => {
    // ignore clicks on checkbox / buttons / links / inputs
    if (e.target.closest("input, button, a, select, textarea, label")) return;

    const tr = e.target.closest("tr[data-line]");
    if (!tr) return;

    const line = tr.dataset.line;
    if (!line) return;

    // optional: UI highlight
    table.querySelectorAll("tr.table-active").forEach((r) => r.classList.remove("table-active"));
    tr.classList.add("table-active");

    const form = new FormData();
    form.append("line", line);

    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: { "X-CSRFToken": getCSRFToken() },
        body: form,
      });

      const data = await resp.json();
      if (!resp.ok) throw new Error(data.error || "Click failed");

      console.log("DSR line clicked:", data.line);
    } catch (err) {
      console.error(err);
      alert(err.message);
    }
  });
}
