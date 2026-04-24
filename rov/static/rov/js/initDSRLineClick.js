import { getCSRFToken } from "../../baseproject/js/csrf.js";

export function initDsrLineClick(containerId = "dsr-line-table-body") {

  const container = document.getElementById(containerId);
  if (!container) return;

  const url = container.dataset.selectUrl;
  if (!url) {
    console.warn("Missing data-select-url on tbody");
    return;
  }

  container.addEventListener("click", async (e) => {

    // Ignore clicks on checkbox or buttons inside row
    if (e.target.closest("input, button, a")) return;

    const tr = e.target.closest("tr.dsr-line");
    if (!tr || !container.contains(tr)) return;

    // Highlight selected row
    container.querySelectorAll("tr.dsr-line.is-active")
      .forEach(r => r.classList.remove("is-active"));

    tr.classList.add("is-active");

    const payload = {
      line: tr.dataset.line,
      day: tr.dataset.day,
      depstart: tr.dataset.depstart,
      depend: tr.dataset.depend
    };

    try {

      const resp = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCSRFToken()
        },
        body: JSON.stringify(payload)
      });

      const data = await resp.json();
      if (!resp.ok) throw new Error(data.error || "Server error");

      console.log("DSR Line clicked:", data);

      // Example UI update
      //document.getElementById("line-nodes").textContent = data.nodes;
      //document.getElementById("line-stations").textContent = data.stations;
      //document.getElementById("line-days").textContent = data.days;

    } catch (err) {
      console.error("DSR line click error:", err);
    }

  });
}
