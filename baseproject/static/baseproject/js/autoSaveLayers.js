import {getCSRFToken} from "./csrf.js";

export function initProjectLayersAutoSave() {
  const table = document.getElementById("layers-table");
  if (!table) return;

  const saveUrl = table.dataset.saveUrl; // set in template
  if (!saveUrl) {
    console.warn("Missing data-save-url on #layers-table");
    return;
  }

  // Debounce per row (avoid spamming server while user clicks/scrolls)
  const timers = new Map();

  function scheduleRowSave(tr) {
    const key = tr.dataset.id || "";
    if (!key) return;

    // clear previous timer for this row
    if (timers.has(key)) clearTimeout(timers.get(key));

    timers.set(
      key,
      setTimeout(() => saveRow(tr).catch(console.error), 400) // 400ms debounce
    );
  }

  async function saveRow(tr) {
    const layerId = tr.dataset.id;
    if (!layerId) return;
    const pointColor = tr.querySelector(".fill_color")?.value || "#000000";
    const pointSizeRaw = tr.querySelector(".point_size")?.value || 0;
    const pointStyle = tr.querySelector(".point-style-select")?.value || "circle";

    let pointSize = parseInt(pointSizeRaw, 10);

    if (Number.isNaN(pointSize)) pointSize = 1;
    if (pointSize < 1) pointSize = 1;

    const payload = {
      layer_id: layerId,
      point_size: pointSize,
      point_color: pointColor,
      point_style: pointStyle,
      // optional: add line_style later if you have it in UI
      // line_style: "solid",
    };

    // Optional UI: mark row as "saving"
    tr.classList.add("table-warning");

    const resp = await fetch(saveUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRFToken": getCSRFToken(),
      },
      body: JSON.stringify(payload),
    });

    const data = await resp.json();

    tr.classList.remove("table-warning");

    if (!resp.ok) {
      tr.classList.add("table-danger");
      console.error(data);
      return;
    }

    // Optional UI: mark row "saved"
    tr.classList.remove("table-danger");
    tr.classList.add("table-success");
    setTimeout(() => tr.classList.remove("table-success"), 600);
  }

  // Listen to changes from any input inside table
  table.addEventListener("change", (e) => {
    const target = e.target;
    if (!(target instanceof HTMLElement)) return;

    // Only react to inputs we care about
    if (
      target.classList.contains("point-style-select") ||
      target.classList.contains("fill_color") ||
      target.classList.contains("point_size")
    ) {
      const tr = target.closest("tr");
      if (tr) scheduleRowSave(tr);
    }
  });

  // Also save while typing in line width (optional)
  table.addEventListener("input", (e) => {
    const target = e.target;
    if (!(target instanceof HTMLElement)) return;

    if (target.classList.contains("line_width")) {
      const tr = target.closest("tr");
      if (tr) scheduleRowSave(tr);
    }
  });
}