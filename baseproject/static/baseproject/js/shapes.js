import { getCSRFToken } from "./csrf.js";
import { renderBokehInto } from "./renderBokeh.js";
import { showAppToast } from "./toast.js";

export function initMainShapeCheckBox() {
  const main = document.getElementById("mainshapefileCheckbox");
  const tbody = document.getElementById("shape-folder-body");
  if (!main || !tbody) return;

  main.addEventListener("change", () => {
    tbody.querySelectorAll(".shape-checkbox").forEach(cb => {
      cb.checked = main.checked;
    });
  });

  tbody.addEventListener("change", (e) => {
    if (!e.target.classList.contains("shape-checkbox")) return;
    const boxes = tbody.querySelectorAll(".shape-checkbox");
    const checked = tbody.querySelectorAll(".shape-checkbox:checked");
    main.checked = boxes.length > 0 && checked.length === boxes.length;
  });
}

export function initAddShapeButton() {
  const btn = document.getElementById("btnAddShapes");
  const tbody = document.getElementById("shape-folder-body");
  const prjShapesBody = document.getElementById("prj-shp-body");
  if (!btn) return;

  btn.addEventListener("click", async () => {
    const checked = Array.from(document.querySelectorAll(".shape-checkbox:checked"));
    const fullNames = checked.map(cb => cb.dataset.fullname).filter(Boolean);

    if (fullNames.length === 0) {
      showAppToast("Select at least one shape.", { title: "Nothing selected", variant: "warning" });
      return;
    }

    const url = btn.dataset.postUrl;

    try {
      btn.disabled = true;

      const resp = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCSRFToken(),
        },
        body: JSON.stringify({ full_names: fullNames }),
      });

      const data = await resp.json();

      if (!resp.ok) {
        showAppToast(data.error || "Failed to add shapes.", { title: "Import failed", variant: "danger" });
        return;
      }
      if (data.preplot_map) renderBokehInto("preplot-map-div", data.preplot_map);
      tbody.innerHTML = data.shapes_in_folder;
      prjShapesBody.innerHTML = data.prj_shp_body;
      checked.forEach(cb => (cb.checked = false));

      showAppToast(`${fullNames.length} shape(s) added to project.`, { title: "Shapes loaded", variant: "success" });
    } catch (err) {
      console.error(err);
      showAppToast("Network error while adding shapes.", { title: "Import failed", variant: "danger" });
    } finally {
      btn.disabled = false;
    }
  });

  const main = document.getElementById("mainshapefileCheckbox");
  if (main) {
    main.addEventListener("change", () => {
      document.querySelectorAll(".shape-checkbox").forEach(cb => (cb.checked = main.checked));
    });
  }
}

export function initProjectShapesAutoSave() {
  const table = document.getElementById("shp-in-db-table");
  if (!table) return;

  const saveUrl = table.dataset.saveUrl;
  if (!saveUrl) {
    console.warn("Missing data-save-url on #shp-in-db-table");
    return;
  }

  const timers = new Map();

  function scheduleRowSave(tr) {
    const key = tr.dataset.fullname || "";
    if (!key) return;

    if (timers.has(key)) clearTimeout(timers.get(key));

    timers.set(key, setTimeout(() => saveRow(tr).catch(console.error), 400));
  }

  async function saveRow(tr) {
    const fullName = tr.dataset.fullname || tr.querySelector('.form-check-input[type="checkbox"]')?.value;
    if (!fullName) return;

    const isFilled = tr.querySelector(".is_shape_fill")?.checked ? 1 : 0;
    const fillColor = tr.querySelector(".fill_color")?.value || "#000000";
    const lineColor = tr.querySelector(".line_color")?.value || "#000000";
    const lineWidthRaw = tr.querySelector(".line_width")?.value ?? "1";
    const hatchpattern = tr.querySelector(".hatch-pattern")?.value || "";
    const linedashed = tr.querySelector(".line-dashed")?.value || "";

    let lineWidth = parseInt(lineWidthRaw, 10);
    if (Number.isNaN(lineWidth)) lineWidth = 1;
    if (lineWidth < 1) lineWidth = 1;
    if (lineWidth > 10) lineWidth = 10;

    const payload = {
      full_name: fullName,
      is_filled: isFilled,
      fill_color: fillColor,
      line_color: lineColor,
      line_width: lineWidth,
      hatch_pattern: hatchpattern,
      line_dashed: linedashed,
    };

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
    if (data.preplot_map) renderBokehInto("preplot-map-div", data.preplot_map);
    tr.classList.remove("table-danger");
    tr.classList.add("table-success");
    setTimeout(() => tr.classList.remove("table-success"), 600);
  }

  table.addEventListener("change", (e) => {
    const target = e.target;
    if (!(target instanceof HTMLElement)) return;

    if (
      target.classList.contains("is_shape_fill") ||
      target.classList.contains("fill_color") ||
      target.classList.contains("line_color") ||
      target.classList.contains("line_width") ||
      target.classList.contains("hatch-pattern") ||
      target.classList.contains("line-dashed")
    ) {
      const tr = target.closest("tr");
      if (tr) scheduleRowSave(tr);
    }
  });

  table.addEventListener("input", (e) => {
    const target = e.target;
    if (!(target instanceof HTMLElement)) return;

    if (target.classList.contains("line_width")) {
      const tr = target.closest("tr");
      if (tr) scheduleRowSave(tr);
    }
  });
}
