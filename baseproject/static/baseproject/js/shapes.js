import {getCSRFToken} from "./csrf.js"
export function initShapes(){
    const btnShapeSearch =document.getElementById("shape-folder-search-button")
    const ShapeDirectory = document.getElementById("shape-folder-input")
    const url =  SHP_SEARCH_URL
    btnShapeSearch.addEventListener("click", async () => {
        const shapes_path = ShapeDirectory.value;
        if (!shapes_path) {
                              alert("Input is empty");
                              return;
        }
        try{
              const resp = await fetch(url, {
              method: "POST",
              headers: {
                         "Content-Type": "application/json",
                         "X-CSRFToken": getCSRFToken(),
                          "X-Requested-With": "XMLHttpRequest",
                       },
              body: JSON.stringify({
                                     input_value: shapes_path,
                                  }),
              });
              //=========================Receive data from view ===========================================
               if (!resp.ok) {
                                const text = await resp.text();
                                throw new Error(text || `HTTP ${resp.status}`);
               }


        }catch (err) {
            console.error(err);
        }finally {

        }

    });


}

export function initMainShapeCheckBox() {
  const main = document.getElementById("mainshapefileCheckbox");
  const tbody = document.getElementById("shape-folder-body");
  if (!main || !tbody) return;

  // Select all
  main.addEventListener("change", () => {
    tbody.querySelectorAll(".shape-checkbox").forEach(cb => {
      cb.checked = main.checked;
    });
  });

  // Если вручную снимают/ставят — обновлять главный чекбокс
  tbody.addEventListener("change", (e) => {
    if (!e.target.classList.contains("shape-checkbox")) return;
    const boxes = tbody.querySelectorAll(".shape-checkbox");
    const checked = tbody.querySelectorAll(".shape-checkbox:checked");
    main.checked = boxes.length > 0 && checked.length === boxes.length;
  });
}
export function initAddShapeButton() {
  const btn = document.getElementById("btnAddShapes");
  if (!btn) return;

  btn.addEventListener("click", async () => {
    const checked = Array.from(
      document.querySelectorAll(".shape-checkbox:checked")
    );

    const fullNames = checked
      .map(cb => cb.dataset.fullname)
      .filter(Boolean);

    if (fullNames.length === 0) {
      alert("Select at least one shape.");
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
        alert(data.error || "Failed to add shapes.");
        return;
      }

      // Optional: uncheck added rows
      checked.forEach(cb => (cb.checked = false));

      alert(`Added/updated: ${data.upserted} shape(s)`);

    } catch (err) {
      console.error(err);
      alert("Network error while adding shapes.");
    } finally {
      btn.disabled = false;
    }
  });

  // Optional: "select all" checkbox
  const main = document.getElementById("mainshapefileCheckbox");
  if (main) {
    main.addEventListener("change", () => {
      document
        .querySelectorAll(".shape-checkbox")
        .forEach(cb => (cb.checked = main.checked));
    });
  }
}

export function initProjectShapesAutoSave() {
  const table = document.getElementById("shp-in-db-table");
  if (!table) return;

  const saveUrl = table.dataset.saveUrl; // set in template
  if (!saveUrl) {
    console.warn("Missing data-save-url on #shp-in-db-table");
    return;
  }

  // Debounce per row (avoid spamming server while user clicks/scrolls)
  const timers = new Map();

  function scheduleRowSave(tr) {
    const key = tr.dataset.fullname || "";
    if (!key) return;

    // clear previous timer for this row
    if (timers.has(key)) clearTimeout(timers.get(key));

    timers.set(
      key,
      setTimeout(() => saveRow(tr).catch(console.error), 400) // 400ms debounce
    );
  }

  async function saveRow(tr) {
    const fullName = tr.dataset.fullname || tr.querySelector('.form-check-input[type="checkbox"]')?.value;
    if (!fullName) return;

    const isFilled = tr.querySelector(".is_shape_fill")?.checked ? 1 : 0;
    const fillColor = tr.querySelector(".fill_color")?.value || "#000000";
    const lineColor = tr.querySelector(".line_color")?.value || "#000000";
    const lineWidthRaw = tr.querySelector(".line_width")?.value ?? "1";

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
      target.classList.contains("is_shape_fill") ||
      target.classList.contains("fill_color") ||
      target.classList.contains("line_color") ||
      target.classList.contains("line_width")
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
