export function initProjectTemplateLoader() {
  const form = document.getElementById("project-template-loader-form");
  if (!form) return;

  const fileInput = document.getElementById("pt-excel-file");
  const sheetSelect = document.getElementById("pt-sheet-select");

  const readSheetsBtn = document.getElementById("pt-read-sheets-btn");
  const readColumnsBtn = document.getElementById("pt-read-columns-btn");
  const saveBtn = document.getElementById("pt-save-btn");

  const headerRowInput = document.getElementById("pt-header-row");
  const startRowInput = document.getElementById("pt-start-row");
  const saveModeSelect = document.getElementById("pt-save-mode");

  const statusEl = document.getElementById("pt-loader-status");
  const previewHead = document.getElementById("pt-preview-head");
  const previewBody = document.getElementById("pt-preview-body");

  const groupsBody = document.getElementById("pt-sl-groups-body");
  const addGroupBtn = document.getElementById("pt-add-sl-group-btn");
  const renumberGroupsBtn = document.getElementById("pt-renumber-sl-groups-btn");

  const confirmModalEl = document.getElementById("ptConfirmReplaceModal");
  const confirmModal = confirmModalEl ? new bootstrap.Modal(confirmModalEl) : null;
  const confirmBtn = document.getElementById("pt-confirm-replace-btn");

  function columnSelects() {
    return document.querySelectorAll(".pt-column-map");
  }

  function csrfToken() {
    const input = form.querySelector("input[name='csrfmiddlewaretoken']");
    return input ? input.value : "";
  }

  function setStatus(message, isError = false) {
    statusEl.textContent = message || "";
    statusEl.classList.toggle("text-danger", isError);
    statusEl.classList.toggle("text-success", !isError && message.includes("Saved"));
    statusEl.classList.toggle("text-muted", !isError && !message.includes("Saved"));
  }

  function getFile() {
    return fileInput.files && fileInput.files.length ? fileInput.files[0] : null;
  }

  function buildFormData() {
    const fd = new FormData(form);
    const file = getFile();
    if (file) fd.set("file", file);
    return fd;
  }

  function setSaveButtonMode() {
    if (!saveBtn || !saveModeSelect) return;

    if (saveModeSelect.value === "replace") {
      saveBtn.classList.remove("btn-success");
      saveBtn.classList.add("btn-danger");
      saveBtn.innerHTML = `<i class="fas fa-trash me-1"></i> Replace DB`;
    } else {
      saveBtn.classList.remove("btn-danger");
      saveBtn.classList.add("btn-success");
      saveBtn.innerHTML = `<i class="fas fa-database me-1"></i> Save DB`;
    }
  }

  function renumberGroups() {
    if (!groupsBody) return;

    groupsBody.querySelectorAll(".pt-sl-group-row").forEach((row, idx) => {
      const groupInput = row.querySelector(".pt-sl-group-no");
      if (groupInput) groupInput.value = idx + 1;
    });
  }

  function makeGroupRow(groupNo = null) {
    const nextGroupNo = groupNo || (groupsBody.querySelectorAll(".pt-sl-group-row").length + 1);

    const tr = document.createElement("tr");
    tr.className = "pt-sl-group-row";

    tr.innerHTML = `
      <td>
        <input type="number"
               class="form-control form-control-sm pt-sl-group-no"
               name="sl_group_no[]"
               value="${nextGroupNo}"
               min="1">
      </td>

      <td>
        <input type="number"
               class="form-control form-control-sm pt-sl-group-start"
               name="sl_group_start[]">
      </td>

      <td>
        <input type="number"
               class="form-control form-control-sm pt-sl-group-end"
               name="sl_group_end[]">
      </td>

      <td>
        <select class="form-select form-select-sm pt-sl-group-direction"
                name="sl_group_direction[]">
          <option value="asc">Ascending</option>
          <option value="desc" selected>Descending</option>
        </select>
      </td>

      <td class="text-end">
        <button type="button"
                class="btn btn-sm btn-outline-danger pt-remove-sl-group-btn">
          <i class="fas fa-trash"></i>
        </button>
      </td>
    `;

    return tr;
  }

  function initGroupButtons() {
    addGroupBtn?.addEventListener("click", () => {
      if (!groupsBody) return;
      groupsBody.appendChild(makeGroupRow());
      renumberGroups();
    });

    renumberGroupsBtn?.addEventListener("click", () => {
      renumberGroups();
    });

    groupsBody?.addEventListener("click", (event) => {
      const btn = event.target.closest(".pt-remove-sl-group-btn");
      if (!btn) return;

      const row = btn.closest(".pt-sl-group-row");
      if (!row) return;

      const rows = groupsBody.querySelectorAll(".pt-sl-group-row");
      if (rows.length <= 1) {
        setStatus("At least one SL group is required.", true);
        return;
      }

      row.remove();
      renumberGroups();
    });
  }

  function autoSelectColumn(selectId, columns, names) {
    const select = document.getElementById(selectId);
    if (!select) return;

    const wanted = names.map(v =>
      String(v).toLowerCase().replaceAll(" ", "").replaceAll("#", "")
    );

    const match = columns.find(col => {
      const label = String(col.label || "")
        .toLowerCase()
        .replaceAll(" ", "")
        .replaceAll("#", "");

      return wanted.includes(label);
    });

    if (match) select.value = match.index;
  }

  function fillColumnSelects(columns) {
    const currentValues = {};
    columnSelects().forEach(select => {
      currentValues[select.id] = select.value;
    });

    const options = [
      `<option value="">---</option>`,
      ...columns.map(col => `<option value="${col.index}">${col.index}: ${col.label}</option>`)
    ].join("");

    columnSelects().forEach(select => {
      select.innerHTML = options;
    });

    autoSelectColumn("pt-col-first-sl", columns, ["Start SL", "FirstSL", "First SL"]);
    autoSelectColumn("pt-col-last-sl", columns, ["End SL", "LastSL", "Last SL"]);
    autoSelectColumn("pt-col-lnum", columns, ["# Lines", "Lines", "LNum"]);
    autoSelectColumn("pt-col-rline", columns, ["Rx", "RX", "RLine", "Receiver Line"]);
    autoSelectColumn("pt-col-tier", columns, ["Tier"]);

    columnSelects().forEach(select => {
      const oldValue = currentValues[select.id];
      if (oldValue && Array.from(select.options).some(opt => opt.value === oldValue)) {
        select.value = oldValue;
      }
    });
  }

  function getSelectedColumnIndexes() {
    return Array.from(columnSelects())
      .map(s => parseInt(s.value || "0", 10))
      .filter(v => v > 0);
  }

  function renderPreview(columns, rows, headerRow, startRow) {
    const selectedCols = getSelectedColumnIndexes();

    previewHead.innerHTML = `
      <tr class="table-primary">
        ${columns.map(col => {
          const selected = selectedCols.includes(col.index) ? "table-success" : "";
          return `<th class="${selected}">${col.index}: ${col.label}</th>`;
        }).join("")}
      </tr>
    `;

    if (!rows || rows.length === 0) {
      previewBody.innerHTML = `
        <tr>
          <td colspan="${columns.length || 1}" class="text-center text-muted py-4">
            No preview rows.
          </td>
        </tr>
      `;
      return;
    }

    previewBody.innerHTML = rows.map(item => {
      const rowClass = item.row_number === headerRow
        ? "table-primary"
        : item.row_number === startRow
          ? "table-warning"
          : "";

      return `
        <tr class="${rowClass}">
          ${columns.map((col, idx) => {
            const selected = selectedCols.includes(col.index) ? "table-success" : "";
            return `<td class="${selected}">${item.values[idx] ?? ""}</td>`;
          }).join("")}
        </tr>
      `;
    }).join("");
  }

  async function readSheets() {
    const file = getFile();

    if (!file) {
      setStatus("Please select Excel file.", true);
      return;
    }

    setStatus("Reading workbook pages...");

    try {
      const response = await fetch(form.dataset.sheetsUrl, {
        method: "POST",
        headers: { "X-CSRFToken": csrfToken() },
        body: buildFormData(),
      });

      const data = await response.json();

      if (!response.ok || !data.ok) {
        throw new Error(data.error || "Failed to read pages.");
      }

      sheetSelect.innerHTML = data.sheets.map(sheet => {
        const selected = sheet.toLowerCase().includes("visual offset") ? "selected" : "";
        return `<option value="${sheet}" ${selected}>${sheet}</option>`;
      }).join("");

      setStatus(`Found ${data.sheets.length} pages.`);
      await readColumns();

    } catch (err) {
      setStatus(err.message, true);
    }
  }

  async function readColumns() {
    const file = getFile();

    if (!file) {
      setStatus("Please select Excel file.", true);
      return;
    }

    if (!sheetSelect.value) {
      setStatus("Please select page / sheet.", true);
      return;
    }

    setStatus("Reading columns...");

    try {
      const response = await fetch(form.dataset.columnsUrl, {
        method: "POST",
        headers: { "X-CSRFToken": csrfToken() },
        body: buildFormData(),
      });

      const data = await response.json();

      if (!response.ok || !data.ok) {
        throw new Error(data.error || "Failed to read columns.");
      }

      fillColumnSelects(data.columns);
      renderPreview(data.columns, data.preview, data.header_row, data.start_row);
      setStatus(`Columns loaded from page: ${data.sheet}`);

    } catch (err) {
      setStatus(err.message, true);
    }
  }

  function validateGroups() {
    const rows = groupsBody?.querySelectorAll(".pt-sl-group-row") || [];

    if (!rows.length) {
      setStatus("At least one SL group is required.", true);
      return false;
    }

    for (const row of rows) {
      const groupNo = row.querySelector(".pt-sl-group-no")?.value;
      const start = row.querySelector(".pt-sl-group-start")?.value;
      const end = row.querySelector(".pt-sl-group-end")?.value;

      if (!groupNo || !start || !end) {
        setStatus("All SL group rows must have group number, start line, and end line.", true);
        return false;
      }
    }

    return true;
  }

  async function doSave() {
    const file = getFile();

    if (!file) {
      setStatus("Please select Excel file.", true);
      return;
    }

    if (!validateGroups()) return;

    setStatus("Saving project template...");

    if (saveBtn) saveBtn.disabled = true;

    try {
      const response = await fetch(form.dataset.saveUrl, {
        method: "POST",
        headers: { "X-CSRFToken": csrfToken() },
        body: buildFormData(),
      });

      const data = await response.json();

      if (!response.ok || !data.ok) {
        throw new Error(data.error || "Failed to save.");
      }

      const skipped = data.skipped_rows ? ` Skipped: ${data.skipped_rows}.` : "";
      const groups = data.groups_saved !== undefined ? ` Groups: ${data.groups_saved}.` : "";
      const mode = data.replace ? "Replaced" : "Saved";

      setStatus(`${mode} ${data.inserted} rows to project_template.${skipped}${groups}`);

      if (window.reloadVisualOffsetTable) {
        window.reloadVisualOffsetTable();
      }

    } catch (err) {
      setStatus(err.message, true);
    } finally {
      if (saveBtn) saveBtn.disabled = false;
    }
  }

  fileInput?.addEventListener("change", readSheets);
  sheetSelect?.addEventListener("change", readColumns);
  readSheetsBtn?.addEventListener("click", readSheets);
  readColumnsBtn?.addEventListener("click", readColumns);

  headerRowInput?.addEventListener("change", async () => {
    const headerRow = parseInt(headerRowInput.value || "1", 10);
    const startRow = parseInt(startRowInput.value || "0", 10);

    if (!startRow || startRow <= headerRow) {
      startRowInput.value = headerRow + 1;
    }

    await readColumns();
  });

  startRowInput?.addEventListener("change", readColumns);

  document.addEventListener("change", (event) => {
    if (!event.target.closest(".pt-column-map")) return;

    const selectedCols = getSelectedColumnIndexes();

    previewHead.querySelectorAll("th").forEach((cell, idx) => {
      cell.classList.toggle("table-success", selectedCols.includes(idx + 1));
    });

    previewBody.querySelectorAll("tr").forEach(row => {
      row.querySelectorAll("td").forEach((cell, idx) => {
        cell.classList.toggle("table-success", selectedCols.includes(idx + 1));
      });
    });
  });

  saveModeSelect?.addEventListener("change", setSaveButtonMode);

  saveBtn?.addEventListener("click", () => {
    const saveMode = saveModeSelect?.value || "append";

    if (saveMode === "replace") {
      if (confirmModal) {
        confirmModal.show();
      } else {
        setStatus("Confirmation modal is missing: ptConfirmReplaceModal", true);
      }
      return;
    }

    doSave();
  });

  confirmBtn?.addEventListener("click", () => {
    if (confirmModal) confirmModal.hide();
    doSave();
  });

  initGroupButtons();
  setSaveButtonMode();
}