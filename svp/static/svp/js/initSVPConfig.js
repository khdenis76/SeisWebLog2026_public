export function initSVPConfig() {
  const btnOpen = document.getElementById("btnSetupSVPConfig");
  const btnSave = document.getElementById("btnSaveSVPConfig");
  const btnDetect = document.getElementById("btnSVPDetectConfig");
  const btnPreview = document.getElementById("btnPreviewSVPConfig");
  const btnImportJson = document.getElementById("btnImportSVPConfigJson");
  const btnExportJson = document.getElementById("btnExportSVPConfigJson");
  const btnDelete = document.getElementById("btnDeleteSVPConfig");

  const modalEl = document.getElementById("svpConfigModal");
  const form = document.getElementById("svpConfigForm");
  const statusEl = document.getElementById("svpConfigStatus");
  const fileExtEl = document.getElementById("svpCfgFileExt");
  const sampleInput = document.getElementById("svpConfigSampleFile");
  const jsonFileInput = document.getElementById("svpConfigJsonFile");
  const savedConfigSelect = document.getElementById("svpSavedConfigSelect");

  const metaPreview = document.getElementById("svpConfigMetaPreview");
  const colsPreview = document.getElementById("svpConfigColumnsPreview");
  const metaCount = document.getElementById("svpDetectedMetaCount");
  const colCount = document.getElementById("svpDetectedColumnCount");
  const formatLabel = document.getElementById("svpDetectedFormatLabel");

  if (!btnOpen || !modalEl || !form) {
    console.warn("SVP config init skipped: missing modal elements");
    return;
  }

  btnOpen.addEventListener("click", async () => {
    if (typeof bootstrap === "undefined") return;

    clearStatus(statusEl);
    resetPreview(metaPreview, colsPreview, metaCount, colCount, formatLabel);
    if (fileExtEl) applyPreset(fileExtEl.value || ".000", form);
    await loadSavedConfigs(savedConfigSelect, statusEl);
    bootstrap.Modal.getOrCreateInstance(modalEl).show();
  });

  fileExtEl?.addEventListener("change", () => {
    applyPreset(fileExtEl.value, form);
    clearStatus(statusEl);
  });

  savedConfigSelect?.addEventListener("change", async () => {
    const id = savedConfigSelect.value;
    if (!id) return;
    await loadConfigIntoForm(id, form, statusEl);
  });

  btnPreview?.addEventListener("click", async () => {
    if (!sampleInput?.files?.length) {
      setStatus(statusEl, "danger", "Select sample file first.");
      return;
    }

    const fd = new FormData();
    fd.append("file", sampleInput.files[0]);

    setStatus(statusEl, "muted", "Reading sample file...");

    try {
      const response = await fetch("/svp/api/config/preview/", {
        method: "POST",
        body: fd,
        headers: {
          "X-CSRFToken": getCSRFToken(form),
          "X-Requested-With": "XMLHttpRequest",
        },
      });

      const data = await safeJson(response);
      if (!response.ok || !data.success) {
        setStatus(statusEl, "danger", data.error || "Failed to preview file.");
        return;
      }

      const metaKeys = Array.isArray(data.meta_keys) ? data.meta_keys : [];
      const columns = Array.isArray(data.columns) ? data.columns : [];
      const detected = data.detected || {};

      populateSelects(form, metaKeys, columns);
      fillDetectedSetup(form, detected);
      applySmartMatches(form, metaKeys, columns);

      if (metaPreview) metaPreview.textContent = metaKeys.length ? metaKeys.join("\n") : "No header fields detected";
      if (colsPreview) colsPreview.textContent = columns.length ? columns.join("\n") : "No columns detected";
      if (metaCount) metaCount.textContent = String(metaKeys.length);
      if (colCount) colCount.textContent = String(columns.length);
      if (formatLabel) formatLabel.textContent = detected.format_name || "Detected";

      setStatus(statusEl, "success", "Sample parsed successfully.");
    } catch (err) {
      console.error("SVP preview failed:", err);
      setStatus(statusEl, "danger", "Preview failed.");
    }
  });

  btnDetect?.addEventListener("click", () => {
    if (sampleInput?.files?.length) {
      btnPreview?.click();
      return;
    }
    if (fileExtEl) applyPreset(fileExtEl.value || ".000", form);
    setStatus(statusEl, "muted", "Preset applied.");
  });

  btnSave?.addEventListener("click", async () => {
    const formData = new FormData(form);
    btnSave.disabled = true;
    setStatus(statusEl, "muted", "Saving config...");

    try {
      const response = await fetch("/svp/api/config/save/", {
        method: "POST",
        body: formData,
        headers: {
          "X-CSRFToken": getCSRFToken(form),
          "X-Requested-With": "XMLHttpRequest",
        },
      });

      const data = await safeJson(response);
      if (!response.ok || !data.success) {
        setStatus(statusEl, "danger", data.error || "Failed to save config.");
        return;
      }

      setStatus(statusEl, "success", data.message || "Config saved.");
      await loadSavedConfigs(savedConfigSelect, statusEl, data.config_id);
    } catch (err) {
      console.error("SVP config save failed:", err);
      setStatus(statusEl, "danger", "Server error while saving config.");
    } finally {
      btnSave.disabled = false;
    }
  });

  btnImportJson?.addEventListener("click", () => {
    jsonFileInput?.click();
  });

  jsonFileInput?.addEventListener("change", async () => {
    if (!jsonFileInput.files?.length) return;

    const fd = new FormData();
    fd.append("file", jsonFileInput.files[0]);

    setStatus(statusEl, "muted", "Importing JSON config...");

    try {
      const response = await fetch("/svp/api/config/import/", {
        method: "POST",
        body: fd,
        headers: {
          "X-CSRFToken": getCSRFToken(form),
          "X-Requested-With": "XMLHttpRequest",
        },
      });

      const data = await safeJson(response);
      if (!response.ok || !data.success) {
        setStatus(statusEl, "danger", data.error || "Failed to import config.");
        return;
      }

      setStatus(statusEl, "success", data.message || "Config imported.");
      await loadSavedConfigs(savedConfigSelect, statusEl, data.config_id);
      if (data.config_id) {
        await loadConfigIntoForm(data.config_id, form, statusEl);
      }
    } catch (err) {
      console.error("SVP config import failed:", err);
      setStatus(statusEl, "danger", "Import failed.");
    } finally {
      jsonFileInput.value = "";
    }
  });

  btnExportJson?.addEventListener("click", () => {
    const configId = savedConfigSelect?.value;
    if (!configId) {
      setStatus(statusEl, "danger", "Select a saved config first.");
      return;
    }
    window.open(`/svp/api/config/export/${configId}/`, "_blank");
  });

    btnDelete?.addEventListener("click", async () => {
    const configId = savedConfigSelect?.value;
    if (!configId) {
      setStatus(statusEl, "danger", "Select a saved config first.");
      return;
    }

    const ok = window.confirm("Delete selected config?");
    if (!ok) return;

    setStatus(statusEl, "muted", "Deleting config...");

    try {
      const response = await fetch(`/svp/api/config/delete/${configId}/`, {
        method: "POST",
        headers: {
          "X-CSRFToken": getCSRFToken(form),
          "X-Requested-With": "XMLHttpRequest",
        },
      });

      const contentType = response.headers.get("content-type") || "";
      let data = {};

      if (contentType.includes("application/json")) {
        data = await response.json();
      } else {
        const text = await response.text();
        console.error("Delete returned non-JSON:", text);
        setStatus(statusEl, "danger", "Delete endpoint did not return JSON.");
        return;
      }

      console.log("Delete config response:", data);

      if (!response.ok || !data.success) {
        setStatus(statusEl, "danger", data.error || "Failed to delete config.");
        return;
      }

      form.reset();
      resetPreview(metaPreview, colsPreview, metaCount, colCount, formatLabel);
      setStatus(statusEl, "success", data.message || "Config deleted.");
      await loadSavedConfigs(savedConfigSelect, statusEl);
    } catch (err) {
      console.error("SVP config delete failed:", err);
      setStatus(statusEl, "danger", "Delete failed.");
    }
  });
}

async function loadSavedConfigs(selectEl, statusEl, selectedId = null) {
  if (!selectEl) return;

  try {
    const response = await fetch("/svp/api/config/list/", {
      headers: { "X-Requested-With": "XMLHttpRequest" },
    });

    const data = await safeJson(response);
    if (!response.ok || !data.success) {
      setStatus(statusEl, "danger", data.error || "Failed to load saved configs.");
      return;
    }

    const rows = Array.isArray(data.rows) ? data.rows : [];
    selectEl.innerHTML = `<option value="">-- select saved config --</option>`;

    rows.forEach((row) => {
      const opt = document.createElement("option");
      opt.value = row.id;
      opt.textContent = row.name || `Config ${row.id}`;
      selectEl.appendChild(opt);
    });

    if (selectedId) {
      selectEl.value = String(selectedId);
    }
  } catch (err) {
    console.error("Failed to load saved SVP configs:", err);
    setStatus(statusEl, "danger", "Failed to load saved configs.");
  }
}

async function loadConfigIntoForm(configId, form, statusEl) {
  try {
    const response = await fetch(`/svp/api/config/get/${configId}/`, {
      headers: { "X-Requested-With": "XMLHttpRequest" },
    });

    const data = await safeJson(response);
    if (!response.ok || !data.success) {
      setStatus(statusEl, "danger", data.error || "Failed to load config.");
      return;
    }

    const cfg = data.config || {};
    fillFormFromSavedConfig(form, cfg);
    setStatus(statusEl, "success", "Config loaded.");
  } catch (err) {
    console.error("Failed to load config into form:", err);
    setStatus(statusEl, "danger", "Failed to load config.");
  }
}

function fillFormFromSavedConfig(form, cfg) {
  setValue(form, "config_name", cfg.name);
  setValue(form, "file_ext", cfg.file_ext);
  setValue(form, "delimiter", cfg.delimiter);
  setValue(form, "header_line_count", cfg.header_line_count);
  setValue(form, "data_header_line_index", cfg.data_header_line_index);
  setValue(form, "data_start_line_index", cfg.data_start_line_index);

  setValue(form, "meta_coordinates_key", cfg.meta_coordinates_key);
  setValue(form, "meta_lat_key", cfg.meta_lat_key);
  setValue(form, "meta_lon_key", cfg.meta_lon_key);
  setValue(form, "meta_rov_key", cfg.meta_rov_key);
  setValue(form, "meta_timestamp_key", cfg.meta_timestamp_key);
  setValue(form, "meta_name_key", cfg.meta_name_key);
  setValue(form, "meta_location_key", cfg.meta_location_key);
  setValue(form, "meta_serial_key", cfg.meta_serial_key);
  setValue(form, "meta_make_key", cfg.meta_make_key);
  setValue(form, "meta_model_key", cfg.meta_model_key);

  setValue(form, "col_timestamp", cfg.col_timestamp);
  setValue(form, "col_depth", cfg.col_depth);
  setValue(form, "col_velocity", cfg.col_velocity);
  setValue(form, "col_temperature", cfg.col_temperature);
  setValue(form, "col_salinity", cfg.col_salinity);
  setValue(form, "col_density", cfg.col_density);

  setCheckbox(form, "sort_by_depth", cfg.sort_by_depth);
  setCheckbox(form, "clamp_negative_depth_to_zero", cfg.clamp_negative_depth_to_zero);
  setCheckbox(form, "pressure_is_depth", cfg.pressure_is_depth);

  setValue(form, "notes", cfg.notes);
}

function setValue(form, name, value) {
  const el = form.querySelector(`[name="${name}"]`);
  if (!el) return;
  if (el.tagName === "SELECT") {
    ensureSelectHasValue(el, value ?? "");
    el.value = value ?? "";
  } else {
    el.value = value ?? "";
  }
}

function setCheckbox(form, name, value) {
  const el = form.querySelector(`[name="${name}"]`);
  if (el) el.checked = !!value;
}

function applyPreset(ext, form) {
  setValue(form, "config_name", ext === ".000" ? "SVX2 Raw .000" : ext === ".svp" ? "SVX2 Processed .svp" : "Generic CSV SVP");
  setValue(form, "file_ext", ext);

  if (ext === ".000") {
    setValue(form, "delimiter", "\t");
    setValue(form, "header_line_count", 0);
    setValue(form, "data_header_line_index", 0);
    setValue(form, "data_start_line_index", 1);
    setValue(form, "meta_timestamp_key", "Time Stamp");
    setValue(form, "meta_name_key", "File Name");
    setValue(form, "meta_location_key", "Site Information");
    setValue(form, "meta_serial_key", "Serial No.");
    setValue(form, "meta_model_key", "Model Name");
    setValue(form, "col_timestamp", "Date / Time");
    setValue(form, "col_depth", "PRESSURE;M");
    setValue(form, "col_velocity", "Calc. SOUND VELOCITY;M/SEC");
    setValue(form, "col_temperature", "TEMPERATURE;C");
    setValue(form, "col_salinity", "Calc. SALINITY;PSU");
    setValue(form, "col_density", "Calc. DENSITY;KG/M3");
    setCheckbox(form, "sort_by_depth", true);
    setCheckbox(form, "clamp_negative_depth_to_zero", true);
    setCheckbox(form, "pressure_is_depth", true);
    return;
  }

  if (ext === ".svp") {
    setValue(form, "delimiter", ",");
    setValue(form, "header_line_count", 0);
    setValue(form, "data_header_line_index", 0);
    setValue(form, "data_start_line_index", 1);
    setValue(form, "meta_coordinates_key", "Coordinates");
    setValue(form, "meta_lat_key", "Latitude");
    setValue(form, "meta_lon_key", "Longitude");
    setValue(form, "meta_rov_key", "ROV");
    setValue(form, "meta_name_key", "Name");
    setValue(form, "meta_location_key", "Location");
    setValue(form, "meta_serial_key", "Serial");
    setValue(form, "meta_make_key", "Instrument:Make");
    setValue(form, "meta_model_key", "Instrument:Model");
    setValue(form, "col_depth", "Depth:Meter");
    setValue(form, "col_velocity", "Calculated Sound Velocity:m/sec");
    setValue(form, "col_temperature", "Temperature:C");
    setValue(form, "col_salinity", "Salinity:PSU");
    setValue(form, "col_density", "Density:kg/m^3");
    setCheckbox(form, "sort_by_depth", true);
    setCheckbox(form, "clamp_negative_depth_to_zero", false);
    setCheckbox(form, "pressure_is_depth", false);
    return;
  }

  setValue(form, "delimiter", ",");
  setValue(form, "header_line_count", 1);
  setValue(form, "data_header_line_index", 0);
  setValue(form, "data_start_line_index", 1);
  setCheckbox(form, "sort_by_depth", true);
  setCheckbox(form, "clamp_negative_depth_to_zero", false);
  setCheckbox(form, "pressure_is_depth", false);
}

function populateSelects(form, metaKeys, columns) {
  [
    "meta_coordinates_key","meta_lat_key","meta_lon_key","meta_rov_key","meta_timestamp_key",
    "meta_name_key","meta_location_key","meta_serial_key","meta_make_key","meta_model_key"
  ].forEach((name) => fillSelect(form.querySelector(`[name="${name}"]`), metaKeys));

  [
    "col_timestamp","col_depth","col_velocity","col_temperature","col_salinity","col_density"
  ].forEach((name) => fillSelect(form.querySelector(`[name="${name}"]`), columns));
}

function fillDetectedSetup(form, detected) {
  if (!detected || typeof detected !== "object") return;
  Object.entries(detected).forEach(([key, value]) => {
    const el = form.querySelector(`[name="${key}"]`);
    if (!el) return;
    if (el.type === "checkbox") {
      el.checked = !!value;
    } else if (el.tagName === "SELECT") {
      ensureSelectHasValue(el, value ?? "");
      el.value = value ?? "";
    } else {
      el.value = value ?? "";
    }
  });
}

function applySmartMatches(form, metaKeys, columns) {
  setIfEmptySelect(form, "meta_coordinates_key", bestMatch(metaKeys, ["Coordinates", "Coordinate"]));
  setIfEmptySelect(form, "meta_lat_key", bestMatch(metaKeys, ["Latitude", "Lat"]));
  setIfEmptySelect(form, "meta_lon_key", bestMatch(metaKeys, ["Longitude", "Lon"]));
  setIfEmptySelect(form, "meta_rov_key", bestMatch(metaKeys, ["ROV", "ROV Name"]));
  setIfEmptySelect(form, "meta_timestamp_key", bestMatch(metaKeys, ["Time Stamp", "Timestamp"]));
  setIfEmptySelect(form, "meta_name_key", bestMatch(metaKeys, ["Name", "File Name"]));
  setIfEmptySelect(form, "meta_location_key", bestMatch(metaKeys, ["Location", "Site Information"]));
  setIfEmptySelect(form, "meta_serial_key", bestMatch(metaKeys, ["Serial", "Serial No."]));
  setIfEmptySelect(form, "meta_make_key", bestMatch(metaKeys, ["Instrument:Make", "Make"]));
  setIfEmptySelect(form, "meta_model_key", bestMatch(metaKeys, ["Instrument:Model", "Model", "Model Name"]));

  setIfEmptySelect(form, "col_timestamp", bestMatch(columns, ["Date / Time", "Timestamp"]));
  setIfEmptySelect(form, "col_depth", bestMatch(columns, ["Depth:Meter", "PRESSURE;M", "Depth"]));
  setIfEmptySelect(form, "col_velocity", bestMatch(columns, ["Calculated Sound Velocity:m/sec", "Calc. SOUND VELOCITY;M/SEC", "Velocity"]));
  setIfEmptySelect(form, "col_temperature", bestMatch(columns, ["Temperature:C", "TEMPERATURE;C", "Temperature"]));
  setIfEmptySelect(form, "col_salinity", bestMatch(columns, ["Salinity:PSU", "Calc. SALINITY;PSU", "Salinity"]));
  setIfEmptySelect(form, "col_density", bestMatch(columns, ["Density:kg/m^3", "Calc. DENSITY;KG/M3", "Density"]));
}

function setIfEmptySelect(form, name, value) {
  if (!value) return;
  const el = form.querySelector(`[name="${name}"]`);
  if (!el || el.value) return;
  ensureSelectHasValue(el, value);
  el.value = value;
}

function fillSelect(selectEl, values) {
  if (!selectEl) return;
  const currentValue = selectEl.value || "";
  const items = uniqueClean(values);
  selectEl.innerHTML = `<option value="">-- none --</option>` + items.map((v) => `<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`).join("");
  if (currentValue && items.includes(currentValue)) {
    selectEl.value = currentValue;
  }
}

function ensureSelectHasValue(selectEl, value) {
  if (!selectEl || value === undefined || value === null || value === "") return;
  const exists = Array.from(selectEl.options).some((opt) => opt.value === value);
  if (!exists) {
    const opt = document.createElement("option");
    opt.value = value;
    opt.textContent = value;
    selectEl.appendChild(opt);
  }
}

function uniqueClean(values) {
  return [...new Set((values || []).map((v) => String(v || "").trim()).filter(Boolean))];
}

function bestMatch(values, candidates) {
  const clean = uniqueClean(values);
  for (const c of candidates) {
    const exact = clean.find((v) => v === c);
    if (exact) return exact;
  }
  const lower = clean.map((v) => ({ raw: v, low: v.toLowerCase() }));
  for (const c of candidates) {
    const cLow = c.toLowerCase();
    const found = lower.find((v) => v.low === cLow || v.low.includes(cLow) || cLow.includes(v.low));
    if (found) return found.raw;
  }
  return "";
}

async function safeJson(response) {
  const contentType = response.headers.get("content-type") || "";
  if (!contentType.includes("application/json")) {
    const text = await response.text();
    throw new Error(`Non-JSON response: ${text.slice(0, 300)}`);
  }
  return await response.json();
}

function getCSRFToken(form) {
  const input = form.querySelector('input[name="csrfmiddlewaretoken"]');
  if (input) return input.value;
  const match = document.cookie.match(/csrftoken=([^;]+)/);
  return match ? match[1] : "";
}

function setStatus(statusEl, type, message) {
  if (!statusEl) return;
  const cls = type === "success" ? "text-success" : type === "danger" ? "text-danger" : "text-muted";
  statusEl.innerHTML = `<span class="${cls}">${escapeHtml(message)}</span>`;
}

function clearStatus(statusEl) {
  if (statusEl) statusEl.innerHTML = "";
}

function resetPreview(metaPreview, colsPreview, metaCount, colCount, formatLabel) {
  if (metaPreview) metaPreview.textContent = "No sample loaded";
  if (colsPreview) colsPreview.textContent = "No sample loaded";
  if (metaCount) metaCount.textContent = "0";
  if (colCount) colCount.textContent = "0";
  if (formatLabel) formatLabel.textContent = "No sample loaded";
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}