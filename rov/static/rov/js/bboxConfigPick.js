// bboxConfigPick.js  (FULL FILE)
// Modal logic for BBox Config picker:
// - Load configs list into datalist
// - Load selected config (detail) and apply to form + mapping selects
// - Save config (optional hook: click #btnSaveBBoxCfg if your save handler exists elsewhere)
// - Delete selected config (POST)
// - Export ALL configs to one JSON file (GET export-all endpoint)
// - Import JSON file (single config OR multi-config file) and upsert into DB (POST import endpoint)

function getCookie(name) {
  const m = document.cookie.match(new RegExp("(^|;)\\s*" + name + "\\s*=\\s*([^;]+)"));
  return m ? decodeURIComponent(m.pop()) : "";
}

function buildUrlFromTemplate(templateUrl, id) {
  // expects template like ".../0/" -> replace trailing "/0/" with "/<id>/"
  return templateUrl.replace(/\/0\/?$/, `/${id}/`);
}

function downloadJson(filename, obj) {
  const blob = new Blob([JSON.stringify(obj, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function normalizeKey(s) {
  return String(s ?? "")
    .replace(/\u00A0/g, " ")
    .replace(/\r/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function ensureSelectedOption(selectEl, value) {
  if (!selectEl) return;
  const v = normalizeKey(value);

  // ensure empty option
  if (!selectEl.querySelector('option[value=""]')) {
    const opt0 = document.createElement("option");
    opt0.value = "";
    opt0.textContent = "— Select column —";
    selectEl.appendChild(opt0);
  }

  if (!v) {
    selectEl.value = "";
    return;
  }

  // ensure option exists
  const has = Array.from(selectEl.options).some((o) => normalizeKey(o.value) === v);
  if (!has) {
    const opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    selectEl.appendChild(opt);
  }

  // set
  const opt = Array.from(selectEl.options).find((o) => normalizeKey(o.value) === v);
  selectEl.value = opt ? opt.value : v;
}

function collectMappingFromUI() {
  const mapping = {};
  document.querySelectorAll(".bbox-config-selector").forEach((sel) => {
    const field = sel.dataset.fieldname;
    if (!field) return;
    mapping[field] = sel.value || "";
  });
  return mapping;
}

function applyMappingToSelectors(mapping) {
  const map = mapping || {};
  const norm = (s) => normalizeKey(s).toLowerCase();

  document.querySelectorAll(".bbox-config-selector").forEach((sel) => {
    const field = sel.dataset.fieldname;
    if (!field) return;

    const wantedRaw = map[field];
    if (wantedRaw == null) return;

    const wanted = norm(wantedRaw);

    // try match existing options
    const found = Array.from(sel.options).find((o) => norm(o.value) === wanted);
    if (found) {
      sel.value = found.value;
    } else {
      // create option if missing
      ensureSelectedOption(sel, wantedRaw);
    }
  });
}

function setAlert(msgWrap, msgEl, type, text) {
  if (!msgWrap || !msgEl) return;
  msgWrap.classList.remove("d-none");
  msgEl.className = `alert mb-0 alert-${type}`;
  msgEl.textContent = text;
}

function clearAlert(msgWrap) {
  if (!msgWrap) return;
  msgWrap.classList.add("d-none");
}

export function initBBoxConfigDatalist() {
  // required
  const nameInput = document.getElementById("layer-name");
  const datalist = document.getElementById("bbox-config-names");
  if (!nameInput || !datalist) return;

  // urls (set via data- attributes in modal)
  const listUrl = nameInput.dataset.configsUrl; // GET -> {ok, configs:[{id,name,is_default}]}
  const detailTpl = nameInput.dataset.configDetailUrlTemplate; // GET /<id>/ -> {ok, config:{...}, mapping:{...}}
  const deleteTpl = nameInput.dataset.configDeleteUrlTemplate; // POST /<id>/ -> {ok:true}
  const importUrl = nameInput.dataset.configImportUrl; // POST -> {ok:true, config_id}
  const exportAllUrl = nameInput.dataset.configExportAllUrl; // GET -> {ok:true, schema, configs:[...]}
  if (!listUrl || !detailTpl) return;

  // optional status area (if you have it in modal)
  const msgWrap = document.getElementById("bbox-config-msg-wrap");
  const msgEl = document.getElementById("bbox-config-msg");

  // header fields
  const vesselEl = document.getElementById("vessel-name");
  const rov1El = document.getElementById("rov1-name");
  const rov2El = document.getElementById("rov2-name");
  const gnss1El = document.getElementById("gnss1-name");
  const gnss2El = document.getElementById("gnss2-name");
  const dep1El = document.getElementById("Depth1-name");
  const dep2El = document.getElementById("Depth2-name");

  // buttons
  const btnClear = document.getElementById("btnClearBBoxForm");
  const btnDelete = document.getElementById("btnDeleteBBoxCfg");
  const btnExport = document.getElementById("btnExportBBoxCfgJson");
  const btnImport = document.getElementById("btnImportBBoxCfgJson");
  const jsonInput = document.getElementById("bbox-json-input");

  // state
  let configsCache = []; // [{id,name,is_default}]
  let currentConfigId = null;

  function setIfExists(el, v) {
    if (!el) return;
    el.value = v || "";
  }

  function setDeleteState() {
    if (btnDelete) btnDelete.disabled = !currentConfigId;
  }

  function clearForm() {
    clearAlert(msgWrap);

    [nameInput, vesselEl, rov1El, rov2El, gnss1El, gnss2El, dep1El, dep2El].forEach((el) => {
      if (el) el.value = "";
    });

    const fileInput = document.getElementById("bbox-file-input");
    if (fileInput) fileInput.value = "";

    // reset mapping selectors (keep empty option)
    document.querySelectorAll(".bbox-config-selector").forEach((sel) => {
      // keep existing options if you want; or hard reset:
      // sel.innerHTML = "";
      // const opt = document.createElement("option");
      // opt.value = "";
      // opt.textContent = "— Select column —";
      // sel.appendChild(opt);
      sel.value = "";
    });

    currentConfigId = null;
    setDeleteState();

    const err = document.getElementById("csvErr");
    if (err) {
      err.textContent = "";
      err.classList.add("d-none");
    }
  }

  async function loadConfigs() {
    const res = await fetch(listUrl, { method: "GET" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) throw new Error(data.error || "Failed to load configs");

    configsCache = Array.isArray(data.configs) ? data.configs : [];

    datalist.innerHTML = configsCache
      .map((c) => {
        const label = c.is_default ? `${c.name} (default)` : c.name;
        return `<option value="${c.name}">${label}</option>`;
      })
      .join("");
  }

  async function fetchDetailById(id) {
    const url = buildUrlFromTemplate(detailTpl, id);
    const res = await fetch(url, { method: "GET" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) throw new Error(data.error || "Failed to load config detail");
    return data; // {ok, config, mapping}
  }

  function applyConfigToUI(configObj, mappingObj) {
    const c = configObj || {};
    setIfExists(nameInput, c.name);
    setIfExists(vesselEl, c.vessel_name || c.Vessel_name);
    setIfExists(rov1El, c.rov1_name);
    setIfExists(rov2El, c.rov2_name);
    setIfExists(gnss1El, c.gnss1_name);
    setIfExists(gnss2El, c.gnss2_name);
    setIfExists(dep1El, c.depth1_name || c.Depth1_name);
    setIfExists(dep2El, c.depth2_name || c.Depth2_name);

    applyMappingToSelectors(mappingObj || {});
  }

  async function loadAndApplyConfigByName(name) {
    clearAlert(msgWrap);

    const cfg = configsCache.find(
      (c) => (c.name || "").toLowerCase() === (name || "").toLowerCase()
    );

    if (!cfg) {
      currentConfigId = null;
      setDeleteState();
      return;
    }

    currentConfigId = cfg.id;
    setDeleteState();

    const data = await fetchDetailById(cfg.id);
    applyConfigToUI(data.config, data.mapping);
  }

  async function deleteCurrentConfig() {
    if (!currentConfigId) return;
    if (!deleteTpl) {
      setAlert(msgWrap, msgEl, "warning", "Delete URL is not configured.");
      return;
    }

    const cfg = configsCache.find((c) => c.id === currentConfigId);
    const cfgName = cfg?.name || nameInput.value || `ID ${currentConfigId}`;

    const ok = window.confirm(`Delete BBox config "${cfgName}"?\nThis cannot be undone.`);
    if (!ok) return;

    const url = buildUrlFromTemplate(deleteTpl, currentConfigId);

    const res = await fetch(url, {
      method: "POST",
      headers: {
        "X-CSRFToken": getCookie("csrftoken"),
        "Content-Type": "application/json",
      },
      body: JSON.stringify({}),
    });

    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) {
      setAlert(msgWrap, msgEl, "danger", data.error || `Delete failed (${res.status})`);
      return;
    }

    await loadConfigs();
    clearForm();
    setAlert(msgWrap, msgEl, "success", "Configuration deleted.");
  }

  async function exportAllConfigsJson() {
    if (!exportAllUrl) {
      setAlert(msgWrap, msgEl, "warning", "Export-all URL is not configured.");
      return;
    }

    const res = await fetch(exportAllUrl, { method: "GET" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) {
      setAlert(msgWrap, msgEl, "danger", data.error || `Export failed (${res.status})`);
      return;
    }

    const fnDate = new Date().toISOString().slice(0, 10);
    downloadJson(`BBox_Configs_${fnDate}.json`, data);
    setAlert(msgWrap, msgEl, "success", "Exported all configs to JSON.");
  }

  async function upsertOneConfigToDb(cfg, mapping) {
    if (!importUrl) {
      // If you want "import to UI only", just skip DB
      return { ok: false, error: "Import URL is not configured." };
    }

    const payload = {
      upsert: true,
      config: {
        name: normalizeKey(cfg?.name) || "Imported",
        vessel_name: normalizeKey(cfg?.vessel_name || cfg?.Vessel_name || ""),
        rov1_name: normalizeKey(cfg?.rov1_name || ""),
        rov2_name: normalizeKey(cfg?.rov2_name || ""),
        gnss1_name: normalizeKey(cfg?.gnss1_name || ""),
        gnss2_name: normalizeKey(cfg?.gnss2_name || ""),
        depth1_name: normalizeKey(cfg?.depth1_name || cfg?.Depth1_name || ""),
        depth2_name: normalizeKey(cfg?.depth2_name || cfg?.Depth2_name || ""),
        is_default: !!cfg?.is_default,
      },
      mapping: mapping || {},
    };

    const res = await fetch(importUrl, {
      method: "POST",
      headers: {
        "X-CSRFToken": getCookie("csrftoken"),
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) return { ok: false, error: data.error || `Import failed (${res.status})` };

    return { ok: true, config_id: data.config_id };
  }

  async function importJsonFile(file) {
    clearAlert(msgWrap);

    if (!file) return;

    let obj;
    try {
      obj = JSON.parse(await file.text());
    } catch {
      setAlert(msgWrap, msgEl, "danger", "Invalid JSON file.");
      return;
    }

    // Supports:
    // A) single config file: {config:{...}, mapping:{...}}
    // B) multi-config export file: {configs:[{... mapping ...}]}
    const multi = Array.isArray(obj?.configs) ? obj.configs : null;

    if (multi) {
      // Import all configs
      let okCount = 0;
      let failCount = 0;

      for (const item of multi) {
        const cfg = item?.config ? item.config : item; // accept either shape
        const mapping = item?.mapping || cfg?.mapping || {};
        const res = await upsertOneConfigToDb(cfg, mapping);
        if (res.ok) okCount++;
        else failCount++;
      }

      await loadConfigs();
      setAlert(
        msgWrap,
        msgEl,
        failCount ? "warning" : "success",
        `Imported ${okCount} configs${failCount ? `, ${failCount} failed` : ""}.`
      );
      return;
    }

    const cfg = obj?.config || obj?.Config || {};
    const mapping = obj?.mapping || obj?.Mapping || cfg?.mapping || {};

    // show in UI immediately
    applyConfigToUI(cfg, mapping);

    // save to DB
    const r = await upsertOneConfigToDb(cfg, mapping);
    if (!r.ok) {
      setAlert(msgWrap, msgEl, "warning", r.error || "Imported to UI only (not saved).");
      return;
    }

    await loadConfigs();

    // try set currentConfigId by matching name
    const nm = (cfg?.name || nameInput.value || "").toLowerCase();
    const found = configsCache.find((c) => (c.name || "").toLowerCase() === nm);
    currentConfigId = found ? found.id : null;
    setDeleteState();

    setAlert(msgWrap, msgEl, "success", "Imported config from JSON.");
  }

  // ---- events ----
  nameInput.addEventListener("change", () => {
    loadAndApplyConfigByName(nameInput.value).catch((e) => {
      console.error(e);
      setAlert(msgWrap, msgEl, "danger", e.message || "Failed to load config.");
    });
  });

  if (btnClear) btnClear.addEventListener("click", clearForm);
  if (btnDelete) btnDelete.addEventListener("click", () => deleteCurrentConfig().catch(console.error));
  if (btnExport) btnExport.addEventListener("click", () => exportAllConfigsJson().catch(console.error));

  if (btnImport && jsonInput) {
    btnImport.addEventListener("click", () => {
      jsonInput.value = "";
      jsonInput.click();
    });

    jsonInput.addEventListener("change", () => {
      const file = jsonInput.files?.[0];
      importJsonFile(file).catch((e) => {
        console.error(e);
        setAlert(msgWrap, msgEl, "danger", e.message || "Import failed.");
      });
    });
  }

  // ---- init ----
  loadConfigs()
    .then(() => {
      // if current value matches existing, auto-load it
      if (nameInput.value) return loadAndApplyConfigByName(nameInput.value);
    })
    .catch((e) => {
      console.error(e);
      setAlert(msgWrap, msgEl, "danger", e.message || "Failed to initialize BBox configs.");
    })
    .finally(() => {
      setDeleteState();
    });
}