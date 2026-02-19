// bboxConfigPick.js
// Purpose:
// 1) Load BBox configs into <datalist>
// 2) When user selects config name -> fetch config detail -> fill text inputs
// 3) Apply mapping to all .bbox-config-selector selects IMMEDIATELY from DB
//    (even if CSV headers/options are not loaded yet) by injecting the mapped option.

function ensureSelectedOption(selectEl, value) {
  if (!selectEl) return;
  const v = String(value || "").trim();

  // Ensure placeholder exists
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

  // Create the option if it's not present yet
  const has = Array.from(selectEl.options).some((o) => o.value === v);
  if (!has) {
    const opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    selectEl.appendChild(opt);
  }

  selectEl.value = v;
}

// store last selected mapping globally (used by csv header fetch too)
window.__bboxSelectedMapping = null;

// helper used by initBBCsvHeaderFetch() after it injects CSV header options
window.__bboxApplyMappingToSelectors = function (mapping) {
  const map = mapping || window.__bboxSelectedMapping;
  if (!map) return;

  const norm = (s) =>
    String(s ?? "")
      .replace(/\u00A0/g, " ")   // NBSP -> normal space
      .replace(/\r/g, "")        // remove CR
      .replace(/\s+/g, " ")      // collapse whitespace
      .trim()
      .toLowerCase();

  document.querySelectorAll(".bbox-config-selector").forEach((sel) => {
    const field = sel.dataset.fieldname;
    if (!field) return;

    const wantedRaw = map[field];
    if (!wantedRaw) return;

    const wanted = norm(wantedRaw);

    // find option whose normalized value matches
    const opt = Array.from(sel.options).find((o) => norm(o.value) === wanted);
    if (opt) sel.value = opt.value;
  });
};


function buildDetailUrl(templateUrl, id) {
  // templateUrl ends with "/0/" -> replace "/0/" with "/<id>/"
  return templateUrl.replace(/\/0\/?$/, `/${id}/`);
}

export function initBBoxConfigDatalist() {
  const nameInput = document.getElementById("layer-name");
  const datalist = document.getElementById("bbox-config-names");
  if (!nameInput || !datalist) return;

  const listUrl = nameInput.dataset.configsUrl;
  const detailTpl = nameInput.dataset.configDetailUrlTemplate;
  if (!listUrl || !detailTpl) return;

  // Inputs to fill
  const vesselEl = document.getElementById("vessel-name");
  const rov1El = document.getElementById("rov1-name");
  const rov2El = document.getElementById("rov2-name");
  const gnss1El = document.getElementById("gnss1-name");
  const gnss2El = document.getElementById("gnss2-name");
  const dep1El = document.getElementById("Depth1-name");
  const dep2El = document.getElementById("Depth2-name");

  let configsCache = []; // [{id,name,is_default,...}]

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

  function setIfExists(el, val) {
    if (!el) return;
    el.value = val || "";
  }

  async function loadAndApplyConfigByName(name) {
    const cfg = configsCache.find(
      (c) => (c.name || "").toLowerCase() === (name || "").toLowerCase()
    );
    if (!cfg) return;

    const detailUrl = buildDetailUrl(detailTpl, cfg.id);

    const res = await fetch(detailUrl, { method: "GET" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) throw new Error(data.error || "Failed to load config");

    const c = data.config || {};

    // Fill header inputs
    setIfExists(nameInput, c.name);
    setIfExists(vesselEl, c.vessel_name || c.Vessel_name);
    setIfExists(rov1El, c.rov1_name);
    setIfExists(rov2El, c.rov2_name);
    setIfExists(gnss1El, c.gnss1_name);
    setIfExists(gnss2El, c.gnss2_name);
    setIfExists(dep1El, c.depth1_name || c.Depth1_name);
    setIfExists(dep2El, c.depth2_name || c.Depth2_name);

    // Store mapping globally
    window.__bboxSelectedMapping = data.mapping || {};

    // ✅ Apply mapping IMMEDIATELY from DB:
    // If selects have no options yet, we inject the mapped option and select it.
    document.querySelectorAll(".bbox-config-selector").forEach((sel) => {
      const field = sel.dataset.fieldname;
      if (!field) return;
      ensureSelectedOption(sel, window.__bboxSelectedMapping[field]);
    });
  }

  // Load config names immediately
  loadConfigs().catch(console.error);

  // When user selects a name (datalist selection triggers change)
  nameInput.addEventListener("change", async () => {
    try {
      await loadAndApplyConfigByName(nameInput.value);
    } catch (e) {
      console.error(e);
    }
  });
  const btnClear = document.getElementById("btnClearBBoxForm");

if (btnClear) {
  btnClear.addEventListener("click", () => {

    // 1) Clear text inputs
    [
      nameInput,
      vesselEl,
      rov1El,
      rov2El,
      gnss1El,
      gnss2El,
      dep1El,
      dep2El,
    ].forEach(el => {
      if (el) el.value = "";
    });

    // 2) Clear file input
    const fileInput = document.getElementById("bbox-file-input");
    if (fileInput) fileInput.value = "";

    // 3) Clear all mapping selects
    document.querySelectorAll(".bbox-config-selector").forEach((sel) => {
      sel.innerHTML = "";
      const opt = document.createElement("option");
      opt.value = "";
      opt.textContent = "— Select column —";
      sel.appendChild(opt);
      sel.value = "";
    });

    // 4) Clear stored mapping
    window.__bboxSelectedMapping = null;

    // 5) Clear CSV error message if any
    const err = document.getElementById("csvErr");
    if (err) {
      err.textContent = "";
      err.classList.add("d-none");
    }
  });
}

}
