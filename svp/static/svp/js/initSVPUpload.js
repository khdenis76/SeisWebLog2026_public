export function initSVPUpload() {
  const btnOpen = document.getElementById("btnLoadSVP");
  const btnSubmit = document.getElementById("btnSubmitSVP");
  const form = document.getElementById("svpUploadForm");
  const modalEl = document.getElementById("svpUploadModal");
  const statusEl = document.getElementById("svpUploadStatus");
  const selectedFilesEl = document.getElementById("svpSelectedFiles");
  const filesInput = document.getElementById("svpUploadFiles");
  const configSelect = document.getElementById("svpUploadConfigSelect");

  if (!btnOpen || !btnSubmit || !form || !modalEl || !filesInput) return;

  btnOpen.addEventListener("click", async () => {
    form.reset();
    btnSubmit.disabled = true;
    clearStatus(statusEl);
    clearStatus(selectedFilesEl);
    await loadUploadConfigs(configSelect, statusEl);
    bootstrap.Modal.getOrCreateInstance(modalEl).show();
  });

  filesInput.addEventListener("change", () => {
    const result = inspectSVPFiles(filesInput.files);
    clearStatus(statusEl);
    if (!result.ok) {
      btnSubmit.disabled = true;
      setStatus(selectedFilesEl, "danger", result.error);
      return;
    }
    btnSubmit.disabled = !result.countTotal;
    if (!result.countTotal) {
      clearStatus(selectedFilesEl);
      return;
    }

    const profileCount = result.count000 + (result.count000 ? 0 : result.countSVP);
    const parts = [
      `<div><b>${result.count000}</b> .000 file(s), <b>${result.countSVP}</b> .svp file(s) selected.</div>`,
    ];
    if (result.count000) {
      parts.push(`<div>Ready to import ${profileCount} raw profile(s). Matching .svp files will supply metadata; unpaired .svp files will also be imported as processed profiles.</div>`);
    } else {
      parts.push(`<div class="text-success">Ready to import ${result.countSVP} processed SVP profile(s).</div>`);
    }
    setStatusHtml(selectedFilesEl, "muted", parts.join(""));
  });

  btnSubmit.addEventListener("click", async () => {
    const result = inspectSVPFiles(filesInput.files);
    if (!result.ok || !result.countTotal) {
      setStatus(statusEl, "danger", result.error || "Please select at least one .000 or .svp file.");
      return;
    }

    const formData = buildBatchUploadFormData(form, result.files);
    btnSubmit.disabled = true;
    setStatus(statusEl, "muted", `Uploading ${result.countTotal} file(s)...`);

    try {
      const response = await fetch("/svp/api/upload/", {
        method: "POST",
        body: formData,
        headers: {"X-Requested-With": "XMLHttpRequest"},
      });
      const data = await response.json();
      if (!response.ok || !data.success) {
        setStatusHtml(statusEl, "danger", buildUploadErrorDetails(data));
        btnSubmit.disabled = false;
        return;
      }
      setStatus(statusEl, "success", data.message || `Imported ${data.imported_count || 0} SVP profile(s).`);
      // Keep the modal open so the user can review the result or upload more files.
      // Refresh the profile table, RP-preplot map, and aggregate statistics in place.
      window.dispatchEvent(new CustomEvent("svp:profiles-changed", {detail: data}));
      filesInput.value = "";
      clearStatus(selectedFilesEl);
      btnSubmit.disabled = true;
    } catch (err) {
      console.error(err);
      setStatus(statusEl, "danger", "Server error during upload.");
      btnSubmit.disabled = false;
    }
  });
}

async function loadUploadConfigs(selectEl, statusEl) {
  if (!selectEl) return;
  selectEl.innerHTML = `<option value="">Auto Detect</option>`;
  try {
    const response = await fetch("/svp/api/config/list/", {headers: {"X-Requested-With": "XMLHttpRequest"}});
    const data = await response.json();
    if (!response.ok || !data.success) throw new Error(data.error || "Failed to load configs.");

    const unique = new Map();
    for (const row of (Array.isArray(data.rows) ? data.rows : [])) {
      const label = String(row.name || row.config_name || `Config ${row.id}`).trim();
      const key = label.toLowerCase();
      if (!unique.has(key)) unique.set(key, {...row, label});
    }
    [...unique.values()]
      .sort((a, b) => a.label.localeCompare(b.label))
      .forEach(row => {
        const opt = document.createElement("option");
        opt.value = row.id;
        opt.textContent = row.label;
        selectEl.appendChild(opt);
      });
  } catch (err) {
    console.error(err);
    setStatus(statusEl, "danger", "Saved configs could not be loaded. Auto Detect is still available.");
  }
}

function inspectSVPFiles(fileList) {
  const files = Array.from(fileList || []);
  const file000s = [], fileSVPs = [], unsupported = [];
  for (const file of files) {
    const name = (file.name || "").toLowerCase();
    if (name.endsWith(".000")) file000s.push(file);
    else if (name.endsWith(".svp")) fileSVPs.push(file);
    else unsupported.push(file.name || "Unknown file");
  }
  if (unsupported.length) return {ok: false, error: `Unsupported file type: ${unsupported.join(", ")}. Select only .000 and .svp files.`};
  return {ok: true, files, file000s, fileSVPs, count000: file000s.length, countSVP: fileSVPs.length, countTotal: files.length};
}

function buildBatchUploadFormData(form, files) {
  const data = new FormData();
  for (const name of ["csrfmiddlewaretoken", "name", "notes", "config_id", "rov", "coord_e", "coord_n", "instrument_model"]) {
    const field = form.querySelector(`[name="${name}"]`);
    if (field) data.append(name, field.value || "");
  }
  for (const file of files) data.append("svp_files", file, file.name);
  return data;
}

function buildUploadErrorDetails(data) {
  const parts = [`<div>${escapeHtml(data?.error || data?.message || "Upload failed.")}</div>`];
  if (Array.isArray(data?.failed) && data.failed.length) {
    parts.push(`<div class="mt-2 fw-semibold">Failed file(s):</div><ul class="mb-0">`);
    for (const row of data.failed) parts.push(`<li>${escapeHtml(row.file || row.file_000 || row.file_svp || "Unknown file")}: ${escapeHtml(row.error || "Unknown error")}</li>`);
    parts.push(`</ul>`);
  }
  return parts.join("");
}

function setStatus(el, type, message) { setStatusHtml(el, type, escapeHtml(message)); }
function setStatusHtml(el, type, html) {
  if (!el) return;
  const cls = type === "success" ? "text-success" : type === "danger" ? "text-danger" : "text-muted";
  el.innerHTML = `<div class="${cls}">${html}</div>`;
}
function clearStatus(el) { if (el) el.innerHTML = ""; }
function escapeHtml(value) {
  return String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#039;");
}
