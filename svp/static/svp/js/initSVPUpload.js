export function initSVPUpload() {
  const btnOpen = document.getElementById("btnLoadSVP");
  const btnSubmit = document.getElementById("btnSubmitSVP");
  const form = document.getElementById("svpUploadForm");
  const modalEl = document.getElementById("svpUploadModal");
  const statusEl = document.getElementById("svpUploadStatus");
  const selectedFilesEl = document.getElementById("svpSelectedFiles");
  const filesInput = document.getElementById("svpUploadFiles");
  const configSelect = document.getElementById("svpUploadConfigSelect");

  if (!btnOpen || !btnSubmit || !form || !modalEl || !filesInput) {
    console.warn("SVP upload init skipped: missing modal elements");
    return;
  }

  btnOpen.addEventListener("click", async () => {
    if (typeof bootstrap === "undefined") return;

    form.reset();
    btnSubmit.disabled = false;
    clearStatus(statusEl);
    clearStatus(selectedFilesEl);

    await loadUploadConfigs(configSelect, statusEl);

    bootstrap.Modal.getOrCreateInstance(modalEl).show();
  });

  filesInput.addEventListener("change", () => {
    const result = inspectSVPFiles(filesInput.files);

    if (!result.ok) {
      setStatus(selectedFilesEl, "danger", result.error);
      return;
    }

    if (!result.countTotal) {
      clearStatus(selectedFilesEl);
      return;
    }

    const html = [
      `<div><b>${result.count000}</b> .000 file(s), <b>${result.countSVP}</b> .svp file(s) selected.</div>`,
      `<div>Every .000 file will be imported as one SVP profile. Matching .svp files will be used when found.</div>`,
    ];

    if (result.count000 && result.countSVP && result.count000 !== result.countSVP) {
      html.push(`<div class="text-warning">Number of .000 and .svp files is different. Missing .svp profiles will use manual metadata.</div>`);
    }

    setStatusHtml(selectedFilesEl, "muted", html.join(""));
  });

  btnSubmit.addEventListener("click", async () => {
    const result = inspectSVPFiles(filesInput.files);

    if (!result.ok) {
      setStatus(statusEl, "danger", result.error);
      return;
    }

    if (!result.count000) {
      setStatus(statusEl, "danger", "Please select at least one .000 file.");
      return;
    }

    if (!configSelect || !configSelect.value) {
      setStatus(statusEl, "danger", "Please select .000 config.");
      return;
    }

    const formData = buildBatchUploadFormData(form, result.files);

    btnSubmit.disabled = true;
    setStatus(statusEl, "muted", `Uploading ${result.count000} profile(s)...`);

    try {
      const response = await fetch("/svp/api/upload/", {
        method: "POST",
        body: formData,
        headers: {
          "X-Requested-With": "XMLHttpRequest",
        },
      });

      const contentType = response.headers.get("content-type") || "";
      if (!contentType.includes("application/json")) {
        const text = await response.text();
        console.error("Upload returned non-JSON response:", text.slice(0, 1000));
        setStatus(statusEl, "danger", "Upload endpoint did not return JSON. Check Django error page/log.");
        btnSubmit.disabled = false;
        return;
      }

      const data = await response.json();

      if (!response.ok || !data.success) {
        const details = buildUploadErrorDetails(data);
        setStatusHtml(statusEl, "danger", details || escapeHtml(data.error || data.message || "Upload failed."));
        btnSubmit.disabled = false;
        return;
      }

      const okMsg = data.message || `Imported ${data.imported_count || 0} SVP profile(s).`;
      setStatus(statusEl, "success", okMsg);

      setTimeout(() => {
        window.location.reload();
      }, 700);

    } catch (err) {
      console.error("SVP upload failed:", err);
      setStatus(statusEl, "danger", "Server error during upload.");
      btnSubmit.disabled = false;
    }
  });
}

async function loadUploadConfigs(selectEl, statusEl) {
  if (!selectEl) return;

  try {
    const response = await fetch("/svp/api/config/list/", {
      headers: {
        "X-Requested-With": "XMLHttpRequest",
      },
    });

    const contentType = response.headers.get("content-type") || "";
    if (!contentType.includes("application/json")) {
      const text = await response.text();
      console.error("Config list returned non-JSON response:", text.slice(0, 500));
      setStatus(statusEl, "danger", "Config list endpoint did not return JSON.");
      return;
    }

    const data = await response.json();

    if (!response.ok || !data.success) {
      setStatus(statusEl, "danger", data.error || "Failed to load configs.");
      return;
    }

    const rows = Array.isArray(data.rows) ? data.rows : [];
    selectEl.innerHTML = `<option value="">-- select config --</option>`;

    rows.forEach((row) => {
      const opt = document.createElement("option");
      opt.value = row.id;
      opt.textContent = row.name || row.config_name || `Config ${row.id}`;
      selectEl.appendChild(opt);
    });

    if (!rows.length) {
      setStatus(statusEl, "danger", "No saved .000 configs found. Create config first.");
    }
  } catch (err) {
    console.error("Failed to load upload configs:", err);
    setStatus(statusEl, "danger", "Failed to load configs.");
  }
}

function inspectSVPFiles(fileList) {
  const files = Array.from(fileList || []);
  const file000s = [];
  const fileSVPs = [];
  const unsupported = [];

  for (const file of files) {
    const name = (file.name || "").toLowerCase();

    if (name.endsWith(".000")) {
      file000s.push(file);
    } else if (name.endsWith(".svp")) {
      fileSVPs.push(file);
    } else {
      unsupported.push(file.name || "Unknown file");
    }
  }

  if (unsupported.length) {
    return {
      ok: false,
      error: `Unsupported file type: ${unsupported.join(", ")}. Select only .000 and .svp files.`,
    };
  }

  return {
    ok: true,
    files,
    file000s,
    fileSVPs,
    count000: file000s.length,
    countSVP: fileSVPs.length,
    countTotal: files.length,
  };
}

function buildBatchUploadFormData(form, files) {
  const formData = new FormData();

  const textFields = [
    "csrfmiddlewaretoken",
    "name",
    "notes",
    "config_id",
    "rov",
    "coord_e",
    "coord_n",
    "instrument_model",
  ];

  for (const fieldName of textFields) {
    const field = form.querySelector(`[name="${fieldName}"]`);
    if (field) {
      formData.append(fieldName, field.value || "");
    }
  }

  for (const file of files) {
    formData.append("svp_files", file, file.name);
  }

  return formData;
}

function buildUploadErrorDetails(data) {
  if (!data) return "";

  const parts = [];

  if (data.error) {
    parts.push(`<div>${escapeHtml(data.error)}</div>`);
  } else if (data.message) {
    parts.push(`<div>${escapeHtml(data.message)}</div>`);
  }

  if (Array.isArray(data.failed) && data.failed.length) {
    parts.push(`<div class="mt-2 fw-semibold">Failed file(s):</div>`);
    parts.push(`<ul class="mb-0">`);
    for (const row of data.failed) {
      parts.push(`<li>${escapeHtml(row.file_000 || "Unknown .000")}: ${escapeHtml(row.error || "Unknown error")}</li>`);
    }
    parts.push(`</ul>`);
  }

  return parts.join("");
}

function setStatus(statusEl, type, message) {
  if (!statusEl) return;
  setStatusHtml(statusEl, type, escapeHtml(message));
}

function setStatusHtml(statusEl, type, html) {
  if (!statusEl) return;

  const cls = type === "success"
    ? "text-success"
    : type === "danger"
      ? "text-danger"
      : "text-muted";

  statusEl.innerHTML = `<div class="${cls}">${html}</div>`;
}

function clearStatus(statusEl) {
  if (statusEl) statusEl.innerHTML = "";
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
