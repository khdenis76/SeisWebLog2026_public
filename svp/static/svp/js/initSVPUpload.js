export function initSVPUpload() {
  const btnOpen = document.getElementById("btnLoadSVP");
  const btnSubmit = document.getElementById("btnSubmitSVP");
  const form = document.getElementById("svpUploadForm");
  const modalEl = document.getElementById("svpUploadModal");
  const statusEl = document.getElementById("svpUploadStatus");
  const configSelect = document.getElementById("svpUploadConfigSelect");

  if (!btnOpen || !btnSubmit || !form || !modalEl) {
    console.warn("SVP upload init skipped: missing modal elements");
    return;
  }

  btnOpen.addEventListener("click", async () => {
    if (typeof bootstrap === "undefined") return;

    clearStatus(statusEl);
    await loadUploadConfigs(configSelect, statusEl);

    bootstrap.Modal.getOrCreateInstance(modalEl).show();
  });

  btnSubmit.addEventListener("click", async () => {
    const fileInput = form.querySelector('input[name="file"]');
    const configId = configSelect?.value;

    if (!fileInput || !fileInput.files || !fileInput.files.length) {
      setStatus(statusEl, "danger", "Please select a file.");
      return;
    }

    if (!configId) {
      setStatus(statusEl, "danger", "Please select a config.");
      return;
    }

    const formData = new FormData(form);

    btnSubmit.disabled = true;
    setStatus(statusEl, "muted", "Uploading...");

    try {
      const response = await fetch("/svp/api/upload/", {
        method: "POST",
        body: formData,
        headers: {
          "X-CSRFToken": getCSRFToken(form),
          "X-Requested-With": "XMLHttpRequest",
        },
      });

      const contentType = response.headers.get("content-type") || "";
      if (!contentType.includes("application/json")) {
        const text = await response.text();
        console.error("Upload returned non-JSON response:", text.slice(0, 500));
        setStatus(statusEl, "danger", "Upload endpoint did not return JSON.");
        btnSubmit.disabled = false;
        return;
      }

      const data = await response.json();

      if (!response.ok || !data.success) {
        setStatus(statusEl, "danger", data.error || "Upload failed.");
        btnSubmit.disabled = false;
        return;
      }

      setStatus(statusEl, "success", data.message || "Upload successful.");

      setTimeout(() => {
        window.location.reload();
      }, 500);

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
      opt.textContent = row.name || `Config ${row.id}`;
      selectEl.appendChild(opt);
    });

    if (!rows.length) {
      setStatus(statusEl, "danger", "No saved configs found. Create config first.");
    }
  } catch (err) {
    console.error("Failed to load upload configs:", err);
    setStatus(statusEl, "danger", "Failed to load configs.");
  }
}

function getCSRFToken(form) {
  const input = form.querySelector('input[name="csrfmiddlewaretoken"]');
  if (input) return input.value;

  const match = document.cookie.match(/csrftoken=([^;]+)/);
  return match ? match[1] : "";
}

function setStatus(statusEl, type, message) {
  if (!statusEl) return;

  const cls = type === "success"
    ? "text-success"
    : type === "danger"
      ? "text-danger"
      : "text-muted";

  statusEl.innerHTML = `<span class="${cls}">${escapeHtml(message)}</span>`;
}

function clearStatus(statusEl) {
  if (statusEl) statusEl.innerHTML = "";
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}