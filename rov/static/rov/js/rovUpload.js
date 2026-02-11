import { getCSRFToken } from "../../baseproject/js/csrf.js"; // adjust path if needed

export function initRovUploadModal() {
  const form       = document.getElementById("rov-upload-form");
  const typeSelect = document.getElementById("rov-file-type");
  const fileInput  = document.getElementById("rov-file-input");
  const helpText   = document.getElementById("rov-file-help");
  const uploadBtn  = document.getElementById("rov-upload-btn");
  const statusEl   = document.getElementById("rov-upload-status");
  const countEl    = document.getElementById("rov-files-count");
  const modalEl    = document.getElementById("rovUploadModal");
  const tbody =document.getElementById('bbox-list-tbody')


  // ✅ BlackBox config selector
  const cfgSelect  = document.getElementById("bblog-config-select");
  const cfgGroup   = cfgSelect ? cfgSelect.closest(".input-group") : null;

  if (!form || !typeSelect || !fileInput || !uploadBtn || !modalEl || !tbody) return;

  const rules = {
    DSR: { accept: ".txt,.csv", help: "DSR files (.txt / .csv)", url: "urlDsr" },
    SURVEY_MANAGER: { accept: ".csv,.txt", help: "Survey Manager (.csv / .txt)", url: "urlSurveyManager" },
    BLACK_BOX: { accept: ".csv,.txt", help: "Black Box logs (.csv / .txt)", url: "urlBlackBox" },
    REC_DB: { accept: ".*,.txt", help: "REC_DB (.* / .txt)", url: "urlRecDb" },
  };

  const setStatus = (t = "") => { if (statusEl) statusEl.textContent = t; };
  const setCount  = (t = "") => { if (countEl) countEl.textContent = t; };

  const cleanupBackdrops = () => {
    document.querySelectorAll(".modal-backdrop").forEach((b) => b.remove());
    document.body.classList.remove("modal-open");
    document.body.style.removeProperty("padding-right");
  };

  function setCfgVisibility(fileType) {
    if (!cfgSelect) return;
    const show = fileType === "BLACK_BOX";
    cfgSelect.disabled = !show;
    if (cfgGroup) cfgGroup.classList.toggle("d-none", !show);
  }

  function resetForm() {
    form.reset();

    fileInput.disabled = true;
    fileInput.accept = "";
    uploadBtn.disabled = true;

    if (helpText) helpText.textContent = "Select file type first";
    setStatus("");
    setCount("");

    form.action = "";

    // hide config selector until BLACK_BOX chosen
    setCfgVisibility("");
  }

  // Create instance once
  const modal = bootstrap.Modal.getOrCreateInstance(modalEl, { backdrop: true, keyboard: true });

  modalEl.addEventListener("show.bs.modal", () => {
    cleanupBackdrops();
    resetForm();
  });

  modalEl.addEventListener("hidden.bs.modal", () => {
    cleanupBackdrops();
  });

  // Change by file type
  typeSelect.addEventListener("change", () => {
    const fileType = typeSelect.value;
    const rule = rules[fileType];
    if (!rule) return;

    fileInput.disabled = false;
    uploadBtn.disabled = false;

    fileInput.value = "";
    fileInput.accept = rule.accept;

    if (helpText) helpText.textContent = rule.help;

    const url = typeSelect.dataset[rule.url];
    if (url) form.action = url;

    setStatus("");
    setCount("");

    // ✅ show/hide config selector
    setCfgVisibility(fileType);
  });

  // Show number of selected files
  fileInput.addEventListener("change", () => {
    const n = fileInput.files?.length || 0;
    setCount(n ? `Selected: ${n} file(s)` : "");
  });

  // Submit with fetch (NO reload)
  form.addEventListener("submit", async (e) => {
    e.preventDefault();

    if (!form.action) {
      alert("Select file type first");
      return;
    }

    const files = fileInput.files;
    if (!files || files.length === 0) {
      alert("Select at least one file");
      return;
    }

    const fileType = typeSelect.value;

    // Build FormData manually for multi-file upload
    const fd = new FormData();
    fd.append("file_type", fileType);

    // ✅ IMPORTANT: for BLACK_BOX we must send config_id
    if (fileType === "BLACK_BOX") {
      const cfgId = cfgSelect?.value || "";
      if (!cfgId) {
        alert("Select BBLog configuration");
        return;
      }
      fd.append("config_id", cfgId);
    }

    for (const f of files) fd.append("files", f);

    uploadBtn.disabled = true;
    const oldText = uploadBtn.textContent;
    uploadBtn.textContent = "Uploading...";
    setStatus(`Uploading ${files.length} file(s)...`);

    try {
      const resp = await fetch(form.action, {
        method: "POST",
        headers: { "X-CSRFToken": getCSRFToken() },
        body: fd,
      });

      const data = await resp.json().catch(() => ({}));
      if (!resp.ok) throw new Error(data.error || `Upload failed (${resp.status})`);
      if (data.ok && data.bbox_file_tbody !== undefined) {
         if (tbody) tbody.innerHTML = data.bbox_file_tbody;
      }

      const msg = data.message || "Upload completed";

      // close modal first, then show message after hidden
      modalEl.addEventListener(
        "hidden.bs.modal",
        () => {
          alert(msg); // replace with toast if you want
        },
        { once: true }
      );

      modal.hide();

    } catch (err) {
      console.error(err);
      alert(err.message || "Upload failed");
      setStatus(err.message || "Upload failed");
    } finally {
      uploadBtn.disabled = false;
      uploadBtn.textContent = oldText;
    }
  });

  // Ensure initial state if modal isn't used
  setCfgVisibility("");
}
