// static/source/js/initSourceUpload.js
// FULL COPY-PASTE FILE
// Adjust import path for getCSRFToken if needed.

import { getCSRFToken } from "../../baseproject/js/csrf.js"; // adjust path if needed

export function initSourceUpload() {
  const modalEl   = document.getElementById("sourceUploadModal");
  const form      = document.getElementById("source-upload-form");
  const msgWrap   = document.getElementById("source-upload-msg-wrap");
  const msgEl     = document.getElementById("source-upload-msg");
  const submitBtn = document.getElementById("source-upload-submit");

  // Main inputs
  const fileTypeSelect = document.getElementById("source-file-type");
  const filesInput     = document.getElementById("source-files");

  // Sections
  const spsOptions  = document.getElementById("sps-options");
  const shotOptions = document.getElementById("shot-options");

  // SPS-only inputs (may or may not exist in your modal)
  const vesselSelect = document.getElementById("sps-vessel");              // name="source_vessel_id"
  const detectBySeq  = document.getElementById("detect-vessel-by-seq");    // name="detect_vessel_by_seq"
  const seqNumberInp = document.getElementById("sps-seq-number");          // name="seq_number"
  const spsTbody = document.getElementById("sps-table-tbody")

  if (!modalEl || !form || !fileTypeSelect || !filesInput) return;

  function setAlert(type, text) {
    if (!msgWrap || !msgEl) return;
    msgWrap.classList.remove("d-none");
    msgEl.className = "alert mb-0 alert-" + type;
    msgEl.textContent = text;
  }

  function clearAlert() {
    if (!msgWrap || !msgEl) return;
    msgWrap.classList.add("d-none");
    msgEl.textContent = "";
  }

  // IMPORTANT: Do NOT disable the file input, otherwise FormData won't include files
  function setBusy(busy) {
    if (submitBtn) submitBtn.disabled = !!busy;

    form.querySelectorAll("input, select, button, textarea").forEach((el) => {
      // Keep close buttons usable
      if (el.getAttribute("data-bs-dismiss") === "modal") return;

      // Allow changing type if you want (optional)
      if (el === fileTypeSelect) return;

      // NEVER disable file input (or files won't be posted)
      if (el.type === "file") return;

      el.disabled = !!busy;
    });
  }

  // --- Core fix: required/disabled fields depend on file type ---
  function updateFileTypeUI() {
    const type = (fileTypeSelect.value || "").trim().toUpperCase();

    // Default hide blocks
    if (spsOptions) spsOptions.classList.add("d-none");
    if (shotOptions) shotOptions.classList.add("d-none");

    if (type === "SPS") {
      if (spsOptions) spsOptions.classList.remove("d-none");
      if (shotOptions) shotOptions.classList.add("d-none");

      if (vesselSelect) {
        vesselSelect.disabled = false;
        vesselSelect.required = true; // required only for SPS
      }

      // Seq input enabled only if detect-by-seq checked
      if (seqNumberInp && detectBySeq) {
        seqNumberInp.disabled = !detectBySeq.checked;
        if (!detectBySeq.checked) seqNumberInp.value = "";
      }
    }

    if (type === "SHOT") {
      if (spsOptions) spsOptions.classList.add("d-none");
      if (shotOptions) shotOptions.classList.remove("d-none");

      // For SHOT: vessel not needed
      if (vesselSelect) {
        vesselSelect.required = false;
        vesselSelect.disabled = true;   // IMPORTANT: disabled fields are ignored by validation & POST
        vesselSelect.value = "";        // reset
      }

      // Disable seq-detect (optional)
      if (detectBySeq) detectBySeq.checked = false;
      if (seqNumberInp) {
        seqNumberInp.value = "";
        seqNumberInp.disabled = true;
      }
    }
  }

  function updateSeqInputUI() {
    if (!seqNumberInp || !detectBySeq) return;
    const type = (fileTypeSelect.value || "").trim().toUpperCase();
    if (type !== "SPS") {
      seqNumberInp.disabled = true;
      return;
    }
    seqNumberInp.disabled = !detectBySeq.checked;
    if (!detectBySeq.checked) seqNumberInp.value = "";
  }

  fileTypeSelect.addEventListener("change", () => {
    clearAlert();
    updateFileTypeUI();
    updateSeqInputUI();
  });

  if (detectBySeq) {
    detectBySeq.addEventListener("change", () => {
      clearAlert();
      updateSeqInputUI();
    });
  }

  // Initialize on load and whenever modal opens
  updateFileTypeUI();
  updateSeqInputUI();

  modalEl.addEventListener("shown.bs.modal", () => {
    clearAlert();
    updateFileTypeUI();
    updateSeqInputUI();
  });

  // --- Submit ---
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    clearAlert();

    // Trigger browser validation (only enabled required controls are validated)
    if (!form.checkValidity()) {
      form.reportValidity();
      return;
    }

    // ✅ Build FormData BEFORE disabling any controls
    const fd = new FormData(form);

    // Extra safety: if SHOT selected, make sure SPS-only fields are not submitted
    const type = (fileTypeSelect.value || "").trim().toUpperCase();
    if (type === "SHOT") {
      fd.delete("source_vessel_id");
      fd.delete("detect_vessel_by_seq");
      fd.delete("seq_number");
      fd.delete("sps_revision_id");
      fd.delete("tier");
      fd.delete("year");
      fd.delete("auto_year_by_jday");
    }

    // Debug (optional): ensure files are present
    // console.log("files count:", (fd.getAll("files") || []).length);

    setBusy(true);

    try {
      const url = form.getAttribute("action");
      const res = await fetch(url, {
        method: "POST",
        headers: {
          "X-CSRFToken": getCSRFToken(),
        },
        body: fd,
      });

      // Try parse JSON for nice errors
      let data = null;
      try {
        data = await res.json();
      } catch {
        data = null;
      }

      if (!res.ok) {
        // If backend returns form errors in JSON
        if (data?.errors) {
          setAlert("danger", JSON.stringify(data.errors));
        } else {
          setAlert("danger", data?.error || `Upload failed (HTTP ${res.status})`);
        }
        return;
      }
      spsTbody.innerHTML=data.sps_summary
      // Success
      setAlert("success", data?.message || "Upload complete.");
      // If you want to close modal on success:
      // bootstrap.Modal.getInstance(modalEl)?.hide();

    } catch (err) {
      setAlert("danger", `Upload failed: ${err?.message || err}`);
    } finally {
      setBusy(false);
    }
  });
}