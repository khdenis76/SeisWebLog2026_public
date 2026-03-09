import { getCSRFToken } from "../../baseproject/js/csrf.js";
import { showToast, toastFromResponse } from "./toast.js";

export function initRovUploadModal() {
  const form = document.getElementById("rov-upload-form");
  const typeSelect = document.getElementById("rov-file-type");
  const fileInput = document.getElementById("rov-file-input");
  const helpText = document.getElementById("rov-file-help");
  const uploadBtn = document.getElementById("rov-upload-btn");
  const statusEl = document.getElementById("rov-upload-status");
  const countEl = document.getElementById("rov-files-count");
  const modalEl = document.getElementById("rovUploadModal");

  const tbody = document.getElementById("bbox-list-tbody");
  const dsrbody = document.getElementById("dsr-line-table-body");
  const dsrstatbody = document.getElementById("rov-stat-card-body");

  const cfgSelect = document.getElementById("bblog-config-select");
  const cfgGroup = cfgSelect ? cfgSelect.closest(".input-group") : null;

  if (!form || !typeSelect || !fileInput || !uploadBtn || !modalEl) return;

  const rules = {
    DSR: { accept: ".txt,.csv", help: "DSR files (.txt / .csv)", url: "urlDsr" },
    SURVEY_MANAGER: { accept: ".csv,.txt", help: "Survey Manager (.csv / .txt)", url: "urlSurveyManager" },
    BLACK_BOX: { accept: ".csv,.txt", help: "Black Box logs (.csv / .txt)", url: "urlBlackBox" },
    REC_DB: { accept: ".*,.txt", help: "REC_DB (.* / .txt)", url: "urlRecDb" },
  };

  const setStatus = (t = "") => {
    if (statusEl) statusEl.textContent = t;
  };

  const setCount = (t = "") => {
    if (countEl) countEl.textContent = t;
  };

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
    form.action = "";
    setStatus("");
    setCount("");

    if (helpText) helpText.textContent = "Select file type first";
    setCfgVisibility("");
  }

  function buildSuccessMessage(data) {
    if (data.toast?.message) return data.toast.message;
    if (data.success) return data.success;
    if (data.message) return data.message;

    if (
      data.total_processed !== undefined ||
      data.total_upserted !== undefined ||
      data.total_skipped !== undefined
    ) {
      return `Upload completed. Processed: ${data.total_processed ?? 0}, upserted: ${data.total_upserted ?? 0}, skipped: ${data.total_skipped ?? 0}.`;
    }

    return "Upload completed successfully.";
  }

  const modal = bootstrap.Modal.getOrCreateInstance(modalEl, {
    backdrop: true,
    keyboard: true,
  });

  modalEl.addEventListener("show.bs.modal", () => {
    cleanupBackdrops();
    resetForm();
  });

  modalEl.addEventListener("hidden.bs.modal", () => {
    cleanupBackdrops();
  });

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
    setCfgVisibility(fileType);
  });

  fileInput.addEventListener("change", () => {
    const n = fileInput.files?.length || 0;
    setCount(n ? `Selected: ${n} file(s)` : "");
  });

  form.addEventListener("submit", async (e) => {
    e.preventDefault();

    if (!form.action) {
      showToast({
        title: "Upload",
        message: "Select file type first.",
        type: "warning",
      });
      return;
    }

    const files = fileInput.files;
    if (!files || files.length === 0) {
      showToast({
        title: "Upload",
        message: "Select at least one file.",
        type: "warning",
      });
      return;
    }

    const fileType = typeSelect.value;

    const fd = new FormData();
    fd.append("file_type", fileType);

    if (fileType === "BLACK_BOX") {
      const cfgId = cfgSelect?.value || "";
      if (!cfgId) {
        showToast({
          title: "Black Box upload",
          message: "Select BBLog configuration.",
          type: "warning",
        });
        return;
      }
      fd.append("config_id", cfgId);
    }

    for (const f of files) fd.append("files", f);

    uploadBtn.disabled = true;
    const oldHtml = uploadBtn.innerHTML;
    uploadBtn.innerHTML = `<span class="spinner-border spinner-border-sm me-2"></span>Uploading...`;
    setStatus(`Uploading ${files.length} file(s)...`);

    try {
      const resp = await fetch(form.action, {
        method: "POST",
        headers: { "X-CSRFToken": getCSRFToken() },
        body: fd,
      });

      const data = await resp.json().catch(() => ({}));

      if (!resp.ok) {
        throw new Error(data.error || data.message || `Upload failed (${resp.status})`);
      }

      if (data.ok && data.bbox_file_tbody !== undefined && tbody) {
        tbody.innerHTML = data.bbox_file_tbody;
      }

      if (data.dsr_lines_body && dsrbody) {
        dsrbody.innerHTML = data.dsr_lines_body;
      }

      if (data.dsr_statistics_table && dsrstatbody) {
        dsrstatbody.innerHTML = data.dsr_statistics_table;
      }

      const toastPayload = data.toast || {
        title: "Upload",
        message: buildSuccessMessage(data),
        type: "success",
      };

      modalEl.addEventListener(
        "hidden.bs.modal",
        () => showToast(toastPayload),
        { once: true }
      );

      modal.hide();
    } catch (err) {
      console.error(err);
      const msg = err.message || "Upload failed";
      setStatus(msg);

      showToast({
        title: "Upload failed",
        message: msg,
        type: "danger",
      });
    } finally {
      uploadBtn.disabled = false;
      uploadBtn.innerHTML = oldHtml;
    }
  });

  setCfgVisibility("");
}