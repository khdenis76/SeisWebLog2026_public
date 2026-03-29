import { getCSRFToken } from "../../baseproject/js/csrf.js";

function setRequired(el, on) {
  if (!el) return;
  if (on) el.setAttribute("required", "required");
  else el.removeAttribute("required");
}

function showUploadMsg(text, kind = "success") {
  const wrap = document.getElementById("source-upload-msg-wrap");
  const box = document.getElementById("source-upload-msg");
  if (!wrap || !box) return;

  wrap.classList.remove("d-none");
  box.className = `alert mb-0 alert-${kind}`;
  box.textContent = text;
}

function clearUploadMsg() {
  const wrap = document.getElementById("source-upload-msg-wrap");
  const box = document.getElementById("source-upload-msg");
  if (!wrap || !box) return;

  wrap.classList.add("d-none");
  box.className = "alert mb-0";
  box.textContent = "";
}

function setBusy(form, submitBtn, busy) {
  if (submitBtn) submitBtn.disabled = !!busy;

  form.querySelectorAll("input, select, button, textarea").forEach((el) => {
    if (el.type === "file") return;
    if (el.id === "source-file-type") return;
    el.disabled = !!busy;
  });

  const modalEl = document.getElementById("sourceUploadModal");
  if (modalEl) {
    modalEl.dataset.uploadBusy = busy ? "1" : "0";

    const closeBtns = modalEl.querySelectorAll(
      '[data-bs-dismiss="modal"], .btn-close'
    );
    closeBtns.forEach((btn) => {
      btn.disabled = !!busy;
    });
  }
}

export function initSourceUploadSubmit() {
  const form = document.getElementById("source-upload-form");
  const submitBtn = document.getElementById("source-upload-submit");
  const fileTypeSelect = document.getElementById("source-file-type");

  if (!form || !submitBtn || !fileTypeSelect) return;

  if (form.dataset.uploadBound === "1") return;
  form.dataset.uploadBound = "1";

  form.addEventListener("submit", async function (e) {
    e.preventDefault();
    clearUploadMsg();

    if (!form.checkValidity()) {
      form.reportValidity();
      return;
    }

    const fd = new FormData(form);
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

    setBusy(form, submitBtn, true);

    const oldHTML = submitBtn.innerHTML;
    submitBtn.innerHTML =
      '<span class="spinner-border spinner-border-sm me-2"></span>Uploading...';

    try {
      const resp = await fetch(form.action, {
        method: "POST",
        body: fd,
        headers: {
          "X-CSRFToken": getCSRFToken(),
        },
      });

      let data = null;
      try {
        data = await resp.json();
      } catch {
        data = null;
      }

      if (!resp.ok || !data?.ok) {
        throw new Error(data?.error || `Upload failed (HTTP ${resp.status})`);
      }

      if (data.target_tbody && data.tbody_html) {
        const tbody = document.getElementById(data.target_tbody);
        if (tbody) {
          tbody.innerHTML = data.tbody_html;
        }
      } else {
        if (data.file_type === "SHOT" && data.shot_summary) {
          const tbody = document.getElementById("shot-summary-tbody");
          if (tbody) tbody.innerHTML = data.shot_summary;
        }

        if (data.file_type === "SPS" && data.sps_summary) {
          const tbody = document.getElementById("sps-table-tbody");
          if (tbody) tbody.innerHTML = data.sps_summary;
        }
      }

      if (data.file_type === "SHOT" && typeof data.st_file_name !== "undefined") {
        const fileNameEl = document.getElementById("shot-table-file-name");
        if (fileNameEl) {
          fileNameEl.textContent = `Last uploaded ST: ${data.st_file_name || "—"}`;
        }
      }

      showUploadMsg(
        `${data.file_type || type} uploaded successfully. Rows inserted: ${data.rows_inserted ?? 0}`,
        "success"
      );

      const fileInput = document.getElementById("source-files");
      if (fileInput) fileInput.value = "";

    } catch (err) {
      showUploadMsg(err.message || "Upload failed.", "danger");
    } finally {
      setBusy(form, submitBtn, false);
      submitBtn.innerHTML = oldHTML;
    }
  });
}

export function initSourceUploadModal() {
  const modalEl = document.getElementById("sourceUploadModal");
  const fileType = document.getElementById("source-file-type");
  const spsBlock = document.getElementById("sps-options");
  const shotBlock = document.getElementById("shot-options");

  const spsVessel = document.getElementById("sps-vessel");
  const spsRev = document.getElementById("sps-revision");
  const spsTier = document.getElementById("sps-tier");
  const spsYear = document.getElementById("sps-year");

  const detectBySeq = document.getElementById("detect-vessel-by-seq");
  const spsSeq = document.getElementById("sps-seq-number");
  const autoYear = document.getElementById("auto-year-by-jday");

  if (!fileType) return;

  function selectDefaultSpsRevision() {
    if (!spsRev) return;
    const opt =
      spsRev.querySelector("option[selected]") ||
      Array.from(spsRev.options).find(opt => opt.value);
    if (opt) spsRev.value = opt.value;
  }

  function selectCurrentYear() {
    if (!spsYear) return;
    const y = String(new Date().getFullYear());
    const opt = spsYear.querySelector(`option[value="${y}"]`);
    if (opt) spsYear.value = y;
  }

  function selectTierOne() {
    if (!spsTier) return;
    const opt = spsTier.querySelector('option[value="1"]');
    if (opt) spsTier.value = "1";
  }

  function updateDetectUI() {
    const on = !!(detectBySeq && detectBySeq.checked);
    const isSps = (fileType && (fileType.value || "").toUpperCase() === "SPS");

    if (spsVessel) {
      spsVessel.disabled = on;
      setRequired(spsVessel, isSps && !on);
      if (on) {
        spsVessel.value = "";
      }
    }

    if (spsSeq) {
      spsSeq.disabled = !(isSps && on);
      setRequired(spsSeq, false);
      if (!on) {
        spsSeq.value = "";
      }
    }
  }

  function updateAutoYearUI() {
    const on = !!(autoYear && autoYear.checked);
    if (spsYear) {
      spsYear.disabled = on;
      setRequired(spsYear, !on);
    }
  }

  function updateUI() {
    const v = (fileType.value || "").toUpperCase();
    clearUploadMsg();

    if (v === "SPS") {
      spsBlock && spsBlock.classList.remove("d-none");
      shotBlock && shotBlock.classList.add("d-none");

      setRequired(spsRev, true);
      setRequired(spsTier, true);

      updateAutoYearUI();
      selectDefaultSpsRevision();
      selectCurrentYear();
      selectTierOne();
      updateDetectUI();
      return;
    }

    if (v === "SHOT") {
      spsBlock && spsBlock.classList.add("d-none");
      shotBlock && shotBlock.classList.remove("d-none");
    } else {
      spsBlock && spsBlock.classList.add("d-none");
      shotBlock && shotBlock.classList.add("d-none");
    }

    setRequired(spsVessel, false);
    setRequired(spsRev, false);
    setRequired(spsTier, false);
    setRequired(spsYear, false);

    if (detectBySeq) detectBySeq.checked = false;
    updateDetectUI();

    if (autoYear) autoYear.checked = true;
    updateAutoYearUI();
  }

  if (modalEl && modalEl.dataset.hideGuardBound !== "1") {
    modalEl.addEventListener("hide.bs.modal", function (e) {
      if (modalEl.dataset.uploadBusy === "1") {
        e.preventDefault();
        e.stopImmediatePropagation();
      }
    });

    modalEl.dataset.hideGuardBound = "1";
  }

  if (fileType.dataset.uiBound !== "1") {
    fileType.addEventListener("change", updateUI);
    detectBySeq && detectBySeq.addEventListener("change", updateDetectUI);
    autoYear && autoYear.addEventListener("change", updateAutoYearUI);
    fileType.dataset.uiBound = "1";
  }

  updateUI();
  initSourceUploadSubmit();
}

window.initSourceUploadModal = initSourceUploadModal;

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initSourceUploadModal);
} else {
  initSourceUploadModal();
}