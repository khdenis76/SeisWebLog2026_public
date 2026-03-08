import { getCSRFToken } from "../../baseproject/js/csrf.js"; // adjust path if needed

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

export function initSourceUploadSubmit() {
  const form = document.getElementById("source-upload-form");
  const submitBtn = document.getElementById("source-upload-submit");
  const modalEl = document.getElementById("sourceUploadModal");

  if (!form || !submitBtn) return;

  // avoid duplicate binding if initSourceUploadModal() is called many times
  if (form.dataset.uploadBound === "1") return;
  form.dataset.uploadBound = "1";

  form.addEventListener("submit", async function (e) {
    e.preventDefault();

    clearUploadMsg();

    const fd = new FormData(form);

    submitBtn.disabled = true;
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

      const data = await resp.json();

      if (!resp.ok || !data.ok) {
        throw new Error(data.error || "Upload failed.");
      }

      // preferred generic response
      if (data.target_tbody && data.tbody_html) {
        const tbody = document.getElementById(data.target_tbody);
        if (tbody) {
          tbody.innerHTML = data.tbody_html;
        }
      } else {
        // backward compatibility with your current backend
        if (data.file_type === "SHOT" && data.shot_summary) {
          const tbody = document.getElementById("shot-table-tbody");
          if (tbody) tbody.innerHTML = data.shot_summary;
        }

        if (data.file_type === "SPS" && data.sps_summary) {
          const tbody = document.getElementById("sps-table-tbody");
          if (tbody) tbody.innerHTML = data.sps_summary;
        }
      }

      showUploadMsg(
        `${data.file_type} uploaded successfully. Rows inserted: ${data.rows_inserted ?? 0}`,
        "success"
      );

      // reset only file input, keep selections if you want to upload more
      const fileInput = document.getElementById("source-files");
      if (fileInput) fileInput.value = "";

      if (modalEl && window.bootstrap) {
        const modal = bootstrap.Modal.getInstance(modalEl);
        if (modal) {
          setTimeout(() => modal.hide(), 400);
        }
      }
    } catch (err) {
      showUploadMsg(err.message || "Upload failed.", "danger");
    } finally {
      submitBtn.disabled = false;
      submitBtn.innerHTML = oldHTML;
    }
  });
}

export function initSourceUploadModal() {
  const fileType = document.getElementById("source-file-type");
  const spsBlock = document.getElementById("sps-options");
  const shotBlock = document.getElementById("shot-options");

  const spsVessel = document.getElementById("sps-vessel");
  const spsRev = document.getElementById("sps-revision");
  const spsTier = document.getElementById("sps-tier");
  const spsYear = document.getElementById("sps-year");

  const detectBySeq = document.getElementById("detect-vessel-by-seq");
  const spsSeq = document.getElementById("sps-seq-number"); // optional
  const autoYear = document.getElementById("auto-year-by-jday");

  if (!fileType) return;

  function selectDefaultSpsRevision() {
    if (!spsRev) return;
    const opt =
      spsRev.querySelector('option[data-default="1"]') ||
      spsRev.querySelector("option[selected]");
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

    if (spsVessel) {
      spsVessel.disabled = on;
      setRequired(spsVessel, !on);
      if (on) spsVessel.value = "";
    }

    // seq field currently not used manually
    if (spsSeq) {
      spsSeq.disabled = true;
      setRequired(spsSeq, false);
      spsSeq.value = "";
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

  // avoid duplicate UI binding if init runs multiple times
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