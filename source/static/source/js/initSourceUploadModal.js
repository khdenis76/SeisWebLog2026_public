function setRequired(el, on) {
  if (!el) return;
  if (on) el.setAttribute("required", "required");
  else el.removeAttribute("required");
}

export function initSourceUploadModal() {
  const fileType  = document.getElementById("source-file-type");
  const spsBlock  = document.getElementById("sps-options");
  const shotBlock = document.getElementById("shot-options");

  const spsVessel = document.getElementById("sps-vessel");
  const spsRev    = document.getElementById("sps-revision");
  const spsTier   = document.getElementById("sps-tier");
  const spsYear   = document.getElementById("sps-year");

  const detectBySeq = document.getElementById("detect-vessel-by-seq");
  const spsSeq      = document.getElementById("sps-seq-number"); // optional

  // NEW: auto year checkbox
  const autoYear = document.getElementById("auto-year-by-jday");

  if (!fileType) return;

  function selectDefaultSpsRevision() {
    if (!spsRev) return;
    const opt = spsRev.querySelector('option[data-default="1"]');
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

    // we don't use manual seq anymore
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
      // If auto year is ON, year is not required from UI
      setRequired(spsYear, !on);
    }
  }

  function updateUI() {
    const v = (fileType.value || "").toUpperCase();

    if (v === "SPS") {
      spsBlock && spsBlock.classList.remove("d-none");
      shotBlock && shotBlock.classList.add("d-none");

      setRequired(spsRev, true);
      setRequired(spsTier, true);

      // If auto-year ON -> year not required; else required
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

    if (autoYear) autoYear.checked = true; // optional default reset
    updateAutoYearUI();
  }

  fileType.addEventListener("change", updateUI);
  detectBySeq && detectBySeq.addEventListener("change", updateDetectUI);
  autoYear && autoYear.addEventListener("change", updateAutoYearUI);

  updateUI();
}

window.initSourceUploadModal = initSourceUploadModal;

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initSourceUploadModal);
} else {
  initSourceUploadModal();
}