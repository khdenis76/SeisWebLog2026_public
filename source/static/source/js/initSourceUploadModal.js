  function setRequired(el, on) {
    if (!el) return;
    if (on) el.setAttribute("required", "required");
    else el.removeAttribute("required");
  }

  export function initSourceUploadModal() {
    const fileType = document.getElementById("source-file-type");
    const spsBlock = document.getElementById("sps-options");
    const shotBlock = document.getElementById("shot-options");

    const spsVessel = document.getElementById("sps-vessel");
    const spsRev = document.getElementById("sps-revision");
    const spsTier = document.getElementById("sps-tier");
    const spsYear = document.getElementById("sps-year");

    if (!fileType) return;

    function updateUI() {
      const v = (fileType.value || "").toUpperCase();

      if (v === "SPS") {
        spsBlock && spsBlock.classList.remove("d-none");
        shotBlock && shotBlock.classList.add("d-none");

        setRequired(spsVessel, true);
        setRequired(spsRev, true);
        setRequired(spsTier, true);
        setRequired(spsYear, true);
      } else if (v === "SHOT") {
        spsBlock && spsBlock.classList.add("d-none");
        shotBlock && shotBlock.classList.remove("d-none");

        setRequired(spsVessel, false);
        setRequired(spsRev, false);
        setRequired(spsTier, false);
        setRequired(spsYear, false);
      } else {
        spsBlock && spsBlock.classList.add("d-none");
        shotBlock && shotBlock.classList.add("d-none");

        setRequired(spsVessel, false);
        setRequired(spsRev, false);
        setRequired(spsTier, false);
        setRequired(spsYear, false);
      }
    }

    fileType.addEventListener("change", updateUI);
    updateUI();
  }

  // Expose init for your global init pattern if you want:
  window.initSourceUploadModal = initSourceUploadModal;

  // Auto-init on DOM ready
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initSourceUploadModal);
  } else {
    initSourceUploadModal();
  }
