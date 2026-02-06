import { ensureLoadingModal,cleanupModalArtifacts} from "./modalLoader.js";
import { getCSRFToken } from "./csrf.js";
import { showUploadToast } from "./toast.js";
import { updatePreplotTable } from "./updaterltable.js";
import { renderBokehInto } from "./renderBokeh.js"



export function initPreplotUpload() {
  const form = document.getElementById("preplot-load-form");
  const hdr_editor1 = document.getElementById("sps-header1")
  const hdr_editor2 = document.getElementById("sps-header21")
  if (!form) return;
  let lottieAnim = null;

function initLoadingLottie() {
  const L = window.lottie;
  if (!L) {
    console.error("lottie-web not loaded (window.lottie is undefined)");
    return;
  }

  const box = document.getElementById("loading"); // или #loadingLottie если переименуешь
  if (!box) {
    console.error("Lottie container not found (#loading)");
    return;
  }

  box.innerHTML = "";

  // если уже создавали — не создаём заново
  if (lottieAnim) return;

  lottieAnim = L.loadAnimation({
    container: box,
    renderer: "svg",
    loop: true,
    autoplay: true,
    path: "/static/baseproject/lottie/Loading.json",
  });

  lottieAnim.addEventListener("data_failed", () => {
    console.error("Lottie data_failed (check JSON url/path)");
  });

  lottieAnim.setSpeed(0.8);
}


  const uploadModalEl = document.getElementById("uploadModal");
  const uploadModal = uploadModalEl
    ? bootstrap.Modal.getOrCreateInstance(uploadModalEl)
    : null;

  const submitBtn = document.querySelector(
    'button[form="preplot-load-form"][type="submit"]'
  );

  // --- helpers to avoid race conditions with Bootstrap transitions ---
  function once(el, eventName) {
    return new Promise((resolve) =>
      el.addEventListener(eventName, resolve, { once: true })
    );
  }

  async function showModal(modal, el) {
    modal.show();
    await once(el, "shown.bs.modal");
  }

  async function hideModal(modal, el) {
    modal.hide();
    await once(el, "hidden.bs.modal");
  }

  function safeText(err) {
    if (!err) return "";
    if (typeof err === "string") return err;
    return err.message || String(err);
  }

  form.addEventListener("submit", async (e) => {
    e.preventDefault();

    const formData = new FormData(form);
    const fileType = formData.get("file_type"); // SRC_SPS / REC_SPS / HDR_SPS

    const url =
      window.SPS_UPLOAD_URLS?.[fileType] ||
      form.getAttribute("action") ||
      window.location.href;

    // keep your strict check (but do NOT throw before try/finally)
    if (!window.SPS_UPLOAD_URLS?.[fileType]) {
      showUploadToast({
        title: "Upload failed",
        body: `Unknown file_type: ${fileType}`,
        delay: 10000,
        variant: "danger",
      });
      return;
    }

    if (submitBtn) submitBtn.disabled = true;

    let loadingModal = null;
    let loadingModalEl = null;

    try {
      // 1) Ensure loading modal HTML exists
      await ensureLoadingModal();
      loadingModalEl = document.getElementById("loadingModal");
      if (!loadingModalEl) throw new Error("loadingModal element not found");

      // 2) Create/get ONE instance
      loadingModal = bootstrap.Modal.getOrCreateInstance(loadingModalEl, {
        backdrop: "static",
        keyboard: false,
        focus: false,
      });
      // --- init Lottie ONLY when modal is actually shown ---
   loadingModalEl.addEventListener(
  "shown.bs.modal",
  () => {
    if (!lottieAnim) initLoadingLottie();
    lottieAnim?.play();
  },
  { once: true }
);

      // 3) Hide upload modal first, WAIT until fully hidden
      if (uploadModalEl && uploadModal) {
        uploadModal.hide();
        await once(uploadModalEl, "hidden.bs.modal");
      }

      // 4) Show loading modal and WAIT until fully shown
      await showModal(loadingModal, loadingModalEl);

      // 5) POST
      const resp = await fetch(url, {
        method: "POST",
        body: formData,
        headers: {
          "X-CSRFToken": getCSRFToken(),
          "X-Requested-With": "XMLHttpRequest",
        },
      });

      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(text || `HTTP ${resp.status}`);
      }

      // If your view returns non-JSON, this will throw and still close modal in finally
      const data = await resp.json();
      if (data.preplot_map) {
          renderBokehInto("preplot-map-div", data.preplot_map);
      }
      if (data.prep_stat){
          document.getElementById("preplot-statcard").innerHTML = data.prep_stat
      }
      if (data?.hdr1 !== undefined) {
        hdr_editor1.innerHTML=data.hdr1
       }
      if (data?.hdr2 !== undefined) {
        hdr_editor2.innerHTML=data.hdr2
       }
      let tbodyId = null;
      if (data.point_type === "R") {
        tbodyId = "rlBody";
      } else if (data.point_type === "S") {
        tbodyId = "slBody";
      }

      if (tbodyId) {
        updatePreplotTable(tbodyId, data.rows);
        console.log(`${tbodyId}`)
      } else {
        console.warn("tbodyId is undefined:", data.point_type);
      }

      // Toast success
      let body = `
        Type: <b>${data.upload_type ?? "?"}</b><br>
        Files: <b>${data.files?.length ?? 0}</b><br>
        Points: <b>${data.total_points ?? 0}</b><br>
        Time: <b>${data.elapsed_sec ?? "?"} s</b><br>
      `;
      (data.files || []).forEach((f) => {
        body += `• ${f.name}: ${f.points} points<br>`;
      });

      showUploadToast({ title: "Upload completed", body, delay: 8000 });

      form.reset();
    } catch (err) {
      console.error(err);

      showUploadToast({
        title: "Upload failed",
        body: safeText(err),
        delay: 10000,
        variant: "danger",
      });
    } finally {
      // ✅ ALWAYS close loading modal (and wait until it is actually hidden)
      if (loadingModal && loadingModalEl) {
        if (document.activeElement) document.activeElement.blur();
        submitBtn?.focus();

        try {
          await hideModal(loadingModal, loadingModalEl);
        } catch (e2) {
          // last-resort cleanup if transition got stuck
          console.warn("Could not hide modal cleanly, forcing cleanup:", e2);
          document.body.classList.remove("modal-open");
          document.body.style.removeProperty("overflow");
          document.body.style.removeProperty("padding-right");
          document
            .querySelectorAll(".modal-backdrop")
            .forEach((b) => b.remove());
          loadingModalEl.classList.remove("show");
          loadingModalEl.style.display = "none";
          loadingModalEl.setAttribute("aria-hidden", "true");
          loadingModalEl.removeAttribute("aria-modal");
          loadingModalEl.removeAttribute("role");
        }
      }
      cleanupModalArtifacts();
      if (submitBtn) submitBtn.disabled = false;
    }
  });
}

