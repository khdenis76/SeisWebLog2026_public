import { ensureLoadingModal } from "./modalLoader.js";
import { getCSRFToken } from "./csrf.js";
import { showUploadToast } from "./toast.js";
import { updateRLPreplotTable } from "./updaterltable.js";

export function initPreplotUpload() {
  const form = document.getElementById("preplot-load-form");
  if (!form) return;

  const uploadModalEl = document.getElementById("uploadModal");
  const uploadModal = uploadModalEl
    ? bootstrap.Modal.getOrCreateInstance(uploadModalEl)
    : null;

  const submitBtn = document.querySelector('button[form="preplot-load-form"][type="submit"]');

  form.addEventListener("submit", async (e) => {
    e.preventDefault();

    const url = form.getAttribute("action") || window.location.href;
    const formData = new FormData(form);

    if (submitBtn) submitBtn.disabled = true;

    let loadingModal = null;

    try {
      // 1) Ensure HTML exists
      await ensureLoadingModal();

      const loadingModalEl = document.getElementById("loadingModal");

      // 2) Create/get ONE instance (важно: focus:false)
      loadingModal = bootstrap.Modal.getOrCreateInstance(loadingModalEl, {
        backdrop: "static",
        keyboard: false,
        focus: false,
      });

      // 3) show loading after upload modal hidden (или сразу)
      const showLoading = () => loadingModal.show();

      if (uploadModalEl && uploadModal) {
        uploadModalEl.addEventListener("hidden.bs.modal", showLoading, { once: true });
        uploadModal.hide();
      } else {
        showLoading();
      }

      // 4) POST
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

      // ⚠️ Если твой view НЕ возвращает JSON — тут будет исключение и раньше ты не прятал модалку
      const data = await resp.json();
      updateRLPreplotTable(data.rows);
      // Toast success
      let body = `
        Files: <b>${data.files?.length ?? 0}</b><br>
        Points: <b>${data.total_points ?? 0}</b><br>
        Time: <b>${data.elapsed_sec ?? "?"} s</b><br>
      `;

      (data.files || []).forEach(f => {
        body += `• ${f.name}: ${f.points} points<br>`;
      });

      showUploadToast({ title: "Upload completed", body, delay: 8000 });

      form.reset();

    } catch (err) {
      console.error(err);

      // Toast error
      showUploadToast({
        title: "Upload failed",
        body: (err.message || String(err)),
        delay: 10000,
        variant: "danger",
      });

    } finally {
      // ✅ ВСЕГДА прячем loading modal
      if (loadingModal) {
        // убрать фокус из модалки перед hide (убирает aria-hidden warning)
        if (document.activeElement) document.activeElement.blur();
        submitBtn?.focus();

        loadingModal.hide();
      }

      if (submitBtn) submitBtn.disabled = false;
    }
  });
}
