import { getCSRFToken } from "../../baseproject/js/csrf.js";

function showUploadMsg(text, kind = "success") {
  const wrap = document.getElementById("source-upload-msg-wrap");
  const box = document.getElementById("source-upload-msg");
  if (!wrap || !box) return;

  wrap.classList.remove("d-none");
  box.className = `alert mb-0 alert-${kind}`;
  box.textContent = text;
}

export function initSourceUploadSubmit() {

  const form = document.getElementById("source-upload-form");
  const submitBtn = document.getElementById("source-upload-submit");
  const modalEl = document.getElementById("sourceUploadModal");

  if (!form) return;

  form.addEventListener("submit", async function (e) {

    e.preventDefault();

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
        throw new Error(data.error || "Upload failed");
      }

      // update table
      if (data.target_tbody && data.tbody_html) {
        const tbody = document.getElementById(data.target_tbody);
        if (tbody) {
          tbody.innerHTML = data.tbody_html;
        }
      }

      showUploadMsg(
        `${data.file_type} uploaded. Rows inserted: ${data.rows_inserted}`,
        "success"
      );

      // close modal
      if (modalEl && window.bootstrap) {
        const modal = bootstrap.Modal.getInstance(modalEl);
        modal && modal.hide();
      }

    } catch (err) {

      showUploadMsg(err.message || "Upload failed", "danger");

    } finally {

      submitBtn.disabled = false;
      submitBtn.innerHTML = oldHTML;

    }

  });
}