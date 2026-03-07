function showUploadMsg(text, kind = "success") {
  const wrap = document.getElementById("source-upload-msg-wrap");
  const box = document.getElementById("source-upload-msg");
  if (!wrap || !box) return;

  wrap.classList.remove("d-none");
  box.className = `alert mb-0 alert-${kind}`;
  box.textContent = text;
}

function getCookie(name) {
  const v = `; ${document.cookie}`;
  const parts = v.split(`; ${name}=`);
  if (parts.length === 2) return parts.pop().split(";").shift();
  return "";
}

export function initSourceUploadSubmit() {
  const form = document.getElementById("source-upload-form");
  const submitBtn = document.getElementById("source-upload-submit");
  const modalEl = document.getElementById("sourceUploadModal");

  if (!form) return;

  form.addEventListener("submit", async function (e) {
    e.preventDefault();

    const fd = new FormData(form);
    const fileType = String(fd.get("file_type") || "").toUpperCase();

    if (!fileType) {
      showUploadMsg("Please select file type.", "danger");
      return;
    }

    submitBtn && (submitBtn.disabled = true);
    if (submitBtn) {
      submitBtn.dataset.oldHtml = submitBtn.innerHTML;
      submitBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Uploading...';
    }

    try {
      const resp = await fetch(form.action, {
        method: "POST",
        body: fd,
        headers: {
          "X-CSRFToken": getCookie("csrftoken"),
        },
      });

      const data = await resp.json();

      if (!resp.ok || !data.ok) {
        throw new Error(data.error || "Upload failed.");
      }

      if (data.target_tbody && data.tbody_html) {
        const tbody = document.getElementById(data.target_tbody);
        if (tbody) {
          tbody.innerHTML = data.tbody_html;
        }
      }

      let msg = `${data.file_type} uploaded successfully. Rows inserted: ${data.rows_inserted || 0}`;
      showUploadMsg(msg, "success");

      // optional: reset form after success
      form.reset();

      // optional: re-run modal UI logic after reset
      if (window.initSourceUploadModal) {
        window.initSourceUploadModal();
      }

      // close modal after short delay
      if (modalEl && window.bootstrap) {
        const modal = bootstrap.Modal.getInstance(modalEl) || new bootstrap.Modal(modalEl);
        setTimeout(() => modal.hide(), 500);
      }

    } catch (err) {
      showUploadMsg(err.message || "Upload failed.", "danger");
    } finally {
      if (submitBtn) {
        submitBtn.disabled = false;
        submitBtn.innerHTML = submitBtn.dataset.oldHtml || '<i class="fas fa-cloud-upload-alt me-2"></i>Upload';
      }
    }
  });
}