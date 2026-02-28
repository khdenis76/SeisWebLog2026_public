import { getCSRFToken } from "../../baseproject/js/csrf.js"; // adjust path if needed
export function initSourceUpload() {
  const modalEl   = document.getElementById("sourceUploadModal");
  const form      = document.getElementById("source-upload-form");
  const msgWrap   = document.getElementById("source-upload-msg-wrap");
  const msgEl     = document.getElementById("source-upload-msg");
  const submitBtn = document.getElementById("source-upload-submit");

  if (!modalEl || !form) return;

  function setAlert(type, text) {
    if (!msgWrap || !msgEl) return;
    msgWrap.classList.remove("d-none");
    msgEl.className = "alert mb-0 alert-" + type;
    msgEl.textContent = text;
  }

  function disableForm(disabled) {
    form.querySelectorAll("input, select, button, textarea")
      .forEach(el => el.disabled = disabled);
  }

  async function readResponse(res) {
    const ct = (res.headers.get("content-type") || "").toLowerCase();

    if (ct.includes("application/json")) {
      const data = await res.json();
      return { isJson: true, data, text: null };
    }

    const text = await res.text();
    return { isJson: false, data: null, text };
  }

  form.addEventListener("submit", async (e) => {
    e.preventDefault();

    const formData = new FormData(form);
    setAlert("info", "Uploading and loading...");
    disableForm(true);

    const originalBtnHtml = submitBtn.innerHTML;
    submitBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Working...';
    console.log("POST URL:", form.action);
    //console.log("Response status:", res.status, "Final URL:", res.url);
    try {
      const res = await fetch(form.action, {
        method: "POST",
        headers: { "X-CSRFToken": getCSRFToken() },
        body: formData,
        credentials: "same-origin",   // IMPORTANT
      });

      const parsed = await readResponse(res);

      if (!res.ok) {
        if (parsed.isJson) {
          throw new Error(parsed.data?.error || `Request failed (${res.status})`);
        }
        // HTML error/redirect page
        const snippet = (parsed.text || "").slice(0, 300).replace(/\s+/g, " ").trim();
        throw new Error(`Server returned HTML (${res.status}). ${snippet}`);
      }

      if (!parsed.isJson) {
        const snippet = (parsed.text || "").slice(0, 300).replace(/\s+/g, " ").trim();
        throw new Error(`Expected JSON but got HTML. ${snippet}`);
      }

      const data = parsed.data;

      if (!data.ok) {
        throw new Error(data.error || "Upload failed.");
      }

      setAlert("success", "Inserted rows: " + (data.rows_inserted || 0));

      setTimeout(() => {
        const modalInstance = bootstrap.Modal.getInstance(modalEl);
        if (modalInstance) modalInstance.hide();

        form.reset();
        msgWrap.classList.add("d-none");

        if (typeof refreshSourceData === "function") {
          refreshSourceData(data);
        }
      }, 600);

    } catch (err) {
      setAlert("danger", err.message || "Upload failed.");
      console.error(err);
    } finally {
      disableForm(false);
      submitBtn.innerHTML = originalBtnHtml;
    }
  });
}