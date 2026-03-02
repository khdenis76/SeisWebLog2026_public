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

  function applyReturnedTableHtml(data) {
    // SHOT table
    if (data.shot_summary) {
      const shotTbody = document.getElementById("shot-table-tbody");
      if (shotTbody) shotTbody.innerHTML = data.shot_summary;
    }

    // SPS table
    if (data.sps_summary) {
      const spsTbody = document.getElementById("sps-table-tbody");
      if (spsTbody) spsTbody.innerHTML = data.sps_summary;
    }
  }

  form.addEventListener("submit", async (e) => {
    e.preventDefault();

    const formData = new FormData(form);
    setAlert("info", "Uploading and loading...");
    disableForm(true);

    const originalBtnHtml = submitBtn.innerHTML;
    submitBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Working...';

    try {
      const res = await fetch(form.action, {
        method: "POST",
        headers: { "X-CSRFToken": getCSRFToken() },
        body: formData,
        credentials: "same-origin",
      });

      const parsed = await readResponse(res);

      if (!res.ok) {
        if (parsed.isJson) {
          throw new Error(parsed.data?.error || `Request failed (${res.status})`);
        }
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

      // Update table tbody immediately (SHOT or SPS) if server returned HTML
      applyReturnedTableHtml(data);

      setAlert("success", "Inserted rows: " + (data.rows_inserted || 0));

      setTimeout(() => {
        const modalInstance = bootstrap.Modal.getInstance(modalEl);
        if (modalInstance) modalInstance.hide();

        form.reset();
        msgWrap.classList.add("d-none");

        // Optional: keep your hook if you still want it for other UI updates
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