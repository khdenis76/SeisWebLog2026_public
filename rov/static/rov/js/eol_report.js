export function initEOLReport() {
  const btn = document.getElementById("btn-eol-report");
  const modalEl = document.getElementById("eolReportModal");

  if (!btn || !modalEl) return;

  const runBtn = modalEl.querySelector("#btn-run-eol-report");
  const form = modalEl.querySelector("#eol-report-form");
  const selectedCountEl = modalEl.querySelector("#eol-selected-lines-count");
  const linesJsonEl = modalEl.querySelector("#eol-lines-json");

  if (!runBtn || !form || !selectedCountEl || !linesJsonEl) return;

  function getCSRFToken() {
    const el = document.querySelector("[name=csrfmiddlewaretoken]");
    return el ? el.value : "";
  }

  function getSelectedLines() {
    return Array.from(document.querySelectorAll(".dsr-line-checkbox:checked"))
      .map((cb) => (cb.value || "").trim())
      .filter((v) => v !== "");
  }

  function refreshButtonState() {
    btn.disabled = getSelectedLines().length === 0;
  }

  function updateParentChildrenState() {
    modalEl.querySelectorAll(".eol-parent[data-children]").forEach((parent) => {
      const selector = parent.getAttribute("data-children");
      if (!selector) return;

      const children = modalEl.querySelectorAll(selector);
      children.forEach((child) => {
        child.disabled = !parent.checked;
        if (!parent.checked) {
          child.checked = false;
        }
      });
    });
  }

  function collectPayload(lines) {
    const fd = new FormData(form);

    return {
      lines: lines,
      project_name: fd.get("project_name") || "",
      prepared_by: fd.get("prepared_by") || "",
      client_name: fd.get("client_name") || "",
      client_logo_path: fd.get("client_logo_path") || "",
      comments_text: fd.get("comments_text") || "",
      output_mode: fd.get("output_mode") || "auto",
      page_size: fd.get("page_size") || "A4",
      include_tgs_logo: !!form.querySelector('[name="include_tgs_logo"]')?.checked,
      include_client_logo: !!form.querySelector('[name="include_client_logo"]')?.checked,
      include_page_numbers: !!form.querySelector('[name="include_page_numbers"]')?.checked,
      auto_orientation: !!form.querySelector('[name="auto_orientation"]')?.checked,
      sections: Array.from(
        form.querySelectorAll('input[name="sections"]:checked')
      ).map((el) => el.value),
    };
  }

  async function postGenerate(payload) {
    const generateUrl = btn.dataset.generateUrl;
    if (!generateUrl) {
      throw new Error("EOL generate URL is missing.");
    }

    const resp = await fetch(generateUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRFToken": getCSRFToken(),
        "X-Requested-With": "XMLHttpRequest",
      },
      body: JSON.stringify(payload),
    });

    if (!resp.ok) {
      let message = "Report generation failed.";
      try {
        const err = await resp.json();
        if (err && err.error) {
          message = err.error;
        }
      } catch (_) {
        // ignore JSON parse failure
      }
      throw new Error(message);
    }

    return await resp.blob();
  }

  function downloadBlob(blob, filename) {
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    window.URL.revokeObjectURL(url);
  }

  function updateModalSelectedLines() {
    const lines = getSelectedLines();
    linesJsonEl.value = JSON.stringify(lines);
    selectedCountEl.textContent = String(lines.length);
    return lines;
  }

  function setRunButtonBusy(isBusy) {
    if (isBusy) {
      runBtn.disabled = true;
      runBtn.innerHTML =
        '<span class="spinner-border spinner-border-sm me-2"></span>Generating...';
    } else {
      runBtn.disabled = false;
      runBtn.innerHTML =
        '<i class="fa-solid fa-gears me-2"></i>Generate Report';
    }
  }

  document.addEventListener("change", (ev) => {
    if (
      ev.target.matches(".dsr-line-checkbox") ||
      ev.target.id === "dsr-check-all"
    ) {
      refreshButtonState();
    }
  });

  refreshButtonState();

  btn.addEventListener("click", () => {
    updateModalSelectedLines();
    updateParentChildrenState();
  });

  modalEl.querySelectorAll(".eol-parent[data-children]").forEach((parent) => {
    parent.addEventListener("change", () => {
      updateParentChildrenState();
    });
  });

  runBtn.addEventListener("click", async () => {
    const lines = updateModalSelectedLines();

    if (!lines.length) {
      alert("Please select at least one line.");
      return;
    }

    setRunButtonBusy(true);

    try {
      const payload = collectPayload(lines);

      if (!payload.sections.length) {
        throw new Error("Please select at least one report section.");
      }

      const blob = await postGenerate(payload);

      const filename =
        lines.length === 1 ? `EOL_Line_${lines[0]}.pdf` : "EOL_Reports.zip";

      downloadBlob(blob, filename);

      const bsModal = bootstrap.Modal.getInstance(modalEl);
      if (bsModal) {
        bsModal.hide();
      }
    } catch (err) {
      alert(err.message || "Failed to generate EOL report.");
    } finally {
      setRunButtonBusy(false);
    }
  });
}