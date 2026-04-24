export function initEOLReport() {
  const btn = document.getElementById("btn-eol-report");
  const modalEl = document.getElementById("eolReportModal");

  if (!btn || !modalEl) return;

  const runBtn = modalEl.querySelector("#btn-run-eol-report");
  const form = modalEl.querySelector("#eol-report-form");
  const selectedCountEl = modalEl.querySelector("#eol-selected-lines-count");
  const linesJsonEl = modalEl.querySelector("#eol-lines-json");
  const selectAllSectionsBtn = modalEl.querySelector("#eol-select-all-sections");
  const clearAllSectionsBtn = modalEl.querySelector("#eol-clear-all-sections");

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

  function getSectionInputs() {
    return Array.from(form.querySelectorAll('input[name="sections"]'));
  }

  function getParentInputs() {
    return Array.from(modalEl.querySelectorAll(".eol-parent[data-children]"));
  }

  function getChildInputs() {
    return Array.from(
      modalEl.querySelectorAll(
        ".eol-child-deployment, .eol-child-bbox, .eol-child-source, .eol-child-recovery"
      )
    );
  }

  function updateParentChildrenState() {
    getParentInputs().forEach((parent) => {
      const selector = parent.getAttribute("data-children");
      if (!selector) return;

      const children = Array.from(modalEl.querySelectorAll(selector));
      children.forEach((child) => {
        child.disabled = !parent.checked;
        if (!parent.checked) {
          child.checked = false;
        }
      });
    });
  }

  function syncParentFromChildren(parent) {
    const selector = parent.getAttribute("data-children");
    if (!selector) return;

    const children = Array.from(modalEl.querySelectorAll(selector));
    if (!children.length) return;

    const checkedCount = children.filter((child) => child.checked).length;

    if (checkedCount === 0) {
      parent.checked = false;
      parent.indeterminate = false;
      return;
    }

    if (checkedCount === children.length) {
      parent.checked = true;
      parent.indeterminate = false;
      return;
    }

    parent.checked = true;
    parent.indeterminate = true;
  }

  function syncAllParentsFromChildren() {
    getParentInputs().forEach((parent) => syncParentFromChildren(parent));
  }

  function updateModalSelectedLines() {
    const lines = getSelectedLines();
    linesJsonEl.value = JSON.stringify(lines);
    selectedCountEl.textContent = String(lines.length);
    return lines;
  }

  function collectPayload(lines) {
    const fd = new FormData(form);

    return {
      lines: lines,
      prepared_by: fd.get("prepared_by") || "",
      comments_text: fd.get("comments_text") || "",
      output_mode: fd.get("output_mode") || "auto",
      page_size: fd.get("page_size") || "A4",
      include_tgs_logo: !!form.querySelector('[name="include_tgs_logo"]')?.checked,
      include_page_numbers: !!form.querySelector('[name="include_page_numbers"]')?.checked,
      auto_orientation: !!form.querySelector('[name="auto_orientation"]')?.checked,
      bbox_hours_per_page: Number(fd.get("bbox_hours_per_page") || 6),

      // Front Page and Table of Contents are generated automatically
      sections: getSectionInputs()
        .filter((el) => el.checked)
        .map((el) => el.value),
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
        // ignore
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

  function setRunButtonBusy(isBusy) {
    if (isBusy) {
      runBtn.disabled = true;
      runBtn.innerHTML = "Generating...";
    } else {
      runBtn.disabled = false;
      runBtn.innerHTML = "Generate Report";
    }
  }

  function selectAllSections() {
    getSectionInputs().forEach((input) => {
      input.disabled = false;
      input.checked = true;
    });

    getParentInputs().forEach((parent) => {
      parent.indeterminate = false;
    });

    updateParentChildrenState();
    syncAllParentsFromChildren();
  }

  function clearAllSections() {
    getSectionInputs().forEach((input) => {
      input.checked = false;
      input.disabled = false;
    });

    getParentInputs().forEach((parent) => {
      parent.indeterminate = false;
    });

    updateParentChildrenState();
    syncAllParentsFromChildren();
  }

  document.addEventListener("change", (ev) => {
    if (ev.target.matches(".dsr-line-checkbox") || ev.target.id === "dsr-check-all") {
      refreshButtonState();
    }
  });

  btn.addEventListener("click", () => {
    updateModalSelectedLines();
    updateParentChildrenState();
    syncAllParentsFromChildren();
  });

  getParentInputs().forEach((parent) => {
    parent.addEventListener("change", () => {
      parent.indeterminate = false;
      updateParentChildrenState();

      const selector = parent.getAttribute("data-children");
      if (!selector) return;

      const children = Array.from(modalEl.querySelectorAll(selector));
      if (parent.checked) {
        children.forEach((child) => {
          child.disabled = false;
          child.checked = true;
        });
      }
    });
  });

  getChildInputs().forEach((child) => {
    child.addEventListener("change", () => {
      syncAllParentsFromChildren();
      updateParentChildrenState();
    });
  });

  if (selectAllSectionsBtn) {
    selectAllSectionsBtn.addEventListener("click", () => {
      selectAllSections();
    });
  }

  if (clearAllSectionsBtn) {
    clearAllSectionsBtn.addEventListener("click", () => {
      clearAllSections();
    });
  }

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
        throw new Error("Please select at least one report chapter.");
      }

      if (!Number.isFinite(payload.bbox_hours_per_page) || payload.bbox_hours_per_page < 1) {
        throw new Error("BBox QC: hours per page must be 1 or greater.");
      }

      const blob = await postGenerate(payload);
      const filename = lines.length === 1
        ? `EOL_Line_${lines[0]}.pdf`
        : "EOL_Reports.zip";

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

  refreshButtonState();
  updateParentChildrenState();
  syncAllParentsFromChildren();
}