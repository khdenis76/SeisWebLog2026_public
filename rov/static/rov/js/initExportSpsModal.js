// initExportSpsModal.js
import { getCSRFToken } from "../../baseproject/js/csrf.js";

export function initExportSpsModal() {
    const modalEl = document.getElementById("export-sps-modal");
    if (!modalEl) return;

    const form = document.getElementById("export-sps-form");
    if (!form) return;

    const exportBtn = document.getElementById("export-sps-btn") || modalEl.querySelector("button[data-url]");
    if (!exportBtn) return;

    let statusEl = document.getElementById("export-sps-status");

    if (!statusEl) {
        statusEl = document.createElement("div");
        statusEl.id = "export-sps-status";
        statusEl.className = "px-3 pb-3";
        const modalBody = modalEl.querySelector(".modal-body");
        if (modalBody) {
            modalBody.appendChild(statusEl);
        }
    }

    const setStatus = (html) => {
        statusEl.innerHTML = html || "";
    };

    const setBtnLoading = (isLoading) => {
        exportBtn.disabled = isLoading;
        exportBtn.innerHTML = isLoading
            ? `<span class="spinner-border spinner-border-sm me-2"></span>Exporting...`
            : `<i class="fas fa-file-export me-2"></i>Export SPS`;
    };

    exportBtn.addEventListener("click", async () => {
        const url = exportBtn.dataset.url;
        if (!url) return;

        const selectedLines = Array.from(
            document.querySelectorAll(".dsr-line-checkbox:checked")
        )
            .map((cb) => cb.value || cb.dataset.line)
            .filter(Boolean);

        if (!selectedLines.length) {
            setStatus(`
                <div class="alert alert-warning mb-0">
                    <i class="fas fa-triangle-exclamation me-2"></i>
                    Select at least one line.
                </div>
            `);
            return;
        }

        const fd = new FormData(form);

        ["export_header", "use_seq", "use_line_seq", "use_line_fn"].forEach((name) => {
            if (!fd.has(name)) fd.append(name, "0");
        });

        fd.set("selected_lines", JSON.stringify(selectedLines));

        // New coordinate unit fields
        fd.set("xy_unit", document.getElementById("sps-xy-unit")?.value || "m");
        fd.set("z_unit", document.getElementById("sps-z-unit")?.value || "m");

        try {
            setStatus("");
            setBtnLoading(true);

            const resp = await fetch(url, {
                method: "POST",
                headers: {
                    "X-CSRFToken": getCSRFToken(),
                    "X-Requested-With": "XMLHttpRequest",
                },
                body: fd,
            });

            const data = await resp.json();

            if (!resp.ok || !data?.ok) {
                const msg = data?.message || data?.error || `Export failed HTTP ${resp.status}`;
                const details = data?.errors
                    ? `<pre class="mb-0 mt-2">${JSON.stringify(data.errors, null, 2)}</pre>`
                    : "";

                setStatus(`
                    <div class="alert alert-danger mb-0">
                        <i class="fas fa-circle-xmark me-2"></i>
                        ${msg}
                        ${details}
                    </div>
                `);
                return;
            }

            const filesHtml = Array.isArray(data.files) && data.files.length
                ? `<ul class="mb-0 mt-2">${data.files.map((f) => `<li>${f}</li>`).join("")}</ul>`
                : "";

            setStatus(`
                <div class="alert alert-success mb-0">
                    <i class="fas fa-circle-check me-2"></i>
                    ${data.message || "SPS export completed."}
                    ${filesHtml}
                </div>
            `);

        } catch (err) {
            console.error(err);
            setStatus(`
                <div class="alert alert-danger mb-0">
                    <i class="fas fa-bug me-2"></i>
                    ${err.message || "Unexpected SPS export error."}
                </div>
            `);
        } finally {
            setBtnLoading(false);
        }
    });
}