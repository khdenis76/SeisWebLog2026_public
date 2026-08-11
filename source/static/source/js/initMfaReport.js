function getCookie(name) {
    const prefix = `${name}=`;
    for (const item of document.cookie.split(";")) {
        const cookie = item.trim();
        if (cookie.startsWith(prefix)) {
            return decodeURIComponent(cookie.slice(prefix.length));
        }
    }
    return "";
}

function downloadBlob(blob, filename) {
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
}

export function initMfaReport() {
    const table = document.querySelector(".mfa-files-table");
    const button = document.getElementById("mfa-generate-report-btn");
    const selectAll = document.getElementById("mfa-select-all");
    const status = document.getElementById("mfa-selection-status");
    if (!table || !button || !status) return;

    const checkboxes = () => Array.from(
        table.querySelectorAll(".mfa-file-checkbox")
    );
    const selected = () => checkboxes().filter(item => item.checked);

    function refreshSelection() {
        const all = checkboxes();
        const chosen = selected();
        all.forEach(item => {
            item.closest("tr")?.classList.toggle("mfa-row-selected", item.checked);
        });

        button.disabled = chosen.length !== 1;
        if (chosen.length === 0) {
            status.textContent = "No file selected";
        } else if (chosen.length === 1) {
            status.textContent = "1 file selected";
        } else {
            status.textContent = `${chosen.length} selected - choose one for a report`;
        }

        if (selectAll) {
            selectAll.checked = all.length > 0 && chosen.length === all.length;
            selectAll.indeterminate = chosen.length > 0 && chosen.length < all.length;
        }
    }

    table.addEventListener("change", event => {
        if (event.target.classList.contains("mfa-file-checkbox")) {
            refreshSelection();
        }
    });

    if (selectAll) {
        selectAll.addEventListener("change", () => {
            checkboxes().forEach(item => { item.checked = selectAll.checked; });
            refreshSelection();
        });
    }

    button.addEventListener("click", async () => {
        const chosen = selected();
        if (chosen.length !== 1) return;

        const originalHtml = button.innerHTML;
        button.disabled = true;
        status.classList.remove("text-danger");
        button.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Generating...';
        status.textContent = "Building PDF report...";

        const body = new FormData();
        body.append("mfa_file_id", chosen[0].value);

        try {
            const response = await fetch(button.dataset.reportUrl, {
                method: "POST",
                body,
                headers: {
                    "X-CSRFToken": getCookie("csrftoken"),
                    "X-Requested-With": "XMLHttpRequest",
                },
            });

            if (!response.ok) {
                const contentType = response.headers.get("content-type") || "";
                const error = contentType.includes("application/json")
                    ? (await response.json()).error
                    : `Report generation failed (${response.status})`;
                throw new Error(error);
            }

            const disposition = response.headers.get("content-disposition") || "";
            const utf8Match = disposition.match(/filename\*=UTF-8''([^;]+)/i);
            const plainMatch = disposition.match(/filename="?([^";]+)"?/i);
            const filename = utf8Match
                ? decodeURIComponent(utf8Match[1])
                : (plainMatch ? plainMatch[1] : "MFA_QC_Report.pdf");
            downloadBlob(await response.blob(), filename);
            status.textContent = "Report generated";
        } catch (error) {
            status.textContent = error.message || String(error);
            status.classList.add("text-danger");
        } finally {
            button.innerHTML = originalHtml;
            button.disabled = selected().length !== 1;
        }
    });

    refreshSelection();
}
