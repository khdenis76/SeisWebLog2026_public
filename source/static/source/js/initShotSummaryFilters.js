function getEl(id) {
    return document.getElementById(id);
}

function getValue(id) {
    const el = getEl(id);
    if (!el) return "";
    return String(el.value ?? "").trim();
}

function setLoadingRow(tbody) {
    tbody.innerHTML = `
        <tr>
            <td colspan="100" class="text-center py-4 text-muted">
                <span class="spinner-border spinner-border-sm me-2"></span>
                Loading...
            </td>
        </tr>
    `;
}

function setErrorRow(tbody, message = "Failed to load data.") {
    tbody.innerHTML = `
        <tr>
            <td colspan="100" class="text-center py-4 text-danger">
                ${message}
            </td>
        </tr>
    `;
}

function buildParams() {
    const params = new URLSearchParams();

    const fields = {
        nav_line_code: getValue("filterShotNavLineCode"),
        nav_line: getValue("filterShotLine"),
        attempt: getValue("filterShotAttempt"),
        seq_from: getValue("filterShotSeqFrom"),
        seq_to: getValue("filterShotSeqTo"),
        purpose_id: getValue("filterShotPurposeId"),
        vessel_id: getValue("filterShotVesselId"),
        is_in_sl: getValue("filterShotInSL"),
        qc_all_match: getValue("filterShotQcAll"),
        qc_any_match: getValue("filterShotQcAny"),
        diffsum: getValue("filterShotDiffsum")
    };

    Object.entries(fields).forEach(([key, value]) => {
        if (value !== "") {
            params.append(key, value);
        }
    });

    return params;
}

function updateShotSummaryStatus(count) {
    const statusEl = getEl("shot-summary-filter-status");
    if (!statusEl) return;

    const n = Number.isFinite(Number(count)) ? Number(count) : 0;
    statusEl.textContent = `Showing ${n} rows`;
}

function setApplyLoading(isLoading) {
    const btn = getEl("btnApplyShotSummaryFilters");
    if (!btn) return;

    const label = btn.querySelector(".shot-filter-btn-label");
    const loading = btn.querySelector(".shot-filter-btn-loading");

    btn.disabled = isLoading;

    if (label) {
        label.classList.toggle("d-none", isLoading);
    }
    if (loading) {
        loading.classList.toggle("d-none", !isLoading);
    }
}

function setResetLoading(isLoading) {
    const btn = getEl("btnResetShotSummaryFilters");
    if (!btn) return;

    const label = btn.querySelector(".shot-reset-btn-label");
    const loading = btn.querySelector(".shot-reset-btn-loading");

    btn.disabled = isLoading;

    if (label) {
        label.classList.toggle("d-none", isLoading);
    }
    if (loading) {
        loading.classList.toggle("d-none", !isLoading);
    }
}

async function reloadShotSummaryTbody() {
    const tbody = getEl("shot-summary-tbody");
    if (!tbody) {
        console.error("shot-summary-tbody not found");
        return false;
    }

    const url = tbody.dataset.url;
    if (!url) {
        console.error("shot-summary-tbody data-url is missing");
        return false;
    }

    setLoadingRow(tbody);

    try {
        const response = await fetch(`${url}?${buildParams().toString()}`, {
            method: "GET",
            headers: {
                "X-Requested-With": "XMLHttpRequest"
            }
        });

        const data = await response.json();

        if (!response.ok || !data.ok) {
            throw new Error(data?.error || `HTTP ${response.status}`);
        }

        tbody.innerHTML = data.tbody_html || "";
        updateShotSummaryStatus(data.count ?? 0);
        return true;

    } catch (error) {
        console.error("SHOT summary filter load failed:", error);
        setErrorRow(tbody, error.message || "Failed to load data.");
        updateShotSummaryStatus(0);
        return false;
    }
}

function resetShotSummaryFilters() {
    [
        "filterShotNavLineCode",
        "filterShotLine",
        "filterShotAttempt",
        "filterShotSeqFrom",
        "filterShotSeqTo",
        "filterShotPurposeId",
        "filterShotVesselId",
        "filterShotInSL",
        "filterShotQcAll",
        "filterShotQcAny",
        "filterShotDiffsum"
    ].forEach((id) => {
        const el = getEl(id);
        if (el) {
            el.value = "";
        }
    });
}

function hideShotSummaryFilterModal() {
    const modalEl = getEl("shotSummaryFilterModal");
    if (!modalEl || !window.bootstrap) return;

    const modal =
        bootstrap.Modal.getInstance(modalEl) ||
        new bootstrap.Modal(modalEl);

    modal.hide();
}

function bindApplyButton() {
    const btn = getEl("btnApplyShotSummaryFilters");
    if (!btn) return;

    btn.addEventListener("click", async () => {
        setApplyLoading(true);
        try {
            const ok = await reloadShotSummaryTbody();
            if (ok) {
                hideShotSummaryFilterModal();
            }
        } finally {
            setApplyLoading(false);
        }
    });
}

function bindResetButton() {
    const btn = getEl("btnResetShotSummaryFilters");
    if (!btn) return;

    btn.addEventListener("click", async () => {
        setResetLoading(true);
        try {
            resetShotSummaryFilters();
            await reloadShotSummaryTbody();
        } finally {
            setResetLoading(false);
        }
    });
}

function bindEnterToApply() {
    [
        "filterShotNavLineCode",
        "filterShotLine",
        "filterShotAttempt",
        "filterShotSeqFrom",
        "filterShotSeqTo",
        "filterShotPurposeId",
        "filterShotVesselId",
        "filterShotInSL",
        "filterShotQcAll",
        "filterShotQcAny",
        "filterShotDiffsum"
    ].forEach((id) => {
        const el = getEl(id);
        if (!el) return;

        el.addEventListener("keydown", async (e) => {
            if (e.key === "Enter") {
                e.preventDefault();
                setApplyLoading(true);
                try {
                    const ok = await reloadShotSummaryTbody();
                    if (ok) {
                        hideShotSummaryFilterModal();
                    }
                } finally {
                    setApplyLoading(false);
                }
            }
        });
    });
}

export function initShotSummaryFilters() {
    if (!getEl("shotSummaryFilterModal")) return;
    if (!getEl("shot-summary-tbody")) return;

    bindApplyButton();
    bindResetButton();
    bindEnterToApply();
}

export { reloadShotSummaryTbody, resetShotSummaryFilters };