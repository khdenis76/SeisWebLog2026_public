export function initShotSummaryRecalc() {
    "use strict";

    function getCSRFToken() {
        const input = document.querySelector("[name=csrfmiddlewaretoken]");
        if (input && input.value) return input.value;

        const match = document.cookie.match(/csrftoken=([^;]+)/);
        return match ? decodeURIComponent(match[1]) : "";
    }

    function qs(selector, root = document) {
        return root.querySelector(selector);
    }

    function qsa(selector, root = document) {
        return Array.from(root.querySelectorAll(selector));
    }

    function getTable() {
        return qs("#st-sailline-summary-table");
    }

    function getTbody() {
        return qs("#shot-summary-tbody");
    }

    function getRecalcButton() {
        return qs("#btn-recalc-lines");
    }

    function getCheckAllBox() {
        return qs("#shot-summary-check-all");
    }

    function getRecalcModalEl() {
        return qs("#shotRecalcLinesModal");
    }

    function getConfirmRecalcBtn() {
        return qs("#shot-confirm-recalc-button");
    }

    function getToastEl() {
        return qs("#shotDeleteToast");
    }

    function getToastBodyEl() {
        return qs("#shotDeleteToastBody");
    }

    function getVisibleRows() {
        const tbody = getTbody();
        if (!tbody) return [];

        return qsa("tr[data-line-code]", tbody).filter((row) => row.offsetParent !== null);
    }

    function getSelectedLineCodes() {
        return getVisibleRows()
            .map((row) => qs(".shot-line-check", row))
            .filter((cb) => cb && cb.checked)
            .map((cb) => cb.dataset.lineCode || cb.value || "")
            .map((v) => String(v).trim())
            .filter(Boolean);
    }

    function updateRecalcButtonState() {
        const btn = getRecalcButton();
        if (!btn) return;
        btn.disabled = getSelectedLineCodes().length === 0;
    }

    function showToast(message, isError = false) {
        const toastEl = getToastEl();
        const toastBody = getToastBodyEl();
        if (!toastEl || !toastBody || typeof bootstrap === "undefined") return;

        toastBody.textContent = message;
        toastEl.classList.remove("text-bg-dark", "text-bg-success", "text-bg-danger");
        toastEl.classList.add(isError ? "text-bg-danger" : "text-bg-success");

        bootstrap.Toast.getOrCreateInstance(toastEl, { delay: 4000 }).show();
    }

    function openRecalcModal() {
        const modalEl = getRecalcModalEl();
        if (!modalEl || typeof bootstrap === "undefined") {
            showToast("Recalc modal not found.", true);
            return;
        }

        const selected = getSelectedLineCodes();
        const countEl = qs("#shot-recalc-lines-count");
        const previewEl = qs("#shot-recalc-lines-preview");
        const confirmBtn = getConfirmRecalcBtn();

        if (countEl) {
            countEl.textContent = String(selected.length);
        }

        if (previewEl) {
            if (selected.length) {
                const preview = selected.slice(0, 10).join(", ");
                previewEl.innerHTML = selected.length > 10
                    ? `<strong>Lines:</strong> ${preview} ...`
                    : `<strong>Lines:</strong> ${preview}`;
            } else {
                previewEl.innerHTML = `<span class="text-danger">No lines selected.</span>`;
            }
        }

        if (confirmBtn) {
            confirmBtn.disabled = selected.length === 0;
        }

        bootstrap.Modal.getOrCreateInstance(modalEl).show();
    }

    function closeRecalcModal() {
        const modalEl = getRecalcModalEl();
        if (!modalEl || typeof bootstrap === "undefined") return;

        const modal = bootstrap.Modal.getInstance(modalEl);
        if (modal) modal.hide();
    }

    async function recalcSelectedLines(e) {
        if (e) {
            e.preventDefault();
            e.stopPropagation();
        }

        const table = getTable();
        const tbody = getTbody();
        const selected = getSelectedLineCodes();
        const confirmBtn = getConfirmRecalcBtn();

        if (!table) {
            showToast("SHOT summary table not found.", true);
            return;
        }

        if (!tbody) {
            showToast("SHOT summary tbody not found.", true);
            return;
        }

        if (!selected.length) {
            showToast("No lines selected.", true);
            return;
        }

        const recalcUrl = table.dataset.recalcUrl;
        if (!recalcUrl) {
            showToast("Recalc URL is missing.", true);
            return;
        }

        const originalHtml = confirmBtn ? confirmBtn.innerHTML : "";

        try {
            if (confirmBtn) {
                confirmBtn.disabled = true;
                confirmBtn.innerHTML = `<span class="spinner-border spinner-border-sm me-2"></span>Recalculating...`;
            }

            const response = await fetch(recalcUrl, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "X-CSRFToken": getCSRFToken(),
                    "X-Requested-With": "XMLHttpRequest"
                },
                body: JSON.stringify({
                    nav_line_codes: selected
                })
            });

            const data = await response.json();

            if (!response.ok || !data.success) {
                throw new Error(data.error || "Recalculation failed.");
            }

            if (data.target_tbody === "shot-summary-tbody" && typeof data.tbody_html === "string") {
                tbody.innerHTML = data.tbody_html;
            }

            closeRecalcModal();
            updateRecalcButtonState();
            showToast(`Recalculated ${data.recalculated_lines || 0} line(s).`);

        } catch (error) {
            console.error("Recalc selected lines error:", error);
            showToast(error.message || "Recalculation failed.", true);
        } finally {
            if (confirmBtn) {
                confirmBtn.disabled = false;
                confirmBtn.innerHTML = originalHtml;
            }
        }
    }

    const table = getTable();
    if (!table) return;

    const tbody = getTbody();
    const recalcBtn = getRecalcButton();
    const confirmBtn = getConfirmRecalcBtn();
    const checkAllBox = getCheckAllBox();

    if (tbody && !tbody.dataset.shotRecalcBound) {
        tbody.dataset.shotRecalcBound = "1";
        tbody.addEventListener("change", function (e) {
            if (e.target && e.target.classList.contains("shot-line-check")) {
                updateRecalcButtonState();
            }
        });
    }

    if (checkAllBox && !checkAllBox.dataset.shotRecalcBound) {
        checkAllBox.dataset.shotRecalcBound = "1";
        checkAllBox.addEventListener("change", function () {
            setTimeout(updateRecalcButtonState, 0);
        });
    }

    if (recalcBtn && !recalcBtn.dataset.shotRecalcBound) {
        recalcBtn.dataset.shotRecalcBound = "1";
        recalcBtn.addEventListener("click", function (e) {
            e.preventDefault();
            e.stopPropagation();
            e.stopImmediatePropagation();
            openRecalcModal();
        });
    }

    if (confirmBtn && !confirmBtn.dataset.shotRecalcBound) {
        confirmBtn.dataset.shotRecalcBound = "1";
        confirmBtn.addEventListener("click", recalcSelectedLines);
    }

    updateRecalcButtonState();
}