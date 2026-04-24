export function initShotSummaryDelete() {
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

    function getAllCheckbox() {
        return qs("#shot-summary-check-all");
    }

    function getDeleteButton() {
        return qs("#shot-summary-delete-button");
    }

    function getSelectedCountEl() {
        return qs("#shot-summary-selected-count");
    }

    function getModalEl() {
        return qs("#shotDeleteLinesModal");
    }

    function getConfirmDeleteBtn() {
        return qs("#shot-confirm-delete-button");
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

    function getVisibleCheckboxes() {
        return getVisibleRows()
            .map((row) => qs(".shot-line-check", row))
            .filter(Boolean);
    }

    function getCheckedCheckboxes() {
        return getVisibleCheckboxes().filter((cb) => cb.checked);
    }

    function getSelectedLineCodes() {
        return getCheckedCheckboxes()
            .map((cb) => cb.dataset.lineCode || cb.value || "")
            .map((v) => String(v).trim())
            .filter(Boolean);
    }

    function updateRowHighlight() {
        getVisibleRows().forEach((row) => {
            const cb = qs(".shot-line-check", row);
            row.classList.toggle("table-danger-soft", !!(cb && cb.checked));
        });
    }

    function updateSelectAllState() {
        const selectAll = getAllCheckbox();
        if (!selectAll) return;

        const visible = getVisibleCheckboxes();
        const checked = getCheckedCheckboxes();

        if (!visible.length) {
            selectAll.checked = false;
            selectAll.indeterminate = false;
        } else if (checked.length === 0) {
            selectAll.checked = false;
            selectAll.indeterminate = false;
        } else if (checked.length === visible.length) {
            selectAll.checked = true;
            selectAll.indeterminate = false;
        } else {
            selectAll.checked = false;
            selectAll.indeterminate = true;
        }
    }

    function updateSelectedCounter() {
        const el = getSelectedCountEl();
        if (el) el.textContent = String(getSelectedLineCodes().length);
    }

    function updateDeleteButtonState() {
        const btn = getDeleteButton();
        if (btn) btn.disabled = getSelectedLineCodes().length === 0;
    }

    function updateDeleteUI() {
        updateRowHighlight();
        updateSelectAllState();
        updateSelectedCounter();
        updateDeleteButtonState();
    }

    function showToast(message, isError = false) {
        const toastEl = getToastEl();
        const toastBody = getToastBodyEl();
        if (!toastEl || !toastBody || typeof bootstrap === "undefined") return;

        toastBody.textContent = message;
        toastEl.classList.remove("text-bg-dark", "text-bg-success", "text-bg-danger");
        toastEl.classList.add(isError ? "text-bg-danger" : "text-bg-success");

        bootstrap.Toast.getOrCreateInstance(toastEl, { delay: 3500 }).show();
    }

    function openDeleteModal() {
        const selected = getSelectedLineCodes();
        if (!selected.length) return;

        const modalEl = getModalEl();
        if (!modalEl || typeof bootstrap === "undefined") return;

        const countEl = qs("#shot-delete-lines-count");
        const previewEl = qs("#shot-delete-lines-preview");

        if (countEl) countEl.textContent = String(selected.length);
        if (previewEl) {
            const preview = selected.slice(0, 10).join(", ");
            previewEl.innerHTML = selected.length > 10
                ? `<strong>Lines:</strong> ${preview} ...`
                : `<strong>Lines:</strong> ${preview}`;
        }

        bootstrap.Modal.getOrCreateInstance(modalEl).show();
    }

    function closeDeleteModal() {
        const modalEl = getModalEl();
        if (!modalEl || typeof bootstrap === "undefined") return;

        const modal = bootstrap.Modal.getInstance(modalEl);
        if (modal) modal.hide();
    }

    function removeDeletedRowsFromDom(lineCodes) {
        const tbody = getTbody();
        if (!tbody) return;

        lineCodes.forEach((code) => {
            const row = qs(`tr[data-line-code="${CSS.escape(code)}"]`, tbody);
            if (row) row.remove();
        });
    }

    async function deleteSelectedLines() {
        const table = getTable();
        const selected = getSelectedLineCodes();
        const confirmBtn = getConfirmDeleteBtn();

        if (!table) {
            showToast("SHOT summary table not found.", true);
            return;
        }

        if (!selected.length) {
            showToast("No lines selected.", true);
            return;
        }

        const deleteUrl = table.dataset.deleteUrl;
        if (!deleteUrl) {
            showToast("Delete URL is missing.", true);
            return;
        }

        const originalHtml = confirmBtn ? confirmBtn.innerHTML : "";

        try {
            if (confirmBtn) {
                confirmBtn.disabled = true;
                confirmBtn.innerHTML = `<span class="spinner-border spinner-border-sm me-2"></span>Deleting...`;
            }

            const response = await fetch(deleteUrl, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "X-CSRFToken": getCSRFToken(),
                    "X-Requested-With": "XMLHttpRequest"
                },
                body: JSON.stringify({ nav_line_codes: selected })
            });

            const data = await response.json();

            if (!response.ok || !data.success) {
                throw new Error(data.error || "Delete failed.");
            }

            removeDeletedRowsFromDom(selected);

            const selectAll = getAllCheckbox();
            if (selectAll) {
                selectAll.checked = false;
                selectAll.indeterminate = false;
            }

            updateDeleteUI();
            closeDeleteModal();

            showToast(
                `Deleted ${data.deleted_lines || 0} line(s). SHOT_TABLE rows: ${data.deleted_shots || data.deleted_shot_rows || 0}.`,
                false
            );
        } catch (error) {
            console.error("Delete selected lines error:", error);
            showToast(error.message || "Delete failed.", true);
        } finally {
            if (confirmBtn) {
                confirmBtn.disabled = false;
                confirmBtn.innerHTML = originalHtml;
            }
        }
    }

    const root = getTable();
    if (!root) return;

    const selectAll = getAllCheckbox();
    const tbody = getTbody();
    const deleteBtn = getDeleteButton();
    const confirmBtn = getConfirmDeleteBtn();

    if (selectAll && !selectAll.dataset.shotDeleteBound) {
        selectAll.dataset.shotDeleteBound = "1";
        selectAll.addEventListener("change", function () {
            const checked = !!selectAll.checked;
            getVisibleCheckboxes().forEach((cb) => {
                cb.checked = checked;
            });
            updateDeleteUI();
        });
    }

    if (tbody && !tbody.dataset.shotDeleteBound) {
        tbody.dataset.shotDeleteBound = "1";
        tbody.addEventListener("change", function (e) {
            if (e.target && e.target.classList.contains("shot-line-check")) {
                updateDeleteUI();
            }
        });
    }

    if (deleteBtn && !deleteBtn.dataset.shotDeleteBound) {
        deleteBtn.dataset.shotDeleteBound = "1";
        deleteBtn.addEventListener("click", openDeleteModal);
    }

    if (confirmBtn && !confirmBtn.dataset.shotDeleteBound) {
        confirmBtn.dataset.shotDeleteBound = "1";
        confirmBtn.addEventListener("click", deleteSelectedLines);
    }

    updateDeleteUI();
}