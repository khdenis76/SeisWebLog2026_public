export function initNoarRLSolutionsSelection(options = {}) {

    const tbodyId = options.tbodyId || "noar-rlsolutions-tbody";

    const tbody = document.getElementById(tbodyId);

    if (!tbody) {
        return;
    }

    const selectAllBtn = document.getElementById("noar-rl-select-all-btn");
    const clearBtn = document.getElementById("noar-rl-clear-selection-btn");
    const countEl = document.getElementById("noar-rl-selected-count");
    const checkAll = document.getElementById("noar-rl-check-all");

    function getCheckboxes() {
        return Array.from(
            tbody.querySelectorAll(".noar-rl-row-check")
        );
    }

    function updateSelectedCount() {

        const boxes = getCheckboxes();

        const selected = boxes.filter(cb => cb.checked);

        if (countEl) {
            countEl.textContent = `${selected.length} selected`;
        }

        if (checkAll) {

            checkAll.checked =
                boxes.length > 0 &&
                selected.length === boxes.length;

            checkAll.indeterminate =
                selected.length > 0 &&
                selected.length < boxes.length;
        }
    }

    /* =====================================
       SELECT ALL BUTTON
    ===================================== */
    if (selectAllBtn) {

        selectAllBtn.addEventListener("click", function () {

            getCheckboxes().forEach(cb => {
                cb.checked = true;
            });

            updateSelectedCount();
        });
    }

    /* =====================================
       CLEAR BUTTON
    ===================================== */
    if (clearBtn) {

        clearBtn.addEventListener("click", function () {

            getCheckboxes().forEach(cb => {
                cb.checked = false;
            });

            updateSelectedCount();
        });
    }

    /* =====================================
       HEADER CHECKBOX
    ===================================== */
    if (checkAll) {

        checkAll.addEventListener("change", function () {

            getCheckboxes().forEach(cb => {
                cb.checked = checkAll.checked;
            });

            updateSelectedCount();
        });
    }

    /* =====================================
       ROW CHECKBOXES
    ===================================== */
    tbody.addEventListener("change", function (event) {

        if (
            event.target.classList.contains("noar-rl-row-check")
        ) {
            updateSelectedCount();
        }
    });

    updateSelectedCount();
}