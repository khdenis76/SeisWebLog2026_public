export function initSpsTableSelection() {

    const table = document.getElementById("sps-table");
    const checkAll = document.getElementById("sps-check-all");

    if (!table || !checkAll) return;

    const getChecks = () =>
        Array.from(table.querySelectorAll("tbody .row-check"));

    function updateHeaderState() {
        const checks = getChecks();
        const total = checks.length;
        const checked = checks.filter(c => c.checked).length;

        if (total === 0) {
            checkAll.checked = false;
            checkAll.indeterminate = false;
            return;
        }

        checkAll.checked = (checked === total);
        checkAll.indeterminate = (checked > 0 && checked < total);
    }

    // remove old listeners if re-init (important for ajax reloads)
    checkAll.onchange = null;
    table.onchange = null;

    // Header checkbox
    checkAll.addEventListener("change", function () {
        const checks = getChecks();
        checks.forEach(c => c.checked = checkAll.checked);
        checkAll.indeterminate = false;
    });

    // Row checkbox change
    table.addEventListener("change", function (e) {
        if (e.target && e.target.classList.contains("row-check")) {
            updateHeaderState();
        }
    });

    updateHeaderState();
}
