// static/source/js/initSPSTableSelection.js
export function initSpsTableSelection(options = {}) {
  const tableId = options.tableId || "sps-table";
  const tbodyId = options.tbodyId || "sps-table-tbody";
  const checkAllId = options.checkAllId || "sps-check-all";

  const table = document.getElementById(tableId);
  const tbody = document.getElementById(tbodyId);
  const checkAll = document.getElementById(checkAllId);

  if (!table || !tbody || !checkAll) return;

  // prevent double init
  if (table.dataset.spsSelectionInit === "1") return;
  table.dataset.spsSelectionInit = "1";

  function getVisibleChecks() {
    return Array.from(
      tbody.querySelectorAll("tr:not(.d-none) input.row-check[type='checkbox']")
    );
  }

  function updateHeaderState() {
    const visible = getVisibleChecks();
    const total = visible.length;
    const checked = visible.filter(cb => cb.checked).length;

    if (total === 0) {
      checkAll.checked = false;
      checkAll.indeterminate = false;
      return;
    }

    checkAll.checked = (checked === total);
    checkAll.indeterminate = (checked > 0 && checked < total);
  }

  // ✅ check all only visible
  checkAll.addEventListener("change", () => {
    const visible = getVisibleChecks();
    visible.forEach(cb => { cb.checked = checkAll.checked; });
    checkAll.indeterminate = false;
  });

  // row checkbox change
  tbody.addEventListener("change", (e) => {
    if (e.target && e.target.matches("input.row-check[type='checkbox']")) {
      updateHeaderState();
    }
  });

  // filter changed -> just recalc header state
  tbody.addEventListener("sps:filtered", () => {
    updateHeaderState();
  });

  updateHeaderState();
}