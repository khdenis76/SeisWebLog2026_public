export function initBBoxFileTable() {
  const tbody = document.getElementById("bbox-list-tbody");
  const checkAll = document.getElementById("bbox-file-check-all");
  const deleteBtn = document.getElementById("btn-delete-bbox-files");

  if (!tbody) return;

  function getChecks() {
    return Array.from(tbody.querySelectorAll(".bbox-file-checkbox"));
  }

  function getSelectedIds() {
    return getChecks()
      .filter(cb => cb.checked)
      .map(cb => parseInt(cb.value, 10))
      .filter(v => !Number.isNaN(v));
  }

  function refreshDeleteButtonState() {
    if (!deleteBtn) return;
    deleteBtn.disabled = getSelectedIds().length === 0;
  }

  function refreshCheckAllState() {
    if (!checkAll) return;

    const checks = getChecks();
    if (!checks.length) {
      checkAll.checked = false;
      checkAll.indeterminate = false;
      return;
    }

    const checkedCount = checks.filter(cb => cb.checked).length;
    checkAll.checked = checkedCount > 0 && checkedCount === checks.length;
    checkAll.indeterminate = checkedCount > 0 && checkedCount < checks.length;
  }

  if (checkAll && !checkAll.dataset.bound) {
    checkAll.dataset.bound = "1";
    checkAll.addEventListener("change", () => {
      const checked = checkAll.checked;
      getChecks().forEach(cb => {
        cb.checked = checked;
      });
      refreshCheckAllState();
      refreshDeleteButtonState();
    });
  }

  tbody.addEventListener("change", (e) => {
    if (!e.target.classList.contains("bbox-file-checkbox")) return;
    refreshCheckAllState();
    refreshDeleteButtonState();
  });

  refreshCheckAllState();
  refreshDeleteButtonState();
}