export function initBboxFileSelectAll() {
  const table = document.getElementById("bbox-file-table");
  const mainCheckbox = document.getElementById("mainBboxFileCheckBox");

  if (!table || !mainCheckbox) return;

  function getRowCheckboxes() {
    return table.querySelectorAll("tbody .bbox-file-checkbox");
  }

  // Header checkbox → toggle all rows
  mainCheckbox.addEventListener("change", () => {
    getRowCheckboxes().forEach(cb => {
      cb.checked = mainCheckbox.checked;
    });
    mainCheckbox.indeterminate = false;
  });

  // Row checkbox → update header state
  table.addEventListener("change", (e) => {
    if (!e.target.classList.contains("bbox-file-checkbox")) return;

    const boxes = getRowCheckboxes();
    const checkedCount = [...boxes].filter(cb => cb.checked).length;

    if (checkedCount === 0) {
      mainCheckbox.checked = false;
      mainCheckbox.indeterminate = false;
    } else if (checkedCount === boxes.length) {
      mainCheckbox.checked = true;
      mainCheckbox.indeterminate = false;
    } else {
      mainCheckbox.checked = false;
      mainCheckbox.indeterminate = true;
    }
  });
}
