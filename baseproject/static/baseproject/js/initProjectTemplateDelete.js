import { getCSRFToken } from "./csrf.js";
import { showConfirmModal } from "./modalConfirm.js";
import { showAppToast } from "./toast.js";

export function initProjectTemplateDelete() {
  const table = document.getElementById("template_table");
  const tbody = document.getElementById("template_body");
  const selectAll = document.getElementById("common_template_checkbox");
  const deleteButton = document.getElementById("btnDeleteProjectTemplates");
  const selectionCount = document.getElementById("project_template_selection_count");

  if (!table || !tbody || !selectAll || !deleteButton) return;

  const rowCheckboxes = () => Array.from(tbody.querySelectorAll(".project-template-checkbox"));
  const selectedCheckboxes = () => rowCheckboxes().filter((checkbox) => checkbox.checked);

  const updateSelectionState = () => {
    const rows = rowCheckboxes();
    const selected = selectedCheckboxes().length;

    selectAll.checked = rows.length > 0 && selected === rows.length;
    selectAll.indeterminate = selected > 0 && selected < rows.length;
    selectAll.disabled = rows.length === 0;
    deleteButton.disabled = selected === 0;

    if (selectionCount) {
      selectionCount.textContent = `${selected} selected`;
    }
  };

  selectAll.addEventListener("change", () => {
    rowCheckboxes().forEach((checkbox) => {
      checkbox.checked = selectAll.checked;
    });
    updateSelectionState();
  });

  tbody.addEventListener("change", (event) => {
    if (event.target.classList.contains("project-template-checkbox")) {
      updateSelectionState();
    }
  });

  deleteButton.addEventListener("click", async () => {
    const ids = selectedCheckboxes().map((checkbox) => Number(checkbox.value));
    if (!ids.length) return;

    const confirmed = await showConfirmModal({
      title: "Delete project templates",
      message: `Delete ${ids.length} selected template row${ids.length === 1 ? "" : "s"}?`,
      details: "This operation cannot be undone.",
      confirmText: "Delete",
      confirmClass: "btn-danger",
      iconClass: "fa-trash-can",
    });
    if (!confirmed) return;

    deleteButton.disabled = true;

    try {
      const response = await fetch(deleteButton.dataset.deleteUrl, {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCSRFToken(),
        },
        body: JSON.stringify({ ids }),
      });
      const data = await response.json();

      if (!response.ok || !data.ok) {
        throw new Error(data.error || "Could not delete the selected template rows.");
      }

      tbody.innerHTML = data.table_body;
      selectAll.checked = false;
      selectAll.indeterminate = false;
      updateSelectionState();

      showAppToast(`${data.deleted} project template row${data.deleted === 1 ? "" : "s"} deleted.`, {
        title: "Templates updated",
        variant: "success",
      });
    } catch (error) {
      updateSelectionState();
      showAppToast(error.message, {
        title: "Delete failed",
        variant: "danger",
      });
    }
  });

  updateSelectionState();
}
