export function initNoarRLSolutionsDelete(options = {}) {
  const deleteUrl = options.deleteUrl;
  const tbodyId = options.tbodyId || "noar-rlsolutions-tbody";

  const tbody = document.getElementById(tbodyId);
  const checkAll = document.getElementById("noar-rl-check-all");
  const selectAllBtn = document.getElementById("noar-rl-select-all-btn");
  const clearBtn = document.getElementById("noar-rl-clear-selection-btn");
  const countEl = document.getElementById("noar-rl-selected-count");

  if (!deleteUrl || !tbody) return;

  function getCookie(name) {
    const cookieValue = document.cookie
      .split("; ")
      .find(row => row.startsWith(name + "="));

    return cookieValue ? decodeURIComponent(cookieValue.split("=")[1]) : "";
  }

  function getCheckboxes() {
    return Array.from(tbody.querySelectorAll(".noar-rl-row-check"));
  }

  function getSelectedIds() {
    return getCheckboxes()
      .filter(cb => cb.checked)
      .map(cb => cb.value);
  }

  function updateSelectedCount() {
    const boxes = getCheckboxes();
    const selected = boxes.filter(cb => cb.checked);

    if (countEl) {
      countEl.textContent = `${selected.length} selected`;
    }

    if (checkAll) {
      checkAll.checked = boxes.length > 0 && selected.length === boxes.length;
      checkAll.indeterminate = selected.length > 0 && selected.length < boxes.length;
    }
  }

  if (checkAll) {
    checkAll.addEventListener("change", function () {
      getCheckboxes().forEach(cb => {
        cb.checked = checkAll.checked;
      });
      updateSelectedCount();
    });
  }

  if (selectAllBtn) {
    selectAllBtn.addEventListener("click", function () {
      getCheckboxes().forEach(cb => {
        cb.checked = true;
      });
      updateSelectedCount();
    });
  }

  if (clearBtn) {
    clearBtn.addEventListener("click", function () {
      getCheckboxes().forEach(cb => {
        cb.checked = false;
      });
      updateSelectedCount();
    });
  }

  tbody.addEventListener("change", function (event) {
    if (event.target.classList.contains("noar-rl-row-check")) {
      updateSelectedCount();
    }
  });

  document.querySelectorAll(".noar-rl-delete-option").forEach(button => {
    button.addEventListener("click", async function () {
      const ids = getSelectedIds();
      const deleteType = button.dataset.deleteType;

      if (!ids.length) {
        alert("Select at least one line.");
        return;
      }

      const label = button.textContent.trim();

      if (!confirm(`Delete ${label} for ${ids.length} selected line(s)?`)) {
        return;
      }

      const response = await fetch(deleteUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCookie("csrftoken"),
        },
        body: JSON.stringify({
          ids: ids,
          delete_type: deleteType,
        }),
      });

      const data = await response.json();

      if (!response.ok || !data.ok) {
        alert(data.error || "Delete failed.");
        return;
      }

      if (data.tbody_html) {
        tbody.innerHTML = data.tbody_html;
      }

      updateSelectedCount();
    });
  });

  updateSelectedCount();
}