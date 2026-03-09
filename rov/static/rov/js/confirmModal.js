let confirmModalEl = null;
let confirmTitleEl = null;
let confirmBodyEl = null;
let confirmOkBtn = null;
let confirmCancelBtn = null;
let confirmInstance = null;
let pendingResolve = null;

function ensureConfirmModal() {
  if (confirmModalEl) return;

  confirmModalEl = document.getElementById("app-confirm-modal");
  if (!confirmModalEl) {
    confirmModalEl = document.createElement("div");
    confirmModalEl.id = "app-confirm-modal";
    confirmModalEl.className = "modal fade";
    confirmModalEl.tabIndex = -1;
    confirmModalEl.setAttribute("aria-hidden", "true");
    confirmModalEl.innerHTML = `
      <div class="modal-dialog modal-dialog-centered">
        <div class="modal-content">
          <div class="modal-header">
            <h5 class="modal-title" id="app-confirm-modal-title">Please confirm</h5>
            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
          </div>
          <div class="modal-body" id="app-confirm-modal-body"></div>
          <div class="modal-footer">
            <button type="button" class="btn btn-secondary" data-confirm-cancel>Cancel</button>
            <button type="button" class="btn btn-primary" data-confirm-ok>OK</button>
          </div>
        </div>
      </div>
    `;
    document.body.appendChild(confirmModalEl);
  }

  confirmTitleEl = confirmModalEl.querySelector("#app-confirm-modal-title");
  confirmBodyEl = confirmModalEl.querySelector("#app-confirm-modal-body");
  confirmOkBtn = confirmModalEl.querySelector("[data-confirm-ok]");
  confirmCancelBtn = confirmModalEl.querySelector("[data-confirm-cancel]");
  confirmInstance = bootstrap.Modal.getOrCreateInstance(confirmModalEl, {
    backdrop: "static",
    keyboard: true,
  });

  confirmOkBtn.addEventListener("click", () => {
    if (pendingResolve) pendingResolve(true);
    pendingResolve = null;
    confirmInstance.hide();
  });

  confirmCancelBtn.addEventListener("click", () => {
    if (pendingResolve) pendingResolve(false);
    pendingResolve = null;
    confirmInstance.hide();
  });

  confirmModalEl.addEventListener("hidden.bs.modal", () => {
    if (pendingResolve) {
      pendingResolve(false);
      pendingResolve = null;
    }
  });
}

export function showConfirmModal({
  title = "Please confirm",
  message = "Are you sure?",
  confirmText = "OK",
  cancelText = "Cancel",
  confirmClass = "btn-primary",
} = {}) {
  ensureConfirmModal();

  confirmTitleEl.textContent = title;
  confirmBodyEl.textContent = message;
  confirmOkBtn.textContent = confirmText;
  confirmCancelBtn.textContent = cancelText;
  confirmOkBtn.className = `btn ${confirmClass}`;

  return new Promise((resolve) => {
    pendingResolve = resolve;
    confirmInstance.show();
  });
}
