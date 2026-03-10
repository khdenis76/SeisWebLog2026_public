let confirmModalEl = null;
let confirmModalInstance = null;
let confirmTitleEl = null;
let confirmBodyEl = null;
let confirmOkBtn = null;
let confirmCancelBtn = null;

let resolver = null;

function ensureConfirmModal() {
  if (confirmModalEl) return;

  let host = document.getElementById("global-confirm-modal");

  if (!host) {
    const wrapper = document.createElement("div");
    wrapper.innerHTML = `
      <div class="modal fade" id="global-confirm-modal" tabindex="-1" aria-hidden="true">
        <div class="modal-dialog modal-dialog-centered">
          <div class="modal-content shadow-lg border-0">
            <div class="modal-header">
              <h5 class="modal-title" id="global-confirm-modal-title">Confirm</h5>
              <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body" id="global-confirm-modal-body">
              Are you sure?
            </div>
            <div class="modal-footer">
              <button type="button" class="btn btn-secondary" id="global-confirm-modal-cancel" data-bs-dismiss="modal">
                Cancel
              </button>
              <button type="button" class="btn btn-primary" id="global-confirm-modal-ok">
                OK
              </button>
            </div>
          </div>
        </div>
      </div>
    `;
    document.body.appendChild(wrapper.firstElementChild);
    host = document.getElementById("global-confirm-modal");
  }

  confirmModalEl = host;
  confirmTitleEl = document.getElementById("global-confirm-modal-title");
  confirmBodyEl = document.getElementById("global-confirm-modal-body");
  confirmOkBtn = document.getElementById("global-confirm-modal-ok");
  confirmCancelBtn = document.getElementById("global-confirm-modal-cancel");

  if (!confirmModalEl || !confirmTitleEl || !confirmBodyEl || !confirmOkBtn || !confirmCancelBtn) {
    console.error("Confirm modal elements were not created correctly.");
    return;
  }

  confirmModalInstance = bootstrap.Modal.getOrCreateInstance(confirmModalEl, {
    backdrop: "static",
    keyboard: true,
  });

  confirmOkBtn.addEventListener("click", () => {
    if (resolver) resolver(true);
    resolver = null;
    confirmModalInstance.hide();
  });

  confirmCancelBtn.addEventListener("click", () => {
    if (resolver) resolver(false);
    resolver = null;
  });

  confirmModalEl.addEventListener("hidden.bs.modal", () => {
    if (resolver) {
      resolver(false);
      resolver = null;
    }
  });
}

export function showConfirmModal({
  title = "Confirm",
  message = "Are you sure?",
  confirmText = "OK",
  cancelText = "Cancel",
  confirmClass = "btn-primary",
} = {}) {
  ensureConfirmModal();

  if (!confirmModalEl || !confirmTitleEl || !confirmBodyEl || !confirmOkBtn || !confirmCancelBtn || !confirmModalInstance) {
    console.error("Confirm modal is not available.");
    return Promise.resolve(false);
  }

  confirmTitleEl.textContent = title;
  confirmBodyEl.textContent = message;
  confirmOkBtn.textContent = confirmText;
  confirmCancelBtn.textContent = cancelText;

  confirmOkBtn.className = `btn ${confirmClass}`;
  confirmCancelBtn.className = "btn btn-secondary";

  return new Promise((resolve) => {
    resolver = resolve;
    confirmModalInstance.show();
  });
}