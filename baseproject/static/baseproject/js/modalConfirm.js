export function showConfirmModal({
  title = "Confirm action",
  message = "Are you sure?",
  details = "",
  confirmText = "Confirm",
  confirmClass = "btn-danger",
  iconClass = "fa-circle-question",
}) {
  const modalEl = document.getElementById("actionConfirmModal");
  if (!modalEl) {
    return Promise.resolve(false);
  }

  const titleEl = modalEl.querySelector("[data-confirm-title]");
  const messageEl = modalEl.querySelector("[data-confirm-message]");
  const detailsEl = modalEl.querySelector("[data-confirm-details]");
  const confirmBtn = modalEl.querySelector("[data-confirm-accept]");
  const iconEl = modalEl.querySelector("[data-confirm-icon]");

  if (titleEl) titleEl.textContent = title;
  if (messageEl) messageEl.textContent = message;
  if (detailsEl) {
    detailsEl.textContent = details || "";
    detailsEl.classList.toggle("d-none", !details);
  }
  if (iconEl) iconEl.className = `fa-solid ${iconClass}`;
  if (confirmBtn) {
    confirmBtn.textContent = confirmText;
    confirmBtn.className = `btn ${confirmClass}`;
  }

  const modal = bootstrap.Modal.getOrCreateInstance(modalEl);

  return new Promise((resolve) => {
    let settled = false;

    const cleanup = (value) => {
      if (settled) return;
      settled = true;
      confirmBtn?.removeEventListener("click", onAccept);
      modalEl.removeEventListener("hidden.bs.modal", onHidden);
      resolve(value);
    };

    const onAccept = () => {
      cleanup(true);
      modal.hide();
    };

    const onHidden = () => cleanup(false);

    confirmBtn?.addEventListener("click", onAccept, { once: true });
    modalEl.addEventListener("hidden.bs.modal", onHidden, { once: true });
    modal.show();
  });
}
