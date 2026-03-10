function ensureToastContainer() {
  let container = document.getElementById("toastContainer");
  if (container) return container;

  container = document.createElement("div");
  container.id = "toastContainer";
  container.className = "toast-container position-fixed top-0 end-0 p-3 seis-toast-stack";
  document.body.appendChild(container);
  return container;
}

function normalizeVariant(variant = "success") {
  const allowed = new Set(["success", "danger", "warning", "info", "primary", "secondary"]);
  return allowed.has(variant) ? variant : "success";
}

export function showUploadToast({ title = "Done", body = "", delay = 5000, variant = "success" }) {
  const container = ensureToastContainer();
  const safeVariant = normalizeVariant(variant);

  const iconMap = {
    success: "fa-circle-check",
    danger: "fa-circle-xmark",
    warning: "fa-triangle-exclamation",
    info: "fa-circle-info",
    primary: "fa-bell",
    secondary: "fa-message",
  };

  const toastEl = document.createElement("div");
  toastEl.className = `toast border-0 seis-toast seis-toast-${safeVariant}`;
  toastEl.role = "alert";
  toastEl.ariaLive = "assertive";
  toastEl.ariaAtomic = "true";

  toastEl.innerHTML = `
    <div class="toast-header seis-toast-header border-0 bg-transparent text-white">
      <span class="seis-toast-icon me-2"><i class="fa-solid ${iconMap[safeVariant]}"></i></span>
      <strong class="me-auto">${title}</strong>
      <small class="opacity-75">SeisWebLog</small>
      <button type="button"
              class="btn-close btn-close-white ms-2"
              data-bs-dismiss="toast"
              aria-label="Close"></button>
    </div>
    <div class="toast-body seis-toast-body">${body}</div>
  `;

  container.appendChild(toastEl);

  const toast = bootstrap.Toast.getOrCreateInstance(toastEl, { delay });
  toast.show();

  toastEl.addEventListener("hidden.bs.toast", () => toastEl.remove(), { once: true });
}

export function showAppToast(message, options = {}) {
  showUploadToast({ body: message, ...options });
}
