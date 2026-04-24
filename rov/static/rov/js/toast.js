function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

export function showToast({
  title = "Notification",
  message = "",
  type = "info",
  delay = 4000,
} = {}) {
  const container = document.getElementById("app-toast-container");
  if (!container) {
    console.warn("Toast container #app-toast-container not found");
    return;
  }

  const safeTypeMap = {
    success: "success",
    danger: "danger",
    error: "danger",
    warning: "warning",
    info: "info",
    secondary: "secondary",
  };

  const bg = safeTypeMap[type] || "info";
  const textClass = bg === "warning" ? "text-dark" : "text-white";
  const closeClass = bg === "warning" ? "" : "btn-close-white";

  const toastEl = document.createElement("div");
  toastEl.className = `toast border-0 text-bg-${bg} ${textClass}`;
  toastEl.setAttribute("role", "alert");
  toastEl.setAttribute("aria-live", "assertive");
  toastEl.setAttribute("aria-atomic", "true");

  toastEl.innerHTML = `
    <div class="d-flex">
      <div class="toast-body">
        <div class="fw-semibold">${escapeHtml(title)}</div>
        <div>${escapeHtml(message)}</div>
      </div>
      <button type="button"
              class="btn-close ${closeClass} me-2 m-auto"
              data-bs-dismiss="toast"
              aria-label="Close"></button>
    </div>
  `;

  container.appendChild(toastEl);

  const toast = new bootstrap.Toast(toastEl, { delay });
  toast.show();

  toastEl.addEventListener("hidden.bs.toast", () => {
    toastEl.remove();
  });
}

export function toastFromResponse(data, fallbackTitle = "Notification") {
  if (!data) return;

  if (data.toast) {
    showToast(data.toast);
    return;
  }

  if (data.error) {
    showToast({
      title: fallbackTitle,
      message: data.error,
      type: "danger",
    });
    return;
  }

  if (data.success) {
    showToast({
      title: fallbackTitle,
      message: data.success,
      type: "success",
    });
    return;
  }

  if (data.message) {
    showToast({
      title: fallbackTitle,
      message: data.message,
      type: data.ok ? "success" : "info",
    });
  }
}
