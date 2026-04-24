export async function ensureLoadingModal() {
  let modalEl = document.getElementById("loadingModal");
  if (modalEl) return modalEl;

  const resp = await fetch("/static/baseproject/html/loading_modal.html", {
    cache: "no-cache",
  });
  if (!resp.ok) {
    throw new Error(`Cannot load loading modal (HTTP ${resp.status})`);
  }

  const html = await resp.text();
  document.body.insertAdjacentHTML("beforeend", html);

  modalEl = document.getElementById("loadingModal");

  // ⚠️ ВАЖНО: сразу создаём instance с focus:false
  bootstrap.Modal.getOrCreateInstance(modalEl, {
    backdrop: "static",
    keyboard: false,
    focus: false,
  });

  return modalEl;
}
export function cleanupModalArtifacts() {
  document.body.classList.remove("modal-open");
  document.body.style.removeProperty("overflow");
  document.body.style.removeProperty("padding-right");
  document.querySelectorAll(".modal-backdrop").forEach(b => b.remove());
}