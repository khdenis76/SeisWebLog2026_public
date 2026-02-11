export function initDsrCardCollapse() {
  const bodyEl   = document.getElementById("dsr-line-card-body");
  const iconEl   = document.getElementById("dsr-line-toggle-icon");
  const toggleBtn = document.getElementById("dsr-card-collapse-btn");

  if (!bodyEl || !iconEl || !toggleBtn) return;

  // 🔴 prevent bubbling (important if card/header itself is clickable)
  toggleBtn.addEventListener("click", (e) => {
    e.stopPropagation();
  });

  // body shown → minimize icon
  bodyEl.addEventListener("shown.bs.collapse", () => {
    toggleBetween(iconEl, "fa-window-minimize", "fa-window-maximize");
    toggleBtn.setAttribute("aria-expanded", "false");
  });

  // body hidden → maximize icon
  bodyEl.addEventListener("hidden.bs.collapse", () => {
    toggleBetween(iconEl, "fa-window-maximize", "fa-window-minimize");
    toggleBtn.setAttribute("aria-expanded", "ftrue");
  });
}
