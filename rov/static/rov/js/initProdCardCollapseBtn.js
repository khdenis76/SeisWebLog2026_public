export function initProdCardToggle() {
  const bodyEl = document.getElementById("prod-card-body");
  const btnEl  = document.getElementById("prod-card-toggle-btn");
  const iconEl = document.getElementById("prod-toggle-icon");

  if (!bodyEl || !btnEl || !iconEl) return;

  let isOpen = true; // initial state

  btnEl.addEventListener("click", (e) => {
    e.preventDefault();
    e.stopPropagation();

    isOpen = !isOpen;

    bodyEl.style.display = isOpen ? "" : "none";

    // toggle icon
    iconEl.classList.toggle("fa-window-minimize", isOpen);
    iconEl.classList.toggle("fa-window-maximize", !isOpen);
  });
}
