export function initHoverTabPopups() {
  document.querySelectorAll(".hover-wrapper").forEach(wrapper => {

    const btn = wrapper.querySelector(".hover-btn");
    if (!btn) return;

    const targetId = btn.dataset.hoverTarget;
    const popup = document.getElementById(targetId);
    if (!popup) return;

    let closeTimer = null;

    const open = () => {
      if (closeTimer) clearTimeout(closeTimer);
      popup.classList.add("is-open");
    };

    const close = () => {
      closeTimer = setTimeout(() => {
        popup.classList.remove("is-open");
      }, 200);
    };

    // Hover on wrapper (covers both button and popup)
    wrapper.addEventListener("mouseenter", open);
    wrapper.addEventListener("mouseleave", close);

    // Keep open while interacting inside popup
    popup.addEventListener("mouseenter", open);
    popup.addEventListener("mouseleave", close);

  });
}
