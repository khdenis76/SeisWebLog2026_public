export function initCardMinMax({
  buttonId,
  bodyId,
  iconId,
}) {
  const btn  = document.getElementById(buttonId);
  const body = document.getElementById(bodyId);
  const icon = document.getElementById(iconId);

  if (!btn || !body || !icon) return;

  btn.addEventListener("click", () => {
    const isHidden = body.classList.toggle("d-none");

    if (isHidden) {
      icon.classList.remove("fa-window-minimize");
      icon.classList.add("fa-window-maximize");
    } else {
      icon.classList.remove("fa-window-maximize");
      icon.classList.add("fa-window-minimize");
    }
  });
}
