export function initColumnToggles(btn_id, div_id, icon_id, opts = {}) {
  const btn  = document.getElementById(btn_id);
  const div  = document.getElementById(div_id);
  const icon = document.getElementById(icon_id);

  if (!btn || !div || !icon) return;

  const {
    // classes to toggle on the DIV
    divOn = "col-12",
    divOff = "col-4",

    // icon classes
    iconOn = "fa-compress",
    iconOff = "fa-expand",

    // optional: another element to show/hide (right column etc.)
    toggleElId = null,

    // how to hide/show the optional element
    hideClass = "d-none",
  } = opts;

  const toggleEl = toggleElId ? document.getElementById(toggleElId) : null;

  btn.addEventListener("click", () => {
    // Toggle div classes (e.g. col-12 <-> col-4)
    div.classList.toggle(divOn);
    div.classList.toggle(divOff);

    // Toggle icon classes (e.g. expand <-> compress)
    icon.classList.toggle(iconOn);
    icon.classList.toggle(iconOff);

    // Optional: toggle another element visibility
    if (toggleEl) toggleEl.classList.toggle(hideClass);
  });
}
