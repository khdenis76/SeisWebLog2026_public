export function initSPPointCheckboxes(containerId = null) {
  const scope = containerId
    ? document.getElementById(containerId)
    : document;

  if (!scope) return;

  const main = scope.querySelector("#mainSPCheckBox");
  if (!main) return;

  const getBoxes = () =>
    Array.from(scope.querySelectorAll(".sp_checkbox"));

  // 1️⃣ Main checkbox → toggle all
  main.addEventListener("change", () => {
    const checked = main.checked;
    getBoxes().forEach(cb => {
      cb.checked = checked;
    });
  });

  // 2️⃣ Any child checkbox → update main checkbox
  scope.addEventListener("change", (e) => {
    const cb = e.target;
    if (!cb.classList.contains("rp_checkbox")) return;

    const boxes = getBoxes();
    const allChecked = boxes.length > 0 && boxes.every(b => b.checked);
    const anyChecked = boxes.some(b => b.checked);

    main.checked = allChecked;
    main.indeterminate = !allChecked && anyChecked; // 🔥 nice UX
  });
}
