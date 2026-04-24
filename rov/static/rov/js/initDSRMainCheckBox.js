export function initDSRLinesSelectAll() {
  const main = document.getElementById("mainDSRLInesCheckBox");
  if (!main) return;

  const getBoxes = () => Array.from(document.querySelectorAll(".dsr-line-checkbox"));

  const updateMainState = () => {
    const boxes = getBoxes();
    if (boxes.length === 0) {
      main.checked = false;
      main.indeterminate = false;
      return;
    }
    const checked = boxes.filter(b => b.checked).length;
    main.checked = checked === boxes.length;
    main.indeterminate = checked > 0 && checked < boxes.length;
  };

  main.addEventListener("change", () => {
    const boxes = getBoxes();
    boxes.forEach(b => (b.checked = main.checked));
    main.indeterminate = false;
  });

  document.addEventListener("change", (e) => {
    if (e.target && e.target.classList.contains("dsr-line-checkbox")) {
      updateMainState();
    }
  });

  updateMainState();
}
