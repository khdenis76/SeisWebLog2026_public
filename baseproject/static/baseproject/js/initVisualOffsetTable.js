export function initVisualOffsetTable(rootSelector = "#pills-matrix-page") {
  const root = document.querySelector(rootSelector);
  if (!root) return;

  const table = root.querySelector(".visual-offset-table");
  if (!table) return;

  const clearHover = () => {
    table.querySelectorAll(".vo-hover-row").forEach(el => el.classList.remove("vo-hover-row"));
    table.querySelectorAll(".vo-hover-col").forEach(el => el.classList.remove("vo-hover-col"));
  };

  table.addEventListener("mouseover", (event) => {
    const cell = event.target.closest("td, th");
    if (!cell || !table.contains(cell)) return;

    const row = cell.closest("tr");
    if (!row) return;

    const colIndex = Array.from(row.children).indexOf(cell);
    if (colIndex < 0) return;

    clearHover();

    row.classList.add("vo-hover-row");

    table.querySelectorAll("tr").forEach((tr) => {
      const colCell = tr.children[colIndex];
      if (colCell) {
        colCell.classList.add("vo-hover-col");
      }
    });
  });

  table.addEventListener("mouseleave", clearHover);
}