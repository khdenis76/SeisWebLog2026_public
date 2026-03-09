export function initShotTableSort() {

  const table = document.getElementById("shot-table-list");
  const tbody = document.getElementById("shot-table-tbody");

  if (!table || !tbody) return;

  const headers = table.querySelectorAll("th[data-sort]");
  let sortDir = 1;

  headers.forEach(th => {

    th.style.cursor = "pointer";

    th.addEventListener("click", () => {

      const col = Number(th.dataset.sort);
      const rows = Array.from(tbody.querySelectorAll("tr"));

      rows.sort((a, b) => {

        let A = a.children[col].innerText.trim();
        let B = b.children[col].innerText.trim();

        let numA = parseFloat(A);
        let numB = parseFloat(B);

        if (!isNaN(numA) && !isNaN(numB)) {
          return (numA - numB) * sortDir;
        }

        return A.localeCompare(B) * sortDir;
      });

      sortDir *= -1;

      const frag = document.createDocumentFragment();
      rows.forEach(r => frag.appendChild(r));
      tbody.appendChild(frag);
    });

  });
}