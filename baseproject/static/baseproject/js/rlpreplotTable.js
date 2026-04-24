export async function loadRLPreplotTable(tbody_name) {
  const body = document.getElementById(tbody_name);
  if (!body) return;

  const resp = await fetch("/rlpreplot/json/", {
    headers: { "X-Requested-With": "XMLHttpRequest" }
  });

  const data = await resp.json();

  let rows = (tbody_name === "rlBody") ? data.rl_rows : data.sl_rows;
  rows = Array.isArray(rows) ? rows : [];
  body.innerHTML = "";

  for (const r of rows) {
    body.insertAdjacentHTML("beforeend", `
      <tr>
        <td>${r.Tier ?? ""}</td>
        <td>${r.TierLine ?? ""}</td>
        <td>${r.Line ?? ""}</td>
        <td>${r.Points ?? ""}</td>
        <td>${r.FirstPoint ?? ""}</td>
        <td>${r.LastPoint ?? ""}</td>
        <td>${r.LineLength ?? ""}</td>
        <td>${r.LineBearing ?? ""}</td>
      </tr>
    `);
  }
}
export function initRLCheckboxes() {
  const main = document.getElementById("MainRLPreplotCheckbox");
  const tbody = document.getElementById("rlBody");
  if (!main || !tbody) return;

  // Select all
  main.addEventListener("change", () => {
    tbody.querySelectorAll(".rl-preplot-checkbox").forEach(cb => {
      cb.checked = main.checked;
    });
  });

  // Если вручную снимают/ставят — обновлять главный чекбокс
  tbody.addEventListener("change", (e) => {
    if (!e.target.classList.contains("rl-preplot-checkbox")) return;
    const boxes = tbody.querySelectorAll(".rl-preplot-checkbox");
    const checked = tbody.querySelectorAll(".rl-preplot-checkbox:checked");
    main.checked = boxes.length > 0 && checked.length === boxes.length;
  });
}
export function initSLCheckboxes() {
  const main = document.getElementById("MainSLPreplotCheckbox");
  const tbody = document.getElementById("slBody");
  if (!main || !tbody) return;

  // Select all
  main.addEventListener("change", () => {
    tbody.querySelectorAll(".sl-preplot-checkbox").forEach(cb => {
      cb.checked = main.checked;
    });
  });

  // Если вручную снимают/ставят — обновлять главный чекбокс
  tbody.addEventListener("change", (e) => {
    if (!e.target.classList.contains("sl-preplot-checkbox")) return;
    const boxes = tbody.querySelectorAll(".sl-preplot-checkbox");
    const checked = tbody.querySelectorAll(".sl-preplot-checkbox:checked");
    main.checked = boxes.length > 0 && checked.length === boxes.length;
  });
}