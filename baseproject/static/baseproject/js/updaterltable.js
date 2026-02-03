export function updateRLPreplotTable(rows) {
  const tbody = document.getElementById("rlBody");
  if (!tbody) return;

  // 1) очистить старые строки
  tbody.innerHTML = "";

  // 2) сбросить главный чекбокс
  const mainCb = document.getElementById("MainRLPreplotCheckbox");
  if (mainCb) mainCb.checked = false;

  // 3) если данных нет
  if (!rows || rows.length === 0) {
    tbody.innerHTML = `
      <tr>
        <td colspan="7" class="text-muted">No data</td>
      </tr>
    `;
    return;
  }

  // 4) вставить новые строки
  for (const r of rows) {
    const lineLength = (r.LineLength ?? 0);

    tbody.insertAdjacentHTML("beforeend", `
      <tr>
        <td>
          <input type="checkbox"
                 class="rl-preplot-checkbox"
                 data-line-id="${r.ID ?? ""}"
                 data-tier-line="${r.TierLine ?? ""}">
        </td>
        <td>${r.Line ?? ""}</td>
        <td>${r.Points ?? ""}</td>
        <td>${r.FirstPoint ?? ""}</td>
        <td>${r.LastPoint ?? ""}</td>
        <td>${Math.round(Number(lineLength) || 0)}</td>
        <td>${r.Tier ?? ""}</td>
      </tr>
    `);
  }
}
export function updateSLPreplotTable(rows) {
  const tbody = document.getElementById("slBody");
  if (!tbody) return;

  // 1) очистить старые строки
  tbody.innerHTML = "";

  // 2) сбросить главный чекбокс
  const mainCb = document.getElementById("MainSLPreplotCheckbox");
  if (mainCb) mainCb.checked = false;

  // 3) если данных нет
  if (!rows || rows.length === 0) {
    tbody.innerHTML = `
      <tr>
        <td colspan="7" class="text-muted">No data</td>
      </tr>
    `;
    return;
  }

  // 4) вставить новые строки
  for (const r of rows) {
    const lineLength = (r.LineLength ?? 0);

    tbody.insertAdjacentHTML("beforeend", `
      <tr>
        <td>
          <input type="checkbox"
                 class="sl-preplot-checkbox"
                 data-line-id="${r.ID ?? ""}"
                 data-tier-line="${r.TierLine ?? ""}">
        </td>
        <td>${r.Line ?? ""}</td>
        <td>${r.Points ?? ""}</td>
        <td>${r.FirstPoint ?? ""}</td>
        <td>${r.LastPoint ?? ""}</td>
        <td>${Math.round(Number(lineLength) || 0)}</td>
        <td>${r.Tier ?? ""}</td>
      </tr>
    `);
  }
}
export function updatePreplotTable(tbodyId, rows,mainCheckboxId = null) {
  const tbody = document.getElementById(tbodyId);
  if (!tbody) return;
  const inputClass =
    tbodyId === "rlBody"
      ? "rl-preplot-checkbox"
      : "sl-preplot-checkbox";
  console.log(inputClass)
  // 1) очистить таблицу
  tbody.innerHTML = "";

  if (mainCheckboxId) {
    const mainCb = document.getElementById(mainCheckboxId);
    if (mainCb) mainCb.checked = false;
  }

  // 3) если данных нет
  if (!Array.isArray(rows) || rows.length === 0) {
    tbody.innerHTML = `
      <tr>
        <td colspan="7" class="text-muted text-center">
          No data
        </td>
      </tr>
    `;
    return;
  }

  // 4) вставка строк
  for (const r of rows) {
    const lineLength = Math.round(Number(r.LineLength) || 0);

    tbody.insertAdjacentHTML("beforeend", `
      <tr>
        <td>
          <input type="checkbox"
                 class="${inputClass}"
                 data-line-id="${r.ID ?? ""}"
                 data-tier-line="${r.TierLine ?? ""}">
        </td>
        <td>${r.Line ?? ""}</td>
        <td>${r.Points ?? ""}</td>
        <td>${r.FirstPoint ?? ""}</td>
        <td>${r.LastPoint ?? ""}</td>
        <td>${lineLength}</td>
        <td>${r.Tier ?? ""}</td>
      </tr>
    `);
  }
}
