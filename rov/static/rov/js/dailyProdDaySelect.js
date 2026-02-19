import { getCSRFToken } from "../../baseproject/js/csrf.js";

export function initDailyProdDaySelect() {
  const input = document.getElementById("production-day-select");
  const container = document.getElementById("daily-prod-container");
  if (!input || !container) return;

  const url = input.dataset.dayurl;
  if (!url) {
    console.warn("production-day-select missing data-dayurl");
    return;
  }

  async function loadDay(day) {
  if (!day) {
    container.innerHTML = "";
    return;
  }

  container.innerHTML = `
    <div class="text-muted small py-2">
      <span class="spinner-border spinner-border-sm me-2"></span>Loading production...
    </div>
  `;

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-CSRFToken": getCSRFToken(),
    },
    body: JSON.stringify({ day }),
  });

  const data = await res.json();

  if (!res.ok) {
    container.innerHTML = `<div class="alert alert-danger mb-0">${data.error}</div>`;
    return;
  }

  container.innerHTML = data.html;
}


  // load when user changes day
  input.addEventListener("change", () => loadDay(input.value));

  // optional: auto-load initial value if input already has a date
  if (input.value) loadDay(input.value);
}
