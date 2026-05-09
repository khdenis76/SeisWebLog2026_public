export function initProjectTemplateMatrixLazy() {
  const tabs = document.querySelectorAll(".lazy-template-matrix-tab");

  if (!tabs.length) return;

  function clearHover(table) {
    table.querySelectorAll(".vo-hover-row").forEach(el => {
      el.classList.remove("vo-hover-row");
    });

    table.querySelectorAll(".vo-hover-col").forEach(el => {
      el.classList.remove("vo-hover-col");
    });
  }

  function initMatrixHover(container) {
    const table = container.querySelector(".visual-offset-table");
    if (!table) return;

    table.addEventListener("mouseover", (event) => {
      const cell = event.target.closest("td, th");
      if (!cell || !table.contains(cell)) return;

      const row = cell.closest("tr");
      if (!row) return;

      const colIndex = Array.from(row.children).indexOf(cell);
      if (colIndex < 0) return;

      clearHover(table);
      row.classList.add("vo-hover-row");

      table.querySelectorAll("tr").forEach((tr) => {
        const colCell = tr.children[colIndex];
        if (colCell) {
          colCell.classList.add("vo-hover-col");
        }
      });
    });

    table.addEventListener("mouseleave", () => {
      clearHover(table);
    });
  }

  function getMatrixOptions() {
    return {
      slOrder: document.getElementById("matrix-sl-order")?.value || "custom_split",
      slSplit: document.getElementById("matrix-sl-split")?.value || "17721",
      leftOrder: document.getElementById("matrix-left-order")?.value || "desc",
      rightOrder: document.getElementById("matrix-right-order")?.value || "desc",
    };
  }

  function buildMatrixUrl(baseUrl) {
    const opts = getMatrixOptions();
    const url = new URL(baseUrl, window.location.origin);

    url.searchParams.set("sl_order_mode", opts.slOrder);
    url.searchParams.set("sl_split_value", opts.slSplit);
    url.searchParams.set("left_group_order", opts.leftOrder);
    url.searchParams.set("right_group_order", opts.rightOrder);

    return url.toString();
  }

  function getActiveMatrixTab() {
    return document.querySelector(".lazy-template-matrix-tab.active")
      || document.querySelector(".lazy-template-matrix-tab");
  }

  async function loadMatrix(tab, forceReload = false) {
    if (!tab) return;

    if (tab.dataset.loaded === "1" && !forceReload) return;

    const url = tab.dataset.url;
    const targetId = tab.dataset.target;
    const container = document.getElementById(targetId);

    if (!url || !container) return;

    container.innerHTML = `
      <div class="p-4 text-muted small">
        <i class="fas fa-spinner fa-spin me-2"></i>
        Loading template matrix...
      </div>
    `;

    try {
      const response = await fetch(buildMatrixUrl(url), {
        method: "GET",
        headers: {
          "X-Requested-With": "XMLHttpRequest",
        },
      });

      const data = await response.json();

      if (!response.ok || !data.ok) {
        throw new Error(data.error || "Failed to load template matrix.");
      }

      container.innerHTML = `
        <div class="table-scroll border rounded w-100" style="height: 70vh; overflow: auto;">
          ${data.html}
        </div>
      `;

      initMatrixHover(container);

      tab.dataset.loaded = "1";
    } catch (error) {
      container.innerHTML = `
        <div class="alert alert-danger m-3">
          <div class="fw-semibold">Template matrix failed to load</div>
          <div class="small">${error.message}</div>
        </div>
      `;
    }
  }

  function refreshMatrix() {
    const tab = getActiveMatrixTab();
    if (!tab) return;

    tab.dataset.loaded = "0";
    loadMatrix(tab, true);
  }

  tabs.forEach((tab) => {
    tab.addEventListener("shown.bs.tab", () => {
      loadMatrix(tab);
    });

    tab.addEventListener("click", () => {
      loadMatrix(tab);
    });
  });

  document.addEventListener("click", (event) => {
    const refreshBtn = event.target.closest("#matrix-refresh-btn");
    if (!refreshBtn) return;

    event.preventDefault();
    refreshMatrix();
  });

  document.addEventListener("change", (event) => {
    const control = event.target.closest(
      "#matrix-sl-order, #matrix-sl-split, #matrix-left-order, #matrix-right-order"
    );

    if (!control) return;

    const tab = getActiveMatrixTab();
    if (tab) {
      tab.dataset.loaded = "0";
    }
  });
}