export function initShotSummaryBackendFilters() {
  const form = document.getElementById("shot-summary-filter-form");
  const tbody = document.getElementById("shot-summary-tbody");
  const clearBtn = document.getElementById("shot-summary-clear-filters");
  const resetBtn = document.getElementById("shot-summary-reset-modal-filters");
  const statusEl = document.getElementById("shot-summary-filter-status");
  const modalEl = document.getElementById("shotSummaryFilterModal");

  if (!form || !tbody) return;

  const modal = modalEl ? bootstrap.Modal.getOrCreateInstance(modalEl) : null;
  const baseUrl = window.location.pathname;

  async function loadFiltered(url) {
    tbody.innerHTML = `
      <tr>
        <td colspan="999" class="text-center py-4 text-muted">
          <span class="spinner-border spinner-border-sm me-2"></span>
          Loading...
        </td>
      </tr>
    `;

    try {
      const resp = await fetch(url, {
        headers: {
          "X-Requested-With": "XMLHttpRequest"
        }
      });

      const data = await resp.json();

      if (!data.ok) {
        throw new Error("Filter request failed");
      }

      tbody.innerHTML = data.tbody_html || `
        <tr><td colspan="999" class="text-center py-4 text-muted">No data</td></tr>
      `;

      if (statusEl) {
        statusEl.textContent = `Showing ${data.count ?? 0} rows`;
      }

      modal?.hide();
    } catch (err) {
      tbody.innerHTML = `
        <tr>
          <td colspan="999" class="text-center py-4 text-danger">
            Failed to load filtered data
          </td>
        </tr>
      `;
      console.error(err);
    }
  }

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const params = new URLSearchParams(new FormData(form));
    const url = `${baseUrl}?${params.toString()}`;
    await loadFiltered(url);
  });

  resetBtn?.addEventListener("click", () => {
    form.reset();
  });

  clearBtn?.addEventListener("click", async () => {
    form.reset();
    await loadFiltered(baseUrl);
  });
}