import {getCSRFToken} from "./csrf.js";

export function initExportSolEolBtn() {
  document.querySelectorAll('.tool-btn').forEach(btn => {

    btn.addEventListener('click', async () => {
      const pointType = btn.dataset.pointType;
      const url = btn.dataset.postUrl;

      if (!url || !pointType) {
        console.error('Missing data-post-url or data-point-type');
        return;
      }

      try {
        const response = await fetch(url, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCSRFToken(),
          },
          body: JSON.stringify({
            point_type: pointType,
          }),
        });

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }

        const data = await response.json(); // expecting { message: "..." }

        showToast(data.message || 'Export finished');

      } catch (err) {
        console.error(err);
        showToast('Export failed', 'danger');
      }
    });

  });
}
function showToast(message, type = 'success') {
  const toastEl = document.getElementById('app-toast');
  const toastBody = toastEl.querySelector('.toast-body');

  toastEl.classList.remove('text-bg-success', 'text-bg-danger');
  toastEl.classList.add(`text-bg-${type}`);

  toastBody.textContent = message;

  bootstrap.Toast.getOrCreateInstance(toastEl).show();
}
