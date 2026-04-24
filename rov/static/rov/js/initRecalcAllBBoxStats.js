import { initBBoxFileTable } from "./initBBoxFileTable.js";

export function initRecalcAllBBoxStats() {
    const btn = document.getElementById("btn-recalc-all-bbox-stats");
    const confirmBtn = document.getElementById("confirm-recalc-all-bbox-stats");
    const modalEl = document.getElementById("recalcAllBBoxStatsModal");
    const tbody = document.getElementById("bbox-list-tbody");

    if (!btn || !confirmBtn || !modalEl || !tbody) {
        return;
    }

    const modal = bootstrap.Modal.getOrCreateInstance(modalEl);

    function showToast(message, type = "success") {
        if (window.showToast) {
            window.showToast(message, type);
        } else {
            console.log(type, message);
        }
    }

    function getCSRFToken() {
        const input = document.querySelector('[name=csrfmiddlewaretoken]');
        if (input) return input.value;
        const match = document.cookie.match(/csrftoken=([^;]+)/);
        return match ? match[1] : "";
    }

    btn.addEventListener("click", () => {
        modal.show();
    });

    confirmBtn.addEventListener("click", async () => {
        const url = btn.dataset.url;
        if (!url) {
            showToast("Recalc URL is missing.", "danger");
            return;
        }

        const originalHtml = confirmBtn.innerHTML;
        confirmBtn.disabled = true;
        btn.disabled = true;
        confirmBtn.innerHTML = `<span class="spinner-border spinner-border-sm me-2"></span>Processing...`;

        try {
            const response = await fetch(url, {
                method: "POST",
                headers: {
                    "X-CSRFToken": getCSRFToken(),
                    "X-Requested-With": "XMLHttpRequest",
                    "Content-Type": "application/json"
                },
                body: JSON.stringify({})
            });

            const data = await response.json();

            if (!response.ok || !data.success) {
                throw new Error(data.error || "Failed to recalc stats.");
            }

            if (data.tbody) {
                tbody.innerHTML = data.tbody;
                initBBoxFileTable();
            }

            modal.hide();
            showToast(`Recalculated ${data.recalculated || 0}/${data.total_files || 0} BBOX file stats.`, "success");
        } catch (err) {
            showToast(err.message || "Unexpected error.", "danger");
        } finally {
            confirmBtn.disabled = false;
            btn.disabled = false;
            confirmBtn.innerHTML = originalHtml;
        }
    });
}