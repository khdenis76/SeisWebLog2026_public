import { getCSRFToken } from "../../baseproject/js/csrf.js"; // adjust path if needed
// sps_delete.js
export function initSpsDelete() {
    const deleteBtn = document.getElementById("sps-delete-button");
    const tbody = document.getElementById("sps-table-tbody");
    const checkAll = document.getElementById("sps-check-all");

    const modalEl = document.getElementById("spsDeleteModal");
    const countEl = document.getElementById("sps-delete-count");
    const confirmBtn = document.getElementById("sps-confirm-delete-btn");

    if (!deleteBtn || !tbody || !modalEl || !confirmBtn) return;

    const modal = new bootstrap.Modal(modalEl);

    // --- helpers ---
    const getCSRF = () => document.querySelector('[name=csrfmiddlewaretoken]')?.value;

    const getSelectedIds = () => {
    const ids = [];

    // only inside this table (prevents picking checkboxes from other tables)
    tbody.querySelectorAll("input.row-check:checked").forEach(cb => {
        // robust: sometimes checkbox is wrapped (label/span), so try multiple climbs
        let tr = cb.closest("tr");

        if (!tr) {
            const td = cb.closest("td");
            if (td) tr = td.closest("tr");
        }

        if (!tr) return;

        const id = tr.getAttribute("data-id"); // or tr.dataset.id
        if (id) ids.push(Number(id));
    });

    return ids;
};

    const updateCheckAllState = () => {
        if (!checkAll) return;
        const all = document.querySelectorAll(".row-check");
        const checked = document.querySelectorAll(".row-check:checked");
        checkAll.checked = all.length > 0 && checked.length === all.length;
    };

    // --- events ---
    if (checkAll) {
        checkAll.addEventListener("change", () => {
            document.querySelectorAll(".row-check").forEach(cb => {
                cb.checked = checkAll.checked;
            });
        });
    }

    document.addEventListener("change", (e) => {
        if (!e.target.classList.contains("row-check")) return;
        updateCheckAllState();
    });

    // open modal
    deleteBtn.addEventListener("click", () => {
        const ids = getSelectedIds();
        if (countEl) countEl.textContent = String(ids.length);
        modal.show();
    });

    // confirm delete -> fetch -> replace tbody with returned HTML
    confirmBtn.addEventListener("click", async () => {
        const ids = getSelectedIds();
        if (!ids.length) {
            modal.hide();
            return;
        }

        const url = deleteBtn.dataset.url;

        if (!url) {
            alert("Missing delete URL (data-url)");
            return;
        }

        confirmBtn.disabled = true;

        try {
            const resp = await fetch(url, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "X-CSRFToken": getCSRFToken()
                },
                body: JSON.stringify({ ids })
            });

            const data = await resp.json();

            if (!resp.ok || !data.ok) {
                alert(data.error || "Delete failed");
                return;
            }

            // IMPORTANT: Django returns rendered <tr>... rows HTML
            tbody.innerHTML = data.sps_summary;

            if (checkAll) checkAll.checked = false;
            modal.hide();

        } catch (err) {
            alert("Network error: " + err);
        } finally {
            confirmBtn.disabled = false;
        }
    });
}