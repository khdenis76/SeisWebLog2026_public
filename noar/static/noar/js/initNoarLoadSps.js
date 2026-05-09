export function initNoarLoadSps() {

    const loadBtn = document.getElementById("btn-noar-load");
    const modalEl = document.getElementById("noarLoadSpsModal");
    const form = document.getElementById("noar-load-sps-form");
    const submitBtn = document.getElementById("noar-load-sps-submit");
    const statusBox = document.getElementById("noar-load-sps-status");

    if (!loadBtn || !modalEl || !form) {
        return;
    }

    const modal = new bootstrap.Modal(modalEl);

    function showStatus(message, type = "info") {
        if (!statusBox) return;

        statusBox.className = `alert alert-${type} mt-3 mb-0`;
        statusBox.textContent = message;
    }

    function clearStatus() {
        if (!statusBox) return;

        statusBox.className = "alert d-none mt-3 mb-0";
        statusBox.textContent = "";
    }

    loadBtn.addEventListener("click", function () {
        clearStatus();
        form.reset();
        modal.show();
    });

    form.addEventListener("submit", async function (event) {
        event.preventDefault();

        const url = form.dataset.loadUrl;
        const formData = new FormData(form);

        if (!url) {
            showStatus("Load URL is missing.", "danger");
            return;
        }

        submitBtn.disabled = true;
        submitBtn.innerHTML =
            `<span class="spinner-border spinner-border-sm me-2"></span>Loading...`;

        clearStatus();

        try {
            const response = await fetch(url, {
                method: "POST",
                body: formData,
                headers: {
                    "X-Requested-With": "XMLHttpRequest",
                },
            });

            const data = await response.json();

            if (!response.ok || !data.ok) {
                throw new Error(data.error || "SPS loading failed.");
            }

            showStatus(data.message || "SPS files loaded successfully.", "success");

            window.dispatchEvent(new CustomEvent("noar:sps-loaded", {
                detail: data,
            }));

        } catch (error) {
            showStatus(error.message, "danger");
        } finally {
            submitBtn.disabled = false;
            submitBtn.innerHTML =
                `<i class="fas fa-upload me-2"></i>Load SPS`;
        }
    });
}