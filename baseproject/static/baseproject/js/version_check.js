// core/static/core/js/version-check.js

const VersionChecker = (function () {

    let config = {
        endpoint: "/api/version/",
        showOncePerSession: true,
        toastDelay: 15000,
    };

    async function check() {

        if (config.showOncePerSession &&
            sessionStorage.getItem("swl_update_shown") === "1") {
            return;
        }

        try {
            const response = await fetch(config.endpoint, { cache: "no-store" });
            const data = await response.json();

            if (data.ok && data.new_available) {
                showToast(data);

                if (config.showOncePerSession) {
                    sessionStorage.setItem("swl_update_shown", "1");
                }
            }

        } catch (err) {
            console.warn("Version check failed:", err);
        }
    }

    function showToast(data) {

        const container = document.getElementById("versionToastContainer");

        if (!container) return;

        const toastHtml = `
            <div class="toast align-items-center text-bg-warning border-0"
                 role="alert"
                 data-bs-delay="${config.toastDelay}">
              <div class="d-flex">
                <div class="toast-body">
                  🚀 New version <strong>${data.remote}</strong> available
                  (you have ${data.local})
                  <a href="${data.download_url}" target="_blank"
                     class="btn btn-sm btn-dark ms-2">
                     Download
                  </a>
                </div>
                <button type="button" class="btn-close me-2 m-auto"
                        data-bs-dismiss="toast"></button>
              </div>
            </div>
        `;

        container.insertAdjacentHTML("beforeend", toastHtml);

        const toastEl = container.lastElementChild;
        const toast = new bootstrap.Toast(toastEl);
        toast.show();
    }

    function init(userConfig = {}) {
        config = { ...config, ...userConfig };
        document.addEventListener("DOMContentLoaded", check);
    }

    return {
        init
    };

})();