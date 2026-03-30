document.addEventListener("DOMContentLoaded", function () {
    checkVersion();
});

function checkVersion() {
    fetch("/api/version/", {
        method: "GET",
        headers: {
            "X-Requested-With": "XMLHttpRequest"
        }
    })
        .then((response) => {
            if (!response.ok) {
                throw new Error("Version check request failed");
            }
            return response.json();
        })
        .then((data) => {
            if (!data || !data.update_available) {
                return;
            }
            showUpdateToast(data);
        })
        .catch((error) => {
            console.warn("Version check failed:", error);
        });
}

function showUpdateToast(data) {
    if (document.getElementById("swl-version-update-toast")) {
        return;
    }

    const localVersion = data.local_version || "unknown";
    const remoteVersion = data.remote_version || "unknown";

    const toastHtml = `
        <div
            id="swl-version-update-toast"
            class="toast align-items-center text-bg-warning border-0 show position-fixed bottom-0 end-0 m-3 shadow"
            role="alert"
            aria-live="assertive"
            aria-atomic="true"
            style="z-index: 1080; min-width: 360px;"
        >
            <div class="d-flex">
                <div class="toast-body">
                    <div class="fw-bold mb-1">
                        <i class="fa-solid fa-arrow-rotate-right me-2"></i>
                        New SeisWebLog version available
                    </div>

                    <div class="small">
                        <div><strong>Current:</strong> ${escapeHtml(localVersion)}</div>
                        <div><strong>Latest:</strong> ${escapeHtml(remoteVersion)}</div>
                    </div>

                    <div class="small mt-2">
                        Close SWL and run <code>update_project.bat</code>.
                    </div>

                    <div class="small mt-1">
                        A local backup will be created before update.
                        You can roll back later with <code>restore_project.bat</code>.
                    </div>
                </div>

                <button
                    type="button"
                    class="btn-close btn-close-white me-2 m-auto"
                    aria-label="Close"
                    onclick="this.closest('.toast').remove()"
                ></button>
            </div>
        </div>
    `;

    document.body.insertAdjacentHTML("beforeend", toastHtml);
}

function escapeHtml(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}