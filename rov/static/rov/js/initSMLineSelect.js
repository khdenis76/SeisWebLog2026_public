import { getCSRFToken } from "../../baseproject/js/csrf.js";

export function initSMLineSelect() {
    const lineInput = document.getElementById("sm-line-input");
    const dayInput = document.getElementById("sm-day-select");

    const deploymentContainer =
        document.getElementById("daily-sm-container");

    const recoveryContainer =
        document.getElementById("daily-sm-recovery-container");

    if (!lineInput || !deploymentContainer || !recoveryContainer) {
        console.warn("SM line filter elements were not found");
        return;
    }

    const url = lineInput.dataset.lineurl;

    if (!url) {
        console.warn("sm-line-input is missing data-lineurl");
        return;
    }

    async function loadLine(line) {
        line = String(line || "").trim();

        if (!line) {
            clearContainers();
            return;
        }

        // Line filter is independent from day filter.
        // Clear the date so the UI shows which filter is active.
        if (dayInput) {
            dayInput.value = "";
        }

        showLoading();

        try {
            const response = await fetch(url, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "X-CSRFToken": getCSRFToken(),
                    "X-Requested-With": "XMLHttpRequest",
                },
                body: JSON.stringify({
                    line: line,
                }),
            });

            let data;

            try {
                data = await response.json();
            } catch {
                throw new Error("Server returned invalid JSON.");
            }

            if (!response.ok || data.ok === false) {
                throw new Error(
                    data.error || "Unable to load the selected line."
                );
            }

            deploymentContainer.innerHTML =
                data.deployment_html || "";

            recoveryContainer.innerHTML =
                data.recovery_html || "";

        } catch (error) {
            console.error("SM line filter error:", error);
            showError(error.message);
        }
    }

    /*
     * Runs when the user selects a datalist option
     * or leaves the manually entered value.
     */
    lineInput.addEventListener("change", () => {
        loadLine(lineInput.value);
    });

    /*
     * Runs when the user manually enters a line
     * and presses Enter.
     */
    lineInput.addEventListener("keydown", event => {
        if (event.key !== "Enter") {
            return;
        }

        event.preventDefault();
        loadLine(lineInput.value);
    });

    function showLoading() {
        const html = `
            <div class="d-flex align-items-center justify-content-center h-100">
                <div class="text-muted">
                    <span class="spinner-border spinner-border-sm me-2"></span>
                    Loading line...
                </div>
            </div>
        `;

        deploymentContainer.innerHTML = html;
        recoveryContainer.innerHTML = html;
    }

    function clearContainers() {
        deploymentContainer.innerHTML = "";
        recoveryContainer.innerHTML = "";
    }

    function showError(message) {
        const html = `
            <div class="alert alert-danger m-2 mb-0">
                ${escapeHtml(message)}
            </div>
        `;

        deploymentContainer.innerHTML = html;
        recoveryContainer.innerHTML = html;
    }
}

function escapeHtml(value) {
    const element = document.createElement("div");
    element.textContent = value ?? "";
    return element.innerHTML;
}