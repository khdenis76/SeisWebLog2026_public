import { getCSRFToken } from "../../baseproject/js/csrf.js";

export function initDailySMDaySelect() {
    const dayInput = document.getElementById("sm-day-select");
    const deploymentContainer = document.getElementById("daily-sm-container");
    const recoveryContainer = document.getElementById(
        "daily-sm-recovery-container"
    );

    if (!dayInput || !deploymentContainer || !recoveryContainer) {
        console.warn("SM daily elements were not found");
        return;
    }

    const url = dayInput.dataset.dayurl;

    if (!url) {
        console.warn("sm-day-select is missing data-dayurl");
        return;
    }

    async function loadSMDay(day) {
        if (!day) {
            deploymentContainer.innerHTML = "";
            recoveryContainer.innerHTML = "";
            return;
        }

        const loadingHtml = `
            <div class="d-flex align-items-center justify-content-center h-100">
                <div class="text-muted">
                    <span class="spinner-border spinner-border-sm me-2"></span>
                    Loading SM data...
                </div>
            </div>
        `;

        deploymentContainer.innerHTML = loadingHtml;
        recoveryContainer.innerHTML = loadingHtml;

        try {
            const response = await fetch(url, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "X-CSRFToken": getCSRFToken(),
                    "X-Requested-With": "XMLHttpRequest",
                },
                body: JSON.stringify({
                    day: day,
                }),
            });

            let data;

            try {
                data = await response.json();
            } catch (jsonError) {
                throw new Error("Server returned an invalid JSON response.");
            }

            if (!response.ok || data.ok === false) {
                throw new Error(data.error || "Unable to load SM data.");
            }

            deploymentContainer.innerHTML =
                data.deployment_html || emptyTableMessage(
                    "No deployment data found."
                );

            recoveryContainer.innerHTML =
                data.recovery_html || emptyTableMessage(
                    "No recovery data found."
                );

            console.log(
                `SM data loaded: ${data.deployment_count ?? 0} deployment, ` +
                `${data.recovery_count ?? 0} recovery`
            );

        } catch (error) {
            console.error("SM daily data error:", error);

            const errorHtml = `
                <div class="alert alert-danger m-2 mb-0">
                    ${escapeHtml(error.message)}
                </div>
            `;

            deploymentContainer.innerHTML = errorHtml;
            recoveryContainer.innerHTML = errorHtml;
        }
    }

    dayInput.addEventListener("change", () => {
        loadSMDay(dayInput.value);
    });

    if (dayInput.value) {
        loadSMDay(dayInput.value);
    }
}

function emptyTableMessage(message) {
    return `
        <div class="d-flex align-items-center justify-content-center h-100">
            <div class="text-muted">
                ${escapeHtml(message)}
            </div>
        </div>
    `;
}

function escapeHtml(value) {
    const element = document.createElement("div");
    element.textContent = value ?? "";
    return element.innerHTML;
}