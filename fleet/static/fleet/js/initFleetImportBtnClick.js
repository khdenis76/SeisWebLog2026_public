import { getCSRFToken } from "../../baseproject/js/csrf.js"; // adjust path if needed
export function initFleetImportBtnClick() {

    const btn = document.getElementById("btn-import-fleet");
    if (!btn) return;

    const importUrl = btn.dataset.importUrl;
    if (!importUrl) {
        console.error("Missing data-import-url on btn-import-fleet");
        return;
    }

    btn.addEventListener("click", async function () {

        btn.disabled = true;
        btn.classList.add("opacity-75");
        console.log("csrf:", getCSRFToken(), "len:", (getCSRFToken() || "").length);
        try {
            const response = await fetch(importUrl, {
                method: "POST",
                headers: {
                    "X-CSRFToken": getCSRFToken(),
                    "Accept": "application/json",
                },
            });

            const data = await response.json().catch(() => ({}));

            if (!response.ok || !data.ok) {
                alert(data.error || "Fleet import failed.");
                return;
            }

            const r = data.result || {};

            alert(
                `Fleet updated successfully.\nCreated: ${r.created}\nSkipped: ${r.skipped}`
            );

            location.reload();

        } catch (error) {
            console.error("Fleet import error:", error);
            alert("Fleet import error.");
        } finally {
            btn.disabled = false;
            btn.classList.remove("opacity-75");
        }

    });
}