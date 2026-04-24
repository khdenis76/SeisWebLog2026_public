// baseproject/js/initThemeToggle.js

import {getCSRFToken} from "./csrf.js";

function applyTheme(theme) {
    document.documentElement.setAttribute("data-bs-theme", theme);

    document.body.classList.toggle("bg-dark", theme === "dark");
    document.body.classList.toggle("bg-light", theme === "light");

    const btn = document.getElementById("themeToggle");
    if (!btn) return;

    btn.dataset.theme = theme;

    const icon = btn.querySelector(".theme-icon");
    if (icon) {
        icon.innerHTML = theme === "dark"
            ? '<i class="fa-solid fa-sun"></i>'
            : '<i class="fa-solid fa-moon"></i>';
    }
}

export function initThemeToggle() {
    const btn = document.getElementById("themeToggle");
    if (!btn) return;

    btn.addEventListener("click", async () => {

        const current = btn.dataset.theme || "dark";
        const next = current === "dark" ? "light" : "dark";

        // instant UI feedback
        applyTheme(next);

        const formData = new FormData();
        formData.append("theme", next);

        try {
            const response = await fetch("/ui/theme/", {
                method: "POST",
                headers: {
                    "X-CSRFToken": getCSRFToken(),
                },
                body: formData,
            });

            const data = await response.json();

            if (!response.ok || !data.ok) {
                applyTheme(current);  // rollback
                console.error("Theme update failed:", data);
            }

        } catch (error) {
            applyTheme(current);  // rollback
            console.error("Theme update error:", error);
        }
    });
}