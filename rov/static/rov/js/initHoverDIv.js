export function initHoverDivs() {
    document.querySelectorAll(".hover-wrapper").forEach(wrapper => {

        const btn = wrapper.querySelector(".hover-btn");
        if (!btn) return;

        const targetId = btn.dataset.hoverTarget;
        const content = document.getElementById(targetId);
        if (!content) return;

        let timeout;

        wrapper.addEventListener("mouseenter", () => {
            clearTimeout(timeout);
            content.style.display = "block";
        });

        wrapper.addEventListener("mouseleave", () => {
            timeout = setTimeout(() => {
                content.style.display = "none";
            }, 150); // small delay prevents flicker
        });

    });
}
