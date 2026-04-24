export function initQCCardCloseButtons() {

    document.addEventListener("click", (e) => {

        const closeBtn = e.target.closest(".btn-close");
        if (!closeBtn) return;

        const card = closeBtn.closest(".qc-card");
        if (!card) return;

        // Option 1: just hide it
        card.classList.add("d-none");

        // Option 2 (alternative): remove from DOM completely
        // card.remove();

    });

}
