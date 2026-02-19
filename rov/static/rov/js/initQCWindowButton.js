export function initQCWindowButtons() {

    const buttons = document.querySelectorAll(".qc-window-button");
    if (!buttons.length) return;

    buttons.forEach(btn => {

        btn.addEventListener("click", (e) => {

            e.preventDefault();
            e.stopPropagation(); // prevents hover wrapper closing issues

            const targetId = btn.dataset.cardTarget;
            if (!targetId) return;

            const targetCard = document.getElementById(targetId);
            if (!targetCard) return;

            // Hide all QC cards
            //document.querySelectorAll(".qc-card").forEach(card => {
                //card.classList.add("d-none");
            //});

            // Show selected card
            targetCard.classList.remove("d-none");

        });

    });
}
