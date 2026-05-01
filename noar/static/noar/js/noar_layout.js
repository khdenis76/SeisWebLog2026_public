export function initNOARLayout() {
    const leftCol = document.getElementById("left-noar-col");
    const rightCol = document.getElementById("right-noar-col");

    const leftBtn = document.getElementById("toggle-left-noar-btn");
    const rightBtn = document.getElementById("toggle-right-noar-btn");

    const leftIcon = document.getElementById("left-noar-toggle-icon");
    const rightIcon = document.getElementById("right-noar-toggle-icon");

    if (!leftCol || !rightCol || !leftBtn || !rightBtn) {
        return;
    }

    function resetColumns() {
        leftCol.className = "col-4";
        rightCol.className = "col-8 show collapse";

        leftCol.style.display = "";
        rightCol.style.display = "";

        if (leftIcon) leftIcon.className = "fas fa-expand";
        if (rightIcon) rightIcon.className = "fas fa-expand";
    }

    leftBtn.addEventListener("click", function () {
        const isExpanded = leftCol.classList.contains("col-12");

        if (isExpanded) {
            resetColumns();
            return;
        }

        leftCol.className = "col-12";
        rightCol.className = "d-none";

        if (leftIcon) leftIcon.className = "fas fa-compress";
        if (rightIcon) rightIcon.className = "fas fa-expand";
    });

    rightBtn.addEventListener("click", function () {
        const isExpanded = rightCol.classList.contains("col-12");

        if (isExpanded) {
            resetColumns();
            return;
        }

        leftCol.className = "d-none";
        rightCol.className = "col-12 show collapse";

        if (rightIcon) rightIcon.className = "fas fa-compress";
        if (leftIcon) leftIcon.className = "fas fa-expand";
    });
}