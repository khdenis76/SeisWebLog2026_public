export function initNoarLayout() {

    const wrap = document.getElementById("noar-split-wrap");
    const resizer = document.getElementById("noar-split-resizer");

    const leftToggle = document.getElementById("toggle-left-noar-btn");
    const rightToggle = document.getElementById("toggle-right-noar-btn");

    const leftIcon = document.getElementById("left-noar-toggle-icon");
    const rightIcon = document.getElementById("right-noar-toggle-icon");

    if (!wrap) {
        return;
    }

    const defaultColumns =
        "minmax(260px, 360px) 7px minmax(0, 1fr)";

    const leftMaxColumns =
        "minmax(0, 1fr) 7px 0";

    const rightMaxColumns =
        "0 7px minmax(0, 1fr)";

    let isDragging = false;

    // =====================================================
    // ICONS
    // =====================================================

    function resetIcons() {

        if (leftIcon) {
            leftIcon.classList.remove("fa-compress");
            leftIcon.classList.add("fa-expand");
        }

        if (rightIcon) {
            rightIcon.classList.remove("fa-compress");
            rightIcon.classList.add("fa-expand");
        }
    }

    // =====================================================
    // DEFAULT LAYOUT
    // =====================================================

    function setDefaultLayout() {

        wrap.classList.remove("noar-left-maximized");
        wrap.classList.remove("noar-right-maximized");

        wrap.style.gridTemplateColumns = defaultColumns;

        resetIcons();
    }

    // =====================================================
    // LEFT MAXIMIZED
    // =====================================================

    function setLeftMaximized() {

        wrap.classList.add("noar-left-maximized");
        wrap.classList.remove("noar-right-maximized");

        wrap.style.gridTemplateColumns = leftMaxColumns;

        resetIcons();

        if (leftIcon) {
            leftIcon.classList.remove("fa-expand");
            leftIcon.classList.add("fa-compress");
        }
    }

    // =====================================================
    // RIGHT MAXIMIZED
    // =====================================================

    function setRightMaximized() {

        wrap.classList.add("noar-right-maximized");
        wrap.classList.remove("noar-left-maximized");

        wrap.style.gridTemplateColumns = rightMaxColumns;

        resetIcons();

        if (rightIcon) {
            rightIcon.classList.remove("fa-expand");
            rightIcon.classList.add("fa-compress");
        }
    }

    // =====================================================
    // SPLITTER RESIZE
    // =====================================================

    if (resizer) {

        resizer.addEventListener("mousedown", function () {

            if (
                wrap.classList.contains("noar-left-maximized") ||
                wrap.classList.contains("noar-right-maximized")
            ) {
                return;
            }

            isDragging = true;

            document.body.style.cursor = "col-resize";
            document.body.style.userSelect = "none";
        });

        window.addEventListener("mousemove", function (event) {

            if (!isDragging) {
                return;
            }

            const rect = wrap.getBoundingClientRect();

            let leftWidth = event.clientX - rect.left;

            const minLeftWidth = 240;
            const minRightWidth = 260;

            const maxLeftWidth = rect.width - minRightWidth;

            leftWidth = Math.max(
                minLeftWidth,
                Math.min(leftWidth, maxLeftWidth)
            );

            wrap.style.gridTemplateColumns =
                `${leftWidth}px 7px minmax(0, 1fr)`;
        });

        window.addEventListener("mouseup", function () {

            if (!isDragging) {
                return;
            }

            isDragging = false;

            document.body.style.cursor = "";
            document.body.style.userSelect = "";
        });
    }

    // =====================================================
    // LEFT BUTTON
    // =====================================================

    if (leftToggle) {

        leftToggle.addEventListener("click", function () {

            if (wrap.classList.contains("noar-left-maximized")) {
                setDefaultLayout();
            }
            else {
                setLeftMaximized();
            }

        });
    }

    // =====================================================
    // RIGHT BUTTON
    // =====================================================

    if (rightToggle) {

        rightToggle.addEventListener("click", function () {

            if (wrap.classList.contains("noar-right-maximized")) {
                setDefaultLayout();
            }
            else {
                setRightMaximized();
            }

        });
    }

    console.log("[NOAR] Layout initialized");
}