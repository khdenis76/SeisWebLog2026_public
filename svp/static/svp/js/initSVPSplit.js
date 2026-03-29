export function initSVPSplit() {
  const wrap = document.getElementById("svpSplitWrap");
  const left = document.getElementById("svpLeftPanel");
  const right = document.getElementById("svpRightPanel");
  const divider = document.getElementById("svpDivider");

  const btnHideLeft = document.getElementById("btnHideLeft");
  const btnHideRight = document.getElementById("btnHideRight");
  const btnReset = document.getElementById("btnResetPanels");

  if (!wrap || !left || !right || !divider) {
    console.warn("SVP split init skipped: missing DOM elements");
    return;
  }

  const MOBILE_WIDTH = 992;
  const DEFAULT_LEFT = 38;
  const MIN_LEFT = 18;
  const MAX_LEFT = 82;

  let dragging = false;

  function isMobile() {
    return window.innerWidth < MOBILE_WIDTH;
  }

  function clearHidden() {
    left.classList.remove("svp-hidden");
    right.classList.remove("svp-hidden");
    divider.style.display = "";
  }

  function setDesktopSplit(percent) {
    const p = Math.max(MIN_LEFT, Math.min(MAX_LEFT, percent));
    clearHidden();

    left.style.width = `${p}%`;
    left.style.flex = `0 0 ${p}%`;

    right.style.width = `calc(${100 - p}% - 8px)`;
    right.style.flex = "1 1 auto";

    localStorage.setItem("svp_split_left", String(p));
    localStorage.setItem("svp_split_mode", "split");
  }

  function resetPanels() {
    setDesktopSplit(DEFAULT_LEFT);
    divider.style.display = "";
  }

  function hideLeft() {
    if (isMobile()) return;
    left.classList.add("svp-hidden");
    right.classList.remove("svp-hidden");
    right.style.width = "100%";
    right.style.flex = "1 1 100%";
    divider.style.display = "none";
    localStorage.setItem("svp_split_mode", "right");
  }

  function hideRight() {
    if (isMobile()) return;
    right.classList.add("svp-hidden");
    left.classList.remove("svp-hidden");
    left.style.width = "100%";
    left.style.flex = "0 0 100%";
    divider.style.display = "none";
    localStorage.setItem("svp_split_mode", "left");
  }

  function applySavedLayout() {
    if (isMobile()) {
      left.classList.remove("svp-hidden");
      right.classList.remove("svp-hidden");
      left.style.width = "100%";
      left.style.flex = "1 1 auto";
      right.style.width = "100%";
      right.style.flex = "1 1 auto";
      divider.style.display = "";
      return;
    }

    const mode = localStorage.getItem("svp_split_mode") || "split";
    const storedLeft = parseFloat(localStorage.getItem("svp_split_left") || `${DEFAULT_LEFT}`);

    if (mode === "left") {
      hideRight();
      return;
    }

    if (mode === "right") {
      hideLeft();
      return;
    }

    setDesktopSplit(Number.isFinite(storedLeft) ? storedLeft : DEFAULT_LEFT);
  }

  function onMouseMove(e) {
    if (!dragging || isMobile()) return;

    const rect = wrap.getBoundingClientRect();
    const offsetX = e.clientX - rect.left;
    const percent = (offsetX / rect.width) * 100;
    setDesktopSplit(percent);
  }

  function stopDragging() {
    if (!dragging) return;
    dragging = false;
    divider.classList.remove("is-dragging");
    document.body.classList.remove("svp-resizing");
  }

  divider.addEventListener("mousedown", (e) => {
    if (isMobile()) return;
    if (left.classList.contains("svp-hidden") || right.classList.contains("svp-hidden")) return;

    dragging = true;
    divider.classList.add("is-dragging");
    document.body.classList.add("svp-resizing");
    e.preventDefault();
  });

  document.addEventListener("mousemove", onMouseMove);
  document.addEventListener("mouseup", stopDragging);

  btnHideLeft?.addEventListener("click", hideLeft);
  btnHideRight?.addEventListener("click", hideRight);
  btnReset?.addEventListener("click", resetPanels);

  window.addEventListener("resize", applySavedLayout);

  applySavedLayout();

  console.log("SVP split initialized");
}