let lottieAnim = null;

function initLottieOnce() {
  const el = document.getElementById("loading");
  if (!el) return;

  if (lottieAnim) return; // уже создано

  lottieAnim = lottie.loadAnimation({
    container: el,
    renderer: "svg",
    loop: true,
    autoplay: true,
    path: "/static/baseproject/lottie/waiting.json", // <-- проверь!
  });

  lottieAnim.setSpeed(0.8);
}
function initLoadingLottie() {
  const box = document.getElementById("loading"); // div внутри модалки
  if (!box) {
    console.error("Lottie container #loading not found");
    return;
  }
  if (!window.lottie) {
    console.error("lottie-web not loaded: window.lottie is undefined");
    return;
  }

  box.innerHTML = ""; // на всякий случай

  lottieAnim = window.lottie.loadAnimation({
    container: box,
    renderer: "svg",
    loop: true,
    autoplay: true,
    path: "/static/baseproject/lottie/waiting.json", // <-- именно так, по твоей структуре
  });

  lottieAnim.addEventListener("data_failed", () => {
    console.error("Lottie data_failed (check JSON url/path)");
  });
}
