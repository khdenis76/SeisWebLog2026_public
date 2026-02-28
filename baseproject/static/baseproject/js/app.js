// app.js

import { initAllJSForRovUpload } from "../../rov/js/init.js";
import {initAllJSForBaseproject} from "./init.js";
import { initAllJSForFleet } from "../../fleet/js/init.js";
import {initAllJSForSource} from "../../source/js/init.js";

document.addEventListener("DOMContentLoaded", () => {
  const current = window.location.pathname;
    document.querySelectorAll("#sidebarOffcanvas .nav-link").forEach(link => {
        if (link.getAttribute("href") === current) {
            link.classList.add("active");
        }
    });
  //baseproject
  initAllJSForBaseproject()
  //rov JS
  initAllJSForRovUpload()
  //fleet js
  initAllJSForFleet();
  //source js
  initAllJSForSource();

  console.log("Init Delete Buttons")

  document.querySelectorAll('[data-bs-toggle="tooltip"]').forEach((el) => {
    // на всякий случай: если уже есть инстанс — не создаём второй
    if (!bootstrap.Tooltip.getInstance(el)) {
      new bootstrap.Tooltip(el, {
        container: "body", // важно: решает проблемы с overflow/clip
      });
    }
  });

});
