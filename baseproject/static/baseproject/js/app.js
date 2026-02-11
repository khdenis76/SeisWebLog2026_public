// app.js

import { initAllJSForRovUpload } from "../../rov/js/init.js";
import {initAllJSForBaseproject} from "./init.js";

document.addEventListener("DOMContentLoaded", () => {
  //baseproject
  initAllJSForBaseproject()
  //rov JS
  initAllJSForRovUpload()

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
