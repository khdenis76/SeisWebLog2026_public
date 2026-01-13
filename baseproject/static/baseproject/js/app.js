// app.js
import { initPreplotUpload } from "./upload.js";
import { initRLCheckboxes, loadRLPreplotTable } from "./rlpreplotTable.js";

document.addEventListener("DOMContentLoaded", () => {
  initRLCheckboxes();
  initPreplotUpload();
  loadRLPreplotTable(); // если хочешь обновлять таблицу через fetch при открытии
});
