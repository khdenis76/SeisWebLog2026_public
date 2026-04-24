import { initSVPSplit } from "./initSVPSplit.js";
import { initSVPTable } from "./initSVPTable.js";
import { initSVPUpload } from "./initSVPUpload.js";
import {initSVPConfig} from "./initSVPConfig.js";

document.addEventListener("DOMContentLoaded", () => {
  initSVPSplit();
  initSVPTable();
  initSVPUpload();
  initSVPConfig();
});