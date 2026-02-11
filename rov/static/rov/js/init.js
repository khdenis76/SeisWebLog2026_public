import {initRovUploadModal} from "./rovUpload.js";
import {initDSRLinesSelectAll} from "./initDSRMainCheckBox.js";
import {initDSRLineFilters} from "./DSRLineFilter.js";
import {initDSRLineRowClick} from "./initDSRLineRowCLick.js";
import {initBBCsvHeaderFetch} from "./initBBCSVHeader.js";
import {initBBoxConfigSave} from "./initSaveBboxConfigBtn.js";
import {initSetDefaultBBoxConfig} from "./initSetDefaultBboxCfg.js";
import {initBboxFileSelectAll} from "./initmainBboxFileCheckbox.js";
import {initProdCardToggle} from "./initProdCardCollapseBtn.js";
import {initDsrDeleteDropdown} from "./initDSRdeleteBtn.js";
import {initDeleteDSRLines} from "./initDeleteDsrLines.js";
import {initDeleteBboxFiles} from "./initDeleteBboxFile.js";
import {initBboxPlotClick} from "./initBboxRowClick.js";

export function initAllJSForRovUpload() {
  initRovUploadModal();
  initDSRLinesSelectAll();
  initDSRLineFilters();
  initDSRLineRowClick();
  initBBCsvHeaderFetch();
  initBBoxConfigSave();
  initSetDefaultBBoxConfig();
  initBboxFileSelectAll();
  initProdCardToggle();
  initDsrDeleteDropdown();
  initDeleteDSRLines();
  initDeleteBboxFiles();
  initBboxPlotClick();
  // initRovTables();
  // initRovCharts();
}