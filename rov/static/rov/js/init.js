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
import {initCardMinMax} from "./initMinMaxCardButton.js";
import {initBBoxConfigDatalist} from "./bboxConfigPick.js";
import {initColumnToggles} from "./initColumnToggles.js";
import {initHoverDivs} from "./initHoverDIv.js";
import {initHoverTabPopups} from "./initHoverTabPopups.js";
import {initQCWindowButtons} from "./initQCWindowButton.js";
import {initQCCardCloseButtons} from "./initQCCardCloseButton.js";
import {initDsrExportSmModal} from "./dsrExportSm.js";
import {initDsrRovListAutoReload} from "./dsrExportRovs.js";
import {initDailyProdDaySelect} from "./dailyProdDaySelect.js";
import {initExportSpsModal} from "./initExportSpsModal.js";
// import {initDsrLineClick} from "./initDSRLineClick.js";
import {initBatteryLifeMap,initBatteryLifeRestMap} from "./initBatteryLifeButton.js";
import {initDSRHistogram} from "./initDSRHistogram.js";
import {initDSRLineSort} from "./initDSRLineSort.js";
import {initLineMinMaxQc} from "./initLineMinMaxQCBtn.js";
import {initDSRLineQCTabs} from "./initDSRLineQCTabs.js";



export function initAllJSForRovUpload() {
  initRovUploadModal();
  initDSRLinesSelectAll();
  initDSRLineFilters();
  initDSRLineRowClick();
  initBBoxConfigDatalist();
  initBBCsvHeaderFetch();
  initBBoxConfigSave();
  initSetDefaultBBoxConfig();
  initBboxFileSelectAll();
  initProdCardToggle();
  initDsrDeleteDropdown();
  initDeleteDSRLines();
  initDeleteBboxFiles();
  initBboxPlotClick();
  initHoverDivs();
  initHoverTabPopups();
  initQCWindowButtons();
  initDsrRovListAutoReload();
  initDailyProdDaySelect();
  initExportSpsModal();
  initDsrExportSmModal({ rovNames: window.DSR_ROV_NAMES || [] });

  initBatteryLifeMap();
  initBatteryLifeRestMap();
  initDSRHistogram();
  initDSRLineSort();
  initLineMinMaxQc();
  initDSRLineQCTabs();


    // GNSS QC
  initCardMinMax({
    buttonId: "gnss-qc-min-max-btn",
    bodyId: "gnss-qc-card-body",
    iconId: "gnss-qc-toggle-icon",
  });

  // HDOP QC
  initCardMinMax({
    buttonId: "hdop-qc-min-max-btn",
    bodyId: "hdop-qc-card-body",
    iconId: "hdop-qc-toggle-icon",
  });
  initCardMinMax({
    buttonId: "depth-qc-min-max-btn",
    bodyId: "rov-depth-qc-card-body",
    iconId: "rov-depth-toggle-icon",
  });
  initCardMinMax({
    buttonId: "vessel-sog-min-max-btn",
    bodyId: "vessel-sog-card-body",
    iconId: "vessel-sog-toggle-icon",
  });
  initCardMinMax({
    buttonId: "hdg-cog-min-max-btn",
    bodyId: "hdg-cog-card-body",
    iconId: "hdg-cog-toggle-icon",
  });
  initCardMinMax({
    buttonId: "drift-min-max-btn",
    bodyId: "drift-card-body",
    iconId: "drift-toggle-icon",
  });


  //initColumnToggles(btn_id, div_id, icon_id, opts = {})
  initColumnToggles(
    "toggle-left-rov-btn",
    "left-rov-col",
    "left-rov-toggle-icon",
    { toggleElId: "right-rov-col", divOn: "col-12", divOff: "col-4" }
  );

  initColumnToggles(
    "toggle-right-rov-btn",
    "right-rov-col",
    "right-rov-toggle-icon",
    { toggleElId: "left-rov-col", divOn: "col-12", divOff: "col-4" }
  );
  initQCCardCloseButtons();
  // initRovTables();
  // initRovCharts();
}