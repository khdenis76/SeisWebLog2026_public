import { initPreplotUpload } from "./upload.js";
import { initRLCheckboxes,initSLCheckboxes} from "./rlpreplotTable.js";
import {initDeleteRL,initDeletePreplot} from "./deletepreplot.js"
import { updatePreplotTable } from "./updaterltable.js";
import {initMainShapeCheckBox, initAddShapeButton} from "./shapes.js"
import { initProjectShapesAutoSave } from "./shapes.js"
import {initDeleteShapesButton, forceInputsFromHtmlDefaults} from "./delete_shapes.js"
import {initShapeFolderSearchButton} from "./shapes_search.js"
import {initPointTypeSwitch} from "./tools.js"
import {initExportSolEolBtn} from "./export_SOL_EOL.js";
import {initCsvHeaderFetch} from "./csvHeader.js"
import {initCsvLayerUpload} from "./uploadCSV.js";
import {initProjectLayersAutoSave} from "./autoSaveLayers.js";
import {initDeleteLayersBtn} from "./deleteCSVLayer.js";
import {initRLLineClick} from "./rlLineClick.js";
import {initSLLineClick} from "./slLineClick.js";
import {initThemeToggle} from "./initThemeToggle.js";

export function initAllJSForBaseproject() {
    initRLCheckboxes();
  initSLCheckboxes();
  initPreplotUpload();
  //initShapes();
  initMainShapeCheckBox();
  initAddShapeButton();
  initProjectShapesAutoSave();
  initDeleteShapesButton();
  forceInputsFromHtmlDefaults();
  initShapeFolderSearchButton();
  initPointTypeSwitch();
  initExportSolEolBtn();
  initCsvHeaderFetch();
  initCsvLayerUpload();
  initProjectLayersAutoSave();
  initDeleteLayersBtn();
  initRLLineClick();
  initSLLineClick();
  initThemeToggle();

  initDeletePreplot([
    {
      deleteBtnId: "btnDeleteRL",
      checkboxClass: "rl-preplot-checkbox",
      mainCheckboxId: "MainRLPreplotCheckbox",
      deleteUrl: window.RL_DELETE_URL,
      rowsKey: "rl_rows",
      title: "Delete RL lines",
      onUpdatedRows: (rows) => updatePreplotTable("rlBody", rows),
    },
    {
      deleteBtnId: "btnDeleteSL",
      checkboxClass: "sl-preplot-checkbox",
      mainCheckboxId: "MainSLPreplotCheckbox",
      deleteUrl: window.SL_DELETE_URL,
      rowsKey: "sl_rows",
      title: "Delete SL lines",
      onUpdatedRows: (rows) => updatePreplotTable("slBody", rows),
    },
  ]);
}