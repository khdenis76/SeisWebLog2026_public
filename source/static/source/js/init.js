import {initSourceUploadModal} from "./initSourceUploadModal.js";
import {initSourceUpload} from "./initSourceUpload.js";
import {initColumnToggles} from "../../rov/js/initColumnToggles.js";
import {initSpsTableSelection} from "./initSPSTableSelection.js";
import {initSpsDelete} from "./initSPSDelete.js";
import {initSpsTableFilterModal} from "./initSPSFilterModal.js";
import {initSpsTableSortModal} from "./initSPSTableSortModal.js";
import { initShotLineSort } from "./shot_line_sort.js";
import {initSourceQCMap} from "./initSourceQCMap.js";
import {initSourceQCStats} from "./initSourceQCStats.js";
import {initDailyProductionTab} from "./initSourceDailyProduction.js";
import {initSpsRowLinePlot} from "./initSpsRowLinePlot.js";

export function initAllJSForSource() {
    initSourceUploadModal();
    initSourceUpload();
    initSpsTableSelection();
    initSpsDelete();
    initSpsTableFilterModal();
    initSourceQCMap();
    initSpsTableFilterModal({
    tbodyId: "sps-table-tbody",
    countId: "sps-filter-count",
    quickClearId: "sps-filter-clear-quick",
    endpoint: "/source/sps/table-data/"
  });
    initSourceQCStats();
    initDailyProductionTab();
    initSpsRowLinePlot();
    initShotLineSort({ tbodyId: "shot-summary-tbody" }); // <-- your tbody id
    initColumnToggles(
    "source_toggle-left-rov-btn",
    "source_left-rov-col",
    "source_left-rov-toggle-icon",
    { toggleElId: "source_right-rov-col", divOn: "col-12", divOff: "col-4" }
    );

  initColumnToggles(
    "source_toggle-right-rov-btn",
    "source_right-rov-col",
    "source_right-rov-toggle-icon",
    { toggleElId: "source_left-rov-col", divOn: "col-12", divOff: "col-4" }
  );
}