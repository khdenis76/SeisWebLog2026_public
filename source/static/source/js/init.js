//import {initSourceUpload} from "./initSourceUpload.js";
import {initColumnToggles} from "../../rov/js/initColumnToggles.js";
import {initSpsTableSelection} from "./initSPSTableSelection.js";
import {initSpsDelete} from "./initSPSDelete.js";
import {initSpsTableFilterModal} from "./initSPSFilterModal.js";
import { initShotLineSort } from "./shot_line_sort.js";
import {initSourceQCMap} from "./initSourceQCMap.js";
import {initSourceQCStats} from "./initSourceQCStats.js";
import {initDailyProductionTab} from "./initSourceDailyProduction.js";
import {initSpsRowLinePlot} from "./initSpsRowLinePlot.js";
import {initSourceUploadSubmit} from "./initSourceUploadModal.js"
import {initSpsTableAdvancedSort} from "./initSPSTableSortModal.js";
import {initShotTableSort} from "./initShotTableSort.js";
//import {initShotSummaryBackendFilters} from "./initShotTableFilterModal.js";
import {initClickOnSTLine} from "./initClickOnSTLine.js";
import {initShotSummaryDelete} from "./initShotSummaryDelete.js";
import {initShotSummaryRecalc} from "./initShotSummaryRecalc.js";
import {initShotSummaryFilters} from "./initShotSummaryFilters.js";
//import {initRecalcLines} from "./initRecalcLines.js";

document.addEventListener("DOMContentLoaded", () => {
  initSourceUploadSubmit()
    //initSourceUpload();
    initSpsTableSelection();
    initSpsDelete();
    //initSpsTableFilterModal();
    initSourceQCMap();
    initShotSummaryRecalc();
    initShotSummaryFilters();
    //initRecalcLines();
    initSpsTableFilterModal({
    tbodyId: "sps-table-tbody",
    countId: "sps-filter-count",
    quickClearId: "sps-filter-clear-quick",
    endpoint: "/source/sps/table-data/"
  });
    initSourceQCStats();
    initClickOnSTLine();
    initShotTableSort();
    initDailyProductionTab();
    initSpsRowLinePlot();
    //initShotSummaryBackendFilters();
    initShotSummaryDelete();
    initShotLineSort({ tbodyId: "shot-summary-tbody" }); // <-- your tbody id
    initSpsTableAdvancedSort({
    tableId: "sps-table",
    theadId: "sps-table-thead",
    tbodyId: "sps-table-tbody",
    labelId: "sps-sort-label",
    storageKey: "seisweblog:sps-sort",
    maxLevels: 4,
  });
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
});
