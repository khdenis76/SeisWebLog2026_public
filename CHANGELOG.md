# Changelog

## [2026.0.9.10] - 2026-03-02
### Added
- Source upload: auto year detection by JDAY
- Fleet: add purpose_id to sequence-vessel assignment
### Fixed
- Fix TooManyFilesSent by limiting uploads
### Changed
- UI: improve dark/light palette for QC badges

## [2026.0.9.9] - 2026-02-28
### Added
- Min/Max line QC plots with global legend
## [2026.1.0.3] - 2026-03-02
### Added
   -Different colors for SPS table highlight issues with Gun Depth 
   -Upload Source SPS files
   -Set colors for SPS table 
### Fixed
   
### Changed
## [2026.1.0.4] - 2026-03-04
### Added
   -Vessel filter for DSR table
   -Vessel sort for DSR table
   -Button  delete black box configuration 
   -Button  load/safe config to the json file for import/export (not tested yet)
   -FTP client updated groups added
   -Max SPI(shot point interval) sort updated 
### Fixed
   -DSR vie shows vessel  
### Changed
## [2026.1.0.5] - 2026-03-04
### Added
SHOT_TABLE QC & comparison with SPS files
Source SPS sorting
SPS project statistics 
### Fixed
Calculation of production/non-production per line
Shot table upload fixed 
### Changed
## [2026.1.0.6] - 2026-03-05
### Added
Source progress map plot added
Source sunbirst diagram added 
Source day by Day production added
### Fixed
Fixed migration removed wrong migration from folder  
Dataviewer fixed error when viewer crushed if bbox_config not exists  or bbox_file was not uploaded for this line 
### Changed
Source summary moved to Statistics tab
## [2026.1.0.7] - 2026-03-06
### Added
In Source QC  Source Line Map plot added 
In dataviewer to DSR table added Vessel and ROVs  
Source day by Day production added
On sps line click line map opened
### Fixed
Compare shot table and SPS 
Fixed sps sort   
Export/Import/Delete BlackBox config  
### Changed
## [2026.1.0.8] - 2026-03-08
### Added
Added toasts for all input from ROV page
Added aditional modals for ROV page 
### Fixed
 Main ROV page fixed.  
In dataviewer fixed wrong ROV name 
### Changed
## [2026.1.0.9] - 2026-03-10
### Added
In dataviewer bbox timegraphs added.
Baseproject module replaced modals and tosats were added 
### Fixed
## [2026.1.1.0] - 2026-03-18
### Added
New nodule OCR studio added
tesseract library added runlocal changed to control instalation of tesseral
Line map, Dep vs Preplot and Timing graphs were added to the browser
Active project location check on start up. if path is wrong and folder/file not exists user will be sent to project page 
Logs functional added to the baseprojecr 
### Fixed
ROV filter fixed 
### Changed
The collumns order on DSR line table was changed in order to better view
## [2026.1.02.00] - 2026-03-24
### Added
 ST Filter added
 ST tail load
 ST SailLine datavie on seq click 
 ST file name show up in window
 Recalc function added 
ST delete function added 
### Fixed
Datum and Static fields were not loaded for Source SPS before this issue was fixed.
Change shot_table loader. loading more fast but database should be isolated

### Changed
## [2026.1.02.01] - 2026-03-30
### Added
  Recover vs Dep plots added 
  BBOX filter button added 
  BBOX recalc button added 
 
### Fixed
Datum and Static fields were not loaded for Source SPS before this issue was fixed.
Change shot_table loader. loading more fast but database should be isolated

### Changed
  BBOX File statistics changed 
  Update loader changed 
## [2026.1.02.02] - 2026-04-25
### Added
   
 
### Fixed
REC_DB optional fixed. 
### Changed
  

  



   