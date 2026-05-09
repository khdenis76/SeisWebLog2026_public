Project: SeisWebLog
git:https://github.com/khdenis76/SeisWebLog2026_public
Stack: Django 5.2, Python 3.11, Bokeh, Matplotlib, SQLite,
Structure:
- apps: source, rov, svp
- heavy plotting (Bokeh interactive, Matplotlib for reports)
- database: custom SQLite schemas (DSR, REC_DB, SHOT_TABLE, etc.)
Applications:
- rov: work with DSR, BBOX files main application for work with Node deployment/recovery 
- source: work with Source SPS files production and non-production  and also with shot table
Coding rules:
- imports at top
- Bootstrap 5.3 UI
- prefer lazy loading via JS init functions
- use Bokeh JSON embedding for plots
- each bokeh plot should have option to is_show=False by default and can be able to save plot into html file 
Task: