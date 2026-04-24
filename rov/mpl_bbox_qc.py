from rov.eol.dsr_line_graphics_matplotlib import DSRLineGraphicsMatplotlib



if __name__ == "__main__":
    project_db_path = r"G:\02_PROJECTS\AW1\16_SWL_DATA\AW1\data\project.sqlite3"
    save_dir = r"G:\02_PROJECTS\AW1\16_SWL_DATA\AW1\plots"
    line = 13513

    g = DSRLineGraphicsMatplotlib(project_db_path)

    common, start_time, end_time = g.get_bbox_common_dataset_for_line(
        line=line,
        pad_minutes=0,
    )

    pages = g.plot_whole_line_node_pages_fast(
        line=line,
        output_dir=save_dir,
        rows=3,
        cols=2,
        dpi=180,
        pad_minutes=10,
        common=common,# important for BB data window
    )

    print("TOTAL PAGES:", len(pages))
    for p in pages:
        print(p)
    gnss_pages = g.plot_bbox_gnss_qc_for_line_paged(
        line=line,
        common=common,
        hours_per_page=24,
        save_dir=save_dir,
        bb_stride=20,
        use_multiprocess=True,
        max_workers=4,
        is_show=False,
        close=True,
    )

    motion_pages = g.plot_bbox_motion_qc_for_line_paged_combined(
        line=line,
        common=common,
        hours_per_page=24,
        save_dir=save_dir,
        bb_stride=20,
        use_multiprocess=True,
        max_workers=4,
        is_show=False,
        close=True,
    )