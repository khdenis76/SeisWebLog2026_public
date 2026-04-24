from django.db import migrations


def create_sps_revisions(apps, schema_editor):
    SPSRevision = apps.get_model("core", "SPSRevision")

    data = [
        {
            "rev_name": "Rev2.1",
            "record_start": 1,
            "record_end": 2,
            "line_start": 2,
            "line_end": 11,
            "point_start": 12,
            "point_end": 21,
            "point_idx_start": 23,
            "point_idx_end": 24,
            "point_code_start": 25,
            "point_code_end": 26,
            "static_start": 27,
            "static_end": 30,
            "point_depth_start": 31,
            "point_depth_end": 34,
            "datum_start": 35,
            "datum_end": 38,
            "uphole_start": 39,
            "uphole_end": 40,
            "water_depth_start": 41,
            "water_depth_end": 46,
            "easting_start": 47,
            "easting_end": 55,
            "northing_start": 56,
            "northing_end": 65,
            "elevation_start": 66,
            "elevation_end": 71,
            "jday_start": 72,
            "jday_end": 74,
            "hour_start": 75,
            "hour_end": 76,
            "minute_start": 77,
            "minute_end": 78,
            "second_start": 79,
            "second_end": 80,
            "msecond_start": 81,
            "msecond_end": 88,
            "default_format": False,
        },
        {
            "rev_name": "Rev01",
            "record_start": 0,
            "record_end": 1,
            "line_start": 1,
            "line_end": 17,
            "point_start": 17,
            "point_end": 25,
            "point_idx_start": 25,
            "point_idx_end": 26,
            "point_code_start": 26,
            "point_code_end": 28,
            "static_start": 28,
            "static_end": 32,
            "point_depth_start": 32,
            "point_depth_end": 36,
            "datum_start": 36,
            "datum_end": 40,
            "uphole_start": 40,
            "uphole_end": 42,
            "water_depth_start": 42,
            "water_depth_end": 46,
            "easting_start": 46,
            "easting_end": 55,
            "northing_start": 55,
            "northing_end": 65,
            "elevation_start": 65,
            "elevation_end": 71,
            "jday_start": 74,
            "jday_end": 74,
            "hour_start": 76,
            "hour_end": 76,
            "minute_start": 78,
            "minute_end": 78,
            "second_start": 80,
            "second_end": 80,
            "msecond_start": 88,
            "msecond_end": 88,
            "default_format": True,
        },
    ]

    for row in data:
        SPSRevision.objects.update_or_create(
            rev_name=row["rev_name"],
            defaults=row
        )


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0005_spsrevision"),
    ]

    operations = [
        migrations.RunPython(create_sps_revisions),
    ]
