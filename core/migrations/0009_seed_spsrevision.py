from django.db import migrations

def seed_spsrevision(apps, schema_editor):
    SpsRevision = apps.get_model("core", "SpsRevision")

    # Row 1: (1, 'Rev2.1', ...)
    SpsRevision.objects.update_or_create(
        id=1,
        defaults=dict(
            rev_name="Rev2.1",
            record_start=1, record_end=2,
            line_start=2, line_end=11,
            point_start=12, point_end=21,
            point_idx_start=23, point_idx_end=24,
            point_code_start=25, point_code_end=26,
            static_start=27, static_end=30,
            point_depth_start=31, point_depth_end=34,
            datum_start=35, datum_end=38,
            uphole_start=39, uphole_end=40,
            water_depth_start=41, water_depth_end=46,
            easting_start=47, easting_end=55,
            northing_start=56, northing_end=65,
            elevation_start=66, elevation_end=71,
            jday_start=72, jday_end=74,
            hour_start=75, hour_end=76,
            minute_start=77, minute_end=78,
            second_start=79, second_end=80,
            msecond_start=81, msecond_end=88,
            default_format="0",
        ),
    )

    # Row 2: (2, 'Rev01', ...)
    SpsRevision.objects.update_or_create(
        id=2,
        defaults=dict(
            rev_name="Rev01",
            record_start=0, record_end=1,
            line_start=1, line_end=17,
            point_start=17, point_end=25,
            point_idx_start=25, point_idx_end=26,
            point_code_start=26, point_code_end=28,
            static_start=28, static_end=32,
            point_depth_start=32, point_depth_end=36,
            datum_start=36, datum_end=40,
            uphole_start=40, uphole_end=42,
            water_depth_start=42, water_depth_end=46,
            easting_start=46, easting_end=55,
            northing_start=55, northing_end=65,
            elevation_start=65, elevation_end=71,
            jday_start=71, jday_end=74,
            hour_start=74, hour_end=76,
            minute_start=76, minute_end=78,
            second_start=78, second_end=80,
            msecond_start=81, msecond_end=88,
            default_format="1",
        ),
    )

def unseed_spsrevision(apps, schema_editor):
    SpsRevision = apps.get_model("core", "SpsRevision")
    SpsRevision.objects.filter(id__in=[1, 2]).delete()

class Migration(migrations.Migration):

    dependencies = [
        ("core", "0008_usersettings_theme_mode"),  # <-- replace with your latest migration file name
    ]

    operations = [
        migrations.RunPython(seed_spsrevision, reverse_code=unseed_spsrevision),
    ]