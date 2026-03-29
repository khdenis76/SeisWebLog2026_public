# Generated starter migration for the SeisWebLog reports app.
from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    initial = True
    dependencies = [migrations.swappable_dependency(settings.AUTH_USER_MODEL)]
    operations = [
        migrations.CreateModel(
            name="ReportTemplate",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=150, unique=True)),
                ("report_type", models.CharField(choices=[("weekly", "Weekly"), ("monthly", "Monthly"), ("survey", "Survey Wide")], max_length=20)),
                ("title_template", models.CharField(blank=True, default="", max_length=255)),
                ("include_summary", models.BooleanField(default=True)),
                ("include_activity", models.BooleanField(default=True)),
                ("include_qc", models.BooleanField(default=True)),
                ("include_maps", models.BooleanField(default=True)),
                ("include_fleet", models.BooleanField(default=True)),
                ("include_narrative", models.BooleanField(default=True)),
                ("logo_left", models.ImageField(blank=True, null=True, upload_to="report_logos/")),
                ("logo_right", models.ImageField(blank=True, null=True, upload_to="report_logos/")),
                ("is_active", models.BooleanField(default=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={"ordering": ["name"]},
        ),
        migrations.CreateModel(
            name="GeneratedReport",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("title", models.CharField(max_length=255)),
                ("report_type", models.CharField(choices=[("weekly", "Weekly"), ("monthly", "Monthly"), ("survey", "Survey Wide")], max_length=20)),
                ("start_date", models.DateField()),
                ("end_date", models.DateField()),
                ("project_name", models.CharField(blank=True, default="", max_length=255)),
                ("status", models.CharField(choices=[("draft", "Draft"), ("final", "Final")], default="draft", max_length=20)),
                ("json_payload", models.JSONField(blank=True, default=dict)),
                ("html_snapshot", models.TextField(blank=True, default="")),
                ("pdf_file", models.FileField(blank=True, null=True, upload_to="reports/pdf/")),
                ("notes", models.TextField(blank=True, default="")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("created_by", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="seisweblog_generated_reports", to=settings.AUTH_USER_MODEL)),
                ("template", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="generated_reports", to="reports.reporttemplate")),
            ],
            options={"ordering": ["-created_at"]},
        ),
    ]
