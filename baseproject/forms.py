# baseproject/forms.py
from django import forms
from django.core.validators import MinValueValidator, MaxValueValidator
from django.db import OperationalError, connection

from core.models import SPSRevision
from .models import BaseProjectFile


class MultiFileInput(forms.ClearableFileInput):
    """
    Custom widget to allow selecting multiple files.
    """
    allow_multiple_selected = True



class BaseProjectUploadForm(forms.Form):
    """
    Upload many files at once.
    All selected files share the same file_type and optional SPS revision.
    """

    file_type = forms.ChoiceField(
        choices=BaseProjectFile.FILE_TYPE_CHOICES,
        label="File type",
        widget=forms.Select(attrs={"class": "form-select"}),
    )

    sps_revision = forms.ModelChoiceField(
        queryset=SPSRevision.objects.all(),
        required=False,
        empty_label="Select SPS revision...",
        label="SPS Revision",
        initial=None,  # IMPORTANT: no DB query here
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        try:
            # fresh install: table may not exist yet
            if "core_spsrevision" not in connection.introspection.table_names():
                return

            # Choose ONE of these depending on your model field type:
            # If default_format is BooleanField:
            # obj = SPSRevision.objects.filter(default_format=True).first()

            # If default_format is CharField with '0'/'1' (your inserts):
            obj = SPSRevision.objects.filter(default_format="1").first()

            if obj:
                self.fields["sps_revision"].initial = obj

        except OperationalError:
            return
    tier = forms.IntegerField(
        required=False,
        label="Tier / phase",
        initial=1,
        validators=[
            MinValueValidator(1),
            MaxValueValidator(5),
        ],
        widget=forms.NumberInput(
            attrs={
                "class": "form-control",
                "min": 1,
                "max": 5,
                "step": 1,
            }
        ),
        help_text="Tier / phase number (1–5).",
    )

    # Bearing in degrees: 0–360 (float)
    bearing = forms.FloatField(
        required=False,
        label="Bearing (deg)",
        initial=0.0,
        validators=[
            MinValueValidator(0.0),
            MaxValueValidator(360.0),
        ],
        widget=forms.NumberInput(
            attrs={
                "class": "form-control",
                "min": 0,
                "max": 360,
                "step": "0.1",
            }
        ),
        help_text="Line bearing in degrees (0–360).",
    )
    files = forms.FileField(
        required=True,
        label="Files",
        widget=MultiFileInput(
            attrs={
                "class": "form-control",
            }
        ),
        help_text="You can select many files at once.",
    )
class BaseProjectCSVForm(forms.Form):
    files = forms.FileField(
        required=True,
        label="Files",
        widget=MultiFileInput(
            attrs={
                "class": "form-control",
            }
        ),
        help_text="You can select many files at once.",
    )
    layer=forms.TextInput(attrs={
                "class": "form-control",
            })
    point = forms.TextInput(attrs={
        "class": "form-control",
    })

