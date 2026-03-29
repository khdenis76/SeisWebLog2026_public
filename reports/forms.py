"""
Forms for interactive SeisWebLog report generation.
"""

from django import forms
from .models import ReportTemplate


class ReportGenerateForm(forms.Form):
    """Main form used to generate an interactive report preview."""
    REPORT_TYPE_CHOICES = [
        ("weekly", "Weekly"),
        ("monthly", "Monthly"),
        ("survey", "Survey Wide"),
    ]

    report_type = forms.ChoiceField(choices=REPORT_TYPE_CHOICES)
    title = forms.CharField(max_length=255, required=False)
    start_date = forms.DateField(widget=forms.DateInput(attrs={"type": "date", "class": "form-control"}))
    end_date = forms.DateField(widget=forms.DateInput(attrs={"type": "date", "class": "form-control"}))
    template = forms.ModelChoiceField(queryset=ReportTemplate.objects.filter(is_active=True), required=False, empty_label="Default SeisWebLog template")
    include_summary = forms.BooleanField(required=False, initial=True)
    include_activity = forms.BooleanField(required=False, initial=True)
    include_qc = forms.BooleanField(required=False, initial=True)
    include_maps = forms.BooleanField(required=False, initial=True)
    include_fleet = forms.BooleanField(required=False, initial=True)
    include_narrative = forms.BooleanField(required=False, initial=True)
    save_report = forms.BooleanField(required=False, initial=True)
    build_pdf = forms.BooleanField(required=False, initial=False)
    narrative = forms.CharField(required=False, widget=forms.Textarea(attrs={"rows": 4, "class": "form-control"}))

    def clean(self):
        cleaned = super().clean()
        start_date = cleaned.get("start_date")
        end_date = cleaned.get("end_date")
        if start_date and end_date and start_date > end_date:
            raise forms.ValidationError("Start date cannot be later than end date.")
        return cleaned
