from django import forms


class StatisticsFilterForm(forms.Form):
    PERIOD_TYPES = (
        ("all", "Whole database"),
        ("day", "Day"),
        ("week", "ISO week"),
        ("month", "Month"),
        ("period", "Date period"),
    )
    TIME_BASIS = (
        ("either", "Deployment or recovery"),
        ("deployment", "Deployment time"),
        ("recovery", "Recovery time"),
    )
    GROUPING = (
        ("day", "Day"),
        ("week", "ISO week"),
        ("month", "Month"),
    )

    lines = forms.MultipleChoiceField(
        required=True,
        label="Receiver lines",
        widget=forms.CheckboxSelectMultiple,
    )
    period_type = forms.ChoiceField(choices=PERIOD_TYPES, initial="all", label="Statistics period")
    day = forms.DateField(required=False, widget=forms.DateInput(attrs={"type": "date"}))
    week = forms.CharField(required=False, widget=forms.TextInput(attrs={"type": "week"}), help_text="ISO week")
    month = forms.CharField(required=False, widget=forms.TextInput(attrs={"type": "month"}))
    date_from = forms.DateField(required=False, label="Date from", widget=forms.DateInput(attrs={"type": "date"}))
    date_to = forms.DateField(required=False, label="Date to", widget=forms.DateInput(attrs={"type": "date"}))
    time_basis = forms.ChoiceField(choices=TIME_BASIS, initial="either")
    deployment_rov = forms.CharField(required=False, label="Deployment ROV")
    recovery_rov = forms.CharField(required=False, label="Recovery ROV")
    grouping = forms.ChoiceField(choices=GROUPING, initial="day")
    comparison = forms.CharField(required=False, widget=forms.HiddenInput())

    def __init__(self, *args, line_choices=(), **kwargs):
        super().__init__(*args, **kwargs)
        choices = [(str(line), str(line)) for line in line_choices]
        self.fields["lines"].choices = choices
        self.fields["lines"].required = bool(choices)

    def clean(self):
        data = super().clean()
        for first, last, label in (
            ("station_from", "station_to", "station"),
            ("date_from", "date_to", "date"),
        ):
            if data.get(first) is not None and data.get(last) is not None and data[first] > data[last]:
                raise forms.ValidationError(f"The {label} start cannot be later than the end.")
        period_type = data.get("period_type")
        required = {"day": "day", "week": "week", "month": "month"}.get(period_type)
        if required and not data.get(required):
            self.add_error(required, f"Select a {required}.")
        if period_type == "period" and (not data.get("date_from") or not data.get("date_to")):
            raise forms.ValidationError("Select both dates for a custom period.")
        return data
