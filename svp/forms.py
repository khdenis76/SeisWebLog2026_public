from django import forms


class SVPUploadForm(forms.Form):
    name = forms.CharField(max_length=200, required=False)
    source = forms.CharField(max_length=120, required=False)
    profile_time = forms.DateTimeField(
        required=False,
        input_formats=['%Y-%m-%dT%H:%M', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'],
        widget=forms.DateTimeInput(attrs={'type': 'datetime-local'})
    )
    notes = forms.CharField(required=False, widget=forms.Textarea(attrs={'rows': 2}))
    file = forms.FileField()
