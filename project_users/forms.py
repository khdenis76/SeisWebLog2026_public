from django import forms
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError

User = get_user_model()


class CreateUserForm(forms.ModelForm):
    password1 = forms.CharField(widget=forms.PasswordInput, required=False)
    password2 = forms.CharField(widget=forms.PasswordInput, required=False)

    class Meta:
        model = User
        fields = ("username", "email", "first_name", "last_name")

    def clean_username(self):
        username = (self.cleaned_data.get("username") or "").strip()
        if not username:
            raise ValidationError("Username is required.")
        return username

    def clean(self):
        cleaned = super().clean()
        p1 = cleaned.get("password1") or ""
        p2 = cleaned.get("password2") or ""
        if p1 or p2:
            if p1 != p2:
                raise ValidationError("Passwords do not match.")
            if len(p1) < 6:
                raise ValidationError("Password must be at least 6 characters.")
        return cleaned


class AddMemberForm(forms.Form):
    username = forms.CharField()
    can_edit = forms.BooleanField(required=False)

    def clean_username(self):
        username = (self.cleaned_data.get("username") or "").strip()
        if not username:
            raise ValidationError("Username is required.")
        return username

    def get_user(self):
        username = self.cleaned_data["username"]
        return User.objects.filter(username=username).first()


class UpdateMemberForm(forms.Form):
    can_edit = forms.BooleanField(required=False)