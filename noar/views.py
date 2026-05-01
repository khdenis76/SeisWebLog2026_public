from django.contrib.auth.decorators import login_required
from django.shortcuts import render

from core.models import UserSettings
from core.projectdb import ProjectDB


@login_required
def noar_home(request):
    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project

    return render(request, "noar/noar_home.html", {
        "project": project,
    })