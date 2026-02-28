# core/views.py
from __future__ import annotations

from pathlib import Path

from django.contrib.auth.decorators import login_required
from django.contrib.auth import logout, login as auth_login
from django.contrib.auth.forms import UserCreationForm
from django.contrib.auth.models import User
from django.shortcuts import render, redirect, get_object_or_404
from django.core.exceptions import PermissionDenied
from django.db.models import Q
from django import forms
from django.http import JsonResponse
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_protect

from .models import Project, ProjectMember, UserSettings
from .projectdb import (
    ProjectDB,
    MainSettings,
    GeometrySettings,
    NodeQCSettings,
    GunQCSettings, FolderSettings,
)


# ======================= DASHBOARD =======================

@login_required
def dashboard_view(request):
    settings, _ = UserSettings.objects.get_or_create(user=request.user)

    if not settings.active_project:
        return redirect("projects")

    project = settings.active_project
    pdb=ProjectDB(project.db_path)
    if not project.can_view(request.user):
        settings.active_project = None
        settings.save()
        return redirect("projects")

    return render(request, "dashboard.html", {"project": project,"color_scheme":pdb.get_main().color_scheme})


# ======================= PROJECT FORMS =======================

class ProjectForm(forms.ModelForm):
    class Meta:
        model = Project
        fields = ["name", "root_path", "folder_name", "note"]
        widgets = {
            "name": forms.TextInput(attrs={"class": "form-control"}),
            "root_path": forms.TextInput(attrs={"class": "form-control"}),
            "folder_name": forms.TextInput(attrs={"class": "form-control"}),
            "note": forms.Textarea(attrs={"class": "form-control", "rows": 3}),
        }


class ProjectMemberForm(forms.ModelForm):
    class Meta:
        model = ProjectMember
        fields = ["user", "can_edit"]
        labels = {
            "user": "User",
            "can_edit": "Can edit",
        }


# ======================= AUTH: SIGNUP & LOGOUT =======================

class CustomUserCreationForm(UserCreationForm):
    email = forms.EmailField(
        required=True,
        label="Email",
        help_text="Required. Enter a valid email address.",
        widget=forms.EmailInput(attrs={"class": "form-control"}),
    )

    username = forms.CharField(
        label="Username",
        widget=forms.TextInput(attrs={"class": "form-control"}),
    )

    password1 = forms.CharField(
        label="Password",
        widget=forms.PasswordInput(attrs={"class": "form-control"}),
    )

    password2 = forms.CharField(
        label="Confirm password",
        widget=forms.PasswordInput(attrs={"class": "form-control"}),
    )

    class Meta:
        model = User
        fields = ("username", "email", "password1", "password2")


def signup_view(request):
    if request.user.is_authenticated:
        return redirect("dashboard")

    if request.method == "POST":
        form = CustomUserCreationForm(request.POST)
        if form.is_valid():
            user = form.save()
            auth_login(request, user)
            return redirect("dashboard")
    else:
        form = CustomUserCreationForm()

    return render(request, "registration/signup.html", {"form": form})


def logout_view(request):
    logout(request)
    return redirect("login")


# ======================= PROJECT CRUD (FUNCTION-BASED) =======================

@login_required
def project_list_view(request):
    if request.user.is_superuser:
        projects = Project.objects.all()
    else:
        projects = Project.objects.filter(
        Q(owner=request.user) |
        Q(memberships__user=request.user)
    ).distinct().order_by("name")

    settings, _ = UserSettings.objects.get_or_create(user=request.user)

    return render(
        request,
        "projects/list.html",
        {
            "projects": projects,
            "active_project": settings.active_project,
        },
    )


@login_required
def project_create_view(request):
    if request.method == "POST":
        form = ProjectForm(request.POST)
        if form.is_valid():
            project = form.save(commit=False)
            project.owner = request.user
            project.save()

            # owner is editor by default
            ProjectMember.objects.get_or_create(
                project=project,
                user=request.user,
                defaults={"can_edit": True},
            )

            return redirect("projects")
    else:
        form = ProjectForm()

    return render(request, "projects/create.html", {"form": form})


@login_required
def project_detail_view(request, pk):
    project = get_object_or_404(Project, pk=pk)

    if not project.can_view(request.user):
        raise PermissionDenied

    return render(request, "projects/detail.html", {"project": project})


@login_required
def project_delete_view(request, pk):
    project = get_object_or_404(Project, pk=pk)

    if not project.can_edit(request.user):
        raise PermissionDenied

    if request.method == "POST":
        project.delete()
        return redirect("projects")

    # simple confirm page (можно сделать отдельный шаблон)
    return render(request, "projects/delete_confirm.html", {"project": project})


# ======================= PROJECT MEMBERS =======================

@login_required
def project_members_view(request, pk):
    project = get_object_or_404(Project, pk=pk)

    # Only owner can manage members
    if project.owner != request.user:
        raise PermissionDenied

    if request.method == "POST":
        form = ProjectMemberForm(request.POST)
        if form.is_valid():
            member = form.save(commit=False)
            member.project = project

            # do not add owner as member
            if member.user == project.owner:
                form.add_error("user", "Owner is already full member.")
            else:
                member.save()
                return redirect("project_members", pk=project.pk)
    else:
        form = ProjectMemberForm()

    members = project.memberships.select_related("user")

    return render(
        request,
        "projects/members.html",
        {
            "project": project,
            "form": form,
            "members": members,
        },
    )


# ======================= SET ACTIVE PROJECT =======================

@login_required
def project_set_active_view(request, pk):
    project = get_object_or_404(Project, pk=pk)

    if not project.can_view(request.user):
        raise PermissionDenied

    settings, _ = UserSettings.objects.get_or_create(user=request.user)
    settings.active_project = project
    settings.save()

    return redirect("dashboard")


# ======================= PROJECT SETTINGS (SQLite) =======================

@login_required
def project_settings_view(request):
    """
    Edit project configuration stored in project-specific SQLite DB.
    """

    user_settings, _ = UserSettings.objects.get_or_create(user=request.user)
    project = user_settings.active_project

    if not project:
        return redirect("projects")

    if not project.can_edit(request.user):
        raise PermissionDenied

    pdb = ProjectDB(project.db_path)

    pdb.init_db()
    pdb.run_sql_file("core/newproject.sql")

    def f_float(name: str, default: str = "0"):
        v = request.POST.get(name, default).strip().replace(",", ".")
        try:
            return float(v)
        except ValueError:
            return float(default)

    def f_int(name: str, default: str = "0"):
        v = request.POST.get(name, default).strip()
        try:
            return int(v)
        except ValueError:
            return int(default)
    def f_str(name:str, default: str="NA"):
        v=request.POST.get(name, default).strip()
        try:
            return Path(v)
        except ValueError:
            return str(default)

    if request.method == "POST":
        # ---- MAIN ----
        main = MainSettings(
            name=request.POST.get("main_name", "").strip(),
            location=request.POST.get("main_location", "").strip(),
            area=request.POST.get("main_area", "").strip(),
            client=request.POST.get("main_client", "").strip(),
            contractor=request.POST.get("main_contractor", "").strip(),
            project_client_id=request.POST.get("main_project_client_id", "").strip(),
            project_contractor_id=request.POST.get("main_project_contractor_id", "").strip(),
            epsg=request.POST.get("main_epsg", "").strip(),
            line_code=request.POST.get("main_line_code", "").strip(),
            start_project=request.POST.get("main_start_project", "").strip(),
            project_duration=f_int("main_project_duration", "30"),

        )

        # ---- GEOMETRY ----
        geom = GeometrySettings(
            rpi=f_float("geom_rpi", "0"),
            rli=f_float("geom_rli", "0"),
            spi=f_float("geom_spi", "0"),
            sli=f_float("geom_sli", "0"),
            rl_heading=f_float("geom_rl_heading", "360"),
            sl_heading=f_float("geom_sl_heading", "0"),
            production_code=request.POST.get("geom_production_code", "").strip() or "AP",
            non_production_code=request.POST.get("geom_non_production_code", "").strip() or "LRMXTK",
            kill_code =request.POST.get("geom_kill_code", "").strip() or "KX",
            rl_mask=request.POST.get("geom_rl_mask", "").strip() or "LLLLPPPP",
            sl_mask=request.POST.get("geom_sl_mask", "").strip() or "LLLLPPPP",
            sail_line_mask=request.POST.get("geom_sail_line_mask", "").strip() or "LLLLXSSSS",
        )

        # ---- NODE QC ----
        node_qc = NodeQCSettings(
            max_il_offset=f_float("node_max_il_offset", "0"),
            max_xl_offset=f_float("node_max_xl_offset", "0"),
            max_radial_offset=f_float("node_max_radial_offset", "0"),
            percent_of_depth=f_float("node_percent_of_depth", "0"),
            use_offset=f_int("node_use_offset", "0"),
            battery_life=f_int("battery_life", "0"),
            gnss_fixed_quality=f_int("gnss_fixed_quality", "0"),
            gnss_diffage_warning=f_int("gnss_diffage_warning", "0"),
            gnss_diffage_error=f_int("gnss_diffage_error", "0"),
            max_sma = f_float("max_sma", "0"),
            warning_sma=f_float("warning_sma", "0"),
        )

        # ---- GUN QC ----
        gun_qc = GunQCSettings(
            num_of_arrays=f_int("gun_num_of_arrays", "3"),
            num_of_strings=f_int("gun_num_of_strings", "3"),
            num_of_guns=f_int("gun_num_of_guns", "3"),
            depth=f_float("gun_depth", "0"),
            depth_tolerance=f_float("gun_depth_tolerance", "5"),
            time_warning=f_float("gun_time_warning", "1"),
            time_error=f_float("gun_time_error", "1.5"),
            pressure=f_float("gun_pressure", "2000"),
            pressure_drop=f_float("gun_pressure_drop", "100"),
            volume=f_float("gun_volume", "4000"),
            max_il_offset=f_float("gun_max_il_offset", "0"),
            max_xl_offset=f_float("gun_max_xl_offset", "0"),
            max_radial_offset=f_float("gun_max_radial_offset", "0"),
            kill_shots_cons=f_int('gun_kill_shots_cons',"0"),
            percentage_of_kill = f_int('gun_percentage_of_kill',"0")
        )
        folders_qc = FolderSettings(
            shapes_folder=f_str("shapes_folder", ""),
            image_folder=f_str("image_folder", ""),
            bb_folder=f_str("bb_folder", ""),
            local_prj_folder=f_str("local_prj_folder", ""),
            segy_folder=f_str("segy_folder", ""),

        )
        pdb.update_main(main)
        pdb.update_geometry(geom)
        pdb.update_node_qc(node_qc)
        pdb.update_gun_qc(gun_qc)
        pdb.update_folders(folders_qc)

        return redirect("project_settings")

    # GET — load current settings
    main = pdb.get_main()
    geom = pdb.get_geometry()
    node_qc = pdb.get_node_qc()
    gun_qc = pdb.get_gun_qc()
    folders_qc=pdb.get_folders()
    return render(
        request,
        "projects/settings.html",  # ВАЖНО: templates/projects/settings.html
        {
            "project": project,
            "main": main,
            "geom": geom,
            "node_qc": node_qc,
            "gun_qc": gun_qc,
            "folders":folders_qc
        },
    )

@require_POST
@login_required
@csrf_protect
def set_theme_view(request):
    mode = (request.POST.get("theme") or "").strip().lower()
    if mode not in ("dark", "light"):
        return JsonResponse({"ok": False, "error": "Invalid theme"}, status=400)

    settings, _ = UserSettings.objects.get_or_create(user=request.user)

    settings.theme_mode = mode
    settings.save(update_fields=["theme_mode"])

    return JsonResponse({"ok": True, "theme": mode})