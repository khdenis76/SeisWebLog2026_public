from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.shortcuts import redirect, render

from core.models import ProjectMember  # adjust if your app name differs
from .forms import AddMemberForm, CreateUserForm, UpdateMemberForm

User = get_user_model()


def _get_active_project_or_redirect(request):
    try:
        project = request.user.settings.active_project
    except Exception:
        project = None
    if not project:
        return None
    return project


def _owner_required(request, project):
    # owner can manage; optionally allow staff to manage too:
    if not request.user.is_superuser and request.user != project.owner and not request.user.is_staff:
        raise PermissionDenied()


@login_required
def members_page(request):
    project = _get_active_project_or_redirect(request)
    if not project:
        messages.warning(request, "Select an active project first.")
        return redirect("project_list")  # <-- your project selector/list url name

    # must at least be able to view
    if not project.can_view(request.user):
        raise PermissionDenied("No access to this project.")

    _owner_required(request, project)

    members = (
        ProjectMember.objects
        .filter(project=project)
        .select_related("user")
        .order_by("user__username")
    )

    add_form = AddMemberForm()
    create_form = CreateUserForm()

    return render(request, "project_users/members.html", {
        "project": project,
        "members": members,
        "add_form": add_form,
        "create_form": create_form,
    })


@login_required
@transaction.atomic
def create_user(request):
    project = _get_active_project_or_redirect(request)
    if not project:
        messages.warning(request, "Select an active project first.")
        return redirect("project_list")

    if not project.can_view(request.user):
        raise PermissionDenied("No access to this project.")

    _owner_required(request, project)

    if request.method != "POST":
        return redirect("project_users_members")

    form = CreateUserForm(request.POST)
    if not form.is_valid():
        members = ProjectMember.objects.filter(project=project).select_related("user")
        return render(request, "project_users/members.html", {
            "project": project,
            "members": members,
            "add_form": AddMemberForm(),
            "create_form": form,
        })

    u = form.save(commit=False)

    p1 = form.cleaned_data.get("password1") or ""
    if p1:
        u.set_password(p1)
    else:
        # create with unusable password (owner can later set/reset)
        u.set_unusable_password()

    u.is_active = True
    u.save()

    # optionally auto-add created user to this project as view-only
    ProjectMember.objects.get_or_create(
        project=project,
        user=u,
        defaults={"can_edit": False},
    )

    messages.success(request, f"User '{u.username}' created and added to project.")
    return redirect("project_users_members")


@login_required
@transaction.atomic
def add_member(request):
    project = _get_active_project_or_redirect(request)
    if not project:
        messages.warning(request, "Select an active project first.")
        return redirect("project_list")

    if not project.can_view(request.user):
        raise PermissionDenied("No access to this project.")

    _owner_required(request, project)

    if request.method != "POST":
        return redirect("project_users_members")

    form = AddMemberForm(request.POST)
    if not form.is_valid():
        members = ProjectMember.objects.filter(project=project).select_related("user")
        return render(request, "project_users/members.html", {
            "project": project,
            "members": members,
            "add_form": form,
            "create_form": CreateUserForm(),
        })

    u = form.get_user()
    if not u:
        messages.error(request, "User not found.")
        return redirect("project_users_members")

    can_edit = bool(form.cleaned_data.get("can_edit"))

    ProjectMember.objects.update_or_create(
        project=project,
        user=u,
        defaults={"can_edit": can_edit},
    )

    messages.success(request, f"User '{u.username}' added/updated.")
    return redirect("project_users_members")


@login_required
@transaction.atomic
def update_member(request, member_id):
    project = _get_active_project_or_redirect(request)
    if not project:
        messages.warning(request, "Select an active project first.")
        return redirect("project_list")

    if not project.can_view(request.user):
        raise PermissionDenied("No access to this project.")

    _owner_required(request, project)

    member = ProjectMember.objects.select_related("user").filter(id=member_id, project=project).first()
    if not member:
        raise PermissionDenied("Member not found.")

    if request.method != "POST":
        return redirect("project_users_members")

    form = UpdateMemberForm(request.POST)
    if not form.is_valid():
        messages.error(request, "Invalid data.")
        return redirect("project_users_members")

    member.can_edit = bool(form.cleaned_data.get("can_edit"))
    member.save(update_fields=["can_edit"])

    messages.success(request, f"Updated '{member.user.username}'.")
    return redirect("project_users_members")


@login_required
@transaction.atomic
def remove_member(request, member_id):
    project = _get_active_project_or_redirect(request)
    if not project:
        messages.warning(request, "Select an active project first.")
        return redirect("project_list")

    if not project.can_view(request.user):
        raise PermissionDenied("No access to this project.")

    _owner_required(request, project)

    member = ProjectMember.objects.select_related("user").filter(id=member_id, project=project).first()
    if not member:
        raise PermissionDenied("Member not found.")

    if request.method != "POST":
        return redirect("project_users_members")

    username = member.user.username
    member.delete()
    messages.success(request, f"Removed '{username}' from project.")
    return redirect("project_users_members")


@login_required
@transaction.atomic
def toggle_user_active(request, user_id):
    """
    This changes GLOBAL user status (is_active). Use carefully.
    Owner/staff can deactivate/reactivate user accounts.
    """
    project = _get_active_project_or_redirect(request)
    if not project:
        messages.warning(request, "Select an active project first.")
        return redirect("project_list")

    if not project.can_view(request.user):
        raise PermissionDenied("No access to this project.")

    _owner_required(request, project)

    if request.method != "POST":
        return redirect("project_users_members")

    u = User.objects.filter(id=user_id).first()
    if not u:
        messages.error(request, "User not found.")
        return redirect("project_users_members")

    if u == project.owner:
        messages.error(request, "You cannot deactivate the project owner.")
        return redirect("project_users_members")

    u.is_active = not u.is_active
    u.save(update_fields=["is_active"])

    messages.success(request, f"User '{u.username}' is_active={u.is_active}.")
    return redirect("project_users_members")