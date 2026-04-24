from django.db.models import Q


def projects_for_user_q(user):
    """
    Returns a Q() that matches projects user can view:
      owner OR membership
    Mirrors Project.can_view() logic :contentReference[oaicite:7]{index=7}.
    """
    if not user or not user.is_authenticated:
        return Q(pk__isnull=True)  # empty

    return Q(owner=user) | Q(memberships__user=user)