from django.http import JsonResponse
from django.views.decorators.http import require_GET

from .version_checker import check_new_version


@require_GET
def version_status(request):
    return JsonResponse(check_new_version())