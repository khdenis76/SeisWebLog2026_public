from django.db import models

# Create your models here.
from django.db import models


class Vessel(models.Model):
    """
    Master list: all vessels company uses/used.
    This is NOT project-specific. ProjectDB will get a copied subset.
    """
    name = models.CharField(max_length=120, unique=True)

    imo = models.CharField(max_length=20, blank=True, null=True, unique=True)
    mmsi = models.CharField(max_length=20, blank=True, null=True, unique=True)
    call_sign = models.CharField(max_length=32, blank=True, null=True)

    vessel_type = models.CharField(max_length=60, blank=True, null=True)  # Node, Source, Support...
    owner = models.CharField(max_length=120, blank=True, null=True)

    is_active = models.BooleanField(default=True)   # currently in use
    is_retired = models.BooleanField(default=False) # historical
    notes = models.TextField(blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name