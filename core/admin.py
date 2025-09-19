# core/admin.py
from django.contrib import admin
from .models import Agent, Certification, Listing, ListingPhoto, FetchLog
from django.utils.html import format_html
from .models import Certification

@admin.register(Listing)
class ListingAdmin(admin.ModelAdmin):
    list_display = ("centris_id", "adresse", "prix", "status", "last_seen_at", "first_seen_at")
    list_filter = ("status", "annee_construction")
    search_fields = ("centris_id", "adresse", "description", "proximites_text", "caracteristiques_text")
    readonly_fields = ("first_seen_at", "updated_at", "last_seen_at", "sold_at")
    date_hierarchy = "first_seen_at"

@admin.register(ListingPhoto)
class ListingPhotoAdmin(admin.ModelAdmin):
    list_display = ("listing", "sequence", "url")
    search_fields = ("listing__centris_id", "url")

@admin.register(FetchLog)
class FetchLogAdmin(admin.ModelAdmin):
    list_display = ("created_at", "file_date", "source_name", "items_total", "items_added", "items_updated", "items_marked_sold", "duration_seconds")
    date_hierarchy = "created_at"



@admin.register(Certification)
class CertificationAdmin(admin.ModelAdmin):
    list_display = ("thumb", "name", "order", "created_at")
    list_editable = ("order",)
    search_fields = ("name",)
    ordering = ("order", "id")

    def thumb(self, obj):
        if obj.logo:
            return format_html('<img src="{}" style="height:40px;object-fit:contain;" />', obj.logo.url)
        return "—"
    thumb.short_description = "Logo"



@admin.register(Agent)
class AgentAdmin(admin.ModelAdmin):
    list_display = ("photo_preview", "name", "title", "phone", "email")
    search_fields = ("name", "title", "email", "phone")
    list_filter = ("title",)
    ordering = ("name",)
    readonly_fields = ("photo_preview",)
    fieldsets = (
        (None, {"fields": ("name", "title", "bio_short")}),
        ("Coordonnées", {"fields": ("phone", "email")}),
        ("Photo", {"fields": ("photo", "photo_preview")}),
    )

    def photo_preview(self, obj):
        if getattr(obj, "photo", None):
            return format_html(
                '<img src="{}" style="height:72px;width:72px;object-fit:cover;border-radius:9999px;border:1px solid #e5e7eb;" />',
                obj.photo.url,
            )
        return "—"
    photo_preview.short_description = "Aperçu"