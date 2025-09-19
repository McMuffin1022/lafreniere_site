from django.db import models
from django.forms import ValidationError
from django.db import models
from django.utils.text import slugify
from django.utils import timezone
from django.shortcuts import render

# Create your models here.
class ContactMessage(models.Model):
    name       = models.CharField("Nom", max_length=100)
    email      = models.EmailField("Courriel", blank=True)
    phone      = models.CharField("Téléphone", max_length=20, blank=True)
    message    = models.TextField("Message")
    created_at = models.DateTimeField("Reçu le", auto_now_add=True)

    class Meta:
        verbose_name = "Message de contact"
        verbose_name_plural = "Messages de contact"
        ordering = ["-created_at"]

    def clean(self):
        if not self.email and not self.phone:
            raise ValidationError("Veuillez fournir un courriel OU un numéro de téléphone.")

    def __str__(self):
        return f"{self.name} - {self.created_at:%Y-%m-%d %H:%M}"
    


class Listing(models.Model):
    STATUS_ACTIVE = "ACTIVE"
    STATUS_SOLD = "SOLD"
    STATUS_CHOICES = [
        (STATUS_ACTIVE, "Active"),
        (STATUS_SOLD, "Sold"),
    ]

    # ID Centris comme clé primaire (string pour rester flexible)
    centris_id = models.CharField(max_length=20, primary_key=True)
    slug = models.SlugField(max_length=64, unique=True)

    prix = models.PositiveIntegerField(null=True, blank=True)
    adresse = models.CharField(max_length=255, blank=True)

    nombre_pieces = models.PositiveSmallIntegerField(null=True, blank=True)
    nombre_chambres = models.PositiveSmallIntegerField(null=True, blank=True)
    nombre_sdb = models.PositiveSmallIntegerField(null=True, blank=True)

    # Garde null si non mappé dans l’export (on pourra brancher plus tard)
    superficie_habitable = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    superficie_terrain = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)

    annee_construction = models.PositiveSmallIntegerField(null=True, blank=True)

    inclus = models.TextField(blank=True)  # si tu veux l’utiliser plus tard
    description = models.TextField(blank=True)

    # Proximités et caractéristiques : à la fois en texte et en JSON structuré (pour UI chips)
    proximites_text = models.TextField(blank=True)
    proximites = models.JSONField(default=list, blank=True)  # ex: ["Autoroute", "École primaire", ...]

    caracteristiques_text = models.TextField(blank=True)
    caracteristiques = models.JSONField(default=list, blank=True)  # ex: [{"cat":"Allée","val":"Non pavé"}, ...]

    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default=STATUS_ACTIVE)
    sold_at = models.DateTimeField(null=True, blank=True)

    # Timestamps pour UI / historique
    first_seen_at = models.DateTimeField(auto_now_add=True)  # date de création en DB
    last_seen_at = models.DateTimeField(null=True, blank=True)  # timestamp du dernier fetch où l’inscription était présente
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=["status"]),
            models.Index(fields=["-last_seen_at"]),
            models.Index(fields=["-first_seen_at"]),
        ]
        ordering = ["-last_seen_at", "-first_seen_at"]

    def __str__(self):
        return f"{self.centris_id} — {self.adresse or ''}".strip()

    def ensure_slug(self):
        if not self.slug:
            self.slug = slugify(f"listing-{self.centris_id}")[:64]


class ListingPhoto(models.Model):
    listing = models.ForeignKey(Listing, on_delete=models.CASCADE, related_name="photos")
    sequence = models.PositiveIntegerField(default=1)
    url = models.CharField(max_length=500)

    class Meta:
        unique_together = [("listing", "sequence")]
        ordering = ["sequence"]

    def __str__(self):
        return f"{self.listing_id}#{self.sequence}"


class FetchLog(models.Model):
    """Historique des imports pour audit/monitoring."""
    created_at = models.DateTimeField(auto_now_add=True)
    file_date = models.DateField(null=True, blank=True)          # date Y-M-D du ZIP si connue
    source_url = models.CharField(max_length=512, blank=True)    # URL du ZIP ou dossier
    source_name = models.CharField(max_length=128, blank=True)   # NOMADESMARKETINGYYYYMMDD.zip
    items_total = models.PositiveIntegerField(default=0)
    items_added = models.PositiveIntegerField(default=0)
    items_updated = models.PositiveIntegerField(default=0)
    items_marked_sold = models.PositiveIntegerField(default=0)
    duration_seconds = models.FloatField(default=0.0)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        return f"Fetch {self.created_at:%Y-%m-%d %H:%M} (total={self.items_total}, +{self.items_added}, ~{self.items_updated}, sold={self.items_marked_sold})"
    

class Certification(models.Model):
    """
    Prix / distinctions affichés dans le carrousel.
    Upload dans MEDIA_ROOT/certifications/
    """
    name = models.CharField(max_length=200)
    logo = models.ImageField(upload_to="certifications/")

    # Optionnel mais utile pour tri manuel dans l’UI (0 = en premier)
    order = models.PositiveIntegerField(default=0, db_index=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["order", "id"]
        verbose_name = "Certification / Distinction"
        verbose_name_plural = "Certifications / Distinctions"

    def __str__(self) -> str:
        return self.name
    
class Agent(models.Model):
    """
    Collaborateur/courtier affiché sur la page 'collaborateurs'.
    """
    name = models.CharField(max_length=150)
    title = models.CharField(max_length=150, blank=True)  # ex: "Courtier immobilier résidentiel et commercial"
    phone = models.CharField(max_length=30, blank=True)
    email = models.EmailField(blank=True)
    photo = models.ImageField(upload_to="agents/", blank=True, null=True)
    bio_short = models.TextField(blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["name"]
        indexes = [
            models.Index(fields=["name"]),
            models.Index(fields=["title"]),
        ]
        verbose_name = "Agent / Collaborateur"
        verbose_name_plural = "Agents / Collaborateurs"

    def __str__(self) -> str:
        return self.name