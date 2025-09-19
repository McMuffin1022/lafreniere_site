from django.shortcuts import render
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_POST
from .forms import ContactForm
from .models import Agent, Certification, ContactMessage
from django.shortcuts import render, get_object_or_404
from django.shortcuts import render
from django.core.paginator import Paginator
from django.db.models import Prefetch
from .models import Listing, ListingPhoto
from types import SimpleNamespace
from datetime import timedelta
from django.db.models import Q
from django.utils import timezone
from django.core.paginator import Paginator
from django.shortcuts import render
from .models import Listing

# Create your views here.
def index(request):
    """
    Page d'accueil: hero + featured + témoignages + blog + certifications
    """
    # testimonials = Testimonial.objects.order_by('-created_at')[:3]
    # blog_posts = BlogPost.objects.order_by('-published_at')[:3]
    # certifications = Certification.objects.all()
    # featured = Property.objects.filter(status="new").order_by('-created_at')
    # how_it_works_steps = HowItWorksStep.objects.all()
    # about_values = AboutValue.objects.all()
    # stat_keys = StatKey.objects.all()
    # blog_posts         = BlogPost.objects.all()[:3]
    # faq_items          = FAQItem.objects.all()
    # newsletter_form = NewsletterSignupForm()
    # contact_form    = ContactForm()
    # donations = CommunityDonation.objects.exclude(logo='')

    context = {
        # 'testimonials': testimonials,
        # 'blog_posts': blog_posts,
        # 'certifications': certifications,
        # 'featured_properties': featured,
        # 'how_it_works_steps': how_it_works_steps,
        # 'about_values': about_values,
        # 'stat_keys': stat_keys,
        # 'blog_posts': blog_posts,
        # 'faq_items': faq_items,
        # 'newsletter_form': newsletter_form,
        # 'contact_form': contact_form,
        # 'donations': donations,
    }

    awards = Certification.objects.all().order_by("id") 
    return render(request, 'index.html', { "awards": awards, })

def contact_page(request):
    return render(request, "contact.html")

@require_POST
def contact_submit(request):
    form = ContactForm(request.POST)
    if not form.is_valid():
        # Retourne la première erreur lisible
        err = "; ".join([f"{k}: {', '.join(v)}" for k, v in form.errors.items()])
        return JsonResponse({"success": False, "error": err or "Formulaire invalide."})
    data = form.cleaned_data
    ContactMessage.objects.create(
        name=data["name"],
        email=data["email"],
        phone=data.get("phone", ""),
        message=data["message"],
    )
    # (Optionnel) envoi email: à brancher si souhaité.
    return JsonResponse({"success": True})



def collaborators_page(request):
    """
    Page collaborateurs:
    - regroupe selon le champ 'title' (icontains 'courtier' vs 'adjointe')
    - photos en N&B par défaut, couleur au hover (géré via classes Tailwind)
    """
    agents_courtiers = (
        Agent.objects.filter(title__icontains="courtier").order_by("name")
    )
    agents_adjointes = (
        Agent.objects.filter(title__icontains="adjointe").order_by("name")
    )
    return render(
        request,
        "collaborateurs.html",
        {
            "agents_courtiers": agents_courtiers,
            "agents_adjointes": agents_adjointes,
        },
    )

def invest_page(request):
    return render(request, "invest.html")

@require_POST
def invest_contact_submit(request):
    form = ContactForm(request.POST)
    if not form.is_valid():
        err = "; ".join([f"{k}: {', '.join(v)}" for k, v in form.errors.items()])
        return JsonResponse({"success": False, "error": err or "Formulaire invalide."})
    data = form.cleaned_data
    ContactMessage.objects.create(
        name=data["name"],
        email=data["email"],
        phone=data.get("phone", ""),
        message="[Invest] " + data["message"],
    )
    return JsonResponse({"success": True})

def a_propos(request):
    return render(request, "about.html")

def properties_list(request):
    """Liste les propriétés actives + vendues depuis ≤ 3 jours, paginées."""
    cutoff = timezone.now() - timedelta(days=3)

    qs = (
        Listing.objects
        .filter(
            Q(status=Listing.STATUS_ACTIVE) |
            Q(status=Listing.STATUS_SOLD, sold_at__gte=cutoff)
        )
        .prefetch_related("photos")
    )

    paginator = Paginator(qs, 50)  # 12 cartes / page (ajuste si besoin)
    page_number = request.GET.get("page") or 1
    page_obj = paginator.get_page(page_number)

    context = {
        "listings": page_obj.object_list,
        "paginator": paginator,
        "page_obj": page_obj,
    }
    return render(request, "properties_list.html", context)


def property_detail(request, slug):
    """
    Détail d'une propriété: on adapte le modèle Listing ⇒ objet `property`
    attendu par le template, et on wrap les photos pour exposer .image.url
    """
    listing = get_object_or_404(
        Listing.objects.prefetch_related(
            Prefetch("photos", queryset=ListingPhoto.objects.order_by("sequence"))
        ),
        slug=slug,
    )

    # --- Wrap pour que le template puisse faire images.X.image.url
    # (image est un namespace avec un attribut url)
    images = [
        SimpleNamespace(image=SimpleNamespace(url=p.url))
        for p in listing.photos.all()
    ]

    # --- Adapter Listing -> interface attendue par le template
    # (title, price, address, bedrooms, bathrooms, surface, year_built, description, included, listing_type, rent_price)
    property_view = SimpleNamespace(
        title=listing.adresse or f"Listing {listing.centris_id}",
        listing_type="sale",            # on force "sale" pour l’instant
        price=listing.prix or 0,        # int
        rent_price=None,                # non utilisé ici
        address=listing.adresse or "",
        bedrooms=listing.nombre_chambres,
        bathrooms=listing.nombre_sdb,
        surface=listing.superficie_habitable,
        year_built=listing.annee_construction,
        description=listing.description or "",
        included=listing.inclus or "",
    )

    ctx = {
        "property": property_view,
        "images": images,  # déjà triées par `sequence`
    }
    return render(request, "property_detail.html", ctx)