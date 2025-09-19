# core/context_processors.py
from django.conf import settings

def tidio_settings(request):
    return {"TIDIO_PUBLIC_KEY": getattr(settings, "TIDIO_PUBLIC_KEY", "")}
