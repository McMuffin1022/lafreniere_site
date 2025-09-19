# core/urls.py
from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='home'),
    path("contact/", views.contact_page, name="contact"),
    path("collaborateurs/", views.collaborators_page, name="collaborators"),
    path("investir/", views.invest_page, name="invest"),
    path("investir/submit/", views.invest_contact_submit, name="invest_contact_submit"),
    path("a-propos/", views.a_propos, name="about"),
    path("properties/", views.properties_list, name="properties_list"),
    path("contact/submit/", views.contact_submit, name="contact_submit"),
    path("properties/<slug:slug>/", views.property_detail, name="property_detail"),
]