# path: core/forms.py
from django import forms

class ContactForm(forms.Form):
    name = forms.CharField(max_length=150)
    email = forms.EmailField()
    phone = forms.CharField(max_length=50, required=False)
    message = forms.CharField(widget=forms.Textarea, max_length=5000)
