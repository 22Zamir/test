"""
Формы для работы с кампаниями.
"""
from django import forms
from .models import Campaign, CampaignOffer


class CampaignCreateForm(forms.Form):
    """Форма для создания кампании."""
    name = forms.CharField(
        label='Название кампании',
        max_length=255,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'имя кампании'
        })
    )
    geo = forms.CharField(
        label='Geo',
        max_length=10,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'MX,AU,RO,...'
        }),
        help_text='Код страны (например, AU, MX, RO)'
    )
    offer_id = forms.IntegerField(
        label='Offer',
        widget=forms.NumberInput(attrs={
            'class': 'form-control',
            'placeholder': 'ID оффера Keitaro'
        }),
        help_text='ID оффера из Keitaro'
    )
    domain = forms.URLField(
        label='Домен',
        required=False,
        widget=forms.URLInput(attrs={
            'class': 'form-control',
            'placeholder': 'https://your-domain.com (опционально)'
        }),
        help_text='Домен для кампании (опционально, можно оставить пустым)'
    )
    group = forms.CharField(
        label='Группа',
        required=False,
        max_length=255,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Название группы (опционально)'
        }),
        help_text='Группа для кампании (опционально)'
    )
    source = forms.CharField(
        label='Источник',
        required=False,
        max_length=255,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Название источника (опционально)'
        }),
        help_text='Источник трафика (опционально)'
    )


class OfferAddForm(forms.Form):
    """Форма для добавления оффера в кампанию."""
    offer_id = forms.IntegerField(
        label='ID оффера',
        widget=forms.NumberInput(attrs={
            'class': 'form-control',
            'placeholder': 'ID оффера Keitaro'
        })
    )
    weight = forms.IntegerField(
        label='Вес',
        initial=1,
        min_value=1,
        widget=forms.NumberInput(attrs={
            'class': 'form-control',
            'min': '1'
        }),
        help_text='Вес оффера для ротации (чем больше, тем чаще показывается)'
    )


class FlowCreateForm(forms.Form):
    """Форма для создания потока в кампании."""
    FLOW_TYPE_CHOICES = [
        ('country_filter', 'Фильтр по стране (редирект на URL)'),
        ('offer_redirect', 'Редирект на оффер(ы)'),
    ]
    
    name = forms.CharField(
        label='Название потока',
        max_length=255,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Название потока'
        })
    )
    flow_type = forms.ChoiceField(
        label='Тип потока',
        choices=FLOW_TYPE_CHOICES,
        widget=forms.Select(attrs={
            'class': 'form-control',
            'id': 'flow_type_select'
        })
    )
    redirect_url = forms.URLField(
        label='URL для редиректа',
        required=False,
        widget=forms.URLInput(attrs={
            'class': 'form-control',
            'placeholder': 'https://example.com',
            'id': 'redirect_url_field'
        }),
        help_text='URL для редиректа (для типа "Фильтр по стране")'
    )
    country = forms.CharField(
        label='Код страны',
        required=False,
        max_length=10,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'RU, US, AU...',
            'id': 'country_field'
        }),
        help_text='Код страны для фильтрации (например, RU, US, AU)'
    )
    offer_ids = forms.CharField(
        label='ID офферов',
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': '1, 2, 3',
            'id': 'offer_ids_field'
        }),
        help_text='ID офферов через запятую (для типа "Редирект на оффер")'
    )

