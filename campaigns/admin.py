from django.contrib import admin
from .models import Campaign, Flow, CampaignOffer


@admin.register(Campaign)
class CampaignAdmin(admin.ModelAdmin):
    list_display = ['name', 'geo', 'offer_id', 'keitaro_id', 'created_at']
    list_filter = ['geo', 'created_at']
    search_fields = ['name', 'geo']


@admin.register(Flow)
class FlowAdmin(admin.ModelAdmin):
    list_display = ['name', 'campaign', 'flow_type', 'country', 'keitaro_id']
    list_filter = ['flow_type', 'country']
    search_fields = ['name', 'campaign__name']


@admin.register(CampaignOffer)
class CampaignOfferAdmin(admin.ModelAdmin):
    list_display = ['campaign', 'offer_id', 'offer_name', 'weight', 'created_at']
    list_filter = ['campaign', 'weight']
    search_fields = ['campaign__name', 'offer_name']




