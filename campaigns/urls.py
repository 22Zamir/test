"""
URL конфигурация для приложения campaigns.
"""
from django.urls import path
from . import views

urlpatterns = [
    path('', views.CampaignListView.as_view(), name='campaign_list'),
    path('create/', views.CampaignCreateView.as_view(), name='campaign_create'),
    path('<int:pk>/', views.CampaignDetailView.as_view(), name='campaign_detail'),
    path('<int:pk>/edit/', views.CampaignEditView.as_view(), name='campaign_edit'),
    path('<int:pk>/fetch-streams/', views.FetchStreamsView.as_view(), name='fetch_streams'),
    path('<int:pk>/flows/<int:flow_id>/push/', views.PushToKTView.as_view(), name='push_to_kt'),
    path('<int:pk>/flows/<int:flow_id>/cancel/', views.CancelChangesView.as_view(), name='cancel_changes'),
    path('<int:pk>/offers/<int:offer_id>/remove/', views.RemoveOfferView.as_view(), name='remove_offer'),
    path('<int:pk>/offers/<int:offer_id>/bring-back/', views.BringBackOfferView.as_view(), name='bring_back_offer'),
    path('<int:pk>/offers/<int:offer_id>/pin-weight/', views.PinWeightView.as_view(), name='pin_weight'),
    path('search-offers/', views.SearchOffersView.as_view(), name='search_offers'),
    path('<int:pk>/diagnostic/', views.DiagnosticView.as_view(), name='diagnostic'),
]

