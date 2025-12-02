"""
Представления для работы с кампаниями.
"""
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.views.generic import ListView, DetailView
from django.views import View
from django.http import JsonResponse
from .models import Campaign, CampaignOffer, Flow
from .forms import CampaignCreateForm, OfferAddForm, FlowCreateForm
from .services import CampaignService
import logging

logger = logging.getLogger(__name__)


class CampaignListView(ListView):
    """Список всех кампаний."""
    model = Campaign
    template_name = 'campaigns/campaign_list.html'
    context_object_name = 'campaigns'
    paginate_by = 20


class CampaignCreateView(View):
    """Создание новой кампании."""

    def get(self, request):
        form = CampaignCreateForm()
        return render(request, 'campaigns/campaign_create.html', {'form': form})

    def post(self, request):
        form = CampaignCreateForm(request.POST)
        if form.is_valid():
            try:
                service = CampaignService()
                campaign = service.create_campaign_with_flows(
                    name=form.cleaned_data['name'],
                    geo=form.cleaned_data['geo'],
                    offer_id=form.cleaned_data['offer_id'],
                    domain=form.cleaned_data.get('domain') or None,
                    group=form.cleaned_data.get('group') or None,
                    source=form.cleaned_data.get('source') or None
                )
                messages.success(request, f'Кампания "{campaign.name}" успешно создана!')
                return redirect('campaign_detail', pk=campaign.pk)
            except Exception as e:
                logger.error(f"Error creating campaign: {e}")
                messages.error(request, f'Ошибка при создании кампании: {str(e)}')
        
        return render(request, 'campaigns/campaign_create.html', {'form': form})


class CampaignDetailView(DetailView):
    """Детальная информация о кампании."""
    model = Campaign
    template_name = 'campaigns/campaign_detail.html'
    context_object_name = 'campaign'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['offers'] = self.object.campaign_offers.all()
        context['flows'] = self.object.flows.all()
        context['add_offer_form'] = OfferAddForm()
        return context


class CampaignEditView(View):
    """Редактор кампании - добавление/удаление офферов."""

    def get(self, request, pk):
        campaign = get_object_or_404(Campaign, pk=pk)
        flows = campaign.flows.all()
        # Получаем все активные офферы (не только те, у которых есть flow)
        offers = campaign.campaign_offers.filter(status='active').select_related('flow')
        add_form = OfferAddForm()
        flow_form = FlowCreateForm()
        
        return render(request, 'campaigns/campaign_edit.html', {
            'campaign': campaign,
            'flows': flows,
            'offers': offers,
            'add_form': add_form,
            'flow_form': flow_form,
        })

    def post(self, request, pk):
        campaign = get_object_or_404(Campaign, pk=pk)
        action = request.POST.get('action')

        if action == 'add_offer':
            form = OfferAddForm(request.POST)
            if form.is_valid():
                try:
                    service = CampaignService()
                    service.add_offer_to_campaign(
                        campaign=campaign,
                        offer_id=form.cleaned_data['offer_id'],
                        weight=form.cleaned_data['weight']
                    )
                    messages.success(request, 'Оффер успешно добавлен в кампанию!')
                except Exception as e:
                    logger.error(f"Error adding offer: {e}")
                    messages.error(request, f'Ошибка при добавлении оффера: {str(e)}')
            else:
                messages.error(request, 'Проверьте правильность введенных данных')
        
        elif action == 'remove_offer':
            offer_id = request.POST.get('offer_id')
            if offer_id:
                try:
                    service = CampaignService()
                    service.remove_offer_from_campaign(
                        campaign=campaign,
                        offer_id=int(offer_id)
                    )
                    messages.success(request, 'Оффер успешно удален из кампании!')
                except Exception as e:
                    logger.error(f"Error removing offer: {e}")
                    messages.error(request, f'Ошибка при удалении оффера: {str(e)}')
        
        elif action == 'create_flow':
            form = FlowCreateForm(request.POST)
            if form.is_valid():
                try:
                    service = CampaignService()
                    flow = service.create_flow_for_campaign(
                        campaign=campaign,
                        name=form.cleaned_data['name'],
                        flow_type=form.cleaned_data['flow_type'],
                        redirect_url=form.cleaned_data.get('redirect_url') or None,
                        country=form.cleaned_data.get('country') or None,
                        offer_ids=form.cleaned_data.get('offer_ids') or None
                    )
                    messages.success(request, f'Поток "{flow.name}" успешно создан!')
                except ValueError as e:
                    logger.error(f"Error creating flow (ValueError): {e}", exc_info=True)
                    error_msg = str(e)
                    # Добавляем более детальную информацию
                    if 'API вернул ошибку' in error_msg:
                        messages.error(request, f'{error_msg}. Проверьте консоль Django для деталей. Убедитесь, что: 1) Кампания существует в Keitaro, 2) Правильные значения schema и action_type, 3) Правильный формат фильтров.')
                    else:
                        messages.error(request, f'Ошибка при создании потока: {error_msg}')
                except Exception as e:
                    logger.error(f"Error creating flow (Exception): {e}", exc_info=True)
                    error_msg = str(e)
                    # Если это ошибка от Keitaro API, показываем более понятное сообщение
                    if 'Keitaro API error' in error_msg:
                        # Извлекаем детали из ошибки
                        if 'Response:' in error_msg:
                            response_part = error_msg.split('Response:')[-1].strip()
                            messages.error(request, f'Ошибка Keitaro API. Ответ сервера: {response_part}. Проверьте логи Django для полной информации.')
                        else:
                            messages.error(request, f'Ошибка Keitaro API: {error_msg}. Проверьте логи Django.')
                    else:
                        messages.error(request, f'Ошибка при создании потока: {error_msg}. Проверьте логи Django для деталей.')
            else:
                messages.error(request, 'Проверьте правильность введенных данных')
                for field, errors in form.errors.items():
                    for error in errors:
                        messages.error(request, f'{field}: {error}')

        return redirect('campaign_edit', pk=campaign.pk)


class RemoveOfferView(View):
    """AJAX view для удаления оффера."""

    def post(self, request, pk, offer_id):
        campaign = get_object_or_404(Campaign, pk=pk)
        try:
            service = CampaignService()
            service.remove_offer_from_campaign(campaign=campaign, offer_id=offer_id)
            return JsonResponse({'success': True, 'message': 'Оффер успешно удален'})
        except Exception as e:
            logger.error(f"Error removing offer: {e}")
            return JsonResponse({'success': False, 'message': str(e)}, status=400)


class FetchStreamsView(View):
    """AJAX view для получения потоков из Keitaro."""

    def post(self, request, pk):
        campaign = get_object_or_404(Campaign, pk=pk)
        try:
            service = CampaignService()
            service.fetch_streams_from_keitaro(campaign)
            return JsonResponse({'success': True, 'message': 'Потоки успешно загружены из Keitaro'})
        except Exception as e:
            logger.error(f"Error fetching streams: {e}")
            return JsonResponse({'success': False, 'message': str(e)}, status=400)


class PushToKTView(View):
    """AJAX view для публикации потока в Keitaro."""

    def post(self, request, pk, flow_id):
        campaign = get_object_or_404(Campaign, pk=pk)
        flow = get_object_or_404(Flow, pk=flow_id, campaign=campaign)
        try:
            service = CampaignService()
            service.push_flow_to_keitaro(flow)
            return JsonResponse({'success': True, 'message': 'Изменения успешно опубликованы в Keitaro'})
        except Exception as e:
            logger.error(f"Error pushing to KT: {e}")
            return JsonResponse({'success': False, 'message': str(e)}, status=400)


class CancelChangesView(View):
    """AJAX view для отмены изменений потока."""

    def post(self, request, pk, flow_id):
        campaign = get_object_or_404(Campaign, pk=pk)
        flow = get_object_or_404(Flow, pk=flow_id, campaign=campaign)
        try:
            service = CampaignService()
            service.cancel_flow_changes(flow)
            return JsonResponse({'success': True, 'message': 'Изменения отменены'})
        except Exception as e:
            logger.error(f"Error canceling changes: {e}")
            return JsonResponse({'success': False, 'message': str(e)}, status=400)


class BringBackOfferView(View):
    """AJAX view для возврата удаленного оффера."""

    def post(self, request, pk, offer_id):
        campaign = get_object_or_404(Campaign, pk=pk)
        try:
            service = CampaignService()
            service.bring_back_offer(campaign=campaign, offer_id=offer_id)
            return JsonResponse({'success': True, 'message': 'Оффер успешно возвращен'})
        except Exception as e:
            logger.error(f"Error bringing back offer: {e}")
            return JsonResponse({'success': False, 'message': str(e)}, status=400)


class PinWeightView(View):
    """AJAX view для закрепления/открепления веса оффера."""

    def post(self, request, pk, offer_id):
        campaign = get_object_or_404(Campaign, pk=pk)
        offer = get_object_or_404(CampaignOffer, pk=offer_id, campaign=campaign)
        try:
            service = CampaignService()
            action = request.POST.get('action', 'pin')
            if action == 'pin':
                service.pin_offer_weight(offer)
                return JsonResponse({'success': True, 'message': 'Вес оффера закреплен'})
            else:
                service.unpin_offer_weight(offer)
                return JsonResponse({'success': True, 'message': 'Вес оффера откреплен'})
        except Exception as e:
            logger.error(f"Error pinning weight: {e}")
            return JsonResponse({'success': False, 'message': str(e)}, status=400)


class SearchOffersView(View):
    """AJAX view для поиска офферов (autocomplete)."""

    def get(self, request):
        query = request.GET.get('q', '')
        limit = int(request.GET.get('limit', 20))
        try:
            service = CampaignService()
            offers = service.search_offers(query, limit)
            return JsonResponse({'success': True, 'offers': offers})
        except Exception as e:
            logger.error(f"Error searching offers: {e}")
            return JsonResponse({'success': False, 'message': str(e)}, status=400)


class DiagnosticView(View):
    """Временный view для диагностики API."""

    def get(self, request, pk):
        """Показывает доступные схемы, действия и фильтры."""
        campaign = get_object_or_404(Campaign, pk=pk)
        try:
            service = CampaignService()
            api = service.api
            
            schemas = []
            actions = []
            filters = []
            streams = []
            selected_schema_redirect = None
            selected_schema_offers = None
            selected_action_redirect = None
            
            try:
                schemas = api.get_stream_schemas()
            except Exception as e:
                schemas = f"Error getting schemas: {str(e)}"
            
            try:
                actions = api.get_streams_actions()
            except Exception as e:
                actions = f"Error getting actions: {str(e)}"
            
            try:
                filters = api.get_stream_filters()
            except Exception as e:
                filters = f"Error getting filters: {str(e)}"
            
            # Пробуем получить потоки кампании
            if campaign.keitaro_id:
                try:
                    streams = api.get_campaign_streams(campaign.keitaro_id)
                except Exception as e:
                    streams = f"Error getting streams: {str(e)}"
            
            # Получаем выбранные значения
            try:
                selected_schema_redirect = service._get_schema_for_redirect()
            except Exception as e:
                selected_schema_redirect = f"Error: {str(e)}"
            
            try:
                selected_schema_offers = service._get_schema_for_offers()
            except Exception as e:
                selected_schema_offers = f"Error: {str(e)}"
            
            try:
                selected_action_redirect = service._get_action_type_for_redirect()
            except Exception as e:
                selected_action_redirect = f"Error: {str(e)}"
            
            return JsonResponse({
                'success': True,
                'campaign_id': campaign.pk,
                'campaign_name': campaign.name,
                'campaign_keitaro_id': campaign.keitaro_id,
                'schemas': schemas,
                'actions': actions,
                'filters': filters,
                'streams': streams,
                'selected_schema_redirect': selected_schema_redirect,
                'selected_schema_offers': selected_schema_offers,
                'selected_action_redirect': selected_action_redirect,
            })
        except Exception as e:
            logger.error(f"Diagnostic error: {e}", exc_info=True)
            import traceback
            error_traceback = traceback.format_exc()
            logger.error(f"Full diagnostic error traceback:\n{error_traceback}")
            return JsonResponse({
                'success': False, 
                'message': str(e),
                'error_type': type(e).__name__,
                'traceback': error_traceback
            }, status=400)

