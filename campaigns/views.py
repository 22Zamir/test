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
    """Список активных кампаний (синхронизируется с Keitaro API)."""
    model = Campaign
    template_name = 'campaigns/campaign_list.html'
    context_object_name = 'campaigns'
    paginate_by = 20

    def get_queryset(self):
        """Получает активные кампании из API и синхронизирует с БД."""
        api_error = None
        try:
            service = CampaignService()
            active_campaigns = service.sync_active_campaigns_from_api()
            
            if not active_campaigns:
                logger.warning("Не получено активных кампаний из API")
                # Проверяем, была ли ошибка API
                try:
                    # Пробуем еще раз, чтобы проверить ошибку
                    test_campaigns = service.api.get_campaigns()
                    if not test_campaigns:
                        messages.info(self.request, 'В Keitaro нет активных кампаний.')
                except Exception as e:
                    error_msg = str(e)
                    if '401' in error_msg or 'Unauthorized' in error_msg:
                        messages.error(self.request, 'Ошибка авторизации Keitaro API. Проверьте KEITARO_API_KEY в файле .env')
                    else:
                        messages.warning(self.request, f'Не удалось получить кампании из Keitaro API: {str(e)}')
                return Campaign.objects.none()
            
            # Получаем только keitaro_id активных кампаний
            active_keitaro_ids = [c.keitaro_id for c in active_campaigns if c.keitaro_id is not None]
            
            if not active_keitaro_ids:
                logger.warning("Нет активных кампаний с keitaro_id")
                return Campaign.objects.none()
            
            logger.info(f"Фильтруем кампании по keitaro_id: {active_keitaro_ids}")
            
            # Возвращаем только активные кампании, отсортированные по дате создания
            # Исключаем кампании без keitaro_id
            queryset = Campaign.objects.filter(
                keitaro_id__in=active_keitaro_ids
            ).exclude(keitaro_id__isnull=True).order_by('-created_at')
            
            logger.info(f"Найдено {queryset.count()} активных кампаний в БД из {len(active_keitaro_ids)} в API")
            return queryset
            
        except Exception as e:
            logger.error(f"Ошибка при получении активных кампаний: {e}", exc_info=True)
            error_msg = str(e)
            if '401' in error_msg or 'Unauthorized' in error_msg:
                messages.error(self.request, 'Ошибка авторизации Keitaro API. Проверьте KEITARO_API_KEY в файле .env')
            else:
                messages.error(self.request, f'Ошибка при синхронизации с Keitaro API: {str(e)}')
            # В случае ошибки возвращаем пустой список, чтобы не показывать удаленные кампании
            return Campaign.objects.none()


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
        
        # Синхронизируем потоки из Keitaro API, если у кампании есть keitaro_id
        if self.object.keitaro_id:
            try:
                service = CampaignService()
                service.fetch_streams_from_keitaro(self.object)
                logger.info(f"Синхронизированы потоки для кампании {self.object.pk} (keitaro_id={self.object.keitaro_id})")
            except Exception as e:
                logger.warning(f"Не удалось синхронизировать потоки для кампании {self.object.pk}: {e}")
        
        # Получаем только активные офферы (не удаленные)
        from .models import CampaignOffer
        context['offers'] = CampaignOffer.objects.filter(
            campaign=self.object,
            status='active'
        ).select_related('flow').order_by('-created_at')
        context['flows'] = self.object.flows.all()
        context['add_offer_form'] = OfferAddForm()
        return context


class CampaignEditView(View):
    """Редактор кампании - добавление/удаление офферов."""

    def get(self, request, pk):
        campaign = get_object_or_404(Campaign, pk=pk)
        
        # Синхронизируем потоки из Keitaro API, если у кампании есть keitaro_id
        if campaign.keitaro_id:
            try:
                service = CampaignService()
                service.fetch_streams_from_keitaro(campaign)
                logger.info(f"Синхронизированы потоки для кампании {campaign.pk} (keitaro_id={campaign.keitaro_id})")
            except Exception as e:
                logger.warning(f"Не удалось синхронизировать потоки для кампании {campaign.pk}: {e}")
        
        flows = campaign.flows.all()
        
        # Получаем все активные офферы напрямую из БД (не через related manager)
        # Это гарантирует, что мы получим актуальные данные после редиректа
        from .models import CampaignOffer
        offers = CampaignOffer.objects.filter(
            campaign=campaign,
            status='active'
        ).select_related('flow').order_by('-created_at')
        
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
                    campaign_offer = service.add_offer_to_campaign(
                        campaign=campaign,
                        offer_id=form.cleaned_data['offer_id'],
                        weight=form.cleaned_data['weight']
                    )
                    # Обновляем объект кампании из БД, чтобы получить актуальные данные
                    campaign.refresh_from_db()
                    
                    # Явно проверяем, что оффер сохранен
                    from .models import CampaignOffer
                    saved_count = CampaignOffer.objects.filter(
                        campaign=campaign,
                        status='active'
                    ).count()
                    logger.info(f"Оффер {campaign_offer.offer_id} добавлен, обновлен объект кампании {campaign.pk}. Всего активных офферов в БД: {saved_count}")
                    
                    messages.success(request, 'Оффер успешно добавлен в кампанию!')
                except Exception as e:
                    logger.error(f"Error adding offer: {e}", exc_info=True)
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

        # После всех действий делаем редирект на страницу редактирования
        # При GET запросе данные будут загружены заново из БД
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


class DeleteFlowView(View):
    """AJAX view для удаления потока."""

    def post(self, request, pk, flow_id):
        campaign = get_object_or_404(Campaign, pk=pk)
        flow = get_object_or_404(Flow, pk=flow_id, campaign=campaign)
        try:
            service = CampaignService()
            success = service.delete_flow(flow)
            if success:
                return JsonResponse({'success': True, 'message': 'Поток успешно удален'})
            else:
                return JsonResponse({'success': False, 'message': 'Не удалось удалить поток из Keitaro'}, status=400)
        except Exception as e:
            logger.error(f"Error deleting flow: {e}", exc_info=True)
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


class CampaignHistoryView(View):
    """История удаленных кампаний."""
    template_name = 'campaigns/campaign_history.html'

    def get(self, request):
        """Отображает список удаленных кампаний из Keitaro API."""
        try:
            service = CampaignService()
            deleted_campaigns = service.get_deleted_campaigns_from_api()
            
            # Получаем также кампании из БД, которых нет в активных
            active_campaigns = service.sync_active_campaigns_from_api()
            active_keitaro_ids = {c.keitaro_id for c in active_campaigns if c.keitaro_id}
            
            # Кампании из БД, которых нет в активных
            db_deleted_campaigns = Campaign.objects.exclude(
                keitaro_id__in=active_keitaro_ids
            ).exclude(keitaro_id__isnull=True).order_by('-created_at')
            
            return render(request, self.template_name, {
                'deleted_campaigns_api': deleted_campaigns,
                'deleted_campaigns_db': db_deleted_campaigns,
            })
        except Exception as e:
            logger.error(f"Ошибка при получении истории кампаний: {e}", exc_info=True)
            messages.error(request, f'Ошибка при загрузке истории: {str(e)}')
            return render(request, self.template_name, {
                'deleted_campaigns_api': [],
                'deleted_campaigns_db': [],
            })


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

