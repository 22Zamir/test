"""
Сервисы для работы с кампаниями.
"""
from typing import Dict, Optional, List
from django.conf import settings
from .models import Campaign, Flow, CampaignOffer
from .keitaro_api import KeitaroAPI
import logging

logger = logging.getLogger(__name__)


class CampaignService:
    """Сервис для создания и управления кампаниями."""

    def __init__(self):
        self.api = KeitaroAPI()
        # Кэш для schemas и actions (чтобы не запрашивать каждый раз)
        self._schemas_cache = None
        self._actions_cache = None

    def _get_schemas(self) -> List[Dict]:
        """Получает схемы с кэшированием."""
        if self._schemas_cache is None:
            try:
                self._schemas_cache = self.api.get_stream_schemas()
            except Exception as e:
                logger.warning(f"Could not get schemas: {e}")
                self._schemas_cache = []
        return self._schemas_cache

    def _get_actions(self) -> List[Dict]:
        """Получает действия с кэшированием."""
        if self._actions_cache is None:
            try:
                self._actions_cache = self.api.get_streams_actions()
            except Exception as e:
                logger.warning(f"Could not get actions: {e}")
                self._actions_cache = []
        return self._actions_cache

    def _get_schema_for_offers(self) -> str:
        """Получает правильную схему для потоков с офферами."""
        schemas = self._get_schemas()
        for schema in schemas:
            if isinstance(schema, dict):
                schema_value = schema.get('value', '')
                if schema_value == 'landings':
                    return 'landings'
        # Если не нашли 'landings', берем первую доступную
        if schemas and len(schemas) > 0:
            first = schemas[0]
            return first.get('value', 'landings') if isinstance(first, dict) else str(first)
        return 'landings'  # Fallback

    def _get_schema_for_redirect(self) -> str:
        """Получает правильную схему для redirect потоков."""
        schemas = self._get_schemas()
        for schema in schemas:
            if isinstance(schema, dict):
                schema_value = schema.get('value', '')
                schema_key = schema.get('key', '')
                if schema_value == 'redirect' or schema_key == 'redirect':
                    return schema_value or schema_key
        # Если не нашли 'redirect', берем первую доступную
        if schemas and len(schemas) > 0:
            first = schemas[0]
            return first.get('value', first.get('key', 'redirect')) if isinstance(first, dict) else str(first)
        return 'redirect'  # Fallback

    def _get_action_type_for_redirect(self) -> str:
        """Получает правильный action_type для redirect."""
        actions = self._get_actions()
        # Ищем стандартный HTTP redirect
        for action in actions:
            if isinstance(action, dict):
                action_key = action.get('key', '')
                action_type = action.get('type', '')
                if action_key == 'http' and action_type == 'redirect':
                    return 'http'
        # Если не нашли, пробуем другие варианты
        for action in actions:
            if isinstance(action, dict):
                action_key = action.get('key', '')
                if action_key in ['http', 'meta', 'js']:
                    return action_key
        return 'http'  # Fallback

    def _get_action_type_for_offers(self) -> str:
        """Получает правильный action_type для потоков с офферами."""
        # Для схемы 'landings' с офферами используем стандартный redirect action
        # Офферы указываются в action_payload, а action_type - это способ редиректа
        return self._get_action_type_for_redirect()

    def _save_offers_to_db(
        self,
        campaign: Campaign,
        flow: Flow,
        offer_id_list: List[int]
    ) -> None:
        """Вспомогательный метод для сохранения офферов в БД."""
        for offer_id in offer_id_list:
            try:
                offer_data = self.api.get_offer(offer_id)
                offer_name = offer_data.get('name', '') if offer_data else ''
            except Exception as e:
                logger.warning(f"Не удалось получить информацию об оффере {offer_id}: {e}")
                offer_name = ''
            
            CampaignOffer.objects.get_or_create(
                campaign=campaign,
                offer_id=offer_id,
                defaults={
                    'flow': flow,
                    'offer_name': offer_name,
                    'weight': 1,
                    'status': 'active'
                }
            )

    def _find_existing_flow(
        self,
        campaign: Campaign,
        name: str,
        flow_type: str,
        offer_id_list: Optional[List[int]] = None,
        country: Optional[str] = None,
        redirect_url: Optional[str] = None
    ) -> Optional[Flow]:
        """Проверяет, создался ли поток в Keitaro, несмотря на ошибку API."""
        try:
            streams = self.api.get_campaign_streams(campaign.keitaro_id)
            logger.info(f"Найдено потоков в кампании: {len(streams)}")
            
            for stream in streams:
                stream_id = stream.get('id')
                stream_name = stream.get('name', '')
                action_payload = stream.get('action_payload', {})
                
                # Проверяем по типу потока
                if flow_type == 'offer_redirect' and offer_id_list:
                    stream_offers = action_payload.get('offers', [])
                    stream_offer_ids = [o.get('id') for o in stream_offers if o.get('id')]
                    matches = (name.lower() in stream_name.lower() or 
                              set(offer_id_list).issubset(set(stream_offer_ids)))
                elif flow_type == 'country_filter':
                    stream_url = action_payload.get('url', '')
                    matches = (name.lower() in stream_name.lower() or 
                              (country and country.upper() in stream_name.upper()) or
                              (redirect_url and redirect_url in stream_url))
                else:
                    matches = name.lower() in stream_name.lower()
                
                if matches:
                    existing_flow = Flow.objects.filter(keitaro_id=stream_id).first()
                    if not existing_flow:
                        # Создаем поток в БД
                        flow = Flow.objects.create(
                            campaign=campaign,
                            keitaro_id=stream_id,
                            name=stream_name,
                            flow_type=flow_type,
                            country=country or '',
                            redirect_url=redirect_url or ''
                        )
                        
                        # Если это поток с офферами, сохраняем их
                        if flow_type == 'offer_redirect' and offer_id_list:
                            self._save_offers_to_db(campaign, flow, offer_id_list)
                        
                        logger.info(f"Найден созданный поток: ID={stream_id}, name={stream_name}")
                        return flow
                    else:
                        logger.info(f"Поток {stream_id} уже существует в БД")
                        return existing_flow
        except Exception as e:
            logger.error(f"Не удалось проверить созданные потоки: {e}", exc_info=True)
        
        return None

    def _check_and_save_flow_if_exists(
        self,
        campaign: Campaign,
        campaign_id: int,
        offer_id: int,
        offer_name: str,
        error_msg: str = ''
    ) -> Optional[Flow]:
        """
        Проверяет, создался ли поток в Keitaro, несмотря на ошибку.
        Если поток найден, сохраняет его в БД.
        """
        try:
            streams = self.api.get_campaign_streams(campaign_id)
            for stream in streams:
                stream_name = stream.get('name', '')
                stream_id = stream.get('id')
                
                # Ищем поток по имени или по офферам в action_payload
                action_payload = stream.get('action_payload', {})
                stream_offers = action_payload.get('offers', [])
                has_our_offer = any(o.get('id') == offer_id for o in stream_offers)
                
                if (f'Offer {offer_id}' in stream_name or 
                    'Flow 2' in stream_name or 
                    has_our_offer):
                    # Проверяем, нет ли уже такого потока в БД
                    existing_flow = Flow.objects.filter(keitaro_id=stream_id).first()
                    if not existing_flow:
                        flow2 = Flow.objects.create(
                            campaign=campaign,
                            keitaro_id=stream_id,
                            name=stream_name,
                            flow_type='offer_redirect'
                        )
                        logger.info(f"Найден созданный поток (несмотря на ошибку): ID={stream_id}, сохраняем в БД")
                        
                        # Сохраняем оффер
                        CampaignOffer.objects.create(
                            campaign=campaign,
                            flow=flow2,
                            offer_id=offer_id,
                            offer_name=offer_name,
                            weight=1,
                            status='active'
                        )
                        return flow2
                    else:
                        # Поток уже есть в БД, просто обновляем оффер
                        logger.info(f"Поток {stream_id} уже существует в БД, обновляем оффер")
                        offer, created = CampaignOffer.objects.get_or_create(
                            campaign=campaign,
                            offer_id=offer_id,
                            defaults={
                                'flow': existing_flow,
                                'offer_name': offer_name,
                                'weight': 1,
                                'status': 'active'
                            }
                        )
                        if not created:
                            offer.flow = existing_flow
                            offer.status = 'active'
                            offer.save()
                        return existing_flow
        except Exception as check_error:
            logger.error(f"Не удалось проверить созданные потоки: {check_error}")
        
        return None

    def create_campaign_with_flows(
        self,
        name: str,
        geo: str,
        offer_id: int,
        domain: Optional[str] = None,
        group: Optional[str] = None,
        source: Optional[str] = None
    ) -> Campaign:
        """
        Создает кампанию с двумя потоками:
        1. Поток для указанной страны - редирект на Google
        2. Поток для остальных - редирект на оффер
        """
        domain = domain or (settings.KEITARO_DOMAIN if settings.KEITARO_DOMAIN else None)
        group = group or (settings.KEITARO_GROUP if settings.KEITARO_GROUP else None)
        source = source or (settings.KEITARO_SOURCE if settings.KEITARO_SOURCE else None)

        # Создаем кампанию
        campaign_data = self.api.create_campaign(
            name=name,
            domain=domain,
            group=group,
            source=source,
            geo=geo
        )

        campaign_id = campaign_data.get('id')
        if not campaign_id:
            raise ValueError("Не удалось получить ID созданной кампании")

        # Сохраняем кампанию в БД
        campaign = Campaign.objects.create(
            keitaro_id=campaign_id,
            name=name,
            geo=geo,
            offer_id=offer_id,
            domain=domain or '',
            group=group or '',
            source=source or ''
        )

        # Создаем первый поток - фильтр по стране (редирект на Google)
        redirect_schema = self._get_schema_for_redirect()
        redirect_action_type = self._get_action_type_for_redirect()
        
        flow1_data = None
        try:
            flow1_data = self.api.create_flow(
                campaign_id=campaign_id,
                name=f"Flow 1 - {geo} to Google",
                action_type=redirect_action_type,
                action_payload={'url': 'https://google.com'},
                schema=redirect_schema,
                filters=[{
                    'name': 'country',
                    'operator': 'is',
                    'value': geo
                }]
            )
            
            if flow1_data and flow1_data.get('id'):
                Flow.objects.create(
                    campaign=campaign,
                    keitaro_id=flow1_data.get('id'),
                    name=flow1_data.get('name', f'Flow 1 - {geo} to Google'),
                    flow_type='country_filter',
                    country=geo,
                    redirect_url='https://google.com'
                )
            elif flow1_data is None:
                # Если получили None (ошибка 500, но allow_500=True), проверяем, создался ли поток
                logger.warning(f"Получен None при создании первого потока, проверяем, создался ли он")
                try:
                    streams = self.api.get_campaign_streams(campaign_id)
                    for stream in streams:
                        stream_name = stream.get('name', '')
                        if f'{geo} to Google' in stream_name or 'Flow 1' in stream_name:
                            stream_id = stream.get('id')
                            existing_flow = Flow.objects.filter(keitaro_id=stream_id).first()
                            if not existing_flow:
                                Flow.objects.create(
                                    campaign=campaign,
                                    keitaro_id=stream_id,
                                    name=stream_name,
                                    flow_type='country_filter',
                                    country=geo,
                                    redirect_url='https://google.com'
                                )
                                logger.info(f"Найден созданный первый поток: ID={stream_id}")
                                break
                except Exception as check_error:
                    logger.error(f"Не удалось проверить созданные потоки для Flow 1: {check_error}")
        except Exception as e:
            logger.error(f"Ошибка при создании первого потока: {e}")
            # Проверяем, может быть поток все-таки создался
            try:
                streams = self.api.get_campaign_streams(campaign_id)
                for stream in streams:
                    stream_name = stream.get('name', '')
                    if f'{geo} to Google' in stream_name or 'Flow 1' in stream_name:
                        stream_id = stream.get('id')
                        existing_flow = Flow.objects.filter(keitaro_id=stream_id).first()
                        if not existing_flow:
                            Flow.objects.create(
                                campaign=campaign,
                                keitaro_id=stream_id,
                                name=stream_name,
                                flow_type='country_filter',
                                country=geo,
                                redirect_url='https://google.com'
                            )
                            logger.info(f"Найден созданный первый поток (несмотря на ошибку): ID={stream_id}")
                            break
            except Exception as check_error:
                logger.error(f"Не удалось проверить созданные потоки для Flow 1: {check_error}")

        # Создаем второй поток - редирект на оффер
        offer_schema = self._get_schema_for_offers()
        offer_action_type = self._get_action_type_for_offers()
        
        # Получаем информацию об оффере
        try:
            offer_data = self.api.get_offer(offer_id)
            offer_name = offer_data.get('name', '') if offer_data else ''
        except Exception as e:
            logger.warning(f"Не удалось получить информацию об оффере {offer_id}: {e}")
            offer_name = ''

        # Пробуем создать поток с офферами
        flow2 = None
        flow2_data = None
        try:
            flow2_data = self.api.create_flow(
                campaign_id=campaign_id,
                name=f"Flow 2 - Offer {offer_id}",
                action_type=offer_action_type,
                action_payload={
                    'offers': [{
                        'id': offer_id,
                        'weight': 1
                    }]
                },
                schema=offer_schema
            )
            
            if flow2_data and flow2_data.get('id'):
                flow2 = Flow.objects.create(
                    campaign=campaign,
                    keitaro_id=flow2_data.get('id'),
                    name=flow2_data.get('name', f'Flow 2 - Offer {offer_id}'),
                    flow_type='offer_redirect'
                )
                
                # Сохраняем оффер в БД
                CampaignOffer.objects.create(
                    campaign=campaign,
                    flow=flow2,
                    offer_id=offer_id,
                    offer_name=offer_name,
                    weight=1,
                    status='active'
                )
                logger.info(f"Второй поток успешно создан: ID={flow2.keitaro_id}")
            elif flow2_data is None:
                # Если получили None (ошибка 500, но allow_500=True), проверяем, создался ли поток
                logger.warning(f"Получен None при создании потока, проверяем, создался ли он на самом деле")
                flow2 = self._check_and_save_flow_if_exists(campaign, campaign_id, offer_id, offer_name)
        except Exception as e:
            logger.error(f"Ошибка при создании второго потока: {e}")
            # Проверяем, может быть поток все-таки создался (особенно при ошибках 500)
            flow2 = self._check_and_save_flow_if_exists(campaign, campaign_id, offer_id, offer_name, str(e))
            
            # Если поток не создался, сохраняем оффер без потока
            if not flow2:
                try:
                    CampaignOffer.objects.create(
                        campaign=campaign,
                        flow=None,
                        offer_id=offer_id,
                        offer_name=offer_name,
                        weight=1,
                        status='active'
                    )
                    logger.info(f"Оффер {offer_id} сохранен без потока (поток будет создан позже)")
                except Exception as offer_error:
                    logger.error(f"Не удалось сохранить оффер: {offer_error}")

        return campaign

    def add_offer_to_campaign(
        self,
        campaign: Campaign,
        offer_id: int,
        weight: int = 1
    ) -> CampaignOffer:
        """Добавляет оффер в кампанию."""
        # Находим поток для офферов
        flow = campaign.flows.filter(flow_type='offer_redirect').first()
        
        if not flow:
            # Если потока нет, создаем его
            if not campaign.keitaro_id:
                raise ValueError("Кампания не имеет keitaro_id")
            
            # Получаем информацию об оффере
            try:
                offer_data = self.api.get_offer(offer_id)
                offer_name = offer_data.get('name', '')
            except Exception as e:
                logger.warning(f"Не удалось получить информацию об оффере {offer_id}: {e}")
                offer_name = ''
            
            # Создаем поток с офферами
            offer_schema = self._get_schema_for_offers()
            offer_action_type = self._get_action_type_for_offers()
            
            flow_data = self.api.create_flow(
                campaign_id=campaign.keitaro_id,
                name=f"Flow 2 - Offer {offer_id}",
                action_type=offer_action_type,
                action_payload={
                    'offers': [{
                        'id': offer_id,
                        'weight': weight
                    }]
                },
                schema=offer_schema
            )
            
            if flow_data and flow_data.get('id'):
                flow = Flow.objects.create(
                    campaign=campaign,
                    keitaro_id=flow_data.get('id'),
                    name=flow_data.get('name', f'Flow 2 - Offer {offer_id}'),
                    flow_type='offer_redirect',
                    is_published=True,
                    has_changes=False
                )
            else:
                # Если получили None (ошибка 500, но allow_500=True), проверяем, создался ли поток
                logger.warning(f"Получен None при создании потока, проверяем, создался ли он")
                flow = self._check_and_save_flow_if_exists(campaign, campaign.keitaro_id, offer_id, offer_name)
                if not flow:
                    raise ValueError("Не удалось создать поток для оффера. Попробуйте позже.")
        else:
            # Если поток есть, добавляем оффер через API
            if flow.keitaro_id:
                try:
                    # Получаем текущий поток
                    current_flow = self.api.get_flow(flow.keitaro_id)
                    if current_flow:
                        current_offers = current_flow.get('action_payload', {}).get('offers', [])
                        
                        # Проверяем, не добавлен ли уже оффер
                        if not any(o.get('id') == offer_id for o in current_offers):
                            current_offers.append({
                                'id': offer_id,
                                'weight': weight
                            })
                            
                            # Обновляем поток
                            self.api.update_flow(flow.keitaro_id, {
                                'action_payload': {
                                    'offers': current_offers
                                }
                            })
                    else:
                        logger.warning(f"Не удалось получить поток {flow.keitaro_id} из Keitaro")
                except Exception as e:
                    logger.warning(f"Не удалось добавить оффер через API: {e}")
            
            # Получаем информацию об оффере
            try:
                offer_data = self.api.get_offer(offer_id)
                offer_name = offer_data.get('name', '')
            except Exception as e:
                logger.warning(f"Не удалось получить информацию об оффере {offer_id}: {e}")
                offer_name = ''

        # Сохраняем в БД
        campaign_offer, created = CampaignOffer.objects.get_or_create(
            campaign=campaign,
            offer_id=offer_id,
            defaults={
                'flow': flow,
                'offer_name': offer_name,
                'weight': weight,
                'status': 'active'
            }
        )

        if not created:
            campaign_offer.flow = flow
            campaign_offer.weight = weight
            campaign_offer.status = 'active'
            campaign_offer.save()

        # Помечаем поток как имеющий изменения
        flow.has_changes = True
        flow.save()

        # Пересчитываем веса
        self.recalculate_weights(flow)

        return campaign_offer

    def remove_offer_from_campaign(
        self,
        campaign: Campaign,
        offer_id: int
    ) -> bool:
        """Удаляет оффер из кампании (помечает как removed)."""
        offer = CampaignOffer.objects.filter(
            campaign=campaign,
            offer_id=offer_id
        ).first()
        
        if not offer:
            raise ValueError("Оффер не найден")

        offer.status = 'removed'
        offer.save()

        if offer.flow:
            offer.flow.has_changes = True
            offer.flow.save()
            self.recalculate_weights(offer.flow)

        return True

    def bring_back_offer(
        self,
        campaign: Campaign,
        offer_id: int
    ) -> CampaignOffer:
        """Возвращает удаленный оффер."""
        offer = CampaignOffer.objects.filter(
            campaign=campaign,
            offer_id=offer_id
        ).first()
        
        if not offer:
            raise ValueError("Оффер не найден")

        offer.status = 'active'
        offer.save()

        if offer.flow:
            offer.flow.has_changes = True
            offer.flow.save()
            self.recalculate_weights(offer.flow)

        return offer

    def recalculate_weights(self, flow: Flow) -> None:
        """Пересчитывает веса офферов в потоке."""
        active_offers = CampaignOffer.objects.filter(
            flow=flow,
            status='active'
        )

        pinned_offers = active_offers.filter(weight_pinned=True)
        unpinned_offers = active_offers.filter(weight_pinned=False)

        pinned_total = sum(offer.weight for offer in pinned_offers)

        if not unpinned_offers.exists():
            return

        unpinned_count = unpinned_offers.count()
        if unpinned_count > 0:
            base_weight = 1
            for offer in unpinned_offers:
                offer.weight = base_weight
                offer.save()

    def fetch_streams_from_keitaro(self, campaign: Campaign) -> None:
        """Получает потоки из Keitaro и синхронизирует с БД."""
        if not campaign.keitaro_id:
            raise ValueError("Кампания не имеет keitaro_id")

        streams_data = self.api.get_campaign_streams(campaign.keitaro_id)
        keitaro_offer_ids = set()

        for stream_data in streams_data:
            stream_id = stream_data.get('id')
            stream_name = stream_data.get('name', '')
            action_type = stream_data.get('action_type', '')
            schema = stream_data.get('schema', '')
            action_payload = stream_data.get('action_payload', {})

            # Получаем или создаем поток
            flow, _ = Flow.objects.get_or_create(
                campaign=campaign,
                keitaro_id=stream_id,
                defaults={
                    'name': stream_name,
                    'flow_type': 'offer_redirect' if action_payload.get('offers') else 'country_filter',
                    'is_published': True,
                    'has_changes': False
                }
            )

            flow.name = stream_name
            flow.save()

            # Если это поток с офферами, обрабатываем офферы
            if action_payload.get('offers'):
                offers = action_payload.get('offers', [])
                
                for offer_data in offers:
                    offer_id = offer_data.get('id')
                    offer_weight = offer_data.get('weight', 1)
                    keitaro_offer_ids.add(offer_id)

                    try:
                        offer_info = self.api.get_offer(offer_id)
                        offer_name = offer_info.get('name', '') if offer_info else ''
                    except Exception as e:
                        logger.warning(f"Не удалось получить информацию об оффере {offer_id}: {e}")
                        offer_name = ''

                    offer, created = CampaignOffer.objects.get_or_create(
                        campaign=campaign,
                        offer_id=offer_id,
                        defaults={
                            'flow': flow,
                            'offer_name': offer_name,
                            'weight': offer_weight,
                            'status': 'active'
                        }
                    )

                    if not created:
                        offer.flow = flow
                        offer.weight = offer_weight
                        offer.status = 'active'
                        offer.save()

        # Помечаем как removed офферы, которых нет в Keitaro
        existing_offers = CampaignOffer.objects.filter(
            campaign=campaign,
            status='active'
        )
        
        for offer in existing_offers:
            if offer.offer_id not in keitaro_offer_ids:
                offer.status = 'removed'
                offer.save()

    def push_flow_to_keitaro(self, flow: Flow) -> None:
        """Публикует изменения потока в Keitaro."""
        if not flow.campaign.keitaro_id or not flow.keitaro_id:
            raise ValueError("Поток не имеет keitaro_id")

        active_offers = CampaignOffer.objects.filter(
            flow=flow,
            status='active'
        )

        offers_payload = []
        for offer in active_offers:
            offers_payload.append({
                'id': offer.offer_id,
                'weight': offer.weight
            })

        self.api.update_flow(
            flow.keitaro_id,
            data={
                'action_payload': {
                    'offers': offers_payload
                }
            }
        )

        flow.is_published = True
        flow.has_changes = False
        flow.save()

    def cancel_flow_changes(self, flow: Flow) -> None:
        """Отменяет неопубликованные изменения потока."""
        CampaignOffer.objects.filter(
            flow=flow,
            status='active',
            updated_at__gt=flow.updated_at
        ).delete()

        CampaignOffer.objects.filter(
            flow=flow,
            status='removed',
            updated_at__gt=flow.updated_at
        ).update(status='active')

        flow.has_changes = False
        flow.save()
        self.recalculate_weights(flow)

    def pin_offer_weight(self, offer: CampaignOffer) -> CampaignOffer:
        """Закрепляет вес оффера."""
        offer.weight_pinned = True
        offer.save()
        return offer

    def unpin_offer_weight(self, offer: CampaignOffer) -> CampaignOffer:
        """Открепляет вес оффера."""
        offer.weight_pinned = False
        offer.save()
        if offer.flow:
            self.recalculate_weights(offer.flow)
        return offer

    def search_offers(self, query: str, limit: int = 20) -> List[Dict]:
        """Поиск офферов для autocomplete."""
        return self.api.search_offers(query, limit)

    def create_flow_for_campaign(
        self,
        campaign: Campaign,
        name: str,
        flow_type: str,
        redirect_url: Optional[str] = None,
        country: Optional[str] = None,
        offer_ids: Optional[str] = None
    ) -> Flow:
        """
        Создает поток для кампании.
        
        Args:
            campaign: Кампания
            name: Название потока
            flow_type: Тип потока ('country_filter' или 'offer_redirect')
            redirect_url: URL для редиректа (для country_filter)
            country: Код страны для фильтра (для country_filter)
            offer_ids: Строка с ID офферов через запятую (для offer_redirect)
        """
        if not campaign.keitaro_id:
            raise ValueError("Кампания не имеет keitaro_id")
        
        if flow_type == 'country_filter':
            # Создаем поток с фильтром по стране
            if not redirect_url:
                raise ValueError("Для потока типа 'country_filter' требуется redirect_url")
            if not country:
                raise ValueError("Для потока типа 'country_filter' требуется country")
            
            schema = self._get_schema_for_redirect()
            action_type = self._get_action_type_for_redirect()
            
            logger.info(f"Using schema: '{schema}', action_type: '{action_type}' for flow '{name}'")
            
            # Получаем доступные фильтры для проверки формата
            try:
                available_filters = self.api.get_stream_filters()
                logger.info(f"Available filters from API: {available_filters}")
            except Exception as e:
                logger.warning(f"Could not get available filters: {e}")
            
            # Формируем фильтр по стране
            # Пробуем разные варианты формата фильтра
            country_filter = {
                'name': 'country',
                'operator': 'is',
                'value': country.upper()  # Убеждаемся, что код страны в верхнем регистре
            }
            logger.info(f"Creating filter: {country_filter}")
            
            try:
                flow_data = self.api.create_flow(
                    campaign_id=campaign.keitaro_id,
                    name=name,
                    action_type=action_type,
                    action_payload={'url': redirect_url},
                    schema=schema,
                    filters=[country_filter]
                )
                
                if flow_data and flow_data.get('id'):
                    flow = Flow.objects.create(
                        campaign=campaign,
                        keitaro_id=flow_data.get('id'),
                        name=flow_data.get('name', name),
                        flow_type='country_filter',
                        country=country,
                        redirect_url=redirect_url,
                        is_published=True,
                        has_changes=False
                    )
                    logger.info(f"Поток успешно создан: ID={flow.keitaro_id}, name={flow.name}")
                    return flow
                else:
                    # Проверяем, создался ли поток (возможно, API вернул ошибку 500, но поток создался)
                    logger.warning(f"Получен None при создании потока '{name}', проверяем, создался ли он")
                    found_flow = self._find_existing_flow(
                        campaign=campaign,
                        name=name,
                        flow_type='country_filter',
                        country=country,
                        redirect_url=redirect_url
                    )
                    if found_flow:
                        return found_flow
                    
                    raise ValueError(f"Не удалось создать поток '{name}'. API вернул ошибку или поток не был найден. Проверьте логи.")
            except Exception as e:
                logger.error(f"Ошибка при создании потока '{name}': {e}", exc_info=True)
                # Если это не ValueError, который мы уже обработали, пробрасываем дальше
                if isinstance(e, ValueError):
                    raise
                raise ValueError(f"Ошибка при создании потока: {str(e)}")
        
        elif flow_type == 'offer_redirect':
            # Создаем поток с офферами
            if not offer_ids:
                raise ValueError("Для потока типа 'offer_redirect' требуется offer_ids")
            
            # Парсим ID офферов
            try:
                offer_id_list = [int(x.strip()) for x in offer_ids.split(',') if x.strip()]
            except ValueError:
                raise ValueError("Некорректный формат ID офферов. Используйте формат: 1, 2, 3")
            
            if not offer_id_list:
                raise ValueError("Не указаны ID офферов")
            
            # Для схемы 'landings' с офферами пробуем разные варианты
            offers_payload = [{'id': oid, 'weight': 1} for oid in offer_id_list]
            flow_data = None
            last_error = None
            
            # Варианты для попытки создания потока
            attempts = [
                {'schema': 'landings', 'action_type': 'meta'},
                {'schema': 'landings', 'action_type': 'js'},
                {'schema': 'landings', 'action_type': 'http'},
                {'schema': 'action', 'action_type': 'http'},
            ]
            
            # Пробуем каждый вариант
            for attempt in attempts:
                try:
                    logger.info(f"Trying schema '{attempt['schema']}' with action_type '{attempt['action_type']}'")
                    flow_data = self.api.create_flow(
                        campaign_id=campaign.keitaro_id,
                        name=name,
                        action_type=attempt['action_type'],
                        action_payload={'offers': offers_payload},
                        schema=attempt['schema']
                    )
                    if flow_data and flow_data.get('id'):
                        logger.info(f"Successfully created flow with {attempt}")
                        break
                except Exception as e:
                    last_error = e
                    logger.warning(f"Failed with {attempt}: {e}")
                    continue
            
            # Если все попытки не удались
            if not flow_data or not flow_data.get('id'):
                error_msg = f"Не удалось создать поток после всех попыток. Последняя ошибка: {str(last_error)}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Если поток успешно создан, сохраняем его в БД
            if flow_data and flow_data.get('id'):
                    flow = Flow.objects.create(
                        campaign=campaign,
                        keitaro_id=flow_data.get('id'),
                        name=flow_data.get('name', name),
                        flow_type='offer_redirect',
                        is_published=True,
                        has_changes=False
                    )
                    
                    # Сохраняем офферы в БД
                    self._save_offers_to_db(campaign, flow, offer_id_list)
                    
                    logger.info(f"Поток с офферами успешно создан: ID={flow.keitaro_id}, name={flow.name}")
                    return flow
            else:
                # Если поток не создался, проверяем, может быть он все-таки создался (ошибка 500)
                logger.warning(f"Получен None при создании потока '{name}', проверяем, создался ли он")
                try:
                    streams = self.api.get_campaign_streams(campaign.keitaro_id)
                    logger.info(f"Найдено потоков в кампании: {len(streams)}")
                    for stream in streams:
                        stream_name = stream.get('name', '')
                        stream_id = stream.get('id')
                        action_payload = stream.get('action_payload', {})
                        stream_offers = action_payload.get('offers', [])
                        stream_offer_ids = [o.get('id') for o in stream_offers if o.get('id')]
                        
                        # Проверяем по имени или по офферам
                        if (name.lower() in stream_name.lower() or 
                            set(offer_id_list).issubset(set(stream_offer_ids))):
                            existing_flow = Flow.objects.filter(keitaro_id=stream_id).first()
                            if not existing_flow:
                                flow = Flow.objects.create(
                                    campaign=campaign,
                                    keitaro_id=stream_id,
                                    name=stream_name,
                                    flow_type='offer_redirect'
                                )
                                
                                # Сохраняем офферы
                                self._save_offers_to_db(campaign, flow, offer_id_list)
                                
                                logger.info(f"Найден созданный поток: ID={stream_id}, name={stream_name}")
                                return flow
                            else:
                                logger.info(f"Поток {stream_id} уже существует в БД")
                                return existing_flow
                except Exception as e:
                    logger.error(f"Не удалось проверить созданные потоки: {e}", exc_info=True)
                raise ValueError(f"Не удалось создать поток '{name}'. API вернул ошибку или поток не был найден. Проверьте логи.")
        
        else:
            raise ValueError(f"Неизвестный тип потока: {flow_type}")
