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
        """Добавляет оффер в кампанию без привязки к потоку."""
        # Получаем информацию об оффере
        try:
            offer_data = self.api.get_offer(offer_id)
            offer_name = offer_data.get('name', '')
        except Exception as e:
            logger.warning(f"Не удалось получить информацию об оффере {offer_id}: {e}")
            offer_name = ''

        # Сохраняем в БД без привязки к потоку (flow=None)
        # Проверяем, не был ли оффер ранее удален
        existing_offer = CampaignOffer.objects.filter(
            campaign=campaign,
            offer_id=offer_id
        ).first()
        
        if existing_offer:
            # Если оффер был удален, восстанавливаем его при добавлении
            if existing_offer.status == 'removed':
                logger.info(f"Восстанавливаем ранее удаленный оффер {offer_id} в кампании {campaign.pk}")
            
            # Обновляем оффер (восстанавливаем, если был удален)
            existing_offer.flow = None  # Оффер добавляется в кампанию, но не в поток
            existing_offer.offer_name = offer_name
            existing_offer.weight = weight
            existing_offer.status = 'active'
            existing_offer.save()
            logger.info(f"Оффер {offer_id} обновлен в кампании {campaign.pk} (ID в БД: {existing_offer.pk})")
            campaign_offer = existing_offer
        else:
            # Создаем новый оффер
            campaign_offer = CampaignOffer.objects.create(
                campaign=campaign,
                offer_id=offer_id,
                flow=None,  # Оффер добавляется в кампанию, но не в поток
                offer_name=offer_name,
                weight=weight,
                status='active'
            )
            logger.info(f"Оффер {offer_id} создан в кампании {campaign.pk} (ID в БД: {campaign_offer.pk})")
        
        # Проверяем, что оффер действительно сохранен
        saved_offer = CampaignOffer.objects.filter(
            campaign=campaign,
            offer_id=offer_id,
            status='active'
        ).first()
        if saved_offer:
            logger.info(f"Проверка: оффер {offer_id} найден в БД, статус: {saved_offer.status}, flow: {saved_offer.flow}")
        else:
            logger.error(f"ОШИБКА: оффер {offer_id} не найден в БД после сохранения!")
        
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

        # Помечаем оффер как удаленный
        offer.status = 'removed'
        offer.save()
        logger.info(f"Оффер {offer_id} помечен как removed в кампании {campaign.pk}. Flow: {offer.flow}, flow_id: {offer.flow_id if offer.flow else None}")

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

        logger.info(f"Загружаем потоки для кампании {campaign.pk} (keitaro_id={campaign.keitaro_id})")
        streams_data = self.api.get_campaign_streams(campaign.keitaro_id)
        logger.info(f"Получено {len(streams_data)} потоков из API для кампании {campaign.keitaro_id}")
        
        if not streams_data:
            logger.warning(f"API вернул пустой список потоков для кампании {campaign.keitaro_id}")
            return
        
        keitaro_offer_ids = set()

        for stream_data in streams_data:
            stream_id = stream_data.get('id')
            if not stream_id:
                logger.warning(f"Пропущен поток без ID: {stream_data}")
                continue
                
            stream_name = stream_data.get('name', '')
            action_type = stream_data.get('action_type', '')
            schema = stream_data.get('schema', '')
            
            # action_payload может быть строкой или словарем
            action_payload = stream_data.get('action_payload', {})
            if isinstance(action_payload, str):
                # Если это строка, пытаемся распарсить или используем пустой dict
                try:
                    import json
                    action_payload = json.loads(action_payload) if action_payload else {}
                except:
                    action_payload = {}
            
            # Офферы могут быть в корне потока или в action_payload
            offers_in_stream = stream_data.get('offers', [])
            offers_in_payload = action_payload.get('offers', []) if isinstance(action_payload, dict) else []
            offers = offers_in_stream or offers_in_payload
            
            # Определяем тип потока
            has_offers = bool(offers)
            flow_type = 'offer_redirect' if has_offers else 'country_filter'
            
            logger.debug(f"Обрабатываем поток: id={stream_id}, name={stream_name}, type={flow_type}, offers_count={len(offers)}")

            # Получаем или создаем поток
            flow, created = Flow.objects.get_or_create(
                campaign=campaign,
                keitaro_id=stream_id,
                defaults={
                    'name': stream_name,
                    'flow_type': flow_type,
                    'is_published': True,
                    'has_changes': False
                }
            )

            if not created:
                # Обновляем существующий поток
                flow.name = stream_name
                flow.flow_type = flow_type
                flow.save()
            else:
                logger.info(f"Создан новый поток в БД: keitaro_id={stream_id}, name={stream_name}")

            # Если это поток с офферами, обрабатываем офферы
            if offers:
                
                for offer_data in offers:
                    if not isinstance(offer_data, dict):
                        logger.warning(f"Пропущен оффер с неверным форматом: {type(offer_data)}, data={offer_data}")
                        continue
                    
                    # В API офферы могут иметь разные структуры
                    # Может быть offer_id или id
                    offer_id = offer_data.get('offer_id') or offer_data.get('id')
                    if not offer_id:
                        logger.warning(f"Пропущен оффер без ID: {offer_data}")
                        continue
                    
                    # Вес может быть в share (процент) или weight
                    offer_weight = offer_data.get('weight', 1)
                    if 'share' in offer_data:
                        # Если есть share (процент), используем его как вес
                        offer_weight = max(1, int(offer_data.get('share', 1)))
                    
                    keitaro_offer_ids.add(offer_id)

                    try:
                        offer_info = self.api.get_offer(offer_id)
                        offer_name = offer_info.get('name', '') if offer_info else ''
                    except Exception as e:
                        logger.warning(f"Не удалось получить информацию об оффере {offer_id}: {e}")
                        offer_name = ''

                    # Проверяем, не был ли оффер удален пользователем
                    existing_removed_offer = CampaignOffer.objects.filter(
                        campaign=campaign,
                        offer_id=offer_id,
                        status='removed'
                    ).first()
                    
                    if existing_removed_offer:
                        # Если оффер был удален пользователем, не восстанавливаем его автоматически
                        logger.debug(f"Оффер {offer_id} был удален пользователем, пропускаем автоматическую активацию при синхронизации")
                        continue
                    
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
                        # Обновляем существующий активный оффер
                        offer.flow = flow
                        offer.weight = offer_weight
                        offer.status = 'active'
                        offer.save()

        # Помечаем как removed офферы, которые привязаны к потокам,
        # но отсутствуют в Keitaro. Офферы без потока (flow=None) не трогаем,
        # так как они могут быть добавлены в кампанию, но еще не привязаны к потоку.
        existing_offers = CampaignOffer.objects.filter(
            campaign=campaign,
            status='active',
            flow__isnull=False  # Только офферы, привязанные к потокам
        )
        
        for offer in existing_offers:
            if offer.offer_id not in keitaro_offer_ids:
                logger.info(f"Оффер {offer.offer_id} не найден в потоках Keitaro, помечаем как removed")
                offer.status = 'removed'
                offer.save()
        
        # Важно: офферы без потока (flow=None), которые были удалены пользователем,
        # не должны восстанавливаться автоматически при синхронизации.
        # Они остаются удаленными до явного восстановления пользователем.

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

    def sync_active_campaigns_from_api(self) -> List[Campaign]:
        """
        Синхронизирует активные кампании из Keitaro API с локальной БД.
        Возвращает список активных кампаний.
        """
        try:
            # Получаем активные кампании из API (без лимита, чтобы получить все)
            api_campaigns = self.api.get_campaigns(limit=None)
            
            if not api_campaigns:
                logger.warning("API вернул пустой список кампаний")
                return []
            
            logger.info(f"Получено {len(api_campaigns)} активных кампаний из API")
            logger.debug(f"Keitaro IDs из API: {[c.get('id') for c in api_campaigns if c.get('id')]}")
            
            active_campaigns = []
            keitaro_ids_from_api = set()
            
            for api_campaign in api_campaigns:
                keitaro_id = api_campaign.get('id')
                if not keitaro_id:
                    logger.warning(f"Пропущена кампания без ID: {api_campaign}")
                    continue
                    
                keitaro_ids_from_api.add(keitaro_id)
                
                # Ищем кампанию в БД по keitaro_id
                try:
                    # Получаем domain, если он None, используем пустую строку
                    domain_value = api_campaign.get('domain') or ''
                    
                    campaign, created = Campaign.objects.get_or_create(
                        keitaro_id=keitaro_id,
                        defaults={
                            'name': api_campaign.get('name', ''),
                            'geo': api_campaign.get('parameters', {}).get('geo', '') if isinstance(api_campaign.get('parameters'), dict) else '',
                            'offer_id': 0,  # Будет обновлено при необходимости
                            'domain': domain_value,
                            'group': api_campaign.get('group', '') or '',
                            'source': api_campaign.get('source', '') or '',
                        }
                    )
                    
                    if created:
                        logger.debug(f"Создана новая кампания в БД: keitaro_id={keitaro_id}, name={campaign.name}")
                    else:
                        logger.debug(f"Найдена существующая кампания в БД: keitaro_id={keitaro_id}, name={campaign.name}")
                    
                    # Обновляем данные существующей кампании
                    if not created:
                        campaign.name = api_campaign.get('name', campaign.name)
                        if isinstance(api_campaign.get('parameters'), dict):
                            campaign.geo = api_campaign.get('parameters', {}).get('geo', campaign.geo)
                        # Обрабатываем None значения
                        domain_value = api_campaign.get('domain') or ''
                        campaign.domain = domain_value
                        campaign.group = api_campaign.get('group', '') or ''
                        campaign.source = api_campaign.get('source', '') or ''
                        campaign.save()
                    
                    active_campaigns.append(campaign)
                    logger.debug(f"Добавлена кампания в список активных: keitaro_id={keitaro_id}, name={campaign.name}")
                except Exception as e:
                    logger.error(f"Ошибка при сохранении кампании с keitaro_id={keitaro_id}, name={api_campaign.get('name', 'N/A')}: {e}", exc_info=True)
                    continue
            
            # Помечаем кампании, которых нет в API, как удаленные (но не удаляем из БД)
            # Это нужно для истории
            campaigns_not_in_api = Campaign.objects.exclude(
                keitaro_id__in=keitaro_ids_from_api
            ).exclude(keitaro_id__isnull=True)
            
            logger.info(f"Найдено {campaigns_not_in_api.count()} кампаний в БД, которых нет в активных API")
            logger.info(f"Возвращаем {len(active_campaigns)} активных кампаний с keitaro_id: {list(keitaro_ids_from_api)}")
            return active_campaigns
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Ошибка при синхронизации кампаний из API: {error_msg}", exc_info=True)
            
            # Если это ошибка авторизации, это критично - не показываем ничего
            if '401' in error_msg or 'Unauthorized' in error_msg:
                logger.error("Ошибка авторизации API. Проверьте KEITARO_API_KEY в .env файле.")
            
            # В случае ошибки возвращаем пустой список
            return []

    def get_deleted_campaigns_from_api(self) -> List[Dict]:
        """Получает удаленные кампании из Keitaro API."""
        try:
            deleted_campaigns = self.api.get_deleted_campaigns()
            logger.info(f"Получено {len(deleted_campaigns)} удаленных кампаний из API")
            return deleted_campaigns
        except Exception as e:
            logger.error(f"Ошибка при получении удаленных кампаний из API: {e}", exc_info=True)
            return []

    def delete_flow(self, flow: Flow) -> bool:
        """
        Удаляет поток из Keitaro и из локальной БД.
        
        Args:
            flow: Поток для удаления
            
        Returns:
            True если удаление успешно, False в противном случае
        """
        if not flow.keitaro_id:
            logger.warning(f"Поток {flow.pk} не имеет keitaro_id, удаляем только из БД")
            flow.delete()
            return True
        
        try:
            # Удаляем поток из Keitaro
            success = self.api.delete_flow(flow.keitaro_id)
            if success:
                logger.info(f"Поток {flow.keitaro_id} успешно удален из Keitaro")
                # Удаляем поток из БД
                flow.delete()
                logger.info(f"Поток {flow.pk} удален из БД")
                return True
            else:
                logger.error(f"Не удалось удалить поток {flow.keitaro_id} из Keitaro")
                return False
        except Exception as e:
            logger.error(f"Ошибка при удалении потока {flow.keitaro_id}: {e}", exc_info=True)
            # Даже если удаление из Keitaro не удалось, удаляем из БД
            flow.delete()
            logger.warning(f"Поток {flow.pk} удален из БД, но не из Keitaro")
            return False

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
            
            # Пробуем разные форматы action_payload и фильтров для redirect потоков
            # Судя по API, action_payload может быть строкой (URL) или словарем
            # Также пробуем разные форматы фильтров
            filter_variants = [
                # Вариант 1: стандартный формат
                [{
                    'name': 'country',
                    'mode': 'accept',
                    'payload': [country.upper()]
                }],
                # Вариант 2: с operator
                [{
                    'name': 'country',
                    'operator': 'is',
                    'value': country.upper()
                }],
                # Вариант 3: просто список значений
                [{
                    'name': 'country',
                    'payload': [country.upper()]
                }],
            ]
            
            action_payload_variants = [
                redirect_url,  # Просто URL как строка
                {'url': redirect_url},  # URL в словаре
            ]
            
            flow_data = None
            last_error = None
            
            # Пробуем все комбинации форматов
            for filter_variant in filter_variants:
                for action_payload_variant in action_payload_variants:
                    try:
                        # Для redirect потоков action_payload может быть строкой (URL)
                        if isinstance(action_payload_variant, str):
                            payload = action_payload_variant
                        elif isinstance(action_payload_variant, dict):
                            payload = action_payload_variant
                        else:
                            payload = redirect_url
                        
                        logger.info(f"Пробуем создать поток: action_payload={payload}, filters={filter_variant}")
                        flow_data = self.api.create_flow(
                            campaign_id=campaign.keitaro_id,
                            name=name,
                            action_type=action_type,
                            action_payload=payload,
                            schema=schema,
                            filters=filter_variant
                        )
                        
                        if flow_data and flow_data.get('id'):
                            logger.info(f"Поток успешно создан!")
                            break
                    except Exception as e:
                        last_error = e
                        error_msg = str(e)
                        logger.warning(f"Не удалось создать поток: {error_msg}")
                        # Если это 500 ошибка, проверяем, может быть поток создался
                        if '500' in error_msg or 'Internal Server Error' in error_msg:
                            logger.info(f"Получена ошибка 500, проверяем, создался ли поток")
                            try:
                                streams = self.api.get_campaign_streams(campaign.keitaro_id)
                                for stream in streams:
                                    stream_name = stream.get('name', '')
                                    stream_id = stream.get('id')
                                    stream_filters = stream.get('filters', [])
                                    
                                    # Проверяем по имени или по фильтрам
                                    if name.lower() in stream_name.lower():
                                        for f in stream_filters:
                                            if f.get('name') == 'country' and country.upper() in str(f.get('payload', [])):
                                                flow_data = {'id': stream_id, 'name': stream_name}
                                                logger.info(f"Найден созданный поток после ошибки 500: ID={stream_id}")
                                                break
                                    if flow_data:
                                        break
                            except Exception as check_error:
                                logger.warning(f"Не удалось проверить созданные потоки: {check_error}")
                        continue
                    if flow_data and flow_data.get('id'):
                        break
                if flow_data and flow_data.get('id'):
                    break
            
            try:
                
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
                
                # Если все попытки не удались
                error_msg = f"Не удалось создать поток '{name}' после всех попыток."
                if last_error:
                    error_msg += f" Последняя ошибка: {str(last_error)}"
                else:
                    error_msg += " API вернул ошибку или поток не был найден. Проверьте логи."
                logger.error(error_msg)
                raise ValueError(error_msg)
            except ValueError:
                # Пробрасываем ValueError как есть
                raise
            except Exception as e:
                logger.error(f"Ошибка при создании потока '{name}': {e}", exc_info=True)
                # Проверяем, может быть поток все-таки создался
                found_flow = self._find_existing_flow(
                    campaign=campaign,
                    name=name,
                    flow_type='country_filter',
                    country=country,
                    redirect_url=redirect_url
                )
                if found_flow:
                    return found_flow
                
                error_msg = f"Ошибка при создании потока: {str(e)}"
                if last_error and str(e) != str(last_error):
                    error_msg += f" (последняя ошибка: {str(last_error)})"
                raise ValueError(error_msg)
        
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
            
            # Для схемы 'landings' с офферами пробуем разные варианты формата офферов
            flow_data = None
            last_error = None
            
            # Варианты формата офферов
            offer_formats = [
                [{'id': oid, 'weight': 1} for oid in offer_id_list],  # Стандартный формат
                [{'offer_id': oid, 'weight': 1} for oid in offer_id_list],  # С offer_id
                [{'id': oid, 'share': 1} for oid in offer_id_list],  # С share вместо weight
                [{'offer_id': oid, 'share': 1} for oid in offer_id_list],  # С offer_id и share
            ]
            
            # Варианты для попытки создания потока
            # Для схемы 'landings' с офферами пробуем сначала без action_type, потом с разными типами
            attempts = [
                {'schema': 'landings', 'action_type': None},  # Без action_type
                {'schema': 'landings', 'action_type': 'meta'},
                {'schema': 'landings', 'action_type': 'js'},
                {'schema': 'landings', 'action_type': 'http'},
                {'schema': 'action', 'action_type': 'http'},
            ]
            
            # Пробуем каждый вариант схемы и формата офферов
            for offer_format in offer_formats:
                if flow_data and flow_data.get('id'):
                    break  # Уже создали поток, выходим из цикла по форматам
                for attempt in attempts:
                    try:
                        logger.info(f"Trying schema '{attempt['schema']}' with action_type '{attempt['action_type']}', offers format: {offer_format[:1] if offer_format else 'empty'}")
                        flow_data = self.api.create_flow(
                            campaign_id=campaign.keitaro_id,
                            name=name,
                            action_type=attempt['action_type'],
                            action_payload={'offers': offer_format},
                            schema=attempt['schema']
                        )
                        if flow_data and flow_data.get('id'):
                            logger.info(f"Successfully created flow with {attempt}")
                            break  # Выходим из цикла по попыткам
                    except Exception as e:
                        last_error = e
                        error_msg = str(e)
                        logger.warning(f"Failed with {attempt}: {error_msg}")
                        # Если это не 500 ошибка, которая может означать успех, продолжаем
                        if '500' not in error_msg and 'Internal Server Error' not in error_msg:
                            continue
                        # Для 500 ошибки проверяем, может быть поток создался
                        logger.info(f"Получена ошибка 500, проверяем, создался ли поток")
                        try:
                            streams = self.api.get_campaign_streams(campaign.keitaro_id)
                            for stream in streams:
                                stream_name = stream.get('name', '')
                                stream_id = stream.get('id')
                                stream_offers = stream.get('offers', [])
                                stream_offer_ids = [o.get('offer_id') or o.get('id') for o in stream_offers if isinstance(o, dict)]
                                
                                if (name.lower() in stream_name.lower() or 
                                    set(offer_id_list).issubset(set(stream_offer_ids))):
                                    flow_data = {'id': stream_id, 'name': stream_name}
                                    logger.info(f"Найден созданный поток после ошибки 500: ID={stream_id}")
                                    break  # Выходим из цикла по потокам
                            if flow_data and flow_data.get('id'):
                                break  # Выходим из цикла по попыткам, если нашли поток
                        except Exception as check_error:
                            logger.warning(f"Не удалось проверить созданные потоки: {check_error}")
                        continue
                if flow_data and flow_data.get('id'):
                    break  # Выходим из цикла по форматам, если создали поток
            
            # Если все попытки не удались
            if not flow_data or not flow_data.get('id'):
                error_msg = f"Не удалось создать поток после всех попыток."
                if last_error:
                    error_msg += f" Последняя ошибка: {str(last_error)}"
                else:
                    error_msg += " API вернул ошибку или поток не был найден. Проверьте логи."
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
