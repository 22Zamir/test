"""
Клиент для работы с Keitaro API.
"""
import requests
import re
from typing import Dict, List, Optional, Any
from django.conf import settings
import logging

logger = logging.getLogger(__name__)


class KeitaroAPI:
    """Клиент для взаимодействия с Keitaro API."""

    def __init__(self):
        self.api_url = settings.KEITARO_API_URL.rstrip('/') if settings.KEITARO_API_URL else ''
        self.api_key = settings.KEITARO_API_KEY
        
        if not self.api_url:
            raise ValueError("KEITARO_API_URL не установлен в settings.py")
        
        if not self.api_key:
            raise ValueError("KEITARO_API_KEY не установлен в settings.py")
        
        self.headers = {
            'Api-Key': self.api_key,
            'Content-Type': 'application/json',
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
        allow_500: bool = False
    ) -> Optional[Dict]:
        """
        Выполняет HTTP запрос к Keitaro API.
        
        Args:
            method: HTTP метод
            endpoint: Endpoint API
            data: Данные для отправки
            params: Query параметры
            allow_500: Если True, при ошибке 500 не выбрасывает исключение, а возвращает None
        """
        url = f"{self.api_url}/{endpoint.lstrip('/')}"
        
        logger.info(f"Keitaro API: {method} {url}")
        if data:
            logger.debug(f"Request data: {data}")
        
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                json=data,
                params=params,
                timeout=30
            )
            
            # Логируем статус ответа
            logger.info(f"Response status: {response.status_code}")
            
            # Логируем тело ответа для отладки
            if response.content:
                try:
                    response_json = response.json()
                    logger.debug(f"Response body: {response_json}")
                except:
                    logger.debug(f"Response text (first 500 chars): {response.text[:500]}")
            
            response.raise_for_status()
            return response.json() if response.content else None
        except requests.exceptions.RequestException as e:
            error_msg = str(e)
            if hasattr(e, 'response') and e.response is not None:
                status_code = e.response.status_code
                try:
                    error_body = e.response.json()
                    error_msg = f"{error_msg}. Response: {error_body}"
                    logger.error(f"Keitaro API error: {method} {url} - Status {status_code}")
                    logger.error(f"Error response body: {error_body}")
                except:
                    error_body = e.response.text[:1000]  # Увеличил до 1000 символов
                    error_msg = f"{error_msg}. Response: {error_body}"
                    logger.error(f"Keitaro API error: {method} {url} - Status {status_code}")
                    logger.error(f"Error response text: {error_body}")
                
                # Для ошибок 500, если allow_500=True, возвращаем None вместо исключения
                if allow_500 and status_code == 500:
                    logger.warning(f"Получена ошибка 500, но продолжаем работу (allow_500=True)")
                    return None
            else:
                logger.error(f"Keitaro API error: {method} {url} - {error_msg}")
            raise Exception(f"Keitaro API error: {error_msg}")

    def create_campaign(
        self,
        name: str,
        domain: Optional[str] = None,
        group: Optional[str] = None,
        source: Optional[str] = None,
        geo: Optional[str] = None
    ) -> Dict:
        """Создает новую кампанию в Keitaro."""
        alias = name.lower().replace(' ', '_').replace('-', '_')
        alias = re.sub(r'[^a-z0-9_]', '', alias)
        if not alias:
            import time
            alias = f"campaign_{int(time.time())}"
        
        data = {
            'name': name,
            'alias': alias,
        }
        if domain:
            data['domain'] = domain
        if group:
            data['group'] = group
        if source:
            data['source'] = source
        if geo:
            data['parameters'] = {'geo': geo}
        
        return self._make_request('POST', '/campaigns', data=data)

    def get_campaigns(self, limit: Optional[int] = None) -> List[Dict]:
        """
        Получает список всех активных кампаний.
        
        Args:
            limit: Максимальное количество кампаний для получения (None = все)
        """
        params = {}
        if limit:
            params['limit'] = limit
        
        response = self._make_request('GET', '/campaigns', params=params if params else None)
        
        if isinstance(response, list):
            logger.info(f"Получено {len(response)} кампаний из API (limit={limit})")
            return response
        elif isinstance(response, dict):
            # Некоторые API возвращают объект с данными
            if 'data' in response:
                campaigns = response['data']
                logger.info(f"Получено {len(campaigns)} кампаний из API (в объекте data)")
                return campaigns if isinstance(campaigns, list) else []
            elif 'campaigns' in response:
                campaigns = response['campaigns']
                logger.info(f"Получено {len(campaigns)} кампаний из API (в объекте campaigns)")
                return campaigns if isinstance(campaigns, list) else []
        
        logger.warning(f"Неожиданный формат ответа API: {type(response)}")
        return []

    def get_deleted_campaigns(self) -> List[Dict]:
        """Получает список удаленных кампаний."""
        response = self._make_request('GET', '/campaigns/deleted')
        return response if isinstance(response, list) else []

    def get_campaign(self, campaign_id: int) -> Dict:
        """Получает информацию о кампании."""
        return self._make_request('GET', f'/campaigns/{campaign_id}')

    def update_campaign(self, campaign_id: int, data: Dict) -> Dict:
        """Обновляет кампанию."""
        return self._make_request('PUT', f'/campaigns/{campaign_id}', data=data)

    def get_campaign_streams(self, campaign_id: int) -> List[Dict]:
        """Получает все потоки кампании."""
        logger.info(f"Запрос потоков для кампании {campaign_id}: GET /campaigns/{campaign_id}/streams")
        response = self._make_request('GET', f'/campaigns/{campaign_id}/streams')
        
        if isinstance(response, list):
            logger.info(f"Получено {len(response)} потоков для кампании {campaign_id}")
            return response
        elif isinstance(response, dict):
            # Некоторые API возвращают объект с данными
            if 'data' in response:
                streams = response['data']
                logger.info(f"Получено {len(streams)} потоков для кампании {campaign_id} (в объекте data)")
                return streams if isinstance(streams, list) else []
            elif 'streams' in response:
                streams = response['streams']
                logger.info(f"Получено {len(streams)} потоков для кампании {campaign_id} (в объекте streams)")
                return streams if isinstance(streams, list) else []
        
        logger.warning(f"Неожиданный формат ответа API для потоков кампании {campaign_id}: {type(response)}")
        return []

    def get_stream_schemas(self) -> List[Dict]:
        """Получает доступные схемы потоков."""
        response = self._make_request('GET', '/stream_schemas')
        if isinstance(response, list):
            return response
        elif isinstance(response, dict):
            return response.get('schemas', []) if 'schemas' in response else []
        return []

    def get_streams_actions(self) -> List[Dict]:
        """Получает доступные действия потоков."""
        response = self._make_request('GET', '/streams_actions')
        if isinstance(response, list):
            return response
        elif isinstance(response, dict):
            return response.get('actions', []) if 'actions' in response else []
        return []

    def get_stream_filters(self) -> List[Dict]:
        """Получает доступные фильтры потоков."""
        try:
            response = self._make_request('GET', '/stream_filters')
            if isinstance(response, list):
                return response
            elif isinstance(response, dict):
                return response.get('filters', []) if 'filters' in response else []
            return []
        except Exception as e:
            logger.warning(f"Could not get stream filters: {e}")
            # Возвращаем пустой список, если endpoint недоступен
            return []

    def create_flow(
        self,
        campaign_id: int,
        name: str,
        action_type: Optional[str],
        action_payload: Dict,
        schema: str,
        filters: Optional[List[Dict]] = None
    ) -> Optional[Dict]:
        """
        Создает поток в кампании.
        
        Args:
            campaign_id: ID кампании
            name: Название потока
            action_type: Тип действия (key из /streams_actions, например 'http', 'meta', 'js')
            action_payload: Полезная нагрузка действия (для офферов: {'offers': [{'id': ..., 'weight': ...}]})
            schema: Схема потока (value из /stream_schemas, например 'landings', 'redirect')
            filters: Список фильтров (опционально)
            
        Returns:
            Dict с данными созданного потока или None, если произошла ошибка 500
        """
        data = {
            'name': name,
            'campaign_id': campaign_id,
            'schema': schema,
        }
        
        # Для схемы 'landings' с офферами используем поле 'offers' напрямую
        if schema == 'landings' and isinstance(action_payload, dict) and 'offers' in action_payload:
            # Для landings с офферами используем поле offers напрямую
            data['offers'] = action_payload['offers']
            # action_payload может быть пустым или не передаваться
            if action_type:
                data['action_type'] = action_type
        else:
            # action_payload может быть строкой (для redirect) или словарем
            if isinstance(action_payload, str):
                # Если это строка (URL), используем её напрямую
                data['action_payload'] = action_payload
            elif isinstance(action_payload, dict):
                data['action_payload'] = action_payload
            else:
                data['action_payload'] = action_payload if action_payload else ''
            
            # action_type может быть необязательным для некоторых схем
            if action_type:
                data['action_type'] = action_type
        
        if filters:
            data['filters'] = filters
        
        logger.debug(f"Creating flow: schema={schema}, action_type={action_type}, name={name}, campaign_id={campaign_id}")
        logger.debug(f"Data: {data}, filters: {filters}, action_payload: {action_payload}")
        
        try:
            result = self._make_request('POST', '/streams', data=data)
            if result is None:
                logger.warning(f"API вернул None при создании потока, возможно ошибка 500")
            else:
                logger.info(f"Flow created successfully: ID={result.get('id')}")
            return result
        except Exception as e:
            error_str = str(e)
            # Если получили ошибку 500, пробуем с allow_500=True
            if '500' in error_str or 'Internal Server Error' in error_str:
                logger.warning(f"Получена ошибка 500 при создании потока, пробуем с allow_500=True. Ошибка: {error_str}")
                try:
                    result = self._make_request('POST', '/streams', data=data, allow_500=True)
                    logger.info(f"Повторный запрос с allow_500=True вернул: {result is not None}")
                    return result
                except Exception as retry_error:
                    logger.error(f"Повторный запрос с allow_500=True также завершился ошибкой: {retry_error}")
                    raise e  # Пробрасываем оригинальную ошибку
            logger.error(f"Ошибка при создании потока (не 500): {error_str}")
            raise

    def get_flow(self, flow_id: int) -> Dict:
        """Получает информацию о потоке."""
        return self._make_request('GET', f'/streams/{flow_id}')

    def update_flow(self, flow_id: int, data: Dict) -> Dict:
        """Обновляет поток."""
        return self._make_request('PUT', f'/streams/{flow_id}', data=data)

    def delete_flow(self, flow_id: int) -> bool:
        """Удаляет поток."""
        try:
            self._make_request('DELETE', f'/streams/{flow_id}')
            return True
        except Exception as e:
            logger.error(f"Error deleting flow: {e}")
            return False

    def get_offer(self, offer_id: int) -> Dict:
        """Получает информацию об оффере."""
        return self._make_request('GET', f'/offers/{offer_id}')

    def get_offers(self, params: Optional[Dict] = None) -> List[Dict]:
        """Получает список офферов."""
        response = self._make_request('GET', '/offers', params=params)
        return response if isinstance(response, list) else []

    def search_offers(self, query: str, limit: int = 20) -> List[Dict]:
        """Поиск офферов по запросу."""
        params = {'limit': limit}
        if query:
            params['search'] = query
        return self.get_offers(params=params)
