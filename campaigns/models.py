from django.db import models
from django.db.models import Sum


class Campaign(models.Model):
    """Модель для хранения информации о кампаниях Keitaro."""
    keitaro_id = models.IntegerField(unique=True, null=True, blank=True)
    name = models.CharField(max_length=255)
    geo = models.CharField(max_length=10, help_text="Geo code страны (например, AU, MX, RO)")
    offer_id = models.IntegerField(help_text="ID оффера Keitaro")
    domain = models.CharField(max_length=255, blank=True)
    group = models.CharField(max_length=255, blank=True)
    source = models.CharField(max_length=255, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Кампания'
        verbose_name_plural = 'Кампании'

    def __str__(self):
        return f"{self.name} ({self.geo})"


class Flow(models.Model):
    """Модель для хранения информации о потоках кампании."""
    campaign = models.ForeignKey(Campaign, on_delete=models.CASCADE, related_name='flows')
    keitaro_id = models.IntegerField(unique=True, null=True, blank=True)
    name = models.CharField(max_length=255)
    flow_type = models.CharField(
        max_length=20,
        choices=[
            ('country_filter', 'Фильтр по стране (редирект на Google)'),
            ('offer_redirect', 'Редирект на оффер'),
        ]
    )
    country = models.CharField(max_length=10, blank=True, help_text="Код страны для фильтра")
    redirect_url = models.URLField(blank=True, help_text="URL для редиректа (Google или оффер)")
    is_published = models.BooleanField(default=True, help_text="Опубликован ли поток в Keitaro")
    has_changes = models.BooleanField(default=False, help_text="Есть ли неопубликованные изменения")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['id']
        verbose_name = 'Поток'
        verbose_name_plural = 'Потоки'

    def __str__(self):
        return f"{self.name} - {self.campaign.name}"
    
    def has_offers(self):
        """Проверяет, есть ли в потоке офферы."""
        return self.offers.filter(status='active').exists()


class CampaignOffer(models.Model):
    """Модель для связи кампании с офферами."""
    campaign = models.ForeignKey(Campaign, on_delete=models.CASCADE, related_name='campaign_offers')
    flow = models.ForeignKey(Flow, on_delete=models.CASCADE, related_name='offers', null=True, blank=True)
    offer_id = models.IntegerField(help_text="ID оффера Keitaro")
    offer_name = models.CharField(max_length=255, blank=True)
    weight = models.IntegerField(default=1, help_text="Вес оффера для ротации")
    weight_pinned = models.BooleanField(default=False, help_text="Закреплен ли вес оффера")
    status = models.CharField(
        max_length=20,
        choices=[
            ('active', 'Активен'),
            ('removed', 'Удален'),
        ],
        default='active'
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-weight', 'id']
        unique_together = ['campaign', 'offer_id']
        verbose_name = 'Оффер кампании'
        verbose_name_plural = 'Офферы кампании'

    def __str__(self):
        return f"{self.campaign.name} - Offer {self.offer_id}"
    
    def calculate_share_percent(self):
        """Вычисляет процент доли оффера."""
        if self.status == 'removed':
            return 0
        
        # Получаем все активные офферы в потоке
        active_offers = CampaignOffer.objects.filter(
            campaign=self.campaign,
            flow=self.flow,
            status='active'
        )
        
        # Сумма весов всех офферов (включая закрепленные)
        total_weight = active_offers.aggregate(
            total=Sum('weight')
        )['total'] or 0
        
        if total_weight == 0:
            return 0
        
        return round((self.weight / total_weight) * 100, 1)

