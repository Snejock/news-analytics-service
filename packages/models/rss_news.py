from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field

STG_RSS_NEWS_SCHEMA = """
{
    "type": "record",
    "name": "RssNews",
    "namespace": "stg",
    "fields": [
        {"name": "source_system", "type": "string"},
        {"name": "feed", "type": "string"},
        {"name": "guid", "type": "string"},
        {"name": "published", "type": "string"},
        {"name": "title", "type": "string"},
        {"name": "summary", "type": ["null", "string"], "default": null},
        {"name": "link", "type": ["null", "string"], "default": null}
    ]
}
"""

DTL_RSS_NEWS_REACTIONS_SCHEMA = """
{
    "type": "record",
    "name": "AnalyticsReport",
    "namespace": "stg",
    "fields": [
        {"name": "ticker", "type": "string"},
        {"name": "impact_score", "type": "int"},
        {"name": "confidence", "type": "float"},
        {"name": "category", "type": "string"},
        {"name": "reason", "type": "string"},
        {"name": "source_system", "type": "string"},
        {"name": "feed", "type": "string"},
        {"name": "guid", "type": "string"},
        {"name": "published", "type": "string"},
        {"name": "title", "type": "string"},
        {"name": "summary", "type": ["null", "string"], "default": null},
        {"name": "link", "type": ["null", "string"], "default": null}
    ]
}
"""


# Справочник категорий
class NewsCategory(str, Enum):
    EARNINGS    = "earnings"
    DIVIDENDS   = "dividends"
    M_AND_A     = "m_and_a"
    CAPITAL     = "capital"
    LEGAL       = "legal"
    MANAGEMENT  = "management"
    CONTRACTS   = "contracts"
    PRODUCTS    = "products"
    OPERATIONS  = "operations"
    RATINGS     = "ratings"
    MACRO       = "macro"
    OTHER       = "other"

# Базовая модель входящей новости
class RSSNews(BaseModel):
    source_system: str
    feed: str
    guid: str
    published: str
    title: str
    summary: Optional[str] = None
    link: Optional[str] = None

# Базовая модель влияния новости на конкретный тикер
class TickerReaction(BaseModel):
    ticker: str
    """Тикер компании из списка (например, SBER)."""
    impact_score: int = Field(ge=-100, le=100)
    """Оценка влияния новости на котировки от -100 (катастрофа) до +100 (прорыв)."""
    confidence: float = Field(ge=0.0, le=1.0)
    """Уверенность модели в оценке от 0.0 до 1.0."""
    category: NewsCategory
    """Категория новости."""
    reason: str
    """Краткое обоснование оценки в одно предложение."""

# Объединенная модель для отправки в брокер
class AnalyticsReport(RSSNews, TickerReaction):
    pass

class NewsAnalysis(BaseModel):
    ticker_reaction: List[TickerReaction] = Field(default_factory=list)
    """Список результатов анализа по тикерам. Пустой список, если влияния нет."""
