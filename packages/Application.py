import asyncio
import logging
import yaml
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from packages.models import AppConfig, NewsAnalysis, RSSNews, AnalyticsReport
from packages.providers import BrokerProvider, ClickhouseProvider, GeminiAIProvider
from packages.agents.gemini_analytic import SYSTEM_INSTRUCTION, WATCHLIST, build_prompt

logger = logging.getLogger(__name__)


class Application:
    def __init__(self, config_path: str = "./config/config.yml", cursor: int | None = None):
        logger.info("Initialize applications...")
        self.config = self._load_config(config_path)
        self.tz_utc = timezone.utc

        logger.debug("Initializing providers...")
        self.br_provider = BrokerProvider(config=self.config)
        self.ch_provider = ClickhouseProvider(config=self.config)
        self.gemini_provider = GeminiAIProvider(config=self.config)

        logger.info("All components have been successfully initialized")

    async def main_process(self, key: str, data: dict):
        try:
            news = RSSNews(**data)
            if not news.title and not news.summary:
                return

            logger.info(f"Analyzing news: [GUID: {key}] -> {news.title}")
            # 1. Формируем промпт
            # Можно передавать watchlist из self.config, если он там есть
            user_prompt = build_prompt(news.title, news.summary, watchlist=WATCHLIST)

            # 2. Запрос к LLM
            response = await self.gemini_provider.generate_structured_content(
                model='gemini-2.5-flash',
                prompt=user_prompt,
                schema=NewsAnalysis,
                system_instruction=SYSTEM_INSTRUCTION,
                temperature=0.1
            )

            # 3. Обработка результатов
            if response and response.ticker_reaction:
                for item in response.ticker_reaction:
                    report = AnalyticsReport(
                        **news.model_dump(),
                        **item.model_dump()
                    )

                    await self.br_provider.produce(
                        topic="dtl.rss_news_reactions",
                        message=report.model_dump()
                    )
            else:
                logger.debug(f"No reaction found for: {news.title[:30]}")

        except Exception as e:
            logger.error(f"Error processing news {key}: {e}", exc_info=True)


    async def run(self):
        """Запуск приложения: инициализация ресурсов и создание корутин"""
        try:
            await asyncio.gather(
                self.ch_provider.connect(),
                self.br_provider.connect(),
            )

            # Запускается бесконечный цикл потребления новостей
            await self.br_provider.consume_loop(self.main_process)

        except asyncio.CancelledError:
            logger.info("Application stopping...")
        finally:
            logger.info("Cleaning up resources...")
            await self.ch_provider.close()
            await self.br_provider.close()
            logger.info("Providers have been successfully closed")

    @staticmethod
    def _load_config(path: str) -> AppConfig:
        try:
            with open(path, "r") as f:
                data = yaml.safe_load(f)
                return AppConfig(**data)
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file not found: {path}")