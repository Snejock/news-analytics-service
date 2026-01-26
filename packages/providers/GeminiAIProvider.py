import logging
import httpx
from google import genai
from google.genai import types
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class GeminiAIProvider:
    """
    Провайдер для генерации текста через Google AI Studio
    """

    def __init__(self, config):
        proxy_url = f"socks5h://{config.proxy.user}:{config.proxy.password}@{config.proxy.host}:{config.proxy.port}"
        http_options = types.HttpOptions(async_client_args={'proxy': proxy_url})
        self.client = genai.Client(api_key=config.google_ai.api_key, http_options=http_options)

    # Отключение фильтров безопасности
        self.safety_settings = [
            types.SafetySetting(
                category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                threshold=types.HarmBlockThreshold.BLOCK_NONE
            ),
            types.SafetySetting(
                category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                threshold=types.HarmBlockThreshold.BLOCK_NONE
            ),
            types.SafetySetting(
                category=types.HarmCategory.HARM_CATEGORY_HARASSMENT,
                threshold=types.HarmBlockThreshold.BLOCK_NONE
            ),
            types.SafetySetting(
                category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                threshold=types.HarmBlockThreshold.BLOCK_NONE
            ),
        ]

    async def generate_structured_content(
        self,
        model: str,
        prompt: str,
        schema: type[BaseModel],
        system_instruction: str | None = None,
        temperature: float = 0.1
    ):
        try:
            config = types.GenerateContentConfig(
                temperature=temperature,
                top_p=0.95,
                top_k=40,
                response_mime_type='application/json',
                response_schema=schema,
                safety_settings=self.safety_settings,
                system_instruction=system_instruction
            )

            response = await self.client.aio.models.generate_content(
                model=model,
                contents=prompt,
                config=config
            )

            return response.parsed

        except Exception as e:
            logger.error(f"Error in structured generation: {e}")
            return None



    #
    # async def generate_content(self, model: str, payload: str) -> str:
    #     try:
    #         # Асинхронный запрос на генерацию контента
    #         response = await self.client.aio.models.generate_content(
    #             model=model,
    #             contents=payload,
    #         )
    #
    #         text = getattr(response, "text", None)
    #         if not text and hasattr(response, "candidates"):
    #             try:
    #                 text = "".join(p.text for p in response.candidates[0].content.parts)
    #             except (IndexError, AttributeError):
    #                 text = str(response)
    #
    #         return text or ""
    #
    #     except httpx.ProxyError:
    #         logger.exception("Proxy connection failed. Check IP/port and VPN/proxy availability")
    #         return ""
    #     except Exception:
    #         logger.exception("Unexpected error while processing message")
    #         return ""
    #
    # async def generate_structured_content(self, model: str, payload: str, schema: type[BaseModel], temperature: float = 0.0):
    #     try:
    #         response = await self.client.aio.models.generate_content(
    #             model=model,
    #             contents=payload,
    #             config={
    #                 'response_mime_type': 'application/json',
    #                 'response_schema': schema,
    #             },
    #         )
    #         return response.parsed
    #
    #     except Exception as e:
    #         logger.error(f"Error in structured generation: {e}")
    #         return None