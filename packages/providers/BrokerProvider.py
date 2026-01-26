import asyncio
import logging
from confluent_kafka import SerializingProducer, DeserializingConsumer, KafkaError
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

from packages.models import STG_RSS_NEWS_SCHEMA, DTL_RSS_NEWS_REACTIONS_SCHEMA

logger = logging.getLogger(__name__)


class BrokerProvider:
    def __init__(self, config):
        self.config = config.broker
        self._producer = None
        self._consumer = None
        self._lock = asyncio.Lock()
        self.running = False

    async def connect(self) -> None:
        async with self._lock:
            if self._producer is None:
                try:
                    schema_registry_url = self.config.schema_registry_url
                    sr_client = SchemaRegistryClient({'url': schema_registry_url})
                    avro_serializer = AvroSerializer(
                        schema_registry_client=sr_client,
                        schema_str=DTL_RSS_NEWS_REACTIONS_SCHEMA,
                        conf={'auto.register.schemas': False}
                    )
                    string_serializer = StringSerializer('utf_8')


                    conf = {
                        'bootstrap.servers': f"{self.config.host}:{self.config.port}",
                        'client.id': self.config.client_id,
                        "linger.ms": self.config.linger_ms,
                        "batch.size": self.config.batch_size,
                        "compression.type": self.config.compression_type,
                        "acks": self.config.acks,
                        'key.serializer': string_serializer,
                        'value.serializer': avro_serializer,
                    }
                    self._producer = SerializingProducer(conf)
                    logger.info("Broker avro producer launched")
                except Exception:
                    logger.exception("Failed to launch broker producer")
                    raise

            if self._consumer is None:
                try:
                    schema_registry_url = self.config.schema_registry_url
                    sr_client = SchemaRegistryClient({'url': schema_registry_url})
                    avro_deserializer = AvroDeserializer(
                        schema_registry_client=sr_client,
                        schema_str=STG_RSS_NEWS_SCHEMA
                    )
                    string_deserializer = StringDeserializer('utf_8')

                    conf = {
                        'bootstrap.servers': f"{self.config.host}:{self.config.port}",
                        'group.id': 'news-analytics-service',
                        'auto.offset.reset': 'earliest',
                        'enable.auto.commit': False,
                        'key.deserializer': string_deserializer,
                        'value.deserializer': avro_deserializer,
                    }

                    self._consumer = DeserializingConsumer(conf)
                    self._consumer.subscribe([self.config.topic_in])
                    self.running = True
                    logger.info(f"Broker avro consumer launched. Subscribed to: {self.config.topic_in}")
                except Exception:
                    logger.exception("Failed to launch broker consumer")
                    raise

    async def consume_loop(self, callback):
        """
        Бесконечный цикл чтения сообщений.
        callback - асинхронная функция, которая будет обрабатывать сообщение.
        """
        if self._consumer is None:
            raise RuntimeError("Consumer is not connected")

        logger.info("Starting consumption loop...")

        while self.running:
            try:
                loop = asyncio.get_running_loop()
                message = await loop.run_in_executor(None, self._consumer.poll, 1.0)

                if message is None:
                    continue

                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {message.error()}")
                        continue

                data = message.value()
                key = message.key()

                # Вызывается бизнес-логика (анализ сообщения)
                await callback(key, data)

                # Фиксируется смещение (commit) после успешной обработки
                await loop.run_in_executor(None, self._consumer.commit, message)

            except Exception as e:
                logger.error(f"Error in consume loop: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def produce(self, topic: str, message: dict) -> None:
        if self._producer is None:
            raise RuntimeError("Producer is not connected")

        loop = asyncio.get_running_loop()

        try:
            # Выполняется синхронный produce в экзекуторе
            await loop.run_in_executor(
                None,
                lambda: self._producer.produce(
                    topic=topic,
                    key=str(message.get("guid", "")),
                    value=message,
                    on_delivery=self._delivery_report
                )
            )

            await loop.run_in_executor(None, self._producer.poll, 0)

        except Exception:
            logger.exception("Error producing to broker")

    async def close(self) -> None:
        async with self._lock:
            self.running = False

            if self._producer is not None:
                try:
                    await asyncio.get_running_loop().run_in_executor(None, self._producer.flush, 10)
                except Exception as e:
                    logger.warning(f"Error closing producer: {e}")
                self._producer = None
                logger.info("Broker producer closed")

            if self._consumer is not None:
                try:
                    self._consumer.close()
                except Exception as e:
                    logger.warning(f"Error closing consumer: {e}")
                self._consumer = None
                logger.info("Broker consumer closed")

    def _delivery_report(self, err, msg):
        """
        Вызывается один раз для каждого сообщения при успешной доставке или ошибке.
        Выполняется внутри метода poll() или flush().
        """
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
