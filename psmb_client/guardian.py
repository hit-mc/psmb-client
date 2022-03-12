import asyncio
from itertools import count
from typing import Awaitable, Callable
from psmb_client.stream import Client
from loguru import logger

class Guardian:
    def __init__(self, host, port, topic, client_id, *handlers: Callable[[bytes], Awaitable]) -> None:
        self.task: asyncio.Task | None = None
        self.client = Client(host, port, topic, client_id, *handlers, close_callback=self.close_callback)
        self.host = host
        self.port = port

    async def close_callback(self):
        logger.error("Connectiin lost, trying to reconnect.")
        self.try_connect()

    async def _try_connect(self):
        
        for i in count():
            logger.info("[{}] trying to open psmb connection to ({}, {}).",
                i + 1, str(self.host), str(self.port))
            try:
                await self.client.establish()
            except BaseException as e:
                logger.warning("Connection failed: {}", str(e))
                await asyncio.sleep(10)
                continue
            logger.info("Connection established")
            break

    def try_connect(self):
        if self.task is None or self.task.done() or self.task.cancelled():
            self.task = asyncio.create_task(self._try_connect())
        
    async def send_msg(self, msg: bytes):
        try:
            await self.client.send_msg(msg)
        except BaseException as e:
            logger.info("Cannot send msg {}, trying to establish connection.", str(
                msg, encoding='UTF-8'))
            self.try_connect()
