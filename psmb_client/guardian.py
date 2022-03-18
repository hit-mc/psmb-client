import asyncio
from concurrent.futures import TimeoutError
from itertools import count
import logging
from typing import Awaitable, Callable
from psmb_client.stream import Client
from loguru import logger
import threading

class Guardian:
    def __init__(self, host, port, topic, client_id, *handlers: Callable[[bytes], Awaitable], reconnect_wait: float = 10) -> None:
        self.task: asyncio.Task | None = None
        self.client = Client(host, port, topic, client_id, *handlers, close_callback=self.close_callback)
        self.host = host
        self.port = port
        self.closing = False
        self._reconnect_wait = reconnect_wait

    async def close_callback(self):
        if self.closing:
            return
        logger.error("Connectiin lost, trying to reconnect in ")
        await asyncio.sleep(self._reconnect_wait)
        self.try_connect()

    async def _try_connect(self):
        try:
            await self.client.close()
        except Exception as e:
            logger.exception(f"{e!r}")
        for i in count():
            logger.info("[{}] trying to open psmb connection to {}:{}.",
                i + 1, str(self.host), str(self.port))
            try:
                await self.client.establish()
            except Exception as e:
                logger.warning(f"Connection failed: {e!r}")
                await asyncio.sleep(self._reconnect_wait)
                continue
            logger.info("Connection established")
            break

    def try_connect(self):
        if self.closing:
            logger.warning("Guardian closing, we cancelled connection task.")
            return
        if self.task is None or self.task.done() or self.task.cancelled():
            self.task = asyncio.create_task(self._try_connect())
        
    async def send_msg(self, msg: bytes):
        if self.closing:
            raise RuntimeError("Guardian closing (or closed).")
        if self.task is not None and not self.task.done():
            await self.task
        try:
            await self.client.send_msg(msg)
        except Exception as e:
            logger.warning(f"Cannot send msg {str(msg, encoding='UTF-8')}: {e!r}, trying to establish connection.", )
            self.try_connect()


    async def close(self):
        self.closing = True
        await self.client.close()
        if self.task is not None and not self.task.done():
            try:
                self.task.cancel()
            except Exception as e:
                logger.exception(f"{e!r}")


class SyncGuardian(Guardian, threading.Thread):

    def __init__(self, host, port, topic, client_id, *handlers: Callable[[bytes], Awaitable], thread_name='SyncGuardian', establish_callback: Callable | None = None) -> None:
        Guardian.__init__(self, host, port, topic, client_id, *handlers)
        threading.Thread.__init__(self, name=thread_name)
        self.callback = establish_callback
        self.loop = asyncio.new_event_loop()

    async def _try_connect(self):
        await super()._try_connect()
        logger.info("Sync guardian calling back for connection establishment")
        if self.callback is not None:
            self.loop.call_soon_threadsafe(self.callback)
    
    def run(self):
        asyncio.set_event_loop(self.loop)
        self.task = self.loop.create_task(self._try_connect())
        try:
            self.loop.run_forever()
        finally:
            self.loop.close()

    
    def send_msg(self, msg: bytes | str, timeout: float):
        if isinstance(msg, str):
            msg = msg.encode('utf-8')
        ft = asyncio.run_coroutine_threadsafe(super().send_msg(msg), self.loop)
        try:
            ft.result(timeout)
        except TimeoutError:
            logger.info('The coroutine took too long, cancelling the task...')
            ft.cancel()
        except Exception as exc:
            logger.warning(f'The coroutine raised an exception: {exc!r}')
        
    def close(self, timeout):
        logger.info("attempting to actively close the connection")
        ft = asyncio.run_coroutine_threadsafe(super().close(), self.loop)
        try:
            ft.result(timeout)
        except TimeoutError:
            logger.info('The coroutine took too long, cancelling the task...')
            ft.cancel()
        except Exception as exc:
            logger.warning(f'The coroutine raised an exception: {exc!r}')
        self.loop.call_soon_threadsafe(self.loop.stop)