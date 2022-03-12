import asyncio
from typing import Any, Awaitable, Callable

from loguru import logger
from .base_protocol import SubscriberOptions


class EasyWriter:

    async def _write(self, data: bytes):
        self._writer: asyncio.StreamWriter
        self._writer.write(data)
        await self._writer.drain()


class Handshaker(EasyWriter):

    _protocol_version = 1

    def __init__(self, host: str | None, port: int | str, **kwargs) -> None:
        self._host = host
        self._port = port
        self._kwargs = kwargs

    async def _handshake(self):
        reader, writer = await asyncio.open_connection(self._host, self._port, **self._kwargs)
        self._reader = reader
        self._writer = writer
        await self._write(b'PSMB')
        await self._write(self._protocol_version.to_bytes(4, 'big'))
        await self._write(b'\x00\x00\x00\x00')
        ack = await reader.readuntil(separator=b'\0')
        if ack != b'OK\0':
            raise IOError("Network unavailable")
        await reader.readexactly(n=4)  # Option int
        # handshake finished


class Subscriber(Handshaker):
    def __init__(self,
                 host: str | None,
                 port: int | str,
                 id_pattern: str,
                 subscriber_id: int | None,
                 *handlers: Callable[[bytes], Awaitable[Any]],
                 keep_alive: float = 25,
                 close_callback: Callable[[], Awaitable[Any]] | None = None,
                 **kwargs) -> None:
        super().__init__(host, port, **kwargs)
        self._id_pattern = id_pattern
        self._subscriber_id = subscriber_id
        self._handlers = handlers
        self._timeout = keep_alive
        self._close_callback = close_callback

    async def _sub(self):
        while True:
            if self._writer.is_closing():
                return
            try:
                cmd = await asyncio.wait_for(self._reader.readexactly(n=3), timeout=self._timeout)
            except asyncio.TimeoutError:
                logger.error("Subscriber read timeout. {}", self._timeout)
                await self.close()
                return
            except asyncio.IncompleteReadError:
                await self.close()
                return
            except BaseException as e:
                logger.warning("Uncaught exception {}", str(e))
                await self.close()
                return
            if cmd == b'MSG':
                data_length = int.from_bytes(await self._reader.readexactly(n=8), 'big')
                msg = await self._reader.readexactly(data_length)
                handlers_coro = [x(msg) for x in self._handlers]
                await asyncio.gather(*handlers_coro)
            elif cmd == b'NOP':
                await self._write(b'NIL')
            elif cmd == b'BYE':
                await self.close()

    async def close(self):
        self._task.cancel()
        self._writer.close()
        try:
            await asyncio.wait_for(self._writer.wait_closed(), 5)
        except asyncio.TimeoutError:
            logger.warning("Subscriber's Write stream close timeout")
        except BaseException as e:
            logger.warning("Uncaught exception {}", str(e))
        finally:
            if self._close_callback is not None:
                await self._close_callback()

    async def open_connection(self):
        await self._handshake()  # May raise errors

        await self._write(b'SUB')
        options = SubscriberOptions.ALLOW_HISTORY.value if self._subscriber_id is not None else 0
        await self._write(options.to_bytes(4, 'big'))
        await self._write(self._id_pattern.encode(encoding='UTF-8'))
        await self._write(b'\0')
        if self._subscriber_id is not None:
            await self._write(self._subscriber_id.to_bytes(8, 'big'))

        ack = await self._reader.readuntil(b'\0')
        if ack != b'OK\0':
            raise IOError(str(ack[:-1], encoding='UTF-8'))
        self._task = asyncio.create_task(self._sub())


class Publisher(Handshaker):

    def __init__(self,
                 host: str | None,
                 port: int | str,
                 topic: str,
                 keep_alive: float = 25,
                 nop_interval: float = 15,
                 close_callback: Callable[[], Awaitable[Any]] | None = None,
                 **kwargs) -> None:
        super().__init__(host, port, **kwargs)
        self._topic = topic
        self._nop_interval = nop_interval
        self._keep_alive = keep_alive
        self._close_callback = close_callback

    async def _nop(self):
        while True:
            await asyncio.sleep(self._nop_interval)
            try:
                await self._write(b'NOP')
                nil = await asyncio.wait_for(self._reader.readexactly(n=3), timeout=self._keep_alive)
                if nil != b'NIL':
                    await self.close(say_bye=False)
                    return
            except asyncio.TimeoutError:
                await self.close(say_bye=False)
                return
            except asyncio.IncompleteReadError:
                await self.close(say_bye=False)
                return
            except BrokenPipeError:
                await self.close(say_bye=False)
                return
            except BaseException as e:
                logger.warning("Uncaught exception {}", str(e))
                await self.close(say_bye=False)
                return

    async def open_connection(self):
        await self._handshake()

        await self._write(b'PUB')
        await self._write(self._topic.encode(encoding='UTF-8'))
        await self._write(b'\0')

        ack = await self._reader.readuntil(b'\0')
        if ack != b'OK\0':
            raise IOError(str(ack[:-1], encoding='UTF-8'))
        self._nop_task = asyncio.create_task(self._nop())

    async def close(self, say_bye: bool = True):
        try:
            self._nop_task.cancel()
            if say_bye:
                await self._write(b'BYE')
            self._writer.write_eof()
            self._writer.close()
            await self._writer.wait_closed()
        except OSError:
            pass
        except BaseException as e:
            logger.warning("Uncaught exception {}", e)
        if self._close_callback is not None:
            await self._close_callback()

    async def send_msg(self, msg: bytes):
        if self._writer.is_closing():
            raise IOError("Writer closing")

        await self._write(b'MSG')
        await self._write(len(msg).to_bytes(8, 'big'))

        await self._write(msg)


class Client:

    def __init__(self,
                 host: str | None,
                 port: str | int,
                 topic: str,
                 client_id: int,
                 *handlers: Callable[[bytes], Awaitable],
                 keep_alive: float = 25,
                 nop_interval: float = 15,
                 close_callback: Callable[[], Awaitable[Any]] | None = None,
                 **kwargs) -> None:
        self._publisher = Publisher(
            host, port, topic, keep_alive, nop_interval, close_callback, **kwargs)
        self._subscriber = Subscriber(host, port, topic, client_id, *handlers, keep_alive=keep_alive,
                                      close_callback=close_callback, **kwargs)

    async def establish(self):
        await asyncio.gather(self._publisher.open_connection(), self._subscriber.open_connection())

    async def send_msg(self, msg: bytes):
        await self._publisher.send_msg(msg)

    async def close(self):
        await self._publisher.close()
        await self._subscriber.close()
