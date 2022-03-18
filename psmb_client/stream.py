import asyncio
from http import client
from typing import Any, Awaitable, Callable

from loguru import logger
import enum
import copy

NETWORK_BYTEORDER = 'big'
ENCODING = 'ASCII'


class ClientState(enum.Enum):
    HANDSHAKING = 1
    MODE_CHOOSING_START = 2
    MODE_CHOOSING_PENDING = 3
    MSG_EXCHANGING = 4


class SubscriberOptions(enum.Enum):
    ALLOW_HISTORY = 1


class EasyWriter:

    async def _write(self, data: bytes):
        self._writer: asyncio.StreamWriter
        self._writer.write(data)
        await self._writer.drain()


class Handshaker(EasyWriter):

    _protocol_version = 2

    def __init__(self,
                 host: str | None,
                 port: int | str,
                 close_callback: Callable[[], Awaitable[Any]] | None = None, **kwargs) -> None:
        super().__init__()
        self._host = host
        self._port = port
        self._close_callback = close_callback

    async def _handshake(self):
        reader, writer = await asyncio.open_connection(self._host, self._port)
        self._reader = reader
        self._writer = writer
        await self._write(b'PSMB')
        await self._write(self._protocol_version.to_bytes(4, NETWORK_BYTEORDER, signed=False))
        await self._write(b'\x00\x00\x00\x00')
        ack = await reader.readuntil(separator=b'\0')
        if ack != b'OK\0':
            raise IOError("Network unavailable")
        await reader.readexactly(n=4)  # parameter `option`
        # handshake finished

    def available(self):
        return not self._writer.transport.is_closing()

    async def close(self):
        try:
            await self._write(b'BYE')
            self._writer.write_eof()
            self._writer.close()
            await self._writer.wait_closed()
            if self._close_callback is not None:
                await self._close_callback()
        except RuntimeError:
            pass # 可能关闭两次
        except asyncio.CancelledError:
            pass
        except OSError as e:
            logger.error(f"Connection lost due to server down? Exception: {e!r}")
        except Exception as e:
            logger.exception(f"{e!r}")


class NopSender(Handshaker):
    def __init__(self, nop_interval: float, **kwargs) -> None:
        self._nop_interval = nop_interval
        super().__init__(**kwargs)

    async def _nop(self):
        while True:
            await asyncio.sleep(self._nop_interval)
            try:
                await self._write(b'NOP')
            except asyncio.TimeoutError:
                await self.close()
                return
            except BrokenPipeError:
                await self.close()
                return
            except Exception as e:
                logger.exception(f"{e!r}")
                await self.close()
                return

    async def close(self):
        self._nop_task.cancel()
        await super().close()

    def _start_nop(self):
        self._nop_task = asyncio.create_task(self._nop())


class BaseReader(Handshaker):
    def __init__(self,
                 read_timeout: float = 25.,
                 **kwargs) -> None:
        self._read_timeout = read_timeout
        super().__init__(**kwargs)

    async def _handle_cmd(self, cmd: bytes):
        if cmd == b'NOP':
            await self._write(b'NIL')
        elif cmd == b'BYE':
            await self.close()

    async def _read(self):
        while True:
            if self._writer.is_closing():
                return
            try:
                cmd = await asyncio.wait_for(self._reader.readexactly(n=3), timeout=self._read_timeout)
            except asyncio.TimeoutError:
                logger.error("Subscriber read timeout. {}", self._read_timeout)
                await self.close()
                return
            except asyncio.IncompleteReadError:
                await self.close()
                return
            except asyncio.CancelledError:
                await self.close()
                return
            except Exception as e:
                logger.exception(f"{e!r}")
                await self.close()
                return
            await self._handle_cmd(cmd)

    def _start_read(self):
        self._read_task = asyncio.create_task(self._read())

    async def close(self):
        self._read_task.cancel()
        await super().close()


class Subscriber(BaseReader, NopSender):
    def __init__(self,
                 id_pattern: str,
                 subscriber_id: int | None,
                 *handlers: Callable[[bytes], Awaitable[Any]],
                 **kwargs) -> None:

        self._handlers = handlers
        self._subscriber_id = subscriber_id
        self._id_pattern = id_pattern
        super().__init__(**kwargs)

    async def _handle_cmd(self, cmd: bytes):
        if cmd == b'MSG':
            data_length = int.from_bytes(await self._reader.readexactly(n=8), 'big')
            msg = await self._reader.readexactly(data_length)
            handlers_coro = [x(msg) for x in self._handlers]
            await asyncio.gather(*handlers_coro)
        elif cmd == b'NOP':
            await self._write(b'NIL')
        elif cmd == b'BYE':
            await self.close()

    async def open_connection(self):
        await self._handshake()  # May raise errors

        await self._write(b'SUB')
        options = SubscriberOptions.ALLOW_HISTORY.value if self._subscriber_id is not None else 0
        await self._write(options.to_bytes(4, NETWORK_BYTEORDER, signed=False))
        await self._write(self._id_pattern.encode(encoding=ENCODING))
        await self._write(b'\0')
        if self._subscriber_id is not None:
            await self._write(self._subscriber_id.to_bytes(8, NETWORK_BYTEORDER, signed=False))

        ack = await self._reader.readuntil(b'\0')
        if ack != b'OK\0':
            raise IOError(str(ack[:-1], encoding=ENCODING))
        self._start_read()
        # self._start_nop()


class Publisher(BaseReader, NopSender):

    def __init__(self,
                 topic: str,
                 **kwargs) -> None:
        self._topic = topic
        super().__init__(**kwargs)

    async def open_connection(self):
        await self._handshake()

        await self._write(b'PUB')
        await self._write(self._topic.encode(encoding=ENCODING))
        await self._write(b'\0')

        ack = await self._reader.readuntil(b'\0')
        if ack != b'OK\0':
            raise IOError(str(ack[:-1], encoding=ENCODING))
        # self._start_nop()
        self._start_read()

    async def send_msg(self, msg: bytes):
        if not self.available():
            raise IOError("Writer closing")

        await self._write(b'MSG')
        await self._write(len(msg).to_bytes(8, NETWORK_BYTEORDER, signed=False))

        await self._write(msg)


class Client:

    def __init__(self,
                 host: str | None,
                 port: str | int,
                 topic: str,
                 client_id: int,
                 *handlers: Callable[[bytes], Awaitable],
                 close_callback: Callable[[], Awaitable[Any]] | None = None,
                 read_timeout: float = 25,
                 nop_interval: float = 15,
                 ) -> None:

        self._publisher = Publisher(host=host,
                                    port=port,
                                    topic=topic,
                                    close_callback=close_callback,
                                    nop_interval=nop_interval,
                                    read_timeout=read_timeout)
        self._subscriber = Subscriber(topic, # 直接让客户端受到的消息就是发送的topic
                                      client_id,
                                      *handlers,
                                      close_callback=close_callback,
                                      host=host,
                                      port=port,
                                      nop_interval=nop_interval,
                                      read_timeout=read_timeout)

    async def establish(self):
        await asyncio.gather(self._publisher.open_connection(), self._subscriber.open_connection())

    async def send_msg(self, msg: bytes):
        await self._publisher.send_msg(msg)

    async def close(self):
        await self._publisher.close()
        await self._subscriber.close()
