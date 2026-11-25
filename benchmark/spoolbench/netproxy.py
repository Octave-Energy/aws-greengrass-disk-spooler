"""
TCP proxy between the Nucleus and the broker, used to break "the internet" the way it breaks in the field.

- `up()`: bytes are forwarded both ways.
- `blackhole()`: like a dead uplink, packets vanish: established connections stay open but nothing is forwarded
  (or acknowledged) any more, and new connections are accepted but never answered (the TLS handshake times out).
  The Nucleus only notices through its MQTT keep-alive / ping timeout, and the broker through its keep-alive
  expiry, exactly as with a real outage.
- `reset()`: all connections are closed at once (a router reboot), new ones are refused while not `up()`.
"""

import asyncio
import threading
from typing import Optional, Set

from .broker import free_port

UP, BLACKHOLE, RESET = "up", "blackhole", "reset"


class NetProxy:
    def __init__(self, target_port: int, listen_port: Optional[int] = None) -> None:
        self.target_port = target_port
        self.listen_port = listen_port or free_port()
        self.mode = UP
        self.bytes_up = 0  # device -> cloud
        self.bytes_down = 0  # cloud -> device
        self._loop = asyncio.new_event_loop()
        self._server: Optional[asyncio.AbstractServer] = None
        self._writers: Set[asyncio.StreamWriter] = set()
        self._resumed: Optional[asyncio.Event] = None
        self._thread = threading.Thread(target=self._loop.run_forever, name="netproxy", daemon=True)

    # --- control, called from the orchestrator thread ----------------------------------------------------------

    def start(self) -> None:
        self._thread.start()
        asyncio.run_coroutine_threadsafe(self._start(), self._loop).result(10)

    def up(self) -> None:
        self._call(self._set_mode, UP)

    def blackhole(self) -> None:
        self._call(self._set_mode, BLACKHOLE)

    def reset(self) -> None:
        self._call(self._set_mode, RESET)

    def stop(self) -> None:
        self._call(self._close_all)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(10)

    def _call(self, fn, *args) -> None:
        async def run():
            fn(*args)
        asyncio.run_coroutine_threadsafe(run(), self._loop).result(10)

    # --- event loop side ----------------------------------------------------------------------------------------

    async def _start(self) -> None:
        self._resumed = asyncio.Event()
        self._resumed.set()
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", self.listen_port)

    def _set_mode(self, mode: str) -> None:
        self.mode = mode
        if mode == UP:
            self._resumed.set()
        else:
            self._resumed.clear()
        if mode == RESET:
            self._close_all()

    def _close_all(self) -> None:
        for writer in list(self._writers):
            writer.transport.abort()
        self._writers.clear()

    async def _handle(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter) -> None:
        self._writers.add(client_writer)
        if self.mode == RESET:
            client_writer.transport.abort()
            return
        # a connection attempted during an outage hangs, like a SYN that is never answered
        await self._resumed.wait()
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection("127.0.0.1", self.target_port)
        except OSError:
            client_writer.transport.abort()
            return
        self._writers.add(upstream_writer)
        await asyncio.gather(self._pump(client_reader, upstream_writer, True),
                             self._pump(upstream_reader, client_writer, False))

    async def _pump(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, upstream: bool) -> None:
        try:
            while True:
                # During an outage nothing is read nor forwarded: the sender's TCP buffers fill up and nothing is
                # acknowledged, as when packets are lost. When the link comes back, a connection that both ends
                # kept open resumes intact, like TCP retransmission; one that an end gave up on is closed.
                await self._resumed.wait()
                data = await reader.read(65536)
                if not data:
                    break
                await self._resumed.wait()
                if upstream:
                    self.bytes_up += len(data)
                else:
                    self.bytes_down += len(data)
                writer.write(data)
                await writer.drain()
        except (ConnectionError, asyncio.CancelledError, OSError):
            pass
        finally:
            self._writers.discard(writer)
            writer.transport.abort()
