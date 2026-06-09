import asyncio
import threading
from typing import Coroutine


class AsyncThreadManager:
    def __init__(self):
        self._loop = None
        self._thread = None
        self._ready = threading.Event()
        self._start_loop()

    def _start_loop(self):
        def run_loop():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._ready.set()
            self._loop.run_forever()

        self._thread = threading.Thread(target=run_loop, daemon=True)
        self._thread.start()
        self._ready.wait()

    def sync_run(self, coro: Coroutine):
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()

    async def async_run(self, coro: Coroutine):
        if threading.current_thread() is self._thread:
            return await coro

        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return await asyncio.wrap_future(future)

    def stop(self):
        self._loop.call_soon_threadsafe(self._loop.stop)
