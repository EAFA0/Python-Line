import time
import asyncio
from collections import namedtuple
from typing import Callable, Awaitable, AsyncIterable


class Line:

    def __init__(self, size: int = 100):
        self.size = size

        self.end = asyncio.Lock()
        self.iter_lock = asyncio.Lock()

        # running status
        self.running = 0
        self.iter_num = 0
        self.finished = 0
        self.data_iterable = True

        # time status
        self.end_time = 0.0
        self.start_time = 0.0
        self.is_running = False

    @property
    def cost(self):
        '''
        Record the time spent.
        '''
        if self.is_running:
            return time.time() - self.start_time
        else:
            return self.end_time - self.start_time

    async def _next(self):
        try:
            async with self.iter_lock:
                _data = await self.data_iter.__anext__()
                self.iter_num += 1
        except StopAsyncIteration:
            self.data_iterable = False
            # check and stop the line
            return self._continue()
        else:
            args, kwargs = self.converter(_data)

            # for callback
            asyncio.create_task(
                self.iter_func(*args, line=self, **kwargs)
            ).add_done_callback(self.task_finished)

            self.running += 1

    def _continue(self):
        if self.is_running:
            if self.data_iterable:
                return True
            else:
                if self.finished == self.iter_num:
                    return self.stop()
                else:
                    return True
        else:
            return False

    def stop(self):
        self.end.release()

        self.is_running = False
        self.end_time = time.time()

        return self.is_running

    # need to deal with the result
    def task_finished(self, result):    
        self.running -= 1
        self.finished += 1

        if self.wind_up is not None:
            self.wind_up(result)

        if self._continue():
            asyncio.create_task(self._next())

    def set_generator(self,
                      _iter: AsyncIterable,
                      _func: Awaitable,

                      _converter: Callable = None,
                      _wind_up_func: Callable = None):
        '''
        _iter: Producer
        _func: Consumer

        _converter: Transform producer data into a form acceptable to consumers.
        _wind_up_func: Handle consumer results.
        '''
        self.data_iter = _iter.__aiter__()
        self.iter_func = _func
        self.wind_up = _wind_up_func

        if _converter is None:
            self.converter = lambda x: [[x], {}]
        else:
            self.converter = _converter

    async def run_until_complete(self):
        self.start_time = time.time()
        # for blocking function
        await self.end.acquire()

        self.is_running = True
        for _ in range(self.size):
            await self._next()

        # for blocking function
        async with self.end:
            return
