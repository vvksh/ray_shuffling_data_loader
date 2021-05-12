import asyncio
import logging
from typing import Optional, Any, List, Dict
from collections.abc import Iterable
import collections

import ray

logger = logging.getLogger(__name__)


class Empty(Exception):
    pass


class Full(Exception):
    pass


# TODO(Clark): Update docstrings and examples.


class BatchQueue:
    """A first-in, first-out queue implementation on Ray.

    The behavior and use cases are similar to those of the asyncio.Queue class.

    Features both sync and async put and get methods.  Provides the option to
    block until space is available when calling put on a full queue,
    or to block until items are available when calling get on an empty queue.

    Optionally supports batched put and get operations to minimize
    serialization overhead.

    Args:
        maxsize (optional, int): maximum size of the queue. If zero, size is
            unbounded.
        actor_options (optional, Dict): Dictionary of options to pass into
            the QueueActor during creation. These are directly passed into
            QueueActor.options(...). This could be useful if you
            need to pass in custom resource requirements, for example.

    Examples:
        >>> q = Queue()
        >>> items = list(range(10))
        >>> for item in items:
        >>>     q.put(item)
        >>> for item in items:
        >>>     assert item == q.get()
        >>> # Create Queue with the underlying actor reserving 1 CPU.
        >>> q = Queue(actor_options={"num_cpus": 1})
    """

    def __init__(self,
                 max_concurrent_epochs: int,
                 maxsize: int = 0,
                 name: str = None,
                 actor_options: Optional[Dict] = None) -> None:
        self.max_concurrent_epochs = max_concurrent_epochs
        self.maxsize = maxsize
        actor_options = actor_options or {}
        if name is not None:
            actor_options["name"] = name
        self.actor = ray.remote(_QueueActor).options(
            **actor_options).remote(self.max_concurrent_epochs, self.maxsize)

    async def add_queue(self, epoch: int):
        return await self.actor.add_queue.remote(epoch)

    def producer_done(self, epoch: int):
        self.actor.producer_done.remote(epoch)

    def __len__(self) -> int:
        return ray.get(self.actor.size.remote())

    def size(self, queue_idx: int) -> int:
        """The size of the queue."""
        return ray.get(self.actor.qsize.remote(queue_idx))

    def qsize(self, queue_idx: int) -> int:
        """The size of the queue."""
        return self.size(queue_idx)

    def empty(self, queue_idx: int) -> bool:
        """Whether the queue is empty."""
        return ray.get(self.actor.empty.remote(queue_idx))

    def full(self, queue_idx: int) -> bool:
        """Whether the queue is full."""
        return ray.get(self.actor.full.remote(queue_idx))

    def put(self,
            queue_idx: int,
            item: Any,
            block: bool = True,
            timeout: Optional[float] = None) -> None:
        """Adds an item to the queue.

        If block is True and the queue is full, blocks until the queue is no
        longer full or until timeout.

        There is no guarantee of order if multiple producers put to the same
        full queue.

        Raises:
            Full: if the queue is full and blocking is False.
            Full: if the queue is full, blocking is True, and it timed out.
            ValueError: if timeout is negative.
        """
        if not block:
            try:
                ray.get(self.actor.put_nowait.remote(queue_idx, item))
            except asyncio.QueueFull:
                raise Full
        else:
            if timeout is not None and timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                ray.get(self.actor.put.remote(queue_idx, item, timeout))

    def put_batch(self,
                  queue_idx: int,
                  items: Iterable,
                  block: bool = True,
                  timeout: Optional[float] = None) -> None:
        """Adds an item to the queue.

        If block is True and the queue is full, blocks until the queue is no
        longer full or until timeout.

        There is no guarantee of order if multiple producers put to the same
        full queue.

        Raises:
            Full: if the queue is full and blocking is False.
            Full: if the queue is full, blocking is True, and it timed out.
            ValueError: if timeout is negative.
        """
        if not block:
            try:
                ray.get(self.actor.put_nowait_batch.remote(queue_idx, items))
            except asyncio.QueueFull:
                raise Full
        else:
            if timeout is not None and timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                ray.get(self.actor.put_batch.remote(queue_idx, items, timeout))

    async def put_async(self,
                        queue_idx: int,
                        item: Any,
                        block: bool = True,
                        timeout: Optional[float] = None) -> None:
        """Adds an item to the queue.

        If block is True and the queue is full,
        blocks until the queue is no longer full or until timeout.

        There is no guarantee of order if multiple producers put to the same
        full queue.

        Raises:
            Full: if the queue is full and blocking is False.
            Full: if the queue is full, blocking is True, and it timed out.
            ValueError: if timeout is negative.
        """
        if not block:
            try:
                await self.actor.put_nowait.remote(queue_idx, item)
            except asyncio.QueueFull:
                raise Full
        else:
            if timeout is not None and timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                await self.actor.put.remote(queue_idx, item, timeout)

    def get(self,
            queue_idx: int,
            block: bool = True,
            timeout: Optional[float] = None) -> Any:
        """Gets an item from the queue.

        If block is True and the queue is empty, blocks until the queue is no
        longer empty or until timeout.

        There is no guarantee of order if multiple consumers get from the
        same empty queue.

        Returns:
            The next item in the queue.

        Raises:
            Empty: if the queue is empty and blocking is False.
            Empty: if the queue is empty, blocking is True, and it timed out.
            ValueError: if timeout is negative.
        """
        if not block:
            try:
                return ray.get(self.actor.get_nowait.remote(queue_idx))
            except asyncio.QueueEmpty:
                raise Empty
        else:
            if timeout is not None and timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                return ray.get(self.actor.get.remote(queue_idx, timeout))

    async def get_async(self,
                        queue_idx: int,
                        block: bool = True,
                        timeout: Optional[float] = None) -> Any:
        """Gets an item from the queue.

        There is no guarantee of order if multiple consumers get from the
        same empty queue.

        Returns:
            The next item in the queue.
        Raises:
            Empty: if the queue is empty and blocking is False.
            Empty: if the queue is empty, blocking is True, and it timed out.
            ValueError: if timeout is negative.
        """
        if not block:
            try:
                return await self.actor.get_nowait.remote(queue_idx)
            except asyncio.QueueEmpty:
                raise Empty
        else:
            if timeout is not None and timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                return await self.actor.get.remote(queue_idx, timeout)

    def put_nowait(self, queue_idx: int, item: Any) -> None:
        """Equivalent to put(item, block=False).

        Raises:
            Full: if the queue is full.
        """
        return self.put(queue_idx, item, block=False)

    def put_nowait_batch(self, queue_idx: int, items: Iterable) -> None:
        """Takes in a list of items and puts them into the queue in order.

        Raises:
            Full: if the items will not fit in the queue
        """
        if not isinstance(items, Iterable):
            raise TypeError("Argument 'items' must be an Iterable")

        ray.get(self.actor.put_nowait_batch.remote(queue_idx, items))

    def get_nowait(self, queue_idx: int) -> Any:
        """Equivalent to get(block=False).

        Raises:
            Empty: if the queue is empty.
        """
        return self.get(queue_idx, block=False)

    def get_nowait_batch(self, queue_idx: int, num_items: int) -> List[Any]:
        """Gets items from the queue and returns them in a
        list in order.

        Raises:
            Empty: if the queue does not contain the desired number of items
        """
        if not isinstance(num_items, int):
            raise TypeError("Argument 'num_items' must be an int")
        if num_items < 0:
            raise ValueError("'num_items' must be nonnegative")

        return ray.get(
            self.actor.get_nowait_batch.remote(queue_idx, num_items))

    def shutdown(self, force: bool = False, grace_period_s: int = 5) -> None:
        """Terminates the underlying QueueActor.

        All of the resources reserved by the queue will be released.

        Args:
            force (bool): If True, forcefully kill the actor, causing an
                immediate failure. If False, graceful
                actor termination will be attempted first, before falling back
                to a forceful kill.
            grace_period_s (int): If force is False, how long in seconds to
                wait for graceful termination before falling back to
                forceful kill.
        """
        if self.actor:
            if force:
                ray.kill(self.actor, no_restart=True)
            else:
                done_ref = self.actor.__ray_terminate__.remote()
                done, not_done = ray.wait([done_ref], timeout=grace_period_s)
                if not_done:
                    ray.kill(self.actor, no_restart=True)
        self.actor = None


class _QueueActor:
    def __init__(self, max_queues, maxsize):
        self.max_queues = max_queues
        self.curr_epochs = collections.deque()
        self.queues = {}
        self.queue_producer_done = {}
        self.maxsize = maxsize

    async def add_queue(self, epoch: int):
        if len(self.queues) == self.max_queues:
            first_epoch = self.curr_epochs.popleft()
            # Wait until queue producer is done.
            await self.queue_producer_done[first_epoch].wait()
            del self.queue_producer_done[first_epoch]
            # Wait until trainers are done with batches.
            await asyncio.wait_for(self.queues[first_epoch].join())
            del self.queues[first_epoch]
        assert len(self.queues) <= self.max_queues
        assert len(self.queues) == len(self.curr_epochs)
        self.curr_epochs.append(epoch)
        self.queues[epoch] = asyncio.Queue(self.maxsize)
        self.queue_producer_done[epoch] = asyncio.Event()

    def producer_done(self, epoch: int):
        self.queue_producer_done[epoch].set()

    def size(self):
        return sum(q.qsize() for q in self.queues.values())

    def qsize(self, epoch: int):
        return self.queues[epoch].qsize()

    def empty(self, epoch: int):
        return self.queues[epoch].empty()

    def full(self, epoch: int):
        return self.queues[epoch].full()

    async def put(self, epoch: int, item, timeout=None):
        try:
            await asyncio.wait_for(self.queues[epoch].put(item), timeout)
        except asyncio.TimeoutError:
            raise Full

    async def put_batch(self, epoch: int, items, timeout=None):
        for item in items:
            try:
                await asyncio.wait_for(self.queues[epoch].put(item),
                                       timeout)
            except asyncio.TimeoutError:
                raise Full

    async def get(self, epoch: int, timeout=None):
        try:
            return await asyncio.wait_for(self.queues[epoch].get(),
                                          timeout)
        except asyncio.TimeoutError:
            raise Empty

    def put_nowait(self, epoch: int, item):
        self.queues[epoch].put_nowait(item)

    def put_nowait_batch(self, epoch: int, items):
        # If maxsize is 0, queue is unbounded, so no need to check size.
        if (self.maxsize > 0
                and len(items) + self.qsize(epoch) > self.maxsize):
            raise Full(f"Cannot add {len(items)} items to queue of size "
                       f"{self.qsize()} and maxsize {self.maxsize}.")
        for item in items:
            self.queues[epoch].put_nowait(item)

    def get_nowait(self, epoch: int):
        return self.queues[epoch].get_nowait()

    def get_nowait_batch(self, epoch: int, num_items):
        if num_items > self.qsize(epoch):
            raise Empty(f"Cannot get {num_items} items from queue of size "
                        f"{self.qsize()}.")
        return [self.queues[epoch].get_nowait() for _ in range(num_items)]
