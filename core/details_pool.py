"""
Shared concurrency primitives for the lister/details split.

The seven target state crawlers (kerala, odisha, rajasthan, bihar, maharashtra,
karnataka, telangana) follow the same two-phase pipeline:

    Phase A (lister)   — sequential pagination over the public listing
    Phase B (details)  — per-project detail fetch, normalize, upsert, documents

Phase B is the wall-clock bottleneck and is embarrassingly parallel — every
project is independent.  This module provides a single `process_details`
helper that fans the per-project work out across a thread pool while keeping
ordering deterministic enough for checkpointing.

`process_details` returns a list of (index, result) tuples in submission
order; callers fold them back into their counters dict in that order so the
behaviour is identical to the previous sequential loop apart from the speedup.

All worker functions must be safe to call concurrently — they share the
httpx connection pool (`_get_shared_http_client`) which is thread-safe, and
the DB connection helpers acquire fresh connections per call.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Iterable, Sequence, TypeVar

from core.config import settings

T = TypeVar("T")
R = TypeVar("R")

# Default worker count.  Tuned to match what most state portals allow before
# throttling kicks in (3-6 simultaneous detail requests).  Configurable per
# call so selenium-driven states can fall back to fewer workers.
DEFAULT_WORKERS = 6


def get_detail_workers(default: int = DEFAULT_WORKERS) -> int:
    """Return the configured number of detail workers, clamped to [1, 16]."""
    n = getattr(settings, "DETAIL_WORKERS", None) or default
    try:
        n = int(n)
    except (TypeError, ValueError):
        n = default
    return max(1, min(n, 16))


def process_details(
    items: Sequence[T],
    worker_fn: Callable[[int, T], R],
    *,
    n_workers: int | None = None,
    ordered: bool = True,
    on_result: Callable[[int, R | None, Exception | None], None] | None = None,
) -> list[tuple[int, R | None, Exception | None]]:
    """
    Run `worker_fn(idx, item)` across `items` using a ThreadPoolExecutor.

    Returns a list of `(idx, result, exc)` tuples.  Exactly one of `result`
    or `exc` is non-None per entry.  When `ordered=True` (the default) the
    list is sorted by `idx` so callers can iterate it identically to a
    sequential `for` loop.  When `ordered=False` the list is returned in
    completion order, which is useful for streaming progress logs.

    `on_result`, when supplied, is invoked as `on_result(idx, result, exc)`
    immediately after each item completes — in the calling thread, serialised
    in completion order — so callers can stream live progress (e.g. update a
    dashboard counter) without waiting for the whole batch.  Exceptions raised
    by the callback are swallowed so a faulty progress hook can never break the
    pool.

    Workers are short-lived — the executor is shut down before returning.
    """
    if not items:
        return []

    workers = n_workers or get_detail_workers()
    workers = max(1, min(workers, len(items)))

    out: list[tuple[int, R | None, Exception | None]] = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="detail") as ex:
        futures = {ex.submit(worker_fn, i, item): i for i, item in enumerate(items)}
        for fut in as_completed(futures):
            idx = futures[fut]
            try:
                result = fut.result()
                entry: tuple[int, R | None, Exception | None] = (idx, result, None)
            except Exception as exc:        # noqa: BLE001 — propagate to caller
                entry = (idx, None, exc)
            out.append(entry)
            if on_result is not None:
                try:
                    on_result(*entry)
                except Exception:           # noqa: BLE001 — never let a hook break the pool
                    pass

    if ordered:
        out.sort(key=lambda t: t[0])
    return out


def iter_in_chunks(seq: Sequence[T], chunk_size: int) -> Iterable[Sequence[T]]:
    """Yield consecutive slices of `seq` no larger than `chunk_size`."""
    if chunk_size <= 0:
        yield seq
        return
    for start in range(0, len(seq), chunk_size):
        yield seq[start:start + chunk_size]


class PagePool:
    """
    Minimal pool of pre-created Selenium pages shared across detail workers.

    Each browser-driven state crawler (odisha, rajasthan, telangana) uses one
    detail page per worker thread.  Constructing pages is expensive (~300 ms)
    so we create them up-front, then acquire/release them around each call.

    Usage:
        with PagePool(browser, size=4) as pool:
            def worker(idx, row):
                with pool.acquire() as page:
                    return scrape_detail(page, row)
            process_details(rows, worker)
    """

    def __init__(self, browser: Any, size: int = 4, *, page_kwargs: dict | None = None):
        self._browser = browser
        self._size = max(1, int(size))
        self._page_kwargs = page_kwargs or {}
        self._pages: list[Any] = []
        from threading import Semaphore
        from queue import Queue
        self._available: "Queue[Any]" = Queue()
        self._sem = Semaphore(self._size)

    def __enter__(self) -> "PagePool":
        for _ in range(self._size):
            page = self._browser.new_page(**self._page_kwargs)
            self._pages.append(page)
            self._available.put(page)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        for page in self._pages:
            try:
                page.close()
            except Exception:
                pass
        self._pages.clear()

    def acquire(self):
        return _PageHandle(self)

    def _checkout(self):
        self._sem.acquire()
        return self._available.get()

    def _release(self, page) -> None:
        self._available.put(page)
        self._sem.release()


class _PageHandle:
    def __init__(self, pool: "PagePool"):
        self._pool = pool
        self._page = None

    def __enter__(self):
        self._page = self._pool._checkout()
        return self._page

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._page is not None:
            self._pool._release(self._page)
            self._page = None
