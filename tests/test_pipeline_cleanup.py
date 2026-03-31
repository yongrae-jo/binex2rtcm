from __future__ import annotations

import asyncio
import unittest

from binex2rtcm.pipeline import _run_cancellation_safe_cleanup


class CancellationSafeCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_cleanup_runs_with_cancellation_temporarily_cleared(self) -> None:
        events: list[tuple[str, int]] = []

        async def worker() -> None:
            cancelled_error: asyncio.CancelledError | None = None
            try:
                await asyncio.sleep(60.0)
            except asyncio.CancelledError as exc:
                cancelled_error = exc

            async def cleanup() -> None:
                task = asyncio.current_task()
                assert task is not None
                events.append(("during-start", task.cancelling()))
                await asyncio.sleep(0)
                events.append(("during-end", task.cancelling()))

            task = asyncio.current_task()
            assert task is not None
            await _run_cancellation_safe_cleanup(cleanup)
            events.append(("after", task.cancelling()))

            if cancelled_error is not None:
                raise cancelled_error

        task = asyncio.create_task(worker())
        await asyncio.sleep(0)
        task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(
            events,
            [
                ("during-start", 0),
                ("during-end", 0),
                ("after", 1),
            ],
        )

    async def test_cleanup_survives_additional_cancellation(self) -> None:
        events: list[tuple[str, int]] = []
        cleanup_started = asyncio.Event()
        allow_cleanup_finish = asyncio.Event()

        async def worker() -> None:
            cancelled_error: asyncio.CancelledError | None = None
            try:
                await asyncio.sleep(60.0)
            except asyncio.CancelledError as exc:
                cancelled_error = exc

            async def cleanup() -> None:
                task = asyncio.current_task()
                assert task is not None
                events.append(("during-start", task.cancelling()))
                cleanup_started.set()
                await allow_cleanup_finish.wait()
                events.append(("during-end", task.cancelling()))

            task = asyncio.current_task()
            assert task is not None
            await _run_cancellation_safe_cleanup(cleanup)
            events.append(("after", task.cancelling()))

            if cancelled_error is not None:
                raise cancelled_error

        task = asyncio.create_task(worker())
        await asyncio.sleep(0)
        task.cancel()
        await cleanup_started.wait()
        task.cancel()
        allow_cleanup_finish.set()

        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(
            events,
            [
                ("during-start", 0),
                ("during-end", 0),
                ("after", 2),
            ],
        )
