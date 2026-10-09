"""Split synchronization task test module."""

import threading
import time
import pytest

from splitio.tasks import split_sync
from splitio.optional.loaders import asyncio


class SplitSynchronizationTests(object):
    """Split synchronization task test cases."""

    def test_normal_operation(self, mocker):
        """Test the normal operation flow: start, periodic sync, and stop."""
        synchronize_definitions = mocker.Mock()
        synchronize_definitions.return_value = None

        task = split_sync.SplitSynchronizationTask(synchronize_definitions, 0.5)
        task.start()
        time.sleep(0.7)

        assert task.is_running()
        assert synchronize_definitions.call_count >= 1

        stop_event = threading.Event()
        task.stop(stop_event)
        stop_event.wait()
        assert not task.is_running()

    def test_that_errors_dont_stop_task(self, mocker):
        """Test that if synchronize_definitions raises, the task keeps running."""
        call_count = {'value': 0}

        def synchronize_definitions():
            call_count['value'] += 1
            if call_count['value'] == 1:
                raise Exception("some exception")
            return None

        task = split_sync.SplitSynchronizationTask(synchronize_definitions, 0.5)
        task.start()
        time.sleep(1.2)

        assert task.is_running()
        assert call_count['value'] >= 2

        stop_event = threading.Event()
        task.stop(stop_event)
        stop_event.wait()
        assert not task.is_running()

    def test_is_running_before_start(self, mocker):
        """Test that is_running returns False before start is called."""
        synchronize_definitions = mocker.Mock()
        task = split_sync.SplitSynchronizationTask(synchronize_definitions, 0.5)
        assert not task.is_running()


class SplitSynchronizationAsyncTests(object):
    """Split synchronization async task test cases."""

    @pytest.mark.asyncio
    async def test_normal_operation(self, mocker):
        """Test the normal operation flow: start, periodic sync, and stop."""
        call_count = {'value': 0}

        async def synchronize_definitions():
            call_count['value'] += 1
            return None

        task = split_sync.SplitSynchronizationTaskAsync(synchronize_definitions, 0.5)
        task.start()
        await asyncio.sleep(0.7)

        assert task.is_running()
        assert call_count['value'] >= 1

        await task.stop()
        assert not task.is_running()

    @pytest.mark.asyncio
    async def test_that_errors_dont_stop_task(self, mocker):
        """Test that if synchronize_definitions raises, the task keeps running."""
        call_count = {'value': 0}

        async def synchronize_definitions():
            call_count['value'] += 1
            if call_count['value'] == 1:
                raise Exception("some exception")
            return None

        task = split_sync.SplitSynchronizationTaskAsync(synchronize_definitions, 0.5)
        task.start()
        await asyncio.sleep(1.2)

        assert task.is_running()
        assert call_count['value'] >= 2

        await task.stop()
        assert not task.is_running()

    @pytest.mark.asyncio
    async def test_is_running_before_start(self, mocker):
        """Test that is_running returns False before start is called."""
        async def synchronize_definitions():
            return None

        task = split_sync.SplitSynchronizationTaskAsync(synchronize_definitions, 0.5)
        assert not task.is_running()
