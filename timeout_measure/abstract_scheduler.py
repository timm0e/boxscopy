import asyncio
import typing
from abc import ABCMeta, abstractmethod
from asyncio import Task
from dataclasses import dataclass
from typing import Dict


@dataclass
class Measurement:
    measurement_task: Task
    supervisor_task: Task


class TemporarilyOutOfValues(Exception):
    pass


class NotFinished(Exception):
    pass


class MeasurementCancelled(Exception):
    def __init__(self, cancellation_exception: asyncio.CancelledError):
        self.cancellation_exception = cancellation_exception


MeasurementValueType = typing.TypeVar("MeasurementValueType")
ResultType = typing.TypeVar("ResultType")


class AbstractMultiagentSearchScheduler(
    typing.Generic[MeasurementValueType, ResultType], metaclass=ABCMeta
):
    def __init__(self):
        self.measurements: Dict[MeasurementValueType, Measurement] = dict()
        self.finished = asyncio.Event()
        self.task_queue: typing.Set[Task] = set()
        self.task_queue_lock = asyncio.Lock()

    @abstractmethod
    def has_finished(self) -> bool:
        pass

    async def report_failed(self, value) -> None:
        self.on_failed(value)
        await self.check_finished()

    @abstractmethod
    def on_failed(self, value) -> None:
        pass

    async def report_success(self, value) -> None:
        self.on_success(value)
        await self.check_finished()

    @abstractmethod
    def on_success(self, value) -> None:
        pass

    async def cleanup(self):
        # Kill all tasks waiting for a value
        async with self.task_queue_lock:
            for task in self.task_queue:
                task.cancel()

        # Kill all measurements and by extension their supervisors
        for _, measurement in self.measurements.items():
            measurement.measurement_task.cancel()

    async def on_finished(self):
        await self.cleanup()
        self.finished.set()

    @abstractmethod
    def _get_result(self) -> ResultType:
        pass

    def get_result(self) -> ResultType:
        if not self.has_finished():
            raise NotFinished()
        return self._get_result()

    async def wait_for_result(self) -> ResultType:
        await self.finished.wait()
        return self.get_result()

    async def check_finished(self):
        if self.has_finished():
            await self.on_finished()

    def cancel_measurements_conditionally(
        self, value_condition: typing.Callable[[MeasurementValueType], bool]
    ):
        for value, measurement in self.measurements.items():
            if value_condition(value):
                measurement.measurement_task.cancel()

    @abstractmethod
    async def get_next_value(self) -> MeasurementValueType:
        pass

    async def supervise_measurement(
        self, value: MeasurementValueType, measurement_task: Task[bool]
    ):
        """
        Supervise the measurement task and report success or failure

        This is done as a separate task, since by only cancelling the supervisor task, the measurement can be
        transferred to a new scheduler by creating new supervisors in the new scheduler
        """
        result = await measurement_task
        if result:
            await self.report_success(value)
        else:
            await self.report_failed(value)

    async def create_measurement(
        self,
        measurement_fun: typing.Callable[
            [MeasurementValueType], typing.Awaitable[bool]
        ],
    ):
        value = await self.get_next_value()

        measurement_task = asyncio.create_task(measurement_fun(value))
        supervisor_task = asyncio.create_task(
            self.supervise_measurement(value, measurement_task)
        )
        measurement = Measurement(measurement_task, supervisor_task)
        self.measurements[value] = measurement
        return measurement

    async def evaluate(
        self, measurement_fun: typing.Callable[[int], typing.Awaitable[bool]]
    ):
        async with self.task_queue_lock:
            measurement_task = asyncio.create_task(
                self.create_measurement(measurement_fun)
            )
            self.task_queue.add(measurement_task)
        measurement = await measurement_task
        await measurement.measurement_task
