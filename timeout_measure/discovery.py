import typing

from timeout_measure.abstract_scheduler import AbstractMultiagentSearchScheduler


class DiscoveryPhaseScheduler(AbstractMultiagentSearchScheduler[int, typing.Tuple[int, int]]):
    def __init__(self, discovery_function: typing.Callable[[int], int]):
        super().__init__()
        self.lower_bound = None
        self.upper_bound = None

        self.i = 0
        self.discovery_function = discovery_function

    def has_finished(self) -> bool:
        return self.lower_bound is not None and self.upper_bound is not None

    def on_failed(self, value) -> None:
        self.upper_bound = value
        self.cancel_measurements_conditionally(lambda value: value > self.upper_bound)

    def on_success(self, value) -> None:
        self.lower_bound = value
        self.cancel_measurements_conditionally(lambda value: value < self.lower_bound)

    def _get_result(self) -> typing.Tuple[int, int]:
        return self.lower_bound, self.upper_bound

    async def get_next_value(self) -> int:
        next_val = self.discovery_function(self.i)
        self.i += 1
        return next_val
