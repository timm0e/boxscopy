import asyncio
from collections import defaultdict
from dataclasses import dataclass
from functools import cached_property
from random import shuffle

from timeout_measure.abstract_scheduler import AbstractMultiagentSearchScheduler


@dataclass
class TreeNode:
    lower: int
    upper: int

    @cached_property
    def value(self) -> int:
        return self.lower + ((self.upper - self.lower) // 2)

    def left(self) -> "TreeNode":
        return TreeNode(self.lower, self.value)

    def right(self) -> "TreeNode":
        return TreeNode(self.value, self.upper)

    def is_leaf(self) -> bool:
        return self.upper - self.lower <= 1

    def bfs(self):
        """
        breadth first search of the tree
        :return:
        """
        level_queue = defaultdict(list, {0: [self]})
        level = 0

        while level in level_queue:
            queue = level_queue[level]
            shuffle(queue)
            while queue:
                node = queue.pop(0)
                yield node

                if not (left_node := node.left()).is_leaf():
                    level_queue[level + 1].append(left_node)

                if not (right_node := node.right()).is_leaf():
                    level_queue[level + 1].append(right_node)

            level += 1


class BinarySearchScheduler(AbstractMultiagentSearchScheduler[int, int]):
    def __init__(self, lower: int, upper: int):
        super().__init__()
        self.root = TreeNode(lower, upper)
        self.root_iterator = self.root.bfs()

        self.tasks_available_event = asyncio.Event()
        self.tasks_available_event.set()

    def has_finished(self) -> bool:
        return self.root.upper - self.root.lower <= 1

    def on_failed(self, value: int):
        print("Failed", value)
        # Set the upper bound to value
        assert value < self.root.upper, f"{value} > {self.root.upper}"
        root = TreeNode(self.root.lower, value)
        self.root = root
        self.root_iterator = root.bfs()
        self.tasks_available_event.set()
        self.cancel_measurements_conditionally(lambda value: value < self.root.upper)

    def on_success(self, value: int):
        print("Success", value)
        # Set the lower bound to value
        assert value > self.root.lower, f"{value} < {self.root.lower}"
        root = TreeNode(value, self.root.upper)
        self.root = root
        self.root_iterator = root.bfs()
        self.tasks_available_event.set()
        self.cancel_measurements_conditionally(lambda value: value < self.root.lower)

    async def get_next_value(self) -> int:
        while True:
            await self.tasks_available_event.wait()
            try:
                value = next(self.root_iterator).value
                if value in self.measurements:
                    continue
                return value
            except StopIteration:
                self.tasks_available_event.clear()

    def _get_result(self) -> int:
        return self.root.upper

    async def cleanup(self):
        # Prevent new tasks from starting additional measurements
        self.tasks_available_event.clear()

        await super().cleanup()
