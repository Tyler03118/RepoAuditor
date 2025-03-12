# -------------------------------------------------------------------------------
# |
# |  Copyright (c) 2024 Scientific Software Engineering Center at Georgia Tech
# |  Distributed under the MIT License.
# |
# -------------------------------------------------------------------------------
"""Contains functionality to process items in parallel and/or sequentially."""

from enum import auto, Enum
import os
from typing import Callable, cast, Optional, TypeVar

from dbrownell_Common import ExecuteTasks  # type: ignore[import-untyped]
from dbrownell_Common.Streams.DoneManager import DoneManager  # type: ignore[import-untyped]
from dbrownell_Common.Streams.StreamDecorator import StreamDecorator  # type: ignore[import-untyped]

import time
# ----------------------------------------------------------------------
class ExecutionStyle(Enum):
    """Controls the way in which a Requirement, Query, and Module can be processed."""

    Sequential = auto()
    Parallel = auto()


# ----------------------------------------------------------------------
ItemType = TypeVar("ItemType")
OutputType = TypeVar("OutputType")


def ParallelSequentialProcessor(
    items: list[ItemType],
    calculate_result_func: Callable[[ItemType], tuple[int, OutputType]],
    dm: Optional[DoneManager] = None,
    *,
    max_num_threads: Optional[int] = None,
) -> list[OutputType]:
    if dm is None:
        with DoneManager.Create(StreamDecorator(None), "", line_prefix="") as dm:
            next_time=time.time()
          #  print(f"next_time是{next_time}")
            return _Impl(
                dm,
                items,
                calculate_result_func,
                max_num_threads,
            )

    return _Impl(
        dm,
        items,
        calculate_result_func,
        max_num_threads,
    )


# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
def _Impl(
    dm: DoneManager,
    items: list[ItemType],
    calculate_result_func: Callable[[ItemType], tuple[int, OutputType]],
    max_num_threads: Optional[int],
) -> list[OutputType]:
    final_time2=time.time()
    #print(f"final_time2是{final_time2}")
    # Divide the items into those that can be run in parallel and those that must be run sequentially
    parallel: list[tuple[int, ItemType]] = []
    sequential: list[tuple[int, ItemType]] = []

    for index, item in enumerate(items):
        execution_style = item.style  # type: ignore
        print(f"  Index: {index}, Name: {item.__class__.__name__}, Style: {item.style}")
        if execution_style == ExecutionStyle.Parallel:
            print(index,item)
            parallel.append((index, item))
        elif execution_style == ExecutionStyle.Sequential:
            sequential.append((index, item))
        else:
            final_time3=time.time()
          #  print(f"final_time3是{final_time3}")
            assert False, execution_style  # pragma: no cover

    if len(parallel) == 1:
        sequential.append(parallel[0])
        parallel = []

    # Calculate the results
    results: list[Optional[OutputType]] = [None] * len(items)

    # ----------------------------------------------------------------------
    def Execute(
        results_index: int,
        item: ItemType,
    ) -> ExecuteTasks.TransformResultComplete:
        if item==None:
            print("33333333333333333333")
       # else:
        #    os.system("cls")
       # print(f"item item的内容是{item}")
        return_code, result = calculate_result_func(item)
        final_time4=time.time()
        #print(f"final_time4是{final_time4}")
        assert results[results_index] is None
        results[results_index] = result
        final_time1=time.time()
        #print(f"final_time1是{final_time1}")
        return ExecuteTasks.TransformResultComplete(None, return_code)

    # ----------------------------------------------------------------------

    if parallel:#parallel为真乃是错误的

        transform_results: list[Optional[Exception]] = ExecuteTasks.TransformTasks(
            dm,
            "Processing",
            [
                ExecuteTasks.TaskData(item.name, (results_index, item))  # type: ignore
                for results_index, item in parallel
            ],
            lambda context, status: Execute(*context),
            max_num_threads=max_num_threads,
            return_exceptions=True,
        )
        
        for transform_result in transform_results:
            #(transform_result)
            if transform_result is not None:
                print("    8989")
               # print(transform_result)
                raise transform_result#此处异常

    for sequential_index, (results_index, item) in enumerate(sequential):
        with dm.Nested(
            "Processing '{}' ({} of {})...".format(
                item.name,  # type: ignore
                sequential_index + 1 + len(parallel),
                len(items),
            ),
        ):
            Execute(results_index, item)
    final_time=time.time()
    print(f"final_time是{final_time}")
    for result in results:
        #os.system("cls")
        print(result)
        
    assert not any(result is None for result in results), results
    return cast(list[OutputType], results)
