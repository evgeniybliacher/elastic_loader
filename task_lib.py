from typing import Any, Callable
from returns.result import safe
from model import InjestTask

@safe
def define_injest_task(data:str, logg: Callable[[str],Any])-> InjestTask:
    logg(f'Validate and fill injest task: {data}')
    return InjestTask.model_validate_json(data)