from typing import Union, List, Dict, Iterable, Any, Callable
from pydantic import BaseModel
from enum import Enum


class Status(str, Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    SKIP = "SKIP"


class Node(BaseModel):
    ID: Union[int, None] = None
    run_id: str = ""
    run_date: str
    start_time: str
    finish_time: Union[str, None] = None
    func: Callable | str
    inputs: Union[str, List[str], Dict[str, str], None]
    outputs: Union[str, List[str], Dict[str, str], None]
    name: Union[str, None] = None
    tags: Union[str, List[str], None] = None
    confirms: Union[str, List[str], None] = None
    namespace: Union[str, None] = None
    status: Status = Status.RUNNING
    detail: str | None = ""


class Pipeline(BaseModel):
    ID: Union[int, None] = None
    dag_name: str = ""
    run_id: str = ""
    run_date: str
    start_time: str
    finish_time: Union[str, None] = None
    nodes: Union[List[str], str]
    inputs: Union[str, set[str], Dict[str, str], None] = None
    outputs: Union[str, set[str], Dict[str, str], None] = None
    parameters: Union[str, set[str], Dict[str, str], None] = None
    tags: Union[str, List[str], None] = None
    namespace: Union[str, None] = None
