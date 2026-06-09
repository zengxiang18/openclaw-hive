from dataclasses import dataclass, field
from typing import Optional, Any, List, Dict, Union
from pydantic import BaseModel


@dataclass
class Result:
    code: int = 200
    msg: str = ""
    data: Dict = field(default_factory=dict)


class EnvMakeResponse(BaseModel):
    env_id: str
    msg: str
    status_code: int


class EnvResetResponse(BaseModel):
    observation: Any
    info: dict = field(default_factory=dict)

    class Config:
        arbitrary_types_allowed = True


class EnvStepResponse(BaseModel):
    observation: Any
    reward: float
    terminated: bool
    truncated: bool
    info: dict = field(default_factory=dict)

    class Config:
        arbitrary_types_allowed = True


class EnvCloseResponse(BaseModel):
    env_id: str
    msg: str
    status_code: int

