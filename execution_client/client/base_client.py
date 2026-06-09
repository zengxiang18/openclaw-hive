import abc
from typing import Any


class BaseClient(abc.ABC):
    def __init__(self, env_id: str, **kwargs):
        self._env_id = env_id

    def step(self, action):
        pass

    def reset(self, seed: int | None = None):
        pass

    def step(self, action: str) -> tuple[str, float, bool, bool, dict[str, Any]]:
        pass

    def close(self) -> None:
        pass

    def extend(self, args: dict[str, Any]) -> Any:
        pass

    def get_env_id(self):
        return self._env_id
