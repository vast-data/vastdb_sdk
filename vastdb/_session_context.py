from dataclasses import dataclass


@dataclass
class SessionContext:
    data_endpoints: list[str]
