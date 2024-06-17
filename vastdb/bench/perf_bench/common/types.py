import datetime as dt
import os
import sys
from typing import Union

PathLikeT = Union[str, bytes, os.PathLike]
DateLikeT = Union[str, dt.date]

if sys.version_info >= (3, 11):
    # noinspection PyUnresolvedReferences
    from enum import StrEnum
else:
    from enum import Enum, auto

    class StrEnum(str, Enum):
        # noinspection PyTypeChecker
        def __new__(cls, value: Union[auto, str], *args, **kwargs):
            if not isinstance(value, (str, auto)):
                raise TypeError(
                    f"Not a string/auto type: {value=!r} [type={type(value)}]"
                )
            return super().__new__(cls, value, *args, **kwargs)

        def __repr__(self):
            """Return a string representation of the enumeration member."""
            return f"<{self.__class__.__name__}.{self.name}: '{self.value}'>"

        def __str__(self):
            """Return the value of the enumeration member."""
            return str(self.value)

        @staticmethod
        def _generate_next_value_(name: str, *args, **kwargs):
            return name
