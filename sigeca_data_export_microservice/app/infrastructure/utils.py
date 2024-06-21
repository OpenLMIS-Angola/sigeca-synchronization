from enum import Enum


class ChangeLogOperation(Enum):
    INSERT = "I"
    UPDATE = "U"
    DELETE = "D"
    SYNC = "S"
