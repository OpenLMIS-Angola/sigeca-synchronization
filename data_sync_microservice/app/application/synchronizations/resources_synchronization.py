from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Optional, Type

from app.domain import FacilityResourceReader
from app.domain.resources.abstract import ResourceReader
from app.infrastructure import ChangeLogOperationEnum, JDBCReader

from .abstract import ResourceSynchronization

class FacilityResourceSynchronization(ResourceSynchronization):
    synchronized_resource = FacilityResourceReader
