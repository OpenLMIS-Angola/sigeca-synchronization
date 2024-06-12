from app.domain import FacilityResourceReader

from .abstract import ResourceSynchronization


class FacilityResourceSynchronization(ResourceSynchronization):
    synchronized_resource = FacilityResourceReader
