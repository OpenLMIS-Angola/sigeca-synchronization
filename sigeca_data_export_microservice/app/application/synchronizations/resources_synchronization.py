from app.application.synchronizations.abstract import ResourceSynchronization
from app.domain.resources import ChangeLogResourceReader, FacilityResourceReader, ProgramResourceReader, \
    GeoLevelResourceReader, GeoZoneResourceReader, LotResourceReader, OrderResourceReader, OrderLineItemResourceReader, \
    OrderableResourceReader, ProgramOrderableResourceReader, ProofOfDeliveryResourceReader, \
    ProofOfDeliveryLineItemResourceReader, RequisitionResourceReader, RequisitionLineItemResourceReader, \
    StockCardResourceReader, StockCardLineItemResourceReader, StockEventResourceReader, \
    StockEventLineItemResourceReader, CalculatedStockOnHandResourceReader, SupportedProgramResourceReader, \
    UserResourceReader


class ChangeLogResourceSynchronization(ResourceSynchronization):
    synchronized_resource = ChangeLogResourceReader


class GeoLevelResourceSynchronization(ResourceSynchronization):
    synchronized_resource = GeoLevelResourceReader


class GeoZoneResourceSynchronization(ResourceSynchronization):
    synchronized_resource = GeoZoneResourceReader


class LotResourceSynchronization(ResourceSynchronization):
    synchronized_resource = LotResourceReader


class OrderResourceSynchronization(ResourceSynchronization):
    synchronized_resource = OrderResourceReader


class OrderLineItemResourceSynchronization(ResourceSynchronization):
    synchronized_resource = OrderLineItemResourceReader


class OrderableResourceSynchronization(ResourceSynchronization):
    synchronized_resource = OrderableResourceReader


class FacilityResourceSynchronization(ResourceSynchronization):
    synchronized_resource = FacilityResourceReader


class ProgramResourceSynchronization(ResourceSynchronization):
    synchronized_resource = ProgramResourceReader


class ProgramOrderableResourceSynchronization(ResourceSynchronization):
    synchronized_resource = ProgramOrderableResourceReader


class ProofOfDeliveryResourceSynchronization(ResourceSynchronization):
    synchronized_resource = ProofOfDeliveryResourceReader


class ProofOfDeliveryLineItemResourceSynchronization(ResourceSynchronization):
    synchronized_resource = ProofOfDeliveryLineItemResourceReader


class RequisitionResourceSynchronization(ResourceSynchronization):
    synchronized_resource = RequisitionResourceReader


class RequisitionLineItemResourceSynchronization(ResourceSynchronization):
    synchronized_resource = RequisitionLineItemResourceReader


class StockCardResourceSynchronization(ResourceSynchronization):
    synchronized_resource = StockCardResourceReader


class StockCardLineItemResourceSynchronization(ResourceSynchronization):
    synchronized_resource = StockCardLineItemResourceReader


class StockEventResourceSynchronization(ResourceSynchronization):
    synchronized_resource = StockEventResourceReader


class StockEventLineItemResourceSynchronization(ResourceSynchronization):
    synchronized_resource = StockEventLineItemResourceReader


class CalculatedStockOnHandResourceSynchronization(ResourceSynchronization):
    synchronized_resource = CalculatedStockOnHandResourceReader


class SupportedProgramResourceSynchronization(ResourceSynchronization):
    synchronized_resource = SupportedProgramResourceReader


class UserResourceSynchronization(ResourceSynchronization):
    synchronized_resource = UserResourceReader


def get_full_sync_list(jdbc_reader, api_client):
    return [
        GeoLevelResourceSynchronization(jdbc_reader, api_client),
        # Has to be after GeoLevel
        GeoZoneResourceSynchronization(jdbc_reader, api_client),
        # Has to be after GeoZone
        FacilityResourceSynchronization(jdbc_reader, api_client),
        LotResourceSynchronization(jdbc_reader, api_client),
        OrderResourceSynchronization(jdbc_reader, api_client),
        # Has to be after Order
        OrderLineItemResourceSynchronization(jdbc_reader, api_client),
        OrderableResourceSynchronization(jdbc_reader, api_client),
        ProgramResourceSynchronization(jdbc_reader, api_client),
        ProgramOrderableResourceSynchronization(jdbc_reader, api_client),
        ProofOfDeliveryResourceSynchronization(jdbc_reader, api_client),
        ProofOfDeliveryLineItemResourceSynchronization(jdbc_reader, api_client),
        RequisitionResourceSynchronization(jdbc_reader, api_client),
        RequisitionLineItemResourceSynchronization(jdbc_reader, api_client),
        StockEventResourceSynchronization(jdbc_reader, api_client),
        # Has to be after Stock Event
        StockEventLineItemResourceSynchronization(jdbc_reader, api_client),
        # Has to be after Stock Event
        StockCardResourceSynchronization(jdbc_reader, api_client),
        # Has to be after Stock Card
        StockCardLineItemResourceSynchronization(jdbc_reader, api_client),
        CalculatedStockOnHandResourceSynchronization(jdbc_reader, api_client),
        SupportedProgramResourceSynchronization(jdbc_reader, api_client),
        UserResourceSynchronization(jdbc_reader, api_client),
    ]
