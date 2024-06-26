schema_map = {
    ("referencedata", "geographic_levels"): "",
    ("referencedata", "geographic_zones"): "",
    ("referencedata", "facilities"): "",
    ("referencedata", "lots"): "",
    ("fulfillment", "orders"): "",
    ("fulfillment", "order_line_items"): "",
    ("referencedata", "orderables"): "",
    ("referencedata", "price_changes"): "",
    ("referencedata", "programs"): "",
    ("referencedata", "program_orderables"): "",
    ("fulfillment", "proofs_of_delivery"): "",
    ("fulfillment", "proof_of_delivery_line_items"): "",
    ("requisition", "requisitions"): "",
    ("requisition", "requisition_line_items"): "",
    ("stockmanagement", "stock_cards"): "",
    ("stockmanagement", "stock_card_line_items"): "",
    ("stockmanagement", "stock_events"): "",
    ("stockmanagement", "stock_event_line_items"): "",
    ("stockmanagement", "calculated_stocks_on_hand"): "",
    ("referencedata", "supported_programs"): "",
    ("referencedata", "users"): "",
}

table_map = {
    ("referencedata", "geographic_levels"): "geographic_level",
    ("referencedata", "geographic_zones"): "geographic_zone",
    ("referencedata", "facilities"): "facility",
    ("referencedata", "lots"): "lot",
    ("fulfillment", "orders"): "order",
    ("fulfillment", "order_line_items"): "order_line",
    ("referencedata", "orderables"): "product",
    ("referencedata", "price_changes"): "price_changes",
    ("referencedata", "programs"): "program",
    ("referencedata", "program_orderables"): "program_product",
    ("fulfillment", "proofs_of_delivery"): "proof_of_delivery",
    ("fulfillment", "proof_of_delivery_line_items"): "proof_of_delivery_line",
    ("requisition", "requisitions"): "requisition",
    ("requisition", "requisition_line_items"): "requisition_line",
    ("stockmanagement", "stock_cards"): "stock_card",
    ("stockmanagement", "stock_card_line_items"): "stock_card_line",
    ("stockmanagement", "stock_events"): "stock_event",
    ("stockmanagement", "stock_event_line_items"): "stock_event_line",
    ("stockmanagement", "calculated_stocks_on_hand"): "stock_on_hand",
    ("referencedata", "supported_programs"): "supported_program",
    ("referencedata", "users"): "user",
}

row_data_map = {
    ("referencedata", "geographic_levels"): {
        "id": "id",
        "code": "code",
        "level": "levelnumber",
        "name": "name",
    },
    ("referencedata", "geographic_zones"): {
        "id": "id",
        "catchment_population": "catchmentpopulation",
        "code": "code",
        "latitude": "latitude",
        "longitude": "longitude",
        "name": "name",
        "level_id": "levelid",
        "parent_id": "parentid",
    },
    ("referencedata", "facilities"): {
        "id": "id",
        "active": "active",
        "code": "code",
        "comment": "comment",
        "description": "description",
        "enabled": "enabled",
        "name": "name",
        "geographic_zone_id": "geographiczoneid",
    },
    ("referencedata", "lots"): {
        "id": "id",
        "active": "active",
        "code": "lotcode",
        "expiration_date": "expirationdate",
        "manufacture_date": "manufacturedate",
    },
    ("fulfillment", "orders"): {
        "id": "id",
        "created_by_id": "createdbyid",
        "created_date": "createddate",
        "emergency": "emergency",
        "facility_id": "facilityid",
        "order_code": "ordercode",
        "program_id": "programid",
        "quoted_cost": "quotedcost",
        "receiving_facility_id": "receivingfacilityid",
        "requesting_facility_id": "requestingfacilityid",
        "status": "status",
        "supplying_facility_id": "supplyingfacilityid",
        "last_updated_date": "lastupdateddate",
        "last_updater_id": "lastupdaterid",
    },
    ("fulfillment", "order_line_items"): {
        "id": "id",
        "order_id": "orderid",
        "product_id": "orderableid",
        "ordered_quantity": "orderedquantity",
        "product_version_number": "orderableversionnumber",
    },
    ("referencedata", "orderables"): {
        "id": "id",
        "version_number": "versionnumber",
        "code": "code",
        "name": "fullproductname",
        "description": "description",
        "pack_rounding_threshold": "packroundingthreshold",
        "net_content": "netcontent",
        "round_to_zero": "roundtozero",
    },
    ("referencedata", "price_changes"): {
        "id": "id",
        "program_product_id": "programorderableid",
        "price": "price",
        "author_id": "authorid",
        "occurred_date": "occurreddate"
    },
    ("referencedata", "programs"): {
        "id": "id",
        "code": "code",
        "name": "name",
        "active": "active",
        "description": "description",
        "period_sskippable": "periodsskippable",
        "skipauthorization": "skipauthorization",
        "shown_on_full_supply_tab": "shownonfullsupplytab",
        "enable_date_physical_stock_count_completed": "enabledatephysicalstockcountcompleted"
    },
    ("referencedata", "program_orderables"): {
        "id": "id",
        "active": "active",
        "doses_per_patient": "dosesperpatient",
        "program_id": "programid",
        "product_id": "orderableid",
        "product_version_number": "orderableversionnumber",
        "price_per_pack": "priceperpack",
    },
    ("fulfillment", "proofs_of_delivery"): {
        "id": "id",
        "status": "status",
        "delivered_by": "deliveredby",
        "received_by": "receivedby",
        "received_date": "receiveddate",
    },
    ("fulfillment", "proof_of_delivery_line_items"): {
        "id": "id",
        "proof_of_delivery_id": "proofofdeliveryid",
        "notes": "notes",
        "quantity_accepted": "quantityaccepted",
        "quantity_rejected": "quantityrejected",
        "product_id": "orderableid",
        "lot_id": "lotid",
        "vvm_status": "vvmstatus",
        "use_vvm": "usevvm",
        "rejection_reason_id": "rejectionreasonid",
        "product_version_number": "orderableversionnumber",
    },
    ("requisition", "requisitions"): {
        "id": "id",
        "created_date": "createddate",
        "modified_date": "modifieddate",
        "emergency": "emergency",
        "facility_id": "facilityid",
        "months_in_period": "numberofmonthsinperiod",
        "program_id": "programid",
        "status": "status",
        "supplying_facility_id": "supplyingfacilityid",
        "stock_count_date": "datephysicalstockcountcompleted",
        "report_only": "reportonly"
    },
    ("requisition", "requisition_line_items"): {
        "id": "id",
        "adjusted_consumption": "adjustedconsumption",
        "approved_quantity": "approvedquantity",
        "average_consumption": "averageconsumption",
        "beginning_balance": "beginningbalance",
        "calculated_order_quantity": "calculatedorderquantity",
        "max_periods_of_stock": "maxperiodsofstock",
        "max_stock_quantity": "maximumstockquantity",
        "non_full_supply": "nonfullsupply",
        "new_patients_added": "numberofnewpatientsadded",
        "product_id": "orderableid",
        "packs_to_ship": "packstoship",
        "price_per_pack": "priceperpack",
        "requested_quantity": "requestedquantity",
        "requested_quantity_explanation": "requestedquantityexplanation",
        "skipped": "skipped",
        "stock_on_hand": "stockonhand",
        "total": "total",
        "total_consumed_quantity": "totalconsumedquantity",
        "total_cost": "totalcost",
        "total_losses_and_adjustments": "totallossesandadjustments",
        "total_received_quantity": "totalreceivedquantity",
        "total_stockout_days": "totalstockoutdays",
        "requisition_id": "requisitionid",
        "ideal_stock_amount": "idealstockamount",
        "calculated_order_quantity_isa": "calculatedorderquantityisa",
        "additional_quantity_required": "additionalquantityrequired",
        "product_version_number": "orderableversionnumber",
        "facility_type_approved_product_id": "facilitytypeapprovedproductid",
        "facility_type_approved_product_version_number": "facilitytypeapprovedproductversionnumber",
        "patients_on_treatment_next_month": "numberofpatientsontreatmentnextmonth",
        "total_requirement": "totalrequirement",
        "total_quantity_needed_by_hf": "totalquantityneededbyhf",
        "quantity_to_issue": "quantitytoissue",
        "converted_quantity_to_issue": "convertedquantitytoissue",
    },
    ("stockmanagement", "stock_cards"): {
        "id": "id",
        "facility_id": "facilityid",
        "lot_id": "lotid",
        "product_id": "orderableid",
        "program_id": "programid",
        "origin_event_id": "origineventid",
        "is_showed": "isshowed",
        "is_active": "isactive",
    },
    ("stockmanagement", "stock_card_line_items"): {
        "id": "id",
        "destination_freetext": "destinationfreetext",
        "document_number": "documentnumber",
        "occurred_date": "occurreddate",
        "processed_date": "processeddate",
        "quantity": "quantity",
        "reason_freetext": "reasonfreetext",
        "signature": "signature",
        "source_freetext": "sourcefreetext",
        "user_id": "userid",
        "origin_event_id": "origineventid",
        "stock_card_id": "stockcardid",
    },
    ("stockmanagement", "stock_events"): {
        "id": "id",
        "document_number": "documentnumber",
        "facility_id": "facilityid",
        "processed_date": "processeddate",
        "program_id": "programid",
        "signature": "signature",
        "user_id": "userid",
        "is_showed": "isshowed",
        "is_active": "isactive",
    },
    ("stockmanagement", "stock_event_line_items"): {
        "id": "id",
        "destination_freetext": "destinationfreetext",
        "destination_id": "destinationid",
        "lot_id": "lotid",
        "occurred_date": "occurreddate",
        "product_id": "orderableid",
        "quantity": "quantity",
        "reason_freetext": "reasonfreetext",
        "reason_id": "reasonid",
        "source_freetext": "sourcefreetext",
        "source_id": "sourceid",
        "stock_event_id": "stockeventid",
    },
    ("stockmanagement", "calculated_stocks_on_hand"): {
        "id": "id",
        'stock_on_hand': 'stockonhand',
        'occurred_date': 'occurreddate',
        'stock_card_id': 'stockcardid',
        'processed_date': 'processeddate'

    },
    ("referencedata", "supported_programs"): {
        "active": "active",
        "start_date": "startdate",
        "facility_id": "facilityid",
        "program_id": "programid",
        "locally_fulfilled": "locallyfulfilled",

    },
    ("referencedata", "users"): {
        "id": "id",
        "active": "active",
        "first_name": "firstname",
        "last_name": "lastname",
        "timezone": "timezone",
        "username": "username",
        "verified": "verified",
        "home_facility_id": "homefacilityid",
        "job_title": "jobtitle",
        "phone_number": "phonenumber",
    },
}


def map_data(data: dict, schema_name: str, table_name: str):
    new_data = {}
    for key, value in row_data_map[(schema_name, table_name)].items():
        new_data[key] = data[value]
    return new_data
