from app.infrastructure.jdbc_reader import JDBCReader


class BaseResourceRepository:

    def __init__(self, jdbc_reader: JDBCReader) -> None:
        self.jdbc_reader = jdbc_reader

    def get_all(self):
        raise NotImplementedError("Subclasses should implement this method.")


class FacilityResourceRepository(BaseResourceRepository):
    def get_all(self):
        query = """(SELECT f.*, jsonb_object_agg(p.code, p.id) AS supported_programs
            FROM referencedata.facilities f
            JOIN referencedata.supported_programs sp ON sp.facilityid = f.id
            JOIN referencedata.programs p ON sp.programid = p.id
            GROUP BY f.id
            ) AS facilities"""
        return self.jdbc_reader.read_data(query)


class GeographicZoneResourceRepository(BaseResourceRepository):
    def get_all(self):
        query = """(
        select z.*, l.levelnumber, z2.name as parentname
            from referencedata.geographic_zones z 
            left join referencedata.geographic_levels l
            on z.levelid = l.id
            left join referencedata.geographic_zones z2
            on z.parentid = z2.id
        ) AS geographic_zones"""

        return self.jdbc_reader.read_data(query)

    def get_levels(self):
        query = "(select * from referencedata.geographic_levels) as tmp"
        return self.jdbc_reader.read_data(query)


class FacilityTypeResourceRepository(BaseResourceRepository):
    def get_all(self):
        query = "(SELECT * FROM referencedata.facility_types) AS facility_types"
        return self.jdbc_reader.read_data(query)


class FacilityOperatorResourceRepository(BaseResourceRepository):
    def get_all(self):
        query = "(SELECT * FROM referencedata.facility_operators) AS facility_operators"
        return self.jdbc_reader.read_data(query)


class ProgramResourceRepository(BaseResourceRepository):
    def get_all(self):
        query = "(SELECT * FROM referencedata.programs) AS programs"
        return self.jdbc_reader.read_data(query)
