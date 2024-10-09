from app.infrastructure.jdbc_reader import JDBCReader


class BaseResourceRepository:

    def __init__(self, jdbc_reader: JDBCReader) -> None:
        self.jdbc_reader = jdbc_reader

    def get_all(self):
        raise NotImplementedError("Subclasses should implement this method.")


class FacilityResourceRepository(BaseResourceRepository):
    def get_all(self):
        # Geo zone parent is taken because by API endpoint assigns the geo location to ward
        query = """(SELECT f.id, f.name,f.code,f.active,f.enabled,f.typeid,gz2.id as"geographiczoneid",
            (CASE 
                WHEN count(p.code) = 0 THEN '{}'::jsonb
                ELSE jsonb_object_agg(
                        coalesce(p.code, 'undefined'), 
                        jsonb_build_object(
                            'id', p.id,
                            'supportActive', sp.active,
                            'supportLocallyFulfilled', sp.locallyfulfilled,
                            'supportStartDate', sp.startdate
                        )
                    )
            END) AS supported_programs
            FROM referencedata.facilities f
            LEFT JOIN referencedata.supported_programs sp ON sp.facilityid = f.id
            LEFT JOIN referencedata.programs p ON sp.programid = p.id
            LEFT JOIN referencedata.geographic_zones gz ON gz.id = f.geographiczoneid
            LEFT JOIN referencedata.geographic_zones gz2 ON gz2.id = gz.parentid
            GROUP BY f.id, gz2.id
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


class ContactDetailsOfUsesrRepository(BaseResourceRepository):
    def get_all(self):
        query = """(
            SELECT rr.rightid, ucd.*
                FROM referencedata.role_rights rr
                INNER JOIN referencedata.role_assignments ra ON ra.roleid = rr.roleid
                INNER JOIN notification.user_contact_details ucd ON ucd.referencedatauserid = ra.userid
        ) AS contact_details
        """
        return self.jdbc_reader.read_data(query)
