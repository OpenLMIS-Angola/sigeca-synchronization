from sqlalchemy.orm import sessionmaker
from app.domain.models import Facility


from app.infrastructure.database import get_engine
from sqlalchemy.orm import joinedload, Query
from sqlalchemy.orm import sessionmaker
from app.domain.models import (
    FacilityType,
    FacilityOperator,
    GeographicLevel,
    GeographicZone,
    Program,
    Facility,
    SupportedProgram,
)
import urllib
from sqlalchemy.dialects import postgresql


class BaseRepository:

    @property
    def model(cls):
        raise NotImplementedError("Model has to be provided")

    def __init__(self, config: dict):
        self.engine = get_engine(config)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def get_all(self):
        return self._add_options(self.session.query(self.model)).all()

    def generate_get_all_sql_query(self):
        query = self.session.query(self.model).statement
        return str(query)

    def get_by_id(self, id):
        return (
            self._add_options(self.session.query(self.model))
            .filter(self.model.id == id)
            .first()
        )

    def get_by_code(self, code):
        return (
            self._add_options(self.session.query(self.model))
            .filter(self.model.code == code)
            .first()
        )

    def get_by_codes(self, codes, chunk_size=1000):
        results = []
        for i in range(0, len(codes), chunk_size):
            chunk = codes[i : i + chunk_size]
            facilities = (
                self._add_options(self.session.query(self.model))
                .filter(self.model.code.in_(chunk))
                .all()
            )
            results.extend(facilities)
        return results

    def _add_options(self, query) -> Query:
        return query


class FacilityRepository(BaseRepository):
    model = Facility

    def _add_options(self, query):
        return query.options(*self.__load_options())

    def __load_options(self):
        return [
            joinedload(Facility.geographic_zone),
            joinedload(Facility.operated_by),
            joinedload(Facility.type),
        ]


class FacilityTypeRepository(BaseRepository):
    model = FacilityType


class FacilityOperatorRepository(BaseRepository):
    model = FacilityOperator


class GeographicLevelRepository(BaseRepository):
    model = GeographicLevel


class GeographicZoneRepository(BaseRepository):
    model = GeographicZone


class ProgramRepository(BaseRepository):
    model = Program


class SupportedProgramRepository(BaseRepository):
    def get_all(self):
        return self.session.query(SupportedProgram).all()

    def get_by_id(self, facility_id, program_id):
        return (
            self.session.query(SupportedProgram)
            .filter(
                SupportedProgram.facilityid == facility_id,
                SupportedProgram.programid == program_id,
            )
            .first()
        )

    def get_by_program_code(self, program_code):
        return (
            self.session.query(SupportedProgram)
            .join(Program)
            .filter(Program.code == program_code)
            .all()
        )

    def get_by_facility_code(self, program_code):
        return (
            self.session.query(SupportedProgram)
            .join(Facility)
            .filter(Facility.code == program_code)
            .all()
        )

    def get_by_facility_and_program(self, facility_code, program_code):
        return (
            self.session.query(SupportedProgram)
            .join(Program)
            .join(Facility)
            .filter(Program.code == program_code, Facility.code == facility_code)
            .all()
        )
