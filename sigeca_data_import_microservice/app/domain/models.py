from sqlalchemy import (
    create_engine,
    Column,
    String,
    Boolean,
    Integer,
    Date,
    ForeignKey,
    UniqueConstraint,
    JSON,
    Numeric,
)
from sqlalchemy.dialects.postgresql import UUID, TEXT, JSONB
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class FacilityType(Base):
    __tablename__ = "facility_types"
    __table_args__ = {"schema": "referencedata"}

    id = Column(UUID, primary_key=True)
    active = Column(Boolean)
    code = Column(TEXT, nullable=False)
    description = Column(TEXT)
    displayorder = Column(Integer)
    name = Column(TEXT)
    primaryhealthcare = Column(Boolean, default=False)

    UniqueConstraint("code", name="uk_g7ooo22v3vokh2qrqbxw7uaps")


class FacilityOperator(Base):
    __tablename__ = "facility_operators"
    __table_args__ = {"schema": "referencedata"}

    id = Column(UUID, primary_key=True)
    code = Column(TEXT, nullable=False)
    description = Column(TEXT)
    displayorder = Column(Integer)
    name = Column(TEXT)

    UniqueConstraint("code", name="uk_g7ooo22v3vokh2qrqbxw7uaps")


class GeographicLevel(Base):
    __tablename__ = "geographic_levels"
    __table_args__ = {"schema": "referencedata"}

    id = Column(UUID, primary_key=True)
    code = Column(TEXT, nullable=False)
    levelnumber = Column(Integer, nullable=False)
    name = Column(TEXT)


class GeographicZone(Base):
    __tablename__ = "geographic_zones"
    __table_args__ = {"schema": "referencedata"}

    id = Column(UUID, primary_key=True)
    catchmentpopulation = Column(Integer)
    code = Column(TEXT, nullable=False)
    latitude = Column(Numeric(8, 5))
    longitude = Column(Numeric(8, 5))
    name = Column(TEXT)
    levelid = Column(
        UUID, ForeignKey("referencedata.geographic_levels.id"), nullable=False
    )
    parentid = Column(UUID, ForeignKey("referencedata.geographic_zones.id"))
    boundary = Column("boundary", String)  # Adjust to your geometry type
    extradata = Column(JSONB)

    parent = relationship("GeographicZone", remote_side=[id])
    level = relationship("GeographicLevel")


class Facility(Base):
    __tablename__ = "facilities"
    __table_args__ = {"schema": "referencedata"}

    id = Column(UUID, primary_key=True)
    active = Column(Boolean, nullable=False)
    code = Column(TEXT, nullable=False)
    comment = Column(TEXT)
    description = Column(TEXT)
    enabled = Column(Boolean, nullable=False)
    godowndate = Column(Date)
    golivedate = Column(Date)
    name = Column(TEXT)
    openlmisaccessible = Column(Boolean)
    geographiczoneid = Column(
        UUID, ForeignKey("referencedata.geographic_zones.id"), nullable=False
    )
    operatedbyid = Column(UUID, ForeignKey("referencedata.facility_operators.id"))
    typeid = Column(UUID, ForeignKey("referencedata.facility_types.id"), nullable=False)
    extradata = Column(JSONB)
    location = Column("location", String)  # Adjust to your geometry type

    geographic_zone = relationship("GeographicZone")
    operated_by = relationship("FacilityOperator")
    type = relationship("FacilityType")


class SupportedProgram(Base):
    __tablename__ = "supported_programs"
    __table_args__ = {"schema": "referencedata"}

    active = Column(Boolean, nullable=False)
    startdate = Column(Date)
    facilityid = Column(
        UUID, ForeignKey("referencedata.facilities.id"), primary_key=True
    )
    programid = Column(UUID, ForeignKey("referencedata.programs.id"), primary_key=True)
    locallyfulfilled = Column(Boolean, default=False)

    facility = relationship("Facility")
    program = relationship("Program")


class Program(Base):
    __tablename__ = "programs"
    __table_args__ = {"schema": "referencedata"}

    id = Column(UUID, primary_key=True)
    active = Column(Boolean)
    code = Column(String(255), nullable=False)
    description = Column(TEXT)
    name = Column(TEXT)
    periodsskippable = Column(Boolean, nullable=False)
    shownonfullsupplytab = Column(Boolean)
    enabledatephysicalstockcountcompleted = Column(Boolean, nullable=False)
    skipauthorization = Column(Boolean, default=False)
    supoprtedprograms = relationship(
        "SupportedProgram",
        order_by=SupportedProgram.programid,
        back_populates="program",
    )


# Example of engine and session creation
# engine = create_engine('your_database_url')
# Session = sessionmaker(bind=engine)
# session = Session()
