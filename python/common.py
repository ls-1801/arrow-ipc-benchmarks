import numpy as np
import pyarrow

ICU_SCHEMA = pyarrow.schema([
    pyarrow.field("Albumin", pyarrow.uint64()),
    pyarrow.field("ALP", pyarrow.uint32()),
    pyarrow.field("ALT", pyarrow.uint8()),
    pyarrow.field("AST", pyarrow.uint8()),
    pyarrow.field("Bilirubin", pyarrow.uint8()),
    pyarrow.field("BUN", pyarrow.uint32()),
    pyarrow.field(
        "Cholesterol", pyarrow.float64(),
        nullable=True),
    pyarrow.field(
        "Ceratinine", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "DiasABP", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "FiO2", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "GCS", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "Glucose", pyarrow.float64()),
    pyarrow.field(
        "HCO3", pyarrow.float64()),
    pyarrow.field(
        "HCT", pyarrow.float64()),
    pyarrow.field(
        "HR", pyarrow.float64()),
    pyarrow.field(
        "K", pyarrow.uint8()),
    pyarrow.field(
        "Lactate", pyarrow.float64()),
    pyarrow.field(
        "Mg", pyarrow.uint32()),
    pyarrow.field(
        "MAP", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "MechVent", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "Na", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "NIDiasABP", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "NIMAP", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "NISysABP", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "PaCO2", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "PaO2", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "pH", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "Paletelts", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "RespRate", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "SaO2", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "SysABP", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "TEMP", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "Tropl", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "TropT", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "Urine", pyarrow.float64(), nullable=True),
    pyarrow.field(
        "WBC", pyarrow.float64(), nullable=True),
])

AQ_SCHEMA = pyarrow.schema(
    [
        ("no", pyarrow.uint64()),
        ("year", pyarrow.uint32()),
        ("month", pyarrow.uint8()),
        ("day", pyarrow.uint8()),
        ("hour", pyarrow.uint8()),
        ("PM2.5", pyarrow.float64()),
        pyarrow.field("PM10", pyarrow.float64(), nullable=True),
        pyarrow.field("SO2", pyarrow.float64(), nullable=True),
        pyarrow.field("NO2", pyarrow.float64(), nullable=True),
        pyarrow.field("CO", pyarrow.float64(), nullable=True),
        pyarrow.field("O3", pyarrow.float64(), nullable=True),
        ("Temp", pyarrow.float64()),
        ("Pressure", pyarrow.float64()),
        ("Dewp", pyarrow.float64()),
        ("Rain", pyarrow.float64()),
        ("wd", pyarrow.uint8()),
        ("WSPM", pyarrow.float64()),
        ("station", pyarrow.uint32()),
    ]
)


def schema_by_name(schemaName):
    if schemaName == "aq":
        return AQ_SCHEMA

    if schemaName == "icu":
        return ICU_SCHEMA

    if schemaName == "wifi":
        return pyarrow.schema([pyarrow.field(f'signal_strength_{i}', pyarrow.uint32(), nullable=True) for i in range(671)])

    raise "Not implemented"


def arrow_dt_to_numpy(dt):
    if dt == pyarrow.uint64():
        return np.uint64
    if dt == pyarrow.uint32():
        return np.uint32
    if dt == pyarrow.uint8():
        return np.uint8
    if dt == pyarrow.float64():
        return np.float64

    raise "Not implemented"


def generate_batch(schema, size, offset=0):
    return [pyarrow.array(np.arange(offset, offset + size, dtype=arrow_dt_to_numpy(field.type))) for field in schema]
