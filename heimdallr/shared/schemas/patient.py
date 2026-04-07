"""Patient-facing response and payload models."""

from __future__ import annotations

from typing import List
from typing import Optional

from pydantic import BaseModel, Field


class PatientResponse(BaseModel):
    case_id: str
    filename: str
    file_size_bytes: int
    file_size_mb: float
    patient_name: str
    patient_id: str = ""
    patient_birth_date: str = ""
    study_date: str
    accession: str
    modality: str
    prepare_elapsed_seconds: int
    elapsed_seconds: int
    has_results: bool
    body_regions: List[str]
    has_hemorrhage: bool
    artifacts_purged: bool = False
    artifacts_purged_at: Optional[str] = None


class PatientListResponse(BaseModel):
    patients: List[PatientResponse]


class BiometricData(BaseModel):
    weight: float = Field(gt=0, le=500, description="Patient weight in kilograms")
    height: float = Field(gt=0, le=3.0, description="Patient height in meters")


class SMIData(BaseModel):
    smi: float = Field(gt=0, le=200, description="Skeletal Muscle Index in cm^2/m^2")
