from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

class PatientResponse(BaseModel):
    case_id: str
    filename: str
    file_size_bytes: int
    file_size_mb: float
    patient_name: str
    study_date: str
    accession: str
    modality: str
    elapsed_seconds: int
    has_results: bool
    body_regions: List[str]
    has_hemorrhage: bool

class PatientListResponse(BaseModel):
    patients: List[PatientResponse]

class BiometricData(BaseModel):
    weight: float = Field(gt=0, le=500, description="Patient weight in kilograms")
    height: float = Field(gt=0, le=3.0, description="Patient height in meters")

class SMIData(BaseModel):
    smi: float = Field(gt=0, le=200, description="Skeletal Muscle Index in cm²/m²")
