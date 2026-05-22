"""
Structure stage output models.

Mirror the NestJS TextExtractionOutputSchema (extraction.dto.ts) so that
field names are identical across the service boundary.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class DocumentType(BaseModel):
    code: str
    confidence: float


class Person(BaseModel):
    fullName: Optional[str] = None
    givenNames: list[str] = Field(default_factory=list)
    surname: Optional[str] = None
    dateOfBirth: Optional[str] = None
    placeOfBirth: Optional[str] = None
    gender: str = "Unknown"


class Document(BaseModel):
    number: Optional[str] = None
    serialNumber: Optional[str] = None
    batchNumber: Optional[str] = None
    issuer: Optional[str] = None
    placeOfIssue: Optional[str] = None
    issueDate: Optional[str] = None
    expiryDate: Optional[str] = None


class AddressComponent(BaseModel):
    type: str
    value: str


class Address(BaseModel):
    raw: Optional[str] = None
    country: Optional[str] = None
    components: list[AddressComponent] = Field(default_factory=list)


class Biometrics(BaseModel):
    photoPresent: bool = False
    fingerprintPresent: bool = False
    signaturePresent: bool = False


class AdditionalField(BaseModel):
    fieldName: str
    fieldValue: str


class RawAudit(BaseModel):
    pagesReferenced: list[int] = Field(default_factory=list)


class Quality(BaseModel):
    ocrConfidence: float
    extractionConfidence: float
    warnings: list[str] = Field(default_factory=list)


class StructureOutput(BaseModel):
    documentType: DocumentType
    country: Optional[str] = None
    person: Person
    document: Document
    address: Address
    biometrics: Biometrics
    additionalFields: list[AdditionalField] = Field(default_factory=list)
    raw: RawAudit
    quality: Quality

    @classmethod
    def from_dict(cls, data: dict) -> StructureOutput:
        """Deserialise a raw dict (e.g. from DB JSONB) into a StructureOutput."""
        return cls.model_validate(data)

    def to_dict(self) -> dict:
        """Serialise to a plain dict suitable for JSONB storage."""
        return self.model_dump()
