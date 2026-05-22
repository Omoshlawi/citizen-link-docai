"""
Typed models for the docai service.

  app.models.vision    — VisionPage, VisionOutput
  app.models.structure — StructureOutput and all nested field models
  app.models.pipeline  — ConversationEntry, UsageEntry, UsageSummary, JobRecord
"""

from app.models.vision import VisionMeta, VisionOutput, VisionPage
from app.models.structure import (
    Address,
    AddressComponent,
    AdditionalField,
    Biometrics,
    Document,
    DocumentType,
    Person,
    Quality,
    RawAudit,
    StructureOutput,
)
from app.models.pipeline import (
    CallRecord,
    ConversationEntry,
    JobRecord,
    UsageEntry,
    UsageSummary,
)

__all__ = [
    # vision
    "VisionMeta",
    "VisionPage",
    "VisionOutput",
    # structure
    "DocumentType",
    "Person",
    "Document",
    "AddressComponent",
    "Address",
    "Biometrics",
    "AdditionalField",
    "RawAudit",
    "Quality",
    "StructureOutput",
    # pipeline
    "ConversationEntry",
    "CallRecord",
    "UsageEntry",
    "UsageSummary",
    "JobRecord",
]
