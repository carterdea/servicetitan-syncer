from __future__ import annotations

from pydantic import BaseModel, Field


class ItemCreate(BaseModel):
    code: str
    name: str
    description: str | None = None
    active: bool = True


class POLineCreate(BaseModel):
    itemId: int
    quantity: float
    unitCost: float | None = None


class POCreate(BaseModel):
    vendorId: int
    warehouseId: int | None = None
    externalNumber: str
    lines: list[POLineCreate] = Field(default_factory=list)


class JobCreate(BaseModel):
    customerId: int
    locationId: int
    jobTypeId: int
    campaignId: int | None = None
    source: str = "stsync"
    externalNumber: str
    notes: str


class APIResponse(BaseModel):
    id: int | None = None
    guid: str | None = None
    externalId: str | None = None


class VendorCreate(BaseModel):
    name: str
    externalNumber: str | None = None


class WarehouseCreate(BaseModel):
    name: str
    externalNumber: str | None = None

