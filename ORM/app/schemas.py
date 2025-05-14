from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from sqlalchemy.orm import relationship

class EntityBase(BaseModel):
    name: str
    quantity: int
    level: int
    parent_id: Optional[int] = None

    class Config:
        orm_mode = True

class EntityCreate(EntityBase):
    pass

class EntityUpdate(BaseModel):
    id: int
    quantity: Optional[int] = None
    name: Optional[str] = None
    level: Optional[int] = None
    parent_id: Optional[int] = None

class Entity(EntityBase):
    id: int
    children: List["Entity"] = []

    class Config:
        orm_mode = True

class OrderBase(BaseModel):
    order_number: str
    customer_name: str
    total_amount: int
    status: Optional[str] = "Pending"
    entity_id: Optional[int] = None

    class Config:
        orm_mode = True

class OrderCreate(OrderBase):
    pass

class OrderUpdate(BaseModel):
    id: int
    order_number: Optional[str] = None
    customer_name: Optional[str] = None
    total_amount: Optional[int] = None
    status: Optional[str] = None
    entity_id: Optional[int] = None

class Order(OrderBase):
    id: int

    class Config:
        orm_mode = True

class StockMovementBase(BaseModel):
    quantity: int
    movement_time: datetime
    movement_type: str

class StockMovementCreate(StockMovementBase):
    entity_id: int
    related_order_id: Optional[int] = None

class StockMovement(StockMovementBase):
    id: int
    entity_id: int

    class Config:
        orm_mode = True
