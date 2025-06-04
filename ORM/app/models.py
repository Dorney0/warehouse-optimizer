from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime
from sqlalchemy.orm import backref

from .database import Base

class BaseModel(Base):
    __abstract__ = True
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

class Entity(BaseModel):
    __tablename__ = "entities"

    name = Column(String, index=True, nullable=False)
    level = Column(Integer, default=0)
    parent_id = Column(
        Integer,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=True
    )
    quantity = Column(Integer, default=0, nullable=False)  # Добавлено количество товара

    children = relationship(
        "Entity",
        cascade="all, delete-orphan",
        backref=backref("parent", remote_side=lambda: [Entity.id])
    )


class Order(BaseModel):
    __tablename__ = "orders"

    order_number = Column(String, index=True, nullable=False)
    customer_name = Column(String, nullable=False)
    total_amount = Column(Integer, nullable=False)
    status = Column(String, default="Pending")  # For example, status could be "Pending", "Shipped", "Delivered"
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    entity_id = Column(Integer, ForeignKey("entities.id"), nullable=True)
    entity = relationship("Entity", backref="orders")

class StockMovement(BaseModel):
    __tablename__ = "stock_movements"

    entity_id = Column(Integer, ForeignKey("entities.id"), nullable=False)
    quantity = Column(Integer, nullable=False)  # Количество изменения
    movement_type = Column(String, nullable=False)  # Например, "incoming" или "outgoing"
    related_order_id = Column(Integer, ForeignKey("orders.id"), nullable=True)  # Связь с заказами
    movement_time = Column(DateTime, default=datetime.utcnow, nullable=False)  # Время движения

    entity = relationship("Entity", backref="stock_movements")
    order = relationship("Order", backref="stock_movements")
    created_at = None
    updated_at = None

class EntityStock(Base):
    __tablename__ = "entity_stock"

    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_id = Column(Integer, ForeignKey("entities.id"), nullable=False)
    quantity = Column(Integer, nullable=False)
    date = Column(DateTime, default=datetime.utcnow, nullable=False)
