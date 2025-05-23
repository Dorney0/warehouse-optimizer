import logging
from sqlalchemy.orm import Session
from app import models, schemas
from datetime import datetime
from .models import Entity
from .models import StockMovement
from sqlalchemy import func
from datetime import datetime, date, time, timedelta
from fastapi import HTTPException, status
from typing import Dict
from .models import Order
from .models import EntityStock
from collections import defaultdict

logger = logging.getLogger(__name__)

def create_entity(db: Session, entity: schemas.EntityCreate):
    if entity.quantity < 0:
        raise ValueError("Quantity cannot be negative")

    # Создание самой сущности
    if entity.parent_id is None:
        db_entity = models.Entity(name=entity.name, level=entity.level, parent_id=None, quantity=entity.quantity)
    else:
        parent_entity = db.query(models.Entity).filter(models.Entity.id == entity.parent_id).first()
        if not parent_entity:
            raise HTTPException(status_code=404, detail=f"Parent entity with id {entity.parent_id} does not exist.")
        db_entity = models.Entity(**entity.dict())

    # Добавление сущности в базу данных
    db.add(db_entity)
    db.commit()
    db.refresh(db_entity)

    return db_entity

def get_entity(db: Session, entity_id: int):
    db_entity = db.query(models.Entity).filter(models.Entity.id == entity_id).first()
    if db_entity is None:
        return None
    return db_entity

def get_entity_with_children(db: Session, entity_id: int):
    # Получаем основную сущность
    entity = db.query(models.Entity).filter(models.Entity.id == entity_id).first()

    if not entity:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Entity with ID {entity_id} not found"
        )

    # Рекурсивная функция для получения всех дочерних элементов
    def get_children(entity_id: int):
        children = db.query(models.Entity).filter(models.Entity.parent_id == entity_id).all()
        result = []
        for child in children:
            result.append({
                **schemas.Entity.from_orm(child).dict(),  # Делаем схему Entity из модели
                "children": get_children(child.id)  # Рекурсивно находим детей
            })
        return result

    # Строим результат с детьми
    entity_data = schemas.Entity.from_orm(entity).dict()
    entity_data["children"] = get_children(entity.id)

    return entity_data

def get_entity_with_children(db: Session, entity_id: int):
    # Получаем основную сущность
    entity = db.query(models.Entity).filter(models.Entity.id == entity_id).first()

    if not entity:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Entity with ID {entity_id} not found"
        )

    # Рекурсивная функция для получения всех дочерних элементов
    def get_children(entity_id: int):
        children = db.query(models.Entity).filter(models.Entity.parent_id == entity_id).all()
        result = []
        for child in children:
            result.append({
                **schemas.Entity.from_orm(child).dict(),  # Делаем схему Entity из модели
                "children": get_children(child.id)  # Рекурсивно находим детей
            })
        return result

    # Строим результат с детьми
    entity_data = schemas.Entity.from_orm(entity).dict()
    entity_data["children"] = get_children(entity.id)

    return entity_data

def get_entities(db: Session, skip: int = 0, limit: int = 100):
    db_entities = db.query(models.Entity).offset(skip).limit(limit).all()
    return [schemas.Entity.from_orm(entity) for entity in db_entities]

def update_entity(db: Session, entity_update: schemas.EntityUpdate):
    db_entity = db.query(models.Entity).filter(models.Entity.id == entity_update.id).first()
    if not db_entity:
        return None

    old_quantity = db_entity.quantity

    # Обновляем остальные поля, кроме id и quantity
    for key, value in entity_update.dict(exclude_unset=True).items():
        if key not in ("id", "quantity"):
            setattr(db_entity, key, value)

    if 'quantity' in entity_update.dict(exclude_unset=True):
        incoming_quantity = entity_update.quantity

        if incoming_quantity < 0:
            raise ValueError("Quantity cannot be negative")

        remaining_difference = incoming_quantity

        # Сначала покрываем дефициты
        deficiency_records = db.query(models.StockMovement).filter(
            models.StockMovement.entity_id == db_entity.id,
            models.StockMovement.movement_type == 'deficiency'
        ).order_by(models.StockMovement.id.asc()).all()

        for deficiency in deficiency_records:
            if remaining_difference <= 0:
                break

            cover_qty = min(deficiency.quantity, remaining_difference)
            related_order_id = deficiency.related_order_id

            # Добавляем запись, что покрыли дефицит
            db.add(models.StockMovement(
                entity_id=db_entity.id,
                quantity=cover_qty,
                movement_type='outgoing',
                related_order_id=related_order_id
            ))

            remaining_difference -= cover_qty

            if cover_qty == deficiency.quantity:
                db.delete(deficiency)
            else:
                deficiency.quantity -= cover_qty

            db.commit()

            # Проверка, остались ли дефициты по заказу
            remaining = db.query(models.StockMovement).filter(
                models.StockMovement.related_order_id == related_order_id,
                models.StockMovement.movement_type == 'deficiency'
            ).first()

            if not remaining:
                order = db.query(models.Order).filter(
                    models.Order.id == related_order_id
                ).first()
                if order:
                    order.status = "fulfilled"
                    db.commit()

        # Если остался излишек — это обычный приход
        if remaining_difference > 0:
            db.add(models.StockMovement(
                entity_id=db_entity.id,
                quantity=remaining_difference,
                movement_type='incoming',
                related_order_id=None
            ))
            db_entity.quantity += remaining_difference
            db.commit()


        elif quantity_difference < 0:
            # Расход
            quantity_out = abs(quantity_difference)
            db.add(models.StockMovement(
                entity_id=db_entity.id,
                quantity=quantity_out,
                movement_type='outgoing',
                related_order_id=None
            ))
            db.commit()

        # Устанавливаем итоговое количество
        db_entity.quantity = new_quantity

    # Финальный коммит и возврат
    db.commit()
    db.refresh(db_entity)

    return schemas.Entity.from_orm(db_entity)

def delete_entity(db: Session, entity_id: int):
    # Находим сущность
    db_entity = db.query(models.Entity).filter(models.Entity.id == entity_id).first()

    if not db_entity:
        return None  # Если сущность не найдена, возвращаем None

    try:
        # Удаляем все связанные записи в таблице stock_movements
        db.query(models.StockMovement).filter(models.StockMovement.entity_id == entity_id).delete()

        # Удаляем саму сущность
        db.delete(db_entity)
        db.commit()

    except Exception as e:
        db.rollback()  # Откатываем транзакцию в случае ошибки
        return str(e)  # Возвращаем ошибку как строку

    return {"message": "Сущность успешно удалена"}  # Возвращаем сообщение об успешном удалении

def delete_stock_movements_by_entity_id(db: Session, entity_id: int):
    # Находим все записи о движении с данным entity_id
    stock_movements = db.query(models.StockMovement).filter(models.StockMovement.entity_id == entity_id).all()

    # Если записи есть, удаляем их
    if stock_movements:
        for stock_movement in stock_movements:
            db.delete(stock_movement)
        db.commit()  # Сохраняем изменения в базе

    return stock_movements

def create_order(db: Session, order: schemas.OrderCreate):
    # Создаем новый заказ
    db_order = models.Order(**order.dict())
    # Обработка случая, когда entity не найден
    if not db_order.entity_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Товар с ID {db_order.entity_id} не найден"
        )
    # Добавляем заказ в базу данных
    db.add(db_order)
    db.commit()

    # Создаем запись в таблице stock_movements
    #stock_movement = StockMovement(
    #    entity_id=db_order.entity_id,
    #    quantity=db_order.total_amount,
    #    movement_type="outgoing",
    #    related_order_id=db_order.id,
    #)

    #db.add(stock_movement)
    #db.commit()

    return db_order

def get_order(db: Session, order_id: int):
    return db.query(models.Order).filter(models.Order.id == order_id).first()

def get_orders(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Order).offset(skip).limit(limit).all()

def update_order(db: Session, order_update: schemas.OrderUpdate):
    db_order = db.query(models.Order).filter(models.Order.id == order_update.id).first()
    if not db_order:
        return None

    for key, value in order_update.dict(exclude_unset=True).items():
        if key != "id":
            setattr(db_order, key, value)

    db.commit()
    db.refresh(db_order)

    # После обновления заказа, обновляем связанные записи в stock_movements
    if 'total_amount' in order_update.dict(exclude_unset=True):
        old_total_amount = db_order.total_amount
        new_total_amount = order_update.total_amount

        if old_total_amount != new_total_amount:
            # Пример изменения связанного количества в stock_movements
            stock_movements = db.query(models.StockMovement).filter(models.StockMovement.related_order_id == db_order.id).all()
            for movement in stock_movements:
                movement.quantity = movement.quantity - (old_total_amount - new_total_amount)
                db.add(movement)
            db.commit()

    return schemas.Order.from_orm(db_order)


    for key, value in order_update.dict(exclude_unset=True).items():
        if key != "id":
            setattr(db_order, key, value)

    db.commit()
    db.refresh(db_order)
    return db_order


def delete_order(db: Session, order_id: int):
    # Находим заказ по ID
    db_order = db.query(models.Order).filter(models.Order.id == order_id).first()
    if not db_order:
        return None

    # Удаляем все связанные записи из таблицы stock_movements
    stock_movements = db.query(models.StockMovement).filter(models.StockMovement.related_order_id == order_id).all()
    for movement in stock_movements:
        db.delete(movement)
    db.commit()  # Убедитесь, что изменения коммитятся

    try:
        db.delete(db_order)
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error deleting order: {str(e)}")

    # Возвращаем удаленный заказ в формате схемы Order
    return schemas.Order.from_orm(db_order)

def create_stock_movement(db: Session, entity_id: int, quantity: int, movement_type: str, related_order_id: int = None):
    # Получаем текущее количество товара из таблицы entities
    entity = db.query(Entity).filter(Entity.id == entity_id).first()

    if not entity:
        raise ValueError("Entity not found")

    if movement_type != 'incoming' or 'outgoing':
        raise ValueError("Invalid movement type. Allowed values are 'incoming' or 'outgoing'.")

    # Создаем запись в таблице stock_movements
    stock_movement = StockMovement(
        entity_id=entity_id,
        quantity=quantity,
        movement_type=movement_type,
        related_order_id=related_order_id,
    )

    db.add(stock_movement)
    db.commit()

    return stock_movement

def get_stock_movements(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.StockMovement).offset(skip).limit(limit).all()

def get_stock_at_time(db: Session, entity_id: int, timestamp: datetime):
    movements = db.query(models.StockMovement).filter(
        models.StockMovement.entity_id == entity_id,
        models.StockMovement.movement_time <= timestamp
    ).all()

    total_quantity = 0
    for movement in movements:
        if movement.movement_type == 'incoming':
            total_quantity += movement.quantity
        elif movement.movement_type == 'outgoing':
            total_quantity -= movement.quantity

    return total_quantity

def get_quantity_entity(db: Session, entity_id: int):
    movements = db.query(models.StockMovement).filter(
        models.StockMovement.entity_id == entity_id,
    ).all()
    total_quantity = 0
    for movement in movements:
        if movement.movement_type == 'incoming':
            total_quantity += movement.quantity
        elif movement.movement_type == 'outgoing':
            total_quantity -= movement.quantity
    return total_quantity

def get_quantity_by_date(db: Session, entity_id: int, target_date: date):
    start_time = datetime.combine(target_date, time.min)
    end_time = datetime.combine(target_date, time.max)

    movements = db.query(StockMovement).filter(
        StockMovement.entity_id == entity_id,
        StockMovement.movement_time <= end_time
    ).order_by(StockMovement.movement_time).all()

    if not movements:
        raise HTTPException(status_code=404, detail="No stock movement found for the given entity and date")

    quantity = 0
    for movement in movements:
        if movement.movement_type == 'incoming':
            quantity += movement.quantity
        elif movement.movement_type == 'outgoing':
            quantity -= movement.quantity

    return {"entity_id": entity_id, "quantity_on_date": quantity, "as_of": end_time}


def get_leaf_breakdown(db_session, entity_id: int, quantity: int | str):
    """
    Возвращает словарь {entity_id: total_quantity} для всех листовых деталей,
    необходимых для сборки entity_id в количестве quantity.
    """
    result = defaultdict(int)

    def dfs(current_id, current_qty):
        try:
            current_qty = int(current_qty)
        except (ValueError, TypeError):
            current_qty = 1  # значение по умолчанию

        entity = db_session.query(Entity).filter(Entity.id == current_id).first()
        if not entity:
            return

        # Если нет дочерних элементов — листовая деталь
        if not entity.children:
            result[entity.id] += current_qty
            return

        # Рекурсивно обходим детей
        for child in entity.children:
            try:
                child_qty = int(child.quantity)
            except (ValueError, TypeError):
                child_qty = 1
            dfs(child.id, current_qty * child_qty)

    dfs(entity_id, quantity)
    return dict(result)


def analyze_deficit_for_orders(db: Session) -> list[Dict]:
    """
    Анализ дефицита: разбивает заказы в статусе 'Pending' за последний месяц до листьев,
    суммирует потребности и сравнивает с остатками на складе (включая разборку узлов).
    """

    # 1. Получаем все заказы в статусе 'Pending'
    orders = db.query(Order).filter(
        Order.status == 'Pending'
    ).all()

    # 2. Суммарные потребности в листовых деталях
    required = defaultdict(int)
    for order in orders:
        if not order.entity_id:
            continue

        quantity = int(order.total_amount) if hasattr(order, 'total_amount') else "error"
        breakdown = get_leaf_breakdown(db, order.entity_id, quantity)

        for leaf_id, leaf_qty in breakdown.items():
            required[leaf_id] += leaf_qty

    # 3. Получаем остатки по всем entity
    stock_by_entity = {}
    for entity in db.query(Entity).all():
        qty = get_quantity_entity(db, entity.id)
        if qty > 0:
            stock_by_entity[entity.id] = qty

    # 4. Разбираем узлы на листовые компоненты
    decomposed_leaf_stock = defaultdict(int)
    for entity_id, qty in stock_by_entity.items():
        entity = db.query(Entity).filter(Entity.id == entity_id).first()
        if entity and entity.children:
            breakdown = get_leaf_breakdown(db, entity_id, qty)
            for leaf_id, leaf_qty in breakdown.items():
                decomposed_leaf_stock[leaf_id] += leaf_qty

    # 5. Собираем финальный отчёт: прямой stock + разобранный
    result = []
    for entity_id, req_qty in required.items():
        stock_direct = stock_by_entity.get(entity_id, 0)
        stock_from_decomposition = decomposed_leaf_stock.get(entity_id, 0)
        total_stock = stock_direct + stock_from_decomposition

        entity = db.query(Entity).filter(Entity.id == entity_id).first()
        if entity:
            deficit = req_qty - total_stock
            result.append({
                "id": entity.id,
                "name": entity.name,
                "required_quantity": req_qty,
                "stock_quantity": total_stock,
                "deficit": deficit if deficit > 0 else None,
            })

    return result




def get_last_snapshot_date(db: Session):
    return db.query(func.max(EntityStock.date)).scalar()



