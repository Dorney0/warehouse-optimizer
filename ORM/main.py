from fastapi import FastAPI, Depends, HTTPException, APIRouter
from typing import List
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app import crud, schemas
from datetime import datetime
from app.crud import get_last_snapshot_date
from app.crud import get_quantity_by_date
from app.crud import get_entity_with_children
from app.crud import delete_stock_movements_by_entity_id
from app.crud import get_leaf_breakdown
from fastapi import FastAPI, Depends, HTTPException, Query
from datetime import datetime, time
from app import models
from datetime import date
from app.Services.stock_snapshot import save_today_stock_snapshot
from apscheduler.schedulers.background import BackgroundScheduler

app = FastAPI()
router = APIRouter()
scheduler = BackgroundScheduler()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_db_session():
    db = SessionLocal()
    return db

def scheduled_snapshot():
    db = get_db_session()
    save_today_stock_snapshot(db)
    db.close()
    print(f"Snapshot saved at {datetime.now()}")

@app.on_event("startup")
def startup_event():
    scheduler.add_job(scheduled_snapshot, 'cron', hour=4, minute=23, timezone='Europe/Moscow')
    scheduler.start()

@app.get("/snapshot/last")
def check_last_snapshot():
    db = SessionLocal()
    last = get_last_snapshot_date(db)
    db.close()
    return {"last_snapshot": last.isoformat() if last else "No snapshots yet"}

@app.post("/entities/", response_model=schemas.Entity)
def create_entity(entity: schemas.EntityCreate, db: Session = Depends(get_db)):
    return crud.create_entity(db=db, entity=entity)

@app.get("/entities/{entity_id}", response_model=schemas.Entity)
def read_entity(entity_id: int, db: Session = Depends(get_db)):
    entity = crud.get_entity(db, entity_id=entity_id)
    if entity is None:
        raise HTTPException(status_code=404, detail="Entity not found")
    return entity

@app.get("/entities_with_children/{entity_id}")
def get_entity_with_children_route(entity_id: int, db: Session = Depends(get_db)):
    return get_entity_with_children(db, entity_id)

@app.get("/entities/", response_model=List[schemas.Entity])
def read_entities(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return crud.get_entities(db, skip=skip, limit=limit)

@app.put("/entities/", response_model=schemas.Entity)
def update_entity(entity_update: schemas.EntityUpdate, db: Session = Depends(get_db)):
    db_entity = crud.update_entity(db, entity_update=entity_update)
    if db_entity is None:
        raise HTTPException(status_code=404, detail="Entity not found")
    return db_entity

@app.delete("/entities/{entity_id}")
def delete_entity(entity_id: int, db: Session = Depends(get_db)):
    result = crud.delete_entity(db, entity_id=entity_id)

    if result is None:
        raise HTTPException(status_code=404, detail="Entity not found")
    elif isinstance(result, str):  # Если результат - строка ошибки
        raise HTTPException(status_code=500, detail=f"Error: {result}")

    return result  # Возвращаем сообщение об успешном удалении

@app.post("/orders/", response_model=schemas.Order)
def create_order(order: schemas.OrderCreate, db: Session = Depends(get_db)):
    db_order = crud.create_order(db=db, order=order)
    return db_order

@app.get("/orders/{order_id}", response_model=schemas.Order)
def read_order(order_id: int, db: Session = Depends(get_db)):
    order = crud.get_order(db, order_id=order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return order

@app.get("/orders/", response_model=List[schemas.Order])
def read_orders(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return crud.get_orders(db, skip=skip, limit=limit)

@app.put("/orders/", response_model=schemas.Order)
def update_order(order_update: schemas.OrderUpdate, db: Session = Depends(get_db)):
    db_order = crud.update_order(db, order_update=order_update)
    if db_order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return db_order

@app.delete("/orders/{order_id}", response_model=schemas.Order)
def delete_order(order_id: int, db: Session = Depends(get_db)):
    order = db.query(models.Order).filter(models.Order.id == order_id).first()

    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    crud.delete_stock_movements_by_entity_id(db, entity_id=order.entity_id)

    deleted_order = crud.delete_order(db, order_id=order_id)

    if not deleted_order:
        raise HTTPException(status_code=404, detail="Failed to delete order")

    # Преобразуем удаленный заказ в схему Order для возвращаемого ответа
    return schemas.Order.from_orm(deleted_order)

@app.post("/stock_movements/", response_model=schemas.StockMovement)
def create_stock_movement(stock_movement: schemas.StockMovementCreate, db: Session = Depends(get_db)):
    db_stock_movement = crud.create_stock_movement(
        db=db,
        entity_id=stock_movement.entity_id,
        quantity=stock_movement.quantity,
        movement_type=stock_movement.movement_type,
        related_order_id=stock_movement.related_order_id  # Это поле может быть None
    )
    return db_stock_movement

@app.get("/stock_movements/", response_model=List[schemas.StockMovement])
def read_stock_movements(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return crud.get_stock_movements(db, skip=skip, limit=limit)

@app.get("/stock_movements/{entity_id}/quantity_at_time")
def get_stock_at_time(entity_id: int, timestamp: date, db: Session = Depends(get_db)):
    entity = db.query(models.Entity).filter(models.Entity.id == entity_id).first()
    if not entity:
        raise HTTPException(status_code=404, detail="Entity not found")

    total_quantity = crud.get_stock_at_time(db, entity_id=entity_id, timestamp=timestamp)

    return {
        "message": f"Сущность '{entity.name}' (ID {entity.id}) имеет количество {total_quantity} на складе на момент {timestamp.strftime('%Y-%m-%d %H:%M:%S')}",
        "entity_id": entity.id,
        "entity_name": entity.name,
        "quantity": total_quantity,
        "as_of": timestamp
    }

@app.get("/stock_movements/quantity_at_time")
def get_stock_at_time(timestamp: date, db: Session = Depends(get_db)):
    entities = db.query(models.Entity).all()
    if not entities:
        raise HTTPException(status_code=404, detail="No entities found")

    stock_movements = []
    for entity in entities:
        total_quantity = crud.get_stock_at_time(db, entity_id=entity.id, timestamp=timestamp)
        stock_movements.append({
            "entity_id": entity.id,
            "entity_name": entity.name,
            "quantity": total_quantity,
            "as_of": timestamp.strftime('%Y-%m-%d %H:%M:%S')
        })

    return {
        "message": f"Stock quantities for all entities as of {timestamp.strftime('%Y-%m-%d %H:%M:%S')}",
        "stock_movements": stock_movements
    }

@app.delete("/stock-movements/{entity_id}")
def delete_stock_movements(entity_id: int, db: Session = Depends(get_db)):
    # Вызываем метод для удаления всех записей с данным entity_id
    deleted_movements = crud.delete_stock_movements_by_entity_id(db, entity_id)

    # Если не удалось найти записи для удаления, генерируем ошибку
    if not deleted_movements:
        raise HTTPException(status_code=404, detail="Stock movements not found for the given entity_id")

    # Возвращаем сообщение о количестве удаленных записей
    return {"detail": f"Deleted {len(deleted_movements)} stock movement(s) with entity_id {entity_id}"}

@app.get("/get-current-quantity/{entity_id}")
def get_quantity(
    entity_id: int,
    date: str = Query(..., description="Дата в формате YYYY-MM-DD"),
    db: Session = Depends(get_db)
):
    try:
        date_obj = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Неверный формат даты. Используй YYYY-MM-DD")

    # Получаем данные: количество и время движения
    result = get_quantity_by_date(db, entity_id, date_obj)

    return {
        "entity_id": entity_id,
        "date": date,
        "current_quantity": result["quantity_on_date"],
        "movement_time": result["as_of"]
    }

@app.get("/entity/{entity_id}/breakdown", response_model=dict[int, int])
def read_entity_breakdown(entity_id: int, quantity: int, db: Session = Depends(get_db)):
    breakdown = get_leaf_breakdown(db, entity_id, quantity)
    if not breakdown:
        raise HTTPException(status_code=404, detail="Entity not found or has no children")
    return breakdown

@app.get("/analyze_deficit")
def get_deficit_analysis(db: Session = Depends(get_db)):
    """
    Эндпоинт для анализа дефицита товаров в заказах с статусом deficiency.
    Возвращает дефицит или избыток по каждому товару.
    """
    result = crud.analyze_deficit_for_orders(db)
    return result

