from datetime import datetime, time
from sqlalchemy.orm import Session
from app.models import StockMovement, EntityStock

def save_today_stock_snapshot(db: Session):
    try:
        today_start = datetime.combine(datetime.now().date(), time.min)
        now = datetime.now()

        print(f"=== Запуск снапшота ===")
        print(f"Сегодняшняя дата начала: {today_start}")
        print(f"Текущее время: {now}")

        entity_ids = db.query(StockMovement.entity_id).distinct().all()
        entity_ids = [e[0] for e in entity_ids]

        for entity_id in entity_ids:
            print(f"\nEntity ID: {entity_id}")

            existing_snapshot = db.query(EntityStock).filter(
                EntityStock.entity_id == entity_id,
                EntityStock.date == today_start
            ).first()

            if existing_snapshot:
                print(f"Снапшот на сегодня уже существует, пропускаем.")
                continue

            last_snapshot = db.query(EntityStock).filter(
                EntityStock.entity_id == entity_id,
                EntityStock.date < today_start
            ).order_by(EntityStock.date.desc()).first()

            if last_snapshot:
                last_snapshot_date = last_snapshot.date
                print(f"Последний снапшот найден: дата = {last_snapshot_date}, количество = {last_snapshot.quantity}")
            else:
                last_snapshot_date = today_start
                print(f"Последний снапшот не найден, считаем количество начальным 0")

            start_quantity = last_snapshot.quantity if last_snapshot else 0

            movements = db.query(StockMovement).filter(
                StockMovement.entity_id == entity_id,
                StockMovement.movement_time >= last_snapshot_date,
                StockMovement.movement_time < now
            ).all()

            quantity_change = 0
            for m in movements:
                if m.movement_type == 'incoming':
                    quantity_change += m.quantity
                elif m.movement_type == 'outgoing':
                    quantity_change -= m.quantity

            print(f"Изменение количества за сегодня: {quantity_change}")

            total_quantity = start_quantity + quantity_change
            print(f"Итоговое количество для снапшота: {total_quantity}")

            snapshot = EntityStock(
                entity_id=entity_id,
                quantity=total_quantity,
                date=today_start
            )
            db.add(snapshot)

        db.commit()
        print("Снапшот сохранён успешно.")
    except Exception as e:
        db.rollback()
        print(f"Ошибка при сохранении снапшота: {e}")
        raise
