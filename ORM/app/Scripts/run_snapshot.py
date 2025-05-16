from app.services.stock_snapshot import daily_entity_stock_snapshot
from app.database import SessionLocal

def main():
    db = SessionLocal()
    try:
        daily_entity_stock_snapshot(db)
    finally:
        db.close()

if __name__ == "__main__":
    main()
