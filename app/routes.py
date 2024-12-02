from fastapi import APIRouter
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from core.database import get_db
from app.models import Stock

router = APIRouter()

@router.get("/")
def root():
    return {"message": "This is the root endpoint!"}

@router.get("/stocks/")
def get_stocks(db: Session = Depends(get_db)):
    return db.query(Stock).count()

@router.get("/status")
def status():
    return {"status": "ok"}
