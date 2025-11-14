import uuid
from fastapi import FastAPI, HTTPException, Depends, Path
from sqlalchemy import create_engine, Column, Integer, String, UUID
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from sqlalchemy import Column, Integer, String, StaticPool
from sqlalchemy.orm import declarative_base
import os
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

from common import *

if not os.getenv("TESTING"):
    DB_USER = os.getenv("POSTGRES_USER", "postgres")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
    DB_NAME = os.getenv("POSTGRES_DB", "postgres")
    DB_HOST = os.getenv("DB_HOST", "postgres")
    DB_PORT = os.getenv("DB_PORT", "5432")

    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base = declarative_base()
else:
    Base = declarative_base()

    SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"

    engine = create_engine(
        SQLALCHEMY_DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

app = FastAPI(title="Tickets API")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class TicketDb(Base):
    __tablename__ = "ticket"

    id = Column(Integer, primary_key=True)
    ticket_uid = Column(UUID(as_uuid=True), nullable=False)
    username = Column(String(80), nullable=False)
    flight_number = Column(String(20), nullable=False)
    price = Column(Integer, nullable=False)
    status = Column(String(20), nullable=False)


@app.get("/tickets/user/{username}", response_model=List[Ticket])
def get_tickets_by_user(username: str, db: Session = Depends(get_db)):
    tickets = db.query(TicketDb).filter(TicketDb.username == username).all()
    return tickets


@app.get("/tickets/{ticket_uid}", response_model=Ticket)
def get_ticket_by_uid(ticket_uid: uuid.UUID, db: Session = Depends(get_db)):
    ticket = db.query(TicketDb).filter(TicketDb.ticket_uid == ticket_uid).first()
    if not ticket:
        raise HTTPException(status_code=404, detail="Ticket not found")
    return ticket


@app.post("/tickets", status_code=201)
def create_ticket(request: TicketCreateRequest, db: Session = Depends(get_db)):
    existing = (
        db.query(TicketDb).filter(TicketDb.ticket_uid == request.ticketUid).first()
    )
    if existing:
        raise HTTPException(
            status_code=403, detail="Ticket with this UUID already exists"
        )

    new_ticket = TicketDb(
        ticket_uid=request.ticketUid,
        username=request.username,
        flight_number=request.flightNumber,
        price=request.price,
        status="PAID",
    )
    db.add(new_ticket)
    db.commit()


@app.delete("/tickets/{ticket_uid}", status_code=204)
def delete_ticket(ticket_uid: uuid.UUID, db: Session = Depends(get_db)):
    ticket = db.query(TicketDb).filter(TicketDb.ticket_uid == ticket_uid).first()
    if not ticket:
        raise HTTPException(status_code=404, detail="Ticket not found")

    db.delete(ticket)
    db.commit()


@app.get("/manage/health", status_code=201)
def health():
    pass
