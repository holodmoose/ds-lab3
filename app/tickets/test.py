from datetime import datetime
from uuid import uuid4
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import sessionmaker
import os

os.environ["TESTING"] = "True"

from main import app, get_db, Base, TicketDb, engine

TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db


@pytest.fixture(scope="function")
def db_session():
    Base.metadata.create_all(bind=engine)

    session = TestingSessionLocal()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(bind=engine)


@pytest.fixture
def client():
    Base.metadata.create_all(bind=engine)
    yield TestClient(app)
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def test_ticket(db_session):
    ticket = TicketDb(
        ticket_uid=uuid4(),
        username="moose",
        flight_number="AAAA",
        price=1000,
        status="PAID",
    )
    db_session.add(ticket)
    db_session.commit()

    return ticket


def test_get_user_ticket_invalid(client):
    response = client.get(f"/tickets/user/213")
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_get_user_ticket_invalid(client):
    response = client.get(f"/tickets/{uuid4()}")
    assert response.status_code == 404


def test_get_user_ticket(client, test_ticket):
    response = client.get(f"/tickets/user/{test_ticket.username}")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["username"] == test_ticket.username
    assert data[0]["flight_number"] == test_ticket.flight_number


def test_get_ticket(client, test_ticket):
    response = client.get(f"/tickets/{test_ticket.ticket_uid}")
    assert response.status_code == 200
    data = response.json()
    assert data["username"] == test_ticket.username
    assert data["flight_number"] == test_ticket.flight_number


def test_post_ticket(client):
    uid = uuid4()
    response = client.post(
        f"/tickets/",
        json={
            "ticketUid": str(uid),
            "username": "moose",
            "flightNumber": "AAAA",
            "price": 1000,
        },
    )
    assert response.status_code == 201
    response = client.get(f"/tickets/{uid}")
    assert response.status_code == 200
    data = response.json()
    assert data["username"] == "moose"
    assert data["flight_number"] == "AAAA"
