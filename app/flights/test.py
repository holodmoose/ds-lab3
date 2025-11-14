from datetime import datetime
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import sessionmaker
import os

os.environ["TESTING"] = "True"

from main import app, get_db, Base, FlightDb, AirportDb, engine

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
def sample_data(db_session):
    airport1 = AirportDb(name="Шереметьево", city="Москва", country="Россия")
    airport2 = AirportDb(name="Пулково", city="Санкт-Петербург", country="Россия")
    db_session.add(airport1)
    db_session.add(airport2)
    db_session.commit()
    flight = FlightDb(
        flight_number="AFL031",
        datetime=datetime.now(),
        from_airport_id=airport1.id,
        to_airport_id=airport2.id,
        price=1500,
    )
    db_session.add(flight)
    db_session.commit()
    return airport1, airport2, flight


def test_get_flights(client, sample_data):
    _, _, flight = sample_data

    response = client.get(f"/flights")
    assert response.status_code == 200

    data = response.json()
    assert data["totalElements"] == 1
    assert len(data["items"]) == 1
    assert data["items"][0]["flightNumber"] == flight.flight_number
    assert data["items"][0]["fromAirport"] == "Москва Шереметьево"
    assert data["items"][0]["toAirport"] == "Санкт-Петербург Пулково"


def test_get_flight(client, sample_data):
    _, _, flight = sample_data

    response = client.get(f"/flights/{flight.flight_number}")
    assert response.status_code == 200

    data = response.json()
    assert data["flightNumber"] == flight.flight_number
    assert data["fromAirport"] == "Москва Шереметьево"
    assert data["toAirport"] == "Санкт-Петербург Пулково"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
