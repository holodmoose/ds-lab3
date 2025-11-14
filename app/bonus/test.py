from datetime import datetime
from uuid import uuid4
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import sessionmaker
import os

os.environ["TESTING"] = "True"

from main import app, get_db, Base, PrivilegeDb, PrivilegeHistoryDb, engine

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
def sample_privilege(db_session):
    privilege = PrivilegeDb(username="api_user", status="BRONZE", balance=100)
    db_session.add(privilege)
    db_session.commit()

    history = PrivilegeHistoryDb(
        privilege_id=privilege.id,
        ticket_uid=uuid4(),
        datetime=datetime.now(),
        balance_diff=+100,
        operation_type="FILL_IN_BALANCE",
    )
    db_session.add(history)
    db_session.commit()

    return privilege, history


# GET /privilege/{username}


def test_get_privilege(client, sample_privilege):
    privilege, _ = sample_privilege

    response = client.get(f"/privilege/{privilege.username}")
    assert response.status_code == 200

    data = response.json()
    assert data["username"] == privilege.username
    assert data["status"] == "BRONZE"
    assert data["balance"] == 100


def test_get_privilege_not_found(client):
    response = client.get("/privilege/unknown_user")
    assert response.status_code == 404


# GET /privilege/{username}/history


def test_get_privilege_history_list(client, sample_privilege):
    privilege, history = sample_privilege

    response = client.get(f"/privilege/{privilege.username}/history")
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["operation_type"] == "FILL_IN_BALANCE"
    assert data[0]["balance_diff"] == 100


# GET /privilege/{username}/history/{ticket_uid}


def test_get_specific_history(client, sample_privilege):
    privilege, history = sample_privilege

    response = client.get(
        f"/privilege/{privilege.username}/history/{history.ticket_uid}"
    )
    assert response.status_code == 200

    data = response.json()
    assert data["ticket_uid"] == str(history.ticket_uid)
    assert data["operation_type"] == "FILL_IN_BALANCE"


def test_get_specific_history_not_found(client, sample_privilege):
    privilege, _ = sample_privilege
    response = client.get(f"/privilege/{privilege.username}/history/{uuid4()}")
    assert response.status_code == 404


# POST /privilege/{username}/history


def test_create_privilege_history(client, sample_privilege):
    privilege, _ = sample_privilege
    ticket_uid = str(uuid4())

    payload = {
        "ticket_uid": ticket_uid,
        "balance_diff": 50,
        "operation_type": "DEBIT_THE_ACCOUNT",
        "privilege_id": privilege.id,
        "datetime": datetime.now().isoformat(),
    }

    response = client.post(f"/privilege/{privilege.username}/history", json=payload)
    assert response.status_code == 201

    check_privilege = client.get(f"/privilege/{privilege.username}")
    assert check_privilege.status_code == 200
    updated = check_privilege.json()
    assert updated["balance"] == 50


def test_create_privilege_history_invalid_type(client, sample_privilege):
    privilege, _ = sample_privilege
    payload = {
        "ticket_uid": str(uuid4()),
        "balance_diff": 100,
        "operation_type": "BONUS_REWARD",  # invalid type
    }

    response = client.post(f"/privilege/{privilege.username}/history", json=payload)
    assert response.status_code == 400 or response.status_code == 422


# DELETE /privilege/{username}/history/{ticket_uid}


def test_delete_privilege_history(client, sample_privilege):
    privilege, history = sample_privilege

    response = client.delete(
        f"/privilege/{privilege.username}/history/{history.ticket_uid}"
    )
    assert response.status_code == 204

    check = client.get(f"/privilege/{privilege.username}/history/{history.ticket_uid}")
    assert check.status_code == 404


def test_delete_nonexistent_history(client, sample_privilege):
    privilege, _ = sample_privilege
    response = client.delete(f"/privilege/{privilege.username}/history/{uuid4()}")
    assert response.status_code == 404


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
