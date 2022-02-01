import json
import uuid

import pytest
from fastapi.testclient import TestClient

from db import execute, query
from server import app, get_all_assignments, pool


def _reset_db():
    with pool.connection() as conn:
        with conn.cursor() as cur:
            tables = ["treatments", "studies"]
            for t in tables:
                cur.execute(f"delete from {t}")
                conn.commit()


@pytest.fixture
def client():
    c = TestClient(app)
    yield c
    _reset_db()


def test_create_study_creates_seed_if_not_given(client):
    res = client.post(
        "/studies", json={"strategy": "SIMPLE", "config": {"treatments": ["a", "b"]}}
    )
    assert res.status_code == 201
    r = res.json()
    assert isinstance(r["seed"], int)
    assert isinstance(r["id"], str)

    studies = list(query(pool, "select * from studies", [], as_dict=True))
    assert len(studies) == 1


def test_create_study_uses_seed_if_given(client):
    res = client.post(
        "/studies",
        json={"strategy": "SIMPLE", "config": {"treatments": ["a", "b"]}, "seed": 123},
    )
    assert res.status_code == 201
    r = res.json()
    assert r["seed"] == 123

    studies = list(query(pool, "select * from studies", [], as_dict=True))
    assert len(studies) == 1


def test_create_study_uses_id_if_given(client):
    id_ = str(uuid.uuid4())
    res = client.post(
        "/studies",
        json={
            "strategy": "SIMPLE",
            "config": {"treatments": ["a", "b"]},
            "seed": 123,
            "id": id_,
        },
    )
    assert res.status_code == 201
    r = res.json()
    assert r["id"] == id_

    studies = list(query(pool, "select * from studies", [], as_dict=True))
    assert len(studies) == 1


def test_create_study_raises_if_not_proper_strategy(client):
    res = client.post("/studies", json={"strategy": "NOT_A_STRATEGY", "config": {}})
    assert res.status_code == 422
    r = res.json()
    assert "SIMPLE" in r["detail"][0]["msg"]


def test_create_study_raises_if_already_exists(client):
    id_ = str(uuid.uuid4())
    _ = client.post(
        "/studies",
        json={
            "strategy": "SIMPLE",
            "config": {"treatments": ["a", "b"]},
            "seed": 123,
            "id": id_,
        },
    )
    res = client.post(
        "/studies", json={"strategy": "SIMPLE", "seed": 123, "id": id_, "config": {}}
    )

    studies = list(query(pool, "select * from studies", [], as_dict=True))
    assert len(studies) == 1

    assert res.status_code == 400
    r = res.json()
    assert "exists" in r["detail"].lower()


def test_assign_treatment_to_non_existent_study_returns_404(client):
    id_ = str(uuid.uuid4())
    res = client.post(f"/treatment/{id_}", json=[{"user_id": "foo"}])
    assert res.status_code == 404


def test_post_treatment_only_returns_assgined_treatments(client):
    id_ = str(uuid.uuid4())

    execute(
        pool,
        "insert into studies(id, strategy, config) values (%s, %s, %s)",
        [id_, "COMPLETE", json.dumps({"treatments": ["a", "b"]})],
    )

    execute(
        pool,
        "insert into treatments(study_id, user_id, treatment, batch) values (%s, %s, %s, %s)",
        [id_, "foo", "a", 1],
    )

    res = client.post(
        f"/treatment/{id_}", json=[{"user_id": "foo"}, {"user_id": "bar"}]
    )
    assert res.status_code == 201

    assert res.json() == [
        {"user_id": "bar", "treatment": "b", "batch": 2, "covariates": None}
    ]
