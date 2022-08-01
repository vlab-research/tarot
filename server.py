import json
import os
from enum import Enum
from functools import cache
from typing import Any, Optional

import psycopg
from fastapi import FastAPI, HTTPException
from psycopg_pool import ConnectionPool
from pydantic.dataclasses import dataclass

from db import insert_optional_query, manyify, query
from treatment_assignment import (TreatmentAssignment,
                                  TreatmentAssignmentRequest, assign)

app = FastAPI()

JsonConfig = dict[str, Any]


class Strategy(Enum):
    SIMPLE = "SIMPLE"
    COMPLETE = "COMPLETE"


@dataclass
class Study:
    strategy: Strategy
    config: JsonConfig
    id: Optional[str] = None
    seed: Optional[int] = None


@cache
def get_pool():
    conninfo = os.environ["DB_CONN_INFO"]
    # conninfo = "postgresql://root@localhost:5434/sorting_hat"
    pool = ConnectionPool(conninfo)
    return pool


pool = get_pool()


@app.get("/health")
def health():
    return "OK"


@app.post("/studies", status_code=201)
def create_study(study: Study):

    vals = {
        "strategy": study.strategy.value,
        "id": study.id,
        "seed": study.seed,
        "config": json.dumps(study.config),
    }

    qq, vals = insert_optional_query(vals)
    q = "insert into studies" + qq + " returning *"

    print(q)
    print(vals)

    try:
        r = list(query(pool, q, vals, as_dict=True))
        return r[0]
    except psycopg.errors.UniqueViolation as e:
        raise HTTPException(status_code=400, detail="Study already exists") from e


def get_strategy(study_id) -> tuple[str, dict[str, Any]]:
    q = "select strategy, config from studies where id = %s limit 1"
    r = list(query(pool, q, [study_id]))

    if not r:
        raise HTTPException(status_code=404, detail=f"Study {study_id} does not exist")

    return r[0]


def get_all_assignments(study_id):
    # get all previous assignments
    q = """
    select user_id, treatment, batch, covariates
    from treatments
    where study_id = %s
    """

    res = query(pool, q, [study_id], as_dict=True)
    r = [TreatmentAssignment(**d) for d in res]

    return r


def insert_assignments(
    study_id: str, assignments: list[TreatmentAssignment]
) -> list[TreatmentAssignment]:
    q = """
    insert into treatments(study_id, user_id, treatment, batch, covariates)
    """

    if not assignments:
        return []

    vals = [
        [study_id, a.user_id, a.treatment, a.batch, a.covariates] for a in assignments
    ]

    q, vals = manyify(q, vals)
    q += " returning user_id, batch, treatment, covariates"
    res = list(query(pool, q, vals, as_dict=True))
    return [TreatmentAssignment(**r) for r in res]


@app.post("/treatment/{study_id}", status_code=201)
def assign_treatment(study_id: str, assignments: list[TreatmentAssignmentRequest]):
    strategy, config = get_strategy(study_id)
    prev_assignments = get_all_assignments(study_id)
    new_assignments = assign(strategy, config, prev_assignments, assignments)
    res = insert_assignments(study_id, new_assignments)
    return res
