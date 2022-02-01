import random
# from pydantic.dataclasses import dataclass
from dataclasses import dataclass
from datetime import datetime
from itertools import repeat
from math import ceil
from typing import Any, Optional, Union

from pydantic import BaseModel
from toolz import countby, map


@dataclass
class Covariate:
    var: str
    val: Any


Covariates = dict[str, Any]


@dataclass
class TreatmentAssignmentRequest:
    user_id: str
    covariates: Optional[Covariates] = None


class TreatmentAssignment(BaseModel):
    user_id: str
    treatment: str
    batch: int
    covariates: Optional[Covariates] = None


@dataclass
class SimpleConfig:
    treatments: list[str]


StrategyConfig = Union[SimpleConfig]


def _get_next_batch(prev_assignments: list[TreatmentAssignment]) -> int:
    batches = {t.batch for t in prev_assignments}
    last_batch = max(batches) if batches else 0
    return last_batch + 1


def simple_assign(
    config: SimpleConfig,
    prev_assignments: list[TreatmentAssignment],
    new_requests: list[TreatmentAssignmentRequest],
) -> list[TreatmentAssignment]:

    treatments = config.treatments
    new_batch = _get_next_batch(prev_assignments)

    assigned = {t.user_id for t in prev_assignments}
    newnew = [r for r in new_requests if r.user_id not in assigned]

    new_assignments = [
        TreatmentAssignment(
            user_id=r.user_id,
            treatment=random.choice(treatments),
            batch=new_batch,
            covariates=r.covariates,
        )
        for r in newnew
    ]

    return new_assignments


# TODO: clean this up, abstract, allow options for dealing
#       with stragglers, consider pandas implementation or
#       implementations that also just work for stratified.
def complete_assign(
    config: SimpleConfig,
    prev_assignments: list[TreatmentAssignment],
    new_requests: list[TreatmentAssignmentRequest],
) -> list[TreatmentAssignment]:

    treatments = config.treatments
    new_batch = _get_next_batch(prev_assignments)

    assigned = {t.user_id for t in prev_assignments}
    newnew = [r for r in new_requests if r.user_id not in assigned]

    group_sizes = countby(lambda x: x.treatment, prev_assignments)
    group_sizes = {k: group_sizes[k] if k in group_sizes else 0 for k in treatments}

    N = sum(group_sizes.values()) + len(newnew)
    goal = N / len(treatments)

    left = {k: ceil(goal - v) for k, v in group_sizes.items()}
    ks = random.sample(left.keys(), k=len(left))

    assignments = [r for k in ks for r in repeat(k, left[k])]
    assignments = assignments[: len(newnew) + 1]

    randomized = random.sample(newnew, k=len(newnew))

    return [
        TreatmentAssignment(
            user_id=r.user_id, treatment=a, batch=new_batch, covariates=r.covariates
        )
        for r, a in zip(randomized, assignments)
    ]


lookup = {
    "SIMPLE": (simple_assign, SimpleConfig),
    "COMPLETE": (complete_assign, SimpleConfig),
}


def assign(
    strategy: str,
    config: dict[str, Any],
    prev_assignments: list[TreatmentAssignment],
    new_requests: list[TreatmentAssignmentRequest],
) -> list[TreatmentAssignment]:

    fn, type_ = lookup[strategy]
    conf = type_(**config)

    return fn(conf, prev_assignments, new_requests)
