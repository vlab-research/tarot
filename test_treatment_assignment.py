from toolz import countby

from treatment_assignment import (TreatmentAssignment,
                                  TreatmentAssignmentRequest, assign)


def _tar(user_id, covariates={}):
    return TreatmentAssignmentRequest(user_id, covariates=covariates)


def _ta(user_id, treatment, batch=1, covariates={}):
    return TreatmentAssignment(
        user_id=user_id, treatment=treatment, batch=batch, covariates=covariates
    )


def test_simple_assign_assigns_all_new():
    new_assignments = assign(
        "SIMPLE", {"treatments": ["0", "1"]}, [], [_tar("foo"), _tar("bar")]
    )
    assert len(new_assignments) == 2
    for a in new_assignments:
        assert isinstance(a.treatment, str)
        assert a.batch == 1


def test_simple_assign_new_batch_when_already_assigned():
    new_assignments = assign(
        "SIMPLE",
        {"treatments": ["0", "1"]},
        [_ta("foo", "0")],
        [_tar("foo"), _tar("bar")],
    )

    assert len(new_assignments) == 1
    a = new_assignments[0]
    a.batch == 2


def test_complete_assign_assigns_all_new():
    new_assignments = assign(
        "COMPLETE", {"treatments": ["0", "1"]}, [], [_tar("foo"), _tar("bar")]
    )
    assert len(new_assignments) == 2
    assert {a.treatment for a in new_assignments} == {"0", "1"}

    for a in new_assignments:
        assert isinstance(a.treatment, str)
        assert a.batch == 1


def test_complete_assign_assigns_new_to_balance_previous_assignments():
    new_assignments = assign(
        "COMPLETE",
        {"treatments": ["0", "1"]},
        [_ta("foo", "0")],
        [_tar("foo"), _tar("bar")],
    )
    assert len(new_assignments) == 1
    assert new_assignments[0].treatment == "1"
    assert new_assignments[0].batch == 2


def test_complete_assign_assigns_many_new_to_balance_previous_assignments():
    new_assignments = assign(
        "COMPLETE",
        {"treatments": ["0", "1"]},
        [_ta("foo", "0")],
        [_tar("bar"), _tar("baz"), _tar("qux")],
    )
    assert len(new_assignments) == 3
    assert countby(lambda x: x.treatment, new_assignments) == {"0": 1, "1": 2}
    for a in new_assignments:
        assert a.batch == 2
