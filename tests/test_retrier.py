import pytest
from pathlib import Path
from pipewatch.checker import CheckResult
from pipewatch.retrier import record_attempt, get_state, exceeds_threshold, reset


@pytest.fixture
def retry_dir(tmp_path):
    return tmp_path / "retries"


def _ok(name="pipe_a"):
    return CheckResult(pipeline=name, healthy=True, message="ok")


def _fail(name="pipe_a", msg="row count too low"):
    return CheckResult(pipeline=name, healthy=False, message=msg)


def test_initial_state_is_zero(retry_dir):
    state = get_state("pipe_a", base_dir=retry_dir)
    assert state.attempts == 0
    assert state.last_error is None


def test_record_attempt_increments_on_failure(retry_dir):
    state = record_attempt(_fail(), base_dir=retry_dir)
    assert state.attempts == 1
    assert state.last_error == "row count too low"
    assert not state.resolved


def test_record_attempt_accumulates(retry_dir):
    record_attempt(_fail(), base_dir=retry_dir)
    state = record_attempt(_fail(), base_dir=retry_dir)
    assert state.attempts == 2


def test_record_attempt_resets_on_healthy(retry_dir):
    record_attempt(_fail(), base_dir=retry_dir)
    state = record_attempt(_ok(), base_dir=retry_dir)
    assert state.attempts == 0
    assert state.last_error is None
    assert state.resolved


def test_exceeds_threshold_false_below(retry_dir):
    record_attempt(_fail(), base_dir=retry_dir)
    assert not exceeds_threshold("pipe_a", max_attempts=3, base_dir=retry_dir)


def test_exceeds_threshold_true_at_limit(retry_dir):
    for _ in range(3):
        record_attempt(_fail(), base_dir=retry_dir)
    assert exceeds_threshold("pipe_a", max_attempts=3, base_dir=retry_dir)


def test_reset_clears_state(retry_dir):
    record_attempt(_fail(), base_dir=retry_dir)
    reset("pipe_a", base_dir=retry_dir)
    state = get_state("pipe_a", base_dir=retry_dir)
    assert state.attempts == 0
    assert state.resolved


def test_multiple_pipelines_isolated(retry_dir):
    record_attempt(_fail("pipe_a"), base_dir=retry_dir)
    record_attempt(_fail("pipe_a"), base_dir=retry_dir)
    record_attempt(_fail("pipe_b"), base_dir=retry_dir)
    assert get_state("pipe_a", base_dir=retry_dir).attempts == 2
    assert get_state("pipe_b", base_dir=retry_dir).attempts == 1
