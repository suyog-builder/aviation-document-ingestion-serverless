"""Smoke test: the local end-to-end sample run indexes every sample document."""

from samples.run_local_pipeline import run


def test_all_samples_get_indexed(env, capsys):
    statuses = run(env)
    assert all(v.startswith("INDEXED") for v in statuses.values()), statuses
