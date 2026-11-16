"""Guard the release matrix and the fan-in gate against configuration drift."""

from pathlib import Path

import pytest
import yaml

from ci import release_bundle as release


@pytest.fixture
def config():
    root = Path(__file__).resolve().parents[1]
    return yaml.safe_load((root / ".circleci/config.yml").read_text())


def workflow_jobs(config):
    return [next(iter(job.items())) for job in config["workflows"]["build-project"]["jobs"]]


def test_wheel_targets_match_validator(config):
    targets = set()
    for kind, job in workflow_jobs(config):
        if kind in ("wheel_linux", "wheel_macos"):
            platform = kind.removeprefix("wheel_")
            targets.add(platform + "-" + job["arch"])
        elif kind == "wheel_windows":
            targets.add("windows-amd64")
    assert targets == set(release.TARGETS)


def test_bundle_requires_all_wheels_and_tests(config):
    jobs = workflow_jobs(config)
    bundle = next(job for kind, job in jobs if kind == "assemble_release_bundle")
    required = {job["name"] for kind, job in jobs
                if kind.startswith("wheel_") or kind in ("build_n_run", "perf_bench")}
    assert set(bundle["requires"]) == required


def test_all_jobs_enable_version_tags(config):
    for _, job in workflow_jobs(config):
        assert job["filters"]["tags"]["only"] == "/^v.*/"


def test_builds_and_regressions_use_shared_sdist(config):
    source = next(job["name"] for kind, job in workflow_jobs(config) if kind == "build_sdist")
    for kind, job in workflow_jobs(config):
        if kind.startswith("wheel_") or kind == "build_n_run":
            assert source in job["requires"]
    command = config["commands"]["build_wheels"]["steps"][1]["run"]["command"]
    assert "workspace/sdist/*.tar.gz" in command
    assert "--platform << parameters.platform >>" in command
    regression = config["jobs"]["build_n_run"]["steps"]
    build = next(step["run"]["command"] for step in regression
                 if isinstance(step, dict) and "run" in step
                 and step["run"]["name"] == "Build, validate, and install the regression wheel")
    assert "pip wheel --no-deps" in build
    assert "workspace/sdist/*.tar.gz" in build


def test_wheel_architectures_and_runners_match(config):
    for kind, job in workflow_jobs(config):
        if kind == "wheel_linux":
            assert job["resource_class"] == {"x86_64": "medium", "aarch64": "arm.medium"}[job["arch"]]
    assert config["jobs"]["wheel_macos"]["resource_class"].startswith("m4pro.")
    assert config["jobs"]["wheel_windows"]["environment"]["CIBW_ARCHS"] == "AMD64"
    assert config["jobs"]["wheel_windows"]["shell"].startswith("bash.exe")


def test_wheel_tests_are_not_skipped(config):
    for kind in ("wheel_linux", "wheel_macos", "wheel_windows"):
        assert not any(name.startswith("CIBW_TEST_SKIP")
                       for name in config["jobs"][kind].get("environment", {}))
    setup = config["jobs"]["wheel_macos"]["steps"][0]["run"]["command"]
    assert "--install-rosetta" in setup
    assert "arch -x86_64 /usr/bin/true" in setup


def test_no_secret_context_on_release_build_jobs(config):
    for kind, job in workflow_jobs(config):
        if kind not in ("build_n_run", "perf_bench"):
            assert "context" not in job


def test_parallel_workspace_paths_are_distinct(config):
    steps = config["commands"]["build_wheels"]["steps"]
    persisted = next(step["persist_to_workspace"] for step in steps if "persist_to_workspace" in step)
    assert persisted == {"root": "workspace", "paths": ["wheels/<< parameters.target >>"]}
