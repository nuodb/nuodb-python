"""Tests for release validation, independent of a compiler or NuoDB server."""

import hashlib
import io
import json
import tarfile
import zipfile

import pytest

from ci import release_bundle as release


VERSION = "5.0"
PROVENANCE = {"ref": "refs/tags/v5.0", "commit": "a" * 40, "workflow_id": "workflow-123"}


def metadata(version=VERSION):
    return ("Metadata-Version: 2.1\nName: pynuodb\nVersion: " + version + "\n").encode()


def write_sdist(dist, missing=None):
    path = dist / ("pynuodb-" + VERSION + ".tar.gz")
    with tarfile.open(path, "w:gz") as archive:
        files = {name: b"placeholder\n" for name in release.SOURCE_FILES if name != missing}
        files["PKG-INFO"] = metadata()
        for name, data in files.items():
            member = tarfile.TarInfo("pynuodb-" + VERSION + "/" + name)
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
    return path


def write_wheel(dist, python="cp39", platform="win_amd64", version=VERSION,
                compiled=True, metadata_version=VERSION, abi=None):
    filename = "pynuodb-{}-{}-{}-{}.whl".format(version, python, abi or python, platform)
    path = dist / filename
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("pynuodb-{}.dist-info/METADATA".format(version),
                         metadata(metadata_version))
        if compiled:
            extension = ".pyd" if platform == "win_amd64" else ".so"
            archive.writestr("pynuodb/_fetch." + python + extension, b"binary placeholder")
    return path


@pytest.fixture
def bundle(tmp_path):
    dist = tmp_path / "dist"
    dist.mkdir()
    write_sdist(dist)
    for python in release.PYTHONS:
        for platform in ("manylinux_2_17_x86_64.manylinux2014_x86_64",
                         "manylinux_2_17_aarch64.manylinux2014_aarch64",
                         "macosx_10_13_x86_64", "macosx_11_0_arm64", "win_amd64"):
            write_wheel(dist, python=python, platform=platform)
    return tmp_path


def test_complete_release(bundle):
    manifest = release.build_manifest(bundle, PROVENANCE)
    assert manifest["version"] == VERSION
    assert manifest["source"] == PROVENANCE
    assert len(manifest["files"]) == 26
    assert manifest["files"] == sorted(manifest["files"], key=lambda entry: entry["filename"])
    for entry in manifest["files"]:
        data = (bundle / "dist" / entry["filename"]).read_bytes()
        assert entry["size"] == len(data)
        assert entry["sha256"] == hashlib.sha256(data).hexdigest()


def test_missing_wheel(bundle):
    next((bundle / "dist").glob("*.whl")).unlink()
    with pytest.raises(ValueError, match="Incomplete wheel matrix"):
        release.build_manifest(bundle, PROVENANCE)


def test_duplicate_coverage(bundle):
    write_wheel(bundle / "dist", platform="macosx_10_15_x86_64")
    with pytest.raises(ValueError, match="Duplicate wheel coverage"):
        release.build_manifest(bundle, PROVENANCE)


@pytest.mark.parametrize("kwargs, message", [
    ({"version": "6.0"}, "name/version"),
    ({"metadata_version": "6.0"}, "metadata mismatch"),
    ({"compiled": False}, "no compiled"),
    ({"python": "cp314"}, "Unexpected Python ABI"),
    ({"abi": "abi3"}, "Unexpected Python ABI"),
    ({"python": "py3", "abi": "none", "platform": "any"}, "Unexpected Python ABI"),
    ({"platform": "linux_x86_64"}, "Unexpected wheel platform"),
    ({"platform": "musllinux_1_2_x86_64"}, "Unexpected wheel platform"),
])
def test_reject_bad_wheel(tmp_path, kwargs, message):
    path = write_wheel(tmp_path, **kwargs)
    with pytest.raises(ValueError, match=message):
        release.validate_wheel(path, release.Version(VERSION))


@pytest.mark.parametrize("missing", ["pynuodb/_fetch.c", "pynuodb/_cutil.h",
                                    "pynuodb/_resultset.pxi", "tests/conftest.py"])
def test_missing_sdist_source(tmp_path, missing):
    path = write_sdist(tmp_path, missing)
    with pytest.raises(ValueError, match="Source distribution is missing"):
        release.validate_sdist(path)


def test_wrong_tag(bundle):
    with pytest.raises(ValueError, match="tag does not match"):
        release.build_manifest(bundle, {"ref": "refs/tags/v6.0"})


def test_unexpected_file(bundle):
    (bundle / "dist" / "notes.txt").write_text("not a distribution")
    with pytest.raises(ValueError, match="only wheel files"):
        release.build_manifest(bundle, PROVENANCE)


def test_missing_sdist(bundle):
    next((bundle / "dist").glob("*.tar.gz")).unlink()
    with pytest.raises(ValueError, match="one source archive"):
        release.build_manifest(bundle, PROVENANCE)


@pytest.fixture
def circleci_env(monkeypatch):
    env = {
        "CIRCLE_PROJECT_USERNAME": "nuodb", "CIRCLE_PROJECT_REPONAME": "nuodb-python",
        "CIRCLE_SHA1": "a" * 40, "CIRCLE_TAG": "v5.0.0", "CIRCLE_BRANCH": "",
        "CIRCLE_WORKFLOW_ID": "workflow-123", "CIRCLE_BUILD_NUM": "456",
        "CIRCLE_BUILD_URL": "https://circleci.com/gh/nuodb/nuodb-python/456",
        "CIRCLE_JOB": "assemble_release_bundle", "CIRCLE_PULL_REQUEST": "",
        "RELEASE_PIPELINE_ID": "pipeline-789", "RELEASE_PIPELINE_NUMBER": "789",
    }
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    return env


def test_circleci_provenance(circleci_env):
    source = release.circleci_provenance()
    assert source["provider"] == "circleci"
    assert source["repository"] == "nuodb/nuodb-python"
    assert source["ref"] == "refs/tags/v5.0.0"
    assert source["commit"] == "a" * 40
    assert source["pipeline_id"] == "pipeline-789"
    assert source["workflow_id"] == "workflow-123"


def test_branch_provenance(circleci_env, monkeypatch):
    monkeypatch.setenv("CIRCLE_TAG", "")
    monkeypatch.setenv("CIRCLE_BRANCH", "ci/platform-wheel-artifacts")
    monkeypatch.setenv("CIRCLE_PULL_REQUEST", "https://github.com/nuodb/nuodb-python/pull/218")
    source = release.circleci_provenance()
    assert source["ref"] == "refs/heads/ci/platform-wheel-artifacts"
    assert source["pull_request"].endswith("/218")


def test_tag_takes_precedence(circleci_env, monkeypatch):
    monkeypatch.setenv("CIRCLE_BRANCH", "master")
    assert release.circleci_provenance()["ref"] == "refs/tags/v5.0.0"


@pytest.mark.parametrize("missing", ["CIRCLE_SHA1", "CIRCLE_WORKFLOW_ID", "CIRCLE_BUILD_NUM",
                                    "RELEASE_PIPELINE_ID", "RELEASE_PIPELINE_NUMBER"])
def test_missing_provenance(circleci_env, monkeypatch, missing):
    monkeypatch.delenv(missing)
    with pytest.raises(ValueError, match="Missing CircleCI provenance"):
        release.circleci_provenance()


def test_missing_ref(circleci_env, monkeypatch):
    monkeypatch.setenv("CIRCLE_TAG", "")
    with pytest.raises(ValueError, match="identify a tag or branch"):
        release.circleci_provenance()


def test_cli_writes_manifest_and_checksums(bundle, monkeypatch, circleci_env):
    monkeypatch.setattr("sys.argv", ["release_bundle.py", str(bundle)])
    release.main()
    manifest = json.loads((bundle / "manifest.json").read_text())
    assert manifest["source"]["job_number"] == "456"
    assert manifest["source"]["job_url"] == "https://circleci.com/gh/nuodb/nuodb-python/456"
    checksums = (bundle / "SHA256SUMS").read_text().splitlines()
    assert len(checksums) == 26
    for line, entry in zip(checksums, manifest["files"]):
        assert line == "{}  dist/{}".format(entry["sha256"], entry["filename"])
