"""Validate the complete distribution set and write its release manifest.

This does not upload anything or execute code from a distribution. Checksums
provide an integrity check, not a signature or authorization to publish.
"""

import argparse
from email.parser import BytesParser
import hashlib
import json
import os
from pathlib import Path
import re
import tarfile
import zipfile

from packaging.utils import parse_sdist_filename, parse_wheel_filename
from packaging.version import Version


# Keep in sync with pyproject.toml and .circleci/config.yml.
PYTHONS = ("cp39", "cp310", "cp311", "cp312", "cp313")
TARGETS = ("linux-x86_64", "linux-aarch64", "macos-x86_64",
           "macos-arm64", "windows-amd64")
SOURCE_FILES = {
    "pyproject.toml", "setup.py", "README.rst", "LICENSE",
    "pynuodb/_fetch.pyx", "pynuodb/_fetch.c", "pynuodb/_cutil.h",
    "pynuodb/_codes.pxi", "pynuodb/_resultset.pxi",
    "tests/__init__.py", "tests/conftest.py", "tests/nuodb_base.py",
    "tests/nuodb_cython_test.py", "tests/nuodb_crypt_test.py",
    "tests/nuodb_datatype_ticks_test.py",
    "tests/holmes.txt", "tests/640px-Starling.JPG",
}


RELEASE_TAG = re.compile(r"refs/tags/v?(?P<release>\d+(?:\.\d+)*)(?:-(?i:rc)(?P<rc>\d+))?")


def release_tag_version(ref):
    match = RELEASE_TAG.fullmatch(ref)
    if match is None:
        return None
    label = match["release"]
    rc = match["rc"]
    if rc is not None:
        if rc.startswith("0"):
            raise ValueError("Release candidate numbers start at rc1 without leading zeros: " + ref)
        label += "rc" + rc
    return Version(label)


def package_version(data):
    metadata = BytesParser().parsebytes(data)
    if metadata["Name"] != "pynuodb" or not metadata["Version"]:
        raise ValueError("Distribution metadata must identify pynuodb and its version")
    return Version(metadata["Version"])


def wheel_target(platform):
    for arch in ("x86_64", "aarch64"):
        if platform in ("manylinux2014_" + arch, "manylinux_2_17_" + arch):
            return "linux-" + arch
    for arch in ("x86_64", "arm64"):
        if platform.startswith("macosx_") and platform.endswith("_" + arch):
            return "macos-" + arch
    if platform == "win_amd64":
        return "windows-amd64"
    raise ValueError("Unexpected wheel platform: " + platform)


def validate_sdist(path):
    name, version = parse_sdist_filename(path.name)
    if name != "pynuodb":
        raise ValueError("Unexpected source distribution: " + path.name)
    with tarfile.open(path) as archive:
        files = {member.name: member for member in archive.getmembers() if member.isfile()}
        metadata = [name for name in files
                    if name.endswith("/PKG-INFO") and name.count("/") == 1]
        if len(metadata) != 1:
            raise ValueError("Source distribution must contain one top-level PKG-INFO")
        root = metadata[0].split("/")[0]
        missing = SOURCE_FILES - {name[len(root) + 1:] for name in files
                                  if name.startswith(root + "/")}
        if missing:
            raise ValueError("Source distribution is missing: " + ", ".join(sorted(missing)))
        with archive.extractfile(files[metadata[0]]) as stream:
            if package_version(stream.read()) != version:
                raise ValueError("Source distribution filename/metadata version mismatch")
    return version


def validate_wheel(path, version):
    name, file_version, _, tags = parse_wheel_filename(path.name)
    if name != "pynuodb" or file_version != version:
        raise ValueError("Wheel name/version does not match source distribution: " + path.name)
    targets = set()
    for tag in tags:
        if tag.interpreter not in PYTHONS or tag.abi != tag.interpreter:
            raise ValueError("Unexpected Python ABI: " + str(tag))
        targets.add((tag.interpreter, wheel_target(tag.platform)))
    if len(targets) != 1:
        raise ValueError("Wheel must cover exactly one Python/target pair: " + path.name)
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        metadata = [name for name in names if name.endswith(".dist-info/METADATA")]
        if len(metadata) != 1 or package_version(archive.read(metadata[0])) != version:
            raise ValueError("Wheel metadata mismatch: " + path.name)
        if not any(name.startswith("pynuodb/_fetch.") and name.endswith((".so", ".pyd"))
                   for name in names):
            raise ValueError("Wheel has no compiled _fetch extension: " + path.name)
    return targets.pop()


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def build_manifest(bundle, provenance):
    dist = bundle / "dist"
    files = sorted(dist.iterdir())
    if any(path.is_symlink() or not path.is_file() for path in files):
        raise ValueError("dist/ must contain only regular distribution files")
    sdists = [path for path in files if path.name.endswith(".tar.gz")]
    wheels = [path for path in files if path.suffix == ".whl"]
    if len(sdists) != 1 or len(files) != len(sdists) + len(wheels):
        raise ValueError("Expected one source archive and only wheel files in dist/")
    version = validate_sdist(sdists[0])
    ref = provenance["ref"]
    if ref.startswith("refs/tags/") and release_tag_version(ref) != version:
        raise ValueError("Release tag does not match distribution version")

    expected = {(python, target) for python in PYTHONS for target in TARGETS}
    found = set()
    for wheel in wheels:
        pair = validate_wheel(wheel, version)
        if pair in found:
            raise ValueError("Duplicate wheel coverage: " + str(pair))
        found.add(pair)
    if found != expected:
        raise ValueError("Incomplete wheel matrix; missing: " + str(sorted(expected - found)))

    manifest = {
        "schema_version": 1,
        "package": "pynuodb",
        "version": str(version),
        "prerelease": version.is_prerelease,
        "source": provenance,
        "files": [
            {"filename": path.name, "size": path.stat().st_size, "sha256": sha256(path)}
            for path in files
        ],
    }
    return manifest


def github_actions_provenance():
    required = (
        "GITHUB_REPOSITORY", "GITHUB_SHA", "GITHUB_REF", "GITHUB_RUN_ID",
        "GITHUB_RUN_NUMBER", "GITHUB_WORKFLOW", "GITHUB_JOB",
    )
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise ValueError("Missing GitHub Actions provenance: " + ", ".join(missing))
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com").rstrip("/")
    run_id = os.environ["GITHUB_RUN_ID"]
    ref = os.environ["GITHUB_REF"]
    pull_request = ""
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if event_path and Path(event_path).is_file():
        with open(event_path, "r", encoding="utf-8") as stream:
            event = json.load(stream)
        pull_number = (event.get("pull_request") or {}).get("number")
        if pull_number:
            pull_request = "%s/%s/pull/%s" % (server, os.environ["GITHUB_REPOSITORY"], pull_number)
    return {
        "provider": "github-actions",
        "repository": os.environ["GITHUB_REPOSITORY"],
        "commit": os.environ["GITHUB_SHA"],
        "ref": ref,
        "run_id": run_id,
        "run_number": os.environ["GITHUB_RUN_NUMBER"],
        "workflow": os.environ["GITHUB_WORKFLOW"],
        "job_name": os.environ["GITHUB_JOB"],
        "job_url": "%s/%s/actions/runs/%s" % (server, os.environ["GITHUB_REPOSITORY"], run_id),
        "pull_request": pull_request,
    }


def circleci_provenance():
    """Record the exact workflow and artifact-producing job, including reruns."""
    required = (
        "CIRCLE_PROJECT_USERNAME", "CIRCLE_PROJECT_REPONAME", "CIRCLE_SHA1",
        "CIRCLE_WORKFLOW_ID", "CIRCLE_BUILD_NUM", "CIRCLE_BUILD_URL", "CIRCLE_JOB",
        "RELEASE_PIPELINE_ID", "RELEASE_PIPELINE_NUMBER",
    )
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise ValueError("Missing CircleCI provenance: " + ", ".join(missing))
    tag, branch = os.environ.get("CIRCLE_TAG"), os.environ.get("CIRCLE_BRANCH")
    if not tag and not branch:
        raise ValueError("CircleCI must identify a tag or branch")
    return {
        "provider": "circleci",
        "repository": os.environ["CIRCLE_PROJECT_USERNAME"] + "/" + os.environ["CIRCLE_PROJECT_REPONAME"],
        "commit": os.environ["CIRCLE_SHA1"],
        "ref": "refs/tags/" + tag if tag else "refs/heads/" + branch,
        "pipeline_id": os.environ["RELEASE_PIPELINE_ID"],
        "pipeline_number": os.environ["RELEASE_PIPELINE_NUMBER"],
        "workflow_id": os.environ["CIRCLE_WORKFLOW_ID"],
        "job_number": os.environ["CIRCLE_BUILD_NUM"],
        "job_name": os.environ["CIRCLE_JOB"],
        "job_url": os.environ["CIRCLE_BUILD_URL"],
        # Informational only: an empty PR URL is NOT authorization to publish.
        "pull_request": os.environ.get("CIRCLE_PULL_REQUEST", ""),
    }


def ci_provenance():
    if os.environ.get("GITHUB_ACTIONS") == "true":
        return github_actions_provenance()
    return circleci_provenance()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path, help="directory containing dist/")
    args = parser.parse_args()
    manifest = build_manifest(args.bundle, ci_provenance())
    (args.bundle / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    checksums = "".join("{}  dist/{}\n".format(file["sha256"], file["filename"])
                        for file in manifest["files"])
    (args.bundle / "SHA256SUMS").write_text(checksums)
    print("Validated {} distributions for pynuodb {}".format(
        len(manifest["files"]), manifest["version"]))


if __name__ == "__main__":
    main()
