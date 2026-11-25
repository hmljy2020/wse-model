"""Exercise the SST entry point and independently challenge its evidence checker."""

from __future__ import annotations

import csv
import json
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration
CALENDAR = Path(__file__).resolve().parents[2] / "sst/calendar"
INPUTS = CALENDAR / "inputs"
ARGS = [
    INPUTS / name
    for name in ("workload-64.json", "architecture-line.json", "deployment-line-64.json")
]


@pytest.fixture(scope="module")
def suite(tmp_path_factory):
    if not (CALENDAR.parents[2] / "SST/env.sh").is_file():
        pytest.skip("requires the workspace SST Core installation")
    output = tmp_path_factory.mktemp("calendar")
    subprocess.run([CALENDAR / "run.sh", "--suite", "--out", output], check=True, timeout=180)
    return output


def test_suite(suite):
    summary = json.loads((suite / "suite.json").read_text())
    assert len(summary) == 8 and all(r["passed"] for r in summary.values())
    assert "V-TREE-ACYCLIC" in summary["invalid"]["diagnostic"]
    for case in ["late", "credit", "duplicate", "delay_ack"]:
        report = json.loads((suite / case / "report.json").read_text())
        trace = json.loads((suite / case / "trace.json").read_text())
        [highlight] = report["highlights"]
        event = next(e for e in trace["events"] if e["id"] == highlight["id"])
        assert event["at"] == str(highlight["cycle"])
        assert highlight["replay"] == "replay.html#event=" + event["id"]


@pytest.mark.parametrize(
    ("kind", "field", "value"),
    [
        ("arrive", "cause", "-1"),
        ("release", "flit", "999"),
        ("shared.ack", "address", "0"),
        ("tile.publish", "detail", "00"),
        ("consumer.read", "cause", "-1"),
        ("drain", "detail", "aborted"),
    ],
)
def test_audit_rejects_corrupt_evidence(suite, tmp_path, kind, field, value):
    with (suite / "A/events.tsv").open() as source:
        rows = list(csv.DictReader(source, delimiter="\t"))
    event = next(r for r in rows if r["kind"] == kind)
    event[field] = value
    path = tmp_path / "changed.tsv"
    with path.open("w") as out:
        writer = csv.DictWriter(out, fieldnames=rows[0].keys(), delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    result = subprocess.run(
        [CALENDAR / "run.sh", *ARGS, "--check-events", path],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert f"e{event['id']}:" in json.loads(result.stdout)["error"]


@pytest.mark.parametrize("edit", ["mapping", "port", "latency", "collision", "credit"])
def test_preflight_rejects_invalid_reservations(monkeypatch, edit):
    monkeypatch.syspath_prepend(str(CALENDAR))
    from model import Model, read

    workload, arch, deployment = map(read, ARGS)
    routes = deployment["routes"]
    if edit == "mapping":
        deployment["mapping"]["B"] = 0
    elif edit == "port":
        routes["A"]["reservations"][1][1].remove("local")
    elif edit == "latency":
        routes["A"]["reservations"][1][2][0] = 2
    elif edit == "collision":
        routes["C"]["reservations"][1][2][0] = 6
    else:
        arch["ingress_depth"] = 1
        arch["credit_cycles"] = 24
    with pytest.raises(ValueError):
        Model(workload, arch, deployment, {})
