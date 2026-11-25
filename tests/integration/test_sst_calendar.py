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


@pytest.mark.parametrize(("label", "count", "edges"), [("A", 4, 3), ("B", 6, 7)])
def test_assembled_mesh_and_replay(suite, label, count, edges):
    case = suite / "two-threads" / label
    graph = json.loads((case / "sst-topology.json").read_text())
    trace = json.loads((case / "trace.json").read_text())
    nodes = {c["name"]: c for c in graph["components"]}
    assert len(nodes) == count
    assert all(c["type"] == "wsecalendar.node" for c in nodes.values())
    assert set(nodes) == {e["id"] for e in trace["topology"]["entities"]}
    physical = {
        frozenset(w[side]["component"] for side in ("left", "right")) for w in graph["links"]
    }
    replay = {
        frozenset(p.split(".")[0] for p in conn["endpoints"])
        for conn in trace["topology"]["connections"]
    }
    assert len(physical) == edges and physical == replay
    assert nodes["R0"]["partition"]["thread"] != nodes["R1"]["partition"]["thread"]
    # Verify provenance and the cross-thread transfer from the component-owned logs.
    with (case / "R0.tsv").open() as stream:
        send = next(
            r
            for r in csv.DictReader(stream, delimiter="\t")
            if r["kind"] == "send" and r["source"] == "0" and r["flit"] == "0"
        )
    with (case / "R1.tsv").open() as stream:
        arrival = next(
            r for r in csv.DictReader(stream, delimiter="\t") if r["cause"] == send["id"]
        )
    assert arrival["kind"] == "arrive" and int(arrival["cycle"]) == int(send["cycle"]) + 2


@pytest.fixture(scope="module")
def boundary(tmp_path_factory):
    if not (CALENDAR.parents[2] / "SST/env.sh").is_file():
        pytest.skip("requires the workspace SST Core installation")
    output = tmp_path_factory.mktemp("calendar_boundary")
    workload, arch, deploy = (json.loads(p.read_text()) for p in ARGS)
    workload.update(layout=["A", "B"], participants={k: workload["participants"][k] for k in "AB"})
    arch.update(
        columns=2, ingress_depth=1, landing_depth=1, link_cycles=2, credit_cycles=3, shared_cycles=5
    )
    deploy["mapping"] = {"A": 0, "B": 1}
    deploy["routes"] = {
        "A": {
            "opcode": 1,
            "routeBits": "0e",
            "reservations": [
                [0, [1], [1, 6]],
                [1, ["local"], [3, 8]],
            ],
        },
        "B": {
            "opcode": 1,
            "routeBits": "0b",
            "reservations": [
                [1, [0], [1, 6]],
                [0, ["local"], [3, 8]],
            ],
        },
    }
    paths = [output / name for name in ("workload.json", "arch.json", "deploy.json")]
    for path, value in zip(paths, (workload, arch, deploy), strict=True):
        path.write_text(json.dumps(value))
    subprocess.run(
        [CALENDAR / "run.sh", *paths, "--out", output, "--threads", "2"], check=True, timeout=90
    )
    origin = json.loads((output / "report.json").read_text())["origin_cycle"]
    with (output / "R1.tsv").open() as stream:
        return [
            (int(r["cycle"]) - origin, r["kind"], r["flit"])
            for r in csv.DictReader(stream, delimiter="\t")
        ]


def test_arrival_and_shared_ack_on_reserved_cycle(boundary):
    assert boundary.index((3, "arrive", "0")) < boundary.index((3, "land", "0"))
    assert boundary.index((8, "shared.ack", "0")) < boundary.index((8, "land", "1"))


def test_credit_return_on_reserved_cycle(boundary):
    # A single token is available on this egress. f1 needs the credit freed by f0.
    assert boundary.index((6, "credit.return", "-1")) < boundary.index((6, "send", "1"))


@pytest.mark.parametrize("threads", [1, 2])
def test_credit_fatal_evidence(suite, threads):
    case = (suite if threads == 1 else suite / "two-threads") / "credit"
    report = json.loads((case / "report.json").read_text())
    fault = report["first_fault"]
    assert report["sst_exit_code"] == 42 and report["outcome"] == "fatal"
    assert report["drain_cycle"] is None and not report["published_tiles"]
    assert (
        fault["epoch"],
        fault["relative_cycle"],
        fault["port"],
        fault["participant"],
        fault["flit"],
        fault["detail"],
    ) == (1, 8, "R1->R0", "C", 0, "no-credit")
    trace = json.loads((case / "trace.json").read_text())
    assert trace["window"]["end"] == str(fault["cycle"])
    assert trace["events"][-1]["id"] == fault["id"]
    assert all(e["kind"] != "drain" for e in trace["events"])


@pytest.mark.parametrize(("code", "diagnostic"), [(0, True), (-11, True), (42, False)])
def test_fault_exit_requires_code_and_diagnostic(suite, monkeypatch, code, diagnostic):
    monkeypatch.syspath_prepend(str(CALENDAR))
    from run import AuditError, check_exit

    case = suite / "credit"
    report = json.loads((case / "report.json").read_text())
    with pytest.raises(AuditError):
        check_exit(code, (case / "sst.log").read_text() if diagnostic else "", report)


def test_single_fatal_run_returns_nonzero(suite, tmp_path):
    result = subprocess.run(
        [CALENDAR / "run.sh", *ARGS, "--case", CALENDAR / "cases/credit.json", "--out", tmp_path],
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    assert result.returncode == 42
    assert "R1->R0 / C/f0 / no-credit" in result.stderr
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["outcome"] == "fatal" and (tmp_path / "replay.html").is_file()
