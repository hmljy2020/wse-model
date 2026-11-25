"""Run or audit one Calendar AllGather, or reproduce its acceptance suite."""

import argparse
import csv
import json
import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path

from checker import AuditError, audit
from model import Model, read
from replay import convert, observations

from wse_model.errors import WseModelError

HERE = Path(__file__).resolve().parent
VIEWER = HERE.parents[2] / "network-viewer"
INPUTS = HERE / "inputs"


def save(path, obj):
    path.write_text(json.dumps(obj, indent=2) + "\n")


def compile_element(directory):
    subprocess.run(["bash", HERE / "build.sh", directory], check=True)


def load(workload, architecture, deployment, case=None):
    return Model(read(workload), read(architecture), read(deployment), read(case) if case else {})


def verify(m, events):
    report = {
        "architecture": m.arch,
        "case": m.case or {"kind": "normal"},
        "generation": 1,
        "reference": m.reference(),
    }
    fault = next((r for r in events if r["kind"] == "fault"), None)
    report["first_fault"] = None
    if fault:
        peer = fault["peer"]
        target = "local" if peer == -1 else f"R{peer}"
        report.update(
            outcome="fatal",
            first_fault={
                **fault,
                "id": f"e{fault['id']}",
                "participant": dict(zip(m.nodes, m.names, strict=True)).get(fault["source"]),
                "port": f"R{fault['node']}->{target}",
            },
        )
    try:
        report.update(audit(m, events))
    except AuditError as exc:
        report.update(passed=False, error=str(exc))
    return report


def check_graph(m, graph, threads):
    components = graph["components"]
    expected = {f"R{n}": n for n in m.mesh.nodes}
    if {c["name"] for c in components} != expected.keys() or len(components) != len(expected):
        raise AuditError("SST configuration has incorrect physical nodes")
    for c in components:
        n = expected[c["name"]]
        if (
            c["type"] != "wsecalendar.node"
            or int(c["params"]["node"]) != n
            or c["partition"] != {"rank": 0, "thread": n % threads}
        ):
            raise AuditError(f"wrong SST component or partition at R{n}")
    edges = []
    for wire in graph["links"]:
        left, right = wire["left"], wire["right"]
        a, b = expected[left["component"]], expected[right["component"]]
        if (left["port"], right["port"]) != (f"peer{b}", f"peer{a}"):
            raise AuditError("SST link connected to wrong ports")
        edges.append(tuple(sorted((a, b))))
    if sorted(edges) != sorted(m.mesh.links()):
        raise AuditError("SST links differ from physical mesh")


def collect(m, stage):
    records = []
    for n in m.mesh.nodes:
        rows = observations(stage / f"R{n}.tsv")
        if any(r["node"] != n for r in rows):
            raise AuditError(f"R{n} log contains another node's observations")
        if any(r["kind"] == "fault" for r in rows[:-1]):
            raise AuditError(f"R{n} continued after a fatal reservation")
        records.extend(rows)
    # A fatal on one SST thread can interrupt another at a different local
    # time. Preserve raw files and limit the merged replay to the failure tick.
    cutoff = min((r["cycle"] for r in records if r["kind"] == "fault"), default=None)
    if cutoff is not None:
        records = [r for r in records if r["cycle"] <= cutoff]
    records.sort(key=lambda r: (r["cycle"], r["kind"] == "fault", r["node"], r["id"]))
    if not records:
        raise AuditError("SST produced no observations")
    with (stage / "events.tsv").open("w") as stream:
        writer = csv.DictWriter(stream, fieldnames=records[0], delimiter="\t")
        writer.writeheader()
        writer.writerows(records)
    return records


def check_exit(code, log, report):
    fault = report.get("first_fault")
    if fault:
        marker = (
            "Calendar reservation fatal epoch={epoch} relative={relative_cycle} node={node} "
            "port={peer} source={source} flit={flit} reason={detail}"
        ).format(**fault)
        if code != 42 or marker not in log.splitlines():
            raise AuditError(f"reservation fault lacks matching SST fatal diagnostic (exit {code})")
    elif code != 0:
        raise AuditError(f"SST exited with status {code} without a verified reservation fault")


def simulate(m, output, library, threads=1, timeout=None):
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output) as tmp:
        stage = Path(tmp)
        save(stage / "reference.json", m.reference())
        save(stage / "architecture.json", m.arch)
        for n in m.mesh.nodes:
            m.write_image(stage / f"R{n}.image", n)
        cmd = [
            "sst",
            "--timebase=1ps",
            f"--num-threads={threads}",
            "--output-partition",
            "--output-json=sst-topology.json",
            f"--add-lib-path={library}",
            HERE / "sdl.py",
            "--",
            "architecture.json",
        ]
        report = {"passed": False}
        code = None
        try:
            with (stage / "sst.log").open("w") as log:
                code = subprocess.run(
                    cmd,
                    cwd=stage,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    timeout=timeout,
                    check=False,
                ).returncode
            graph = read(stage / "sst-topology.json")
            check_graph(m, graph, threads)
            events = collect(m, stage)
            report = verify(m, events)
            trace = convert(m, events, graph)
            save(stage / "trace.json", trace)
            subprocess.run(
                [
                    sys.executable,
                    VIEWER / "build.py",
                    stage / "trace.json",
                    "-o",
                    stage / "replay.html",
                ],
                check=True,
            )
            if report["passed"]:
                check_exit(code, (stage / "sst.log").read_text(), report)
        except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as exc:
            report.update(passed=False, error=str(exc))
        report.update(sst_exit_code=code, threads=threads)
        save(stage / "report.json", report)
        artifacts = [
            "events.tsv",
            "report.json",
            "reference.json",
            "sst.log",
            "sst-topology.json",
            "trace.json",
            "replay.html",
            *(f"R{n}.tsv" for n in m.mesh.nodes),
        ]
        # On a failed retry an old successful replay must not accompany the new log.
        for name in artifacts:
            if (stage / name).exists():
                (stage / name).replace(output / name)
            else:
                (output / name).unlink(missing_ok=True)
        for raw in output.glob("R*.tsv"):
            if raw.stem[1:].isdigit() and raw.name not in artifacts:
                raw.unlink()
    if not report["passed"]:
        raise AuditError(f"{output}: {report['error']} (see sst.log)")
    return report


def compare_threads(single, double, first, second):
    if first["first_fault"]:
        fields = ("epoch", "relative_cycle", "node", "peer", "source", "flit", "detail")
        if any(first["first_fault"][f] != second["first_fault"][f] for f in fields):
            raise AuditError("reservation failure differs across threads")
        return

    def timeline(path, origin):
        return Counter(
            (
                r["cycle"] - origin,
                r["kind"],
                r["node"],
                r["peer"],
                r["source"],
                r["flit"],
                r["address"],
                r["payload"],
                r["detail"],
            )
            for r in observations(path / "events.tsv")
        )

    if (
        timeline(single, first["origin_cycle"]) != timeline(double, second["origin_cycle"])
        or first["published_tiles"] != second["published_tiles"]
        or first["unique_remote_bytes"] != second["unique_remote_bytes"]
    ):
        raise AuditError("relative events, credits or Tile contents differ across threads")


def suite(output, library):
    (output / "suite.json").unlink(missing_ok=True)
    cases = [("A", "line", 64, None), ("B", "mesh", 64, None), ("C", "line", 128, None)]
    cases.extend((kind, "line", 64, kind) for kind in ("late", "credit", "duplicate", "delay_ack"))
    results = {}
    for label, shape, size, kind in cases:
        model = load(
            INPUTS / f"workload-{size}.json",
            INPUTS / f"architecture-{shape}.json",
            INPUTS / f"deployment-{shape}-{size}.json",
            HERE / "cases" / f"{kind}.json" if kind else None,
        )
        single, double = output / label, output / "two-threads" / label
        first = simulate(model, single, library, timeout=60)
        second = simulate(model, double, library, threads=2, timeout=60)
        compare_threads(single, double, first, second)
        results[label] = first
    try:
        load(
            INPUTS / "workload-64.json",
            INPUTS / "architecture-square.json",
            INPUTS / "deployment-square-invalid.json",
        )
    except WseModelError as exc:
        if "V-TREE-ACYCLIC" not in str(exc):
            raise
        invalid = {"passed": True, "outcome": "rejected", "diagnostic": str(exc)}
        (output / "invalid").mkdir(exist_ok=True)
        save(output / "invalid" / "report.json", invalid)
    else:
        raise AuditError("2×2 cyclic deployment was accepted")
    a, b, c = (results[k] for k in "ABC")
    if not (
        a["total_remote_bytes"] == b["total_remote_bytes"] == 768
        and c["total_remote_bytes"] == 1536
        and set(a["published_tiles"].values()) == set(b["published_tiles"].values())
    ):
        raise AuditError("cross-case AllGather conservation or byte comparison failed")
    fault = results["credit"]["first_fault"]
    if (
        fault["relative_cycle"],
        fault["port"],
        fault["participant"],
        fault["flit"],
        fault["detail"],
    ) != (8, "R1->R0", "C", 0, "no-credit"):
        raise AuditError("credit case missed the reference violation")
    hops = {
        (r["kind"], r["node"], r["peer"]): r["cycle"] - a["origin_cycle"]
        for r in observations(output / "A/events.tsv")
        if r["source"] == 0 and r["flit"] == 0
    }
    if [
        hops[key] for key in [("send", 0, 1), ("arrive", 1, 0), ("land", 1, -1), ("send", 1, 2)]
    ] != [1, 3, 6, 6]:
        raise AuditError("incorrect A/f0 reference hop timing")
    if (
        results["late"]["first_injection"] - a["first_injection"]
        != results["late"]["case"]["cycles"]
    ):
        raise AuditError("late reporter failed to shift first injection")
    delayed = results["delay_ack"]
    target = delayed["case"]["router"]
    if (
        delayed["milestones"][target]["last_shared_ack"]
        - a["milestones"][target]["last_shared_ack"]
        != delayed["case"]["cycles"]
        or max(row["last_landing"] for row in delayed["milestones"].values())
        >= delayed["milestones"][target]["publication"]
    ):
        raise AuditError("missing all-arrived/unpublished interval")
    summary = {
        k: {field: r[field] for field in ("passed", "outcome", "total_remote_bytes", "drain_cycle")}
        for k, r in results.items()
    }
    summary["invalid"] = invalid
    subprocess.run(
        [
            sys.executable,
            VIEWER / "build.py",
            *[output / k / "trace.json" for k in results],
            "-o",
            output / "replay.html",
        ],
        check=True,
    )
    save(output / "suite.json", summary)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workload", nargs="?", type=Path)
    parser.add_argument("architecture", nargs="?", type=Path)
    parser.add_argument("deployment", nargs="?", type=Path)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--threads", choices=(1, 2), type=int, help="thread count for a single run")
    parser.add_argument("--case", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--reference-only", action="store_true")
    mode.add_argument("--check-events", type=Path)
    mode.add_argument("--suite", action="store_true")
    args = parser.parse_args()
    if args.suite:
        if (
            any((args.workload, args.architecture, args.deployment, args.case, args.threads))
            or not args.out
        ):
            parser.error("--suite takes --out DIRECTORY")
    else:
        if not all((args.workload, args.architecture, args.deployment)):
            parser.error("provide workload, architecture and deployment")
        model = load(args.workload, args.architecture, args.deployment, args.case)
        if args.reference_only:
            sys.stdout.write(json.dumps(model.reference(), indent=2) + "\n")
            return 0
        if args.check_events:
            report = verify(model, observations(args.check_events))
            sys.stdout.write(json.dumps(report, indent=2) + "\n")
            return 0 if report["passed"] else 1
        if not args.out:
            parser.error("simulation requires --out DIRECTORY")
    with tempfile.TemporaryDirectory(prefix="wsecalendar-") as tmp:
        library = Path(tmp)
        compile_element(library)
        if args.suite:
            suite(args.out, library)
        else:
            report = simulate(model, args.out, library, args.threads or 1)
            if report["outcome"] == "fatal":
                fault = report["first_fault"]
                sys.stderr.write(
                    f"Calendar fatal {fault['port']} / {fault['participant']}/f{fault['flit']} "
                    f"/ {fault['detail']} at relative cycle {fault['relative_cycle']}; "
                    f"replay: {args.out / report['highlights'][0]['replay']}\n"
                )
                return report["sst_exit_code"]
    sys.stdout.write(f"Acceptance passed. Results in {args.out.resolve()}\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (
        ValueError,
        KeyError,
        TypeError,
        WseModelError,
        OSError,
        subprocess.CalledProcessError,
    ) as err:
        sys.exit(f"Calendar: {err}")
