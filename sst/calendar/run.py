"""Run or audit one Calendar AllGather, or reproduce its acceptance suite."""

import argparse
import json
import subprocess
import sys
import tempfile
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
    try:
        report.update(audit(m, events))
    except AuditError as exc:
        report.update(passed=False, error=str(exc))
    return report


def simulate(m, output, library):
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output) as tmp:
        stage = Path(tmp)
        save(stage / "reference.json", m.reference())
        m.write_image(stage / "image")
        # Use the working directory for paths passed through SST's model-options parser.
        subprocess.run(
            [
                "sst",
                "--timebase=1ps",
                f"--add-lib-path={library}",
                HERE / "sdl.py",
                "--",
                "image",
                "events.tsv",
                str(m.arch["period_ps"]),
            ],
            cwd=stage,
            check=True,
        )
        events = observations(stage / "events.tsv")
        report = verify(m, events)
        save(stage / "report.json", report)
        save(stage / "trace.json", convert(m, events))
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
        for name in ("events.tsv", "reference.json", "trace.json", "replay.html", "report.json"):
            (stage / name).replace(output / name)
    if not report["passed"]:
        raise AuditError(report["error"])
    return report


def suite(output, library):
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
        results[label] = simulate(model, output / label, library)
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
    parser.add_argument("--case", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--reference-only", action="store_true")
    mode.add_argument("--check-events", type=Path)
    mode.add_argument("--suite", action="store_true")
    args = parser.parse_args()
    if args.suite:
        if any((args.workload, args.architecture, args.deployment, args.case)) or not args.out:
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
            simulate(model, args.out, library)
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
