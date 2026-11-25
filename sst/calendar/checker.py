"""Replay observed transactions against workload bytes, receive contracts and port reservations."""

from collections import Counter, defaultdict, deque

from wse_model.calendar.receive import DeliveryOutcome, ReceiveAccount, Segment, SegmentId


class AuditError(ValueError):
    pass


def audit(model, records):
    m = model
    variant = m.case.get("kind", "normal")
    slots = {(s.node, s.source, s.seq): s for s in m.slots}
    fragment = {}
    for n in m.nodes:
        address = m.base + m.offset[n]
        for seq, payload in enumerate(m.fragments[n]):
            fragment[n, seq] = address, payload
            address += len(payload)
    ids, active, sends, pending, credit_due = {}, {}, {}, {}, {}
    fifo = defaultdict(deque)
    used = Counter()
    held = {}
    branches = defaultdict(list)
    released, seen_ports = set(), set()
    landed, duplicate = Counter(), Counter()
    duplicate_due = set()
    last_write = Counter()
    reported, reads, published = {}, {}, {}
    setup = defaultdict(dict)
    origin = fault = drain = None
    tiles = {n: bytearray(len(m.tile)) for n in m.nodes}
    coverage = {n: set() for n in m.nodes}
    accounts = {
        n: ReceiveAccount(n, m.opcode, 1, m.base, len(m.tile), m.expected_rx[n]) for n in m.nodes
    }
    milestones = defaultdict(dict)
    accepted = Counter()
    last_event = "start"
    previous_tick = 0

    def check(condition, message):
        if not condition:
            raise AuditError(f"{last_event}: {message}")

    for r in records:
        rid, t, k, n, peer, src, seq, cause = (
            r[key] for key in ("id", "cycle", "kind", "node", "peer", "source", "flit", "cause")
        )
        last_event = f"e{rid}"
        key = (n, src, seq)
        s = slots.get(key)
        check(rid not in ids and t >= previous_tick and drain is None, "invalid event order")
        check(cause == -1 or cause in ids, "cause is absent or in the future")
        ids[rid] = r
        previous_tick = t
        if k in {
            "inject",
            "send",
            "arrive",
            "land",
            "duplicate",
            "release",
            "drop",
            "local.write",
            "shared.ack",
            "credit.withhold",
            "fault",
        }:
            check((src, seq) in fragment and s is not None, "unknown fragment or router")
        if src >= 0:
            check((src, seq) in fragment, "unknown fragment")
            addr, payload = fragment[src, seq]
            check(
                (r["address"], r["payload"], r["opcode"], r["epoch"])
                == (addr, payload.hex(), m.opcode, 1),
                "fragment address, bytes or header mismatch",
            )
        if k in ("source.ready", "range.register", "resource.reserve", "bsp.report"):
            check(n in m.nodes and k not in setup[n], "invalid or repeated core setup")
            prior = {
                "range.register": "source.ready",
                "resource.reserve": "range.register",
                "bsp.report": "resource.reserve",
            }.get(k)
            check(not prior or prior in setup[n], "core reported before preparation")
            setup[n][k] = t
            if k == "range.register":
                check(int(r["detail"]) == m.base, "wrong registered base")
            if k == "resource.reserve":
                check(int(r["detail"]) == len(m.tile), "wrong reserved capacity")
            if k == "bsp.report":
                delay = m.case["cycles"] if variant == "late" and n == m.target else 0
                check(t == setup[n]["source.ready"] + delay, "incorrect readiness delay")
                reported[n] = t
        elif k == "bsp.align":
            check(
                origin is None and set(reported) == set(m.nodes) and t == max(reported.values()),
                "BSP origin does not coincide with last report",
            )
            origin = t
        elif k == "inject":
            check(
                origin is not None
                and fault is None
                and s
                and n == src
                and t == origin + s.cycle
                and key not in active
                and key not in released,
                "unreserved, repeated or post-fault injection",
            )
            active[key] = rid
            accepted["injected"] += 1
        elif k == "arrive":
            tx = sends.pop(cause, None)
            check(
                tx == (peer, n, src, seq, t - m.arch["link_cycles"]),
                "unpaired hop or wrong latency",
            )
            check(key not in active, "input arrived twice")
            active[key] = rid
            fifo[n, peer].append(key)
            accepted["arrived"] += 1
            milestones[n]["last_link_arrival"] = t
        elif k in ("send", "land"):
            check(
                fault is None
                and s
                and origin is not None
                and t == origin + s.cycle
                and peer in s.ports
                and active.get(key) == cause,
                "unreserved or unaccepted branch",
            )
            port = (t, n, peer)
            repeat = k == "land" and key in landed
            check(port not in seen_ports or repeat, "port used twice in one slot")
            seen_ports.add(port)
            if not repeat:
                branches[key].append(peer)
            if k == "send":
                check(peer >= 0 and used[n, peer] < m.arch["ingress_depth"], "credit exhausted")
                used[n, peer] += 1
                sends[rid] = (n, peer, src, seq, t)
            else:
                check(peer == -1 and n != src and "range.register" in setup[n], "invalid landing")
                landed[key] += 1
                milestones[n]["last_landing"] = t
                if repeat:
                    duplicate_due.add(rid)
                else:
                    delay = (
                        m.case["cycles"]
                        if variant == "delay_ack"
                        and (n, src, seq) == (m.case["router"], m.target, m.case["flit"])
                        else 0
                    )
                    check(
                        sum(p[0] == n for p in pending) < m.arch["landing_depth"],
                        "landing FIFO over capacity",
                    )
                    due = max(t, last_write[n]) + m.arch["shared_cycles"] + delay
                    last_write[n] = due
                    pending[key] = (rid, due)
        elif k == "duplicate":
            check(
                cause in duplicate_due
                and ids[cause]["node"] == n
                and (ids[cause]["source"], ids[cause]["flit"]) == (src, seq),
                "unmatched duplicate",
            )
            duplicate_due.remove(cause)
            duplicate[key] += 1
        elif k in ("release", "drop"):
            check(
                active.pop(key, None) == cause and s and peer == s.ingress, "unknown input release"
            )
            if k == "release":
                check(
                    fault is None
                    and origin is not None
                    and t == origin + s.cycle
                    and sorted(branches[key]) == sorted(s.ports),
                    "incomplete or non-atomic fork",
                )
                released.add(key)
            else:
                check(fault is not None and not branches[key], "unjustified or partial drop")
                accepted["dropped"] += 1
            if peer >= 0:
                check(fifo[n, peer] and fifo[n, peer].popleft() == key, "ingress FIFO order")
                credit_due[rid] = (peer, n, t + m.arch["credit_cycles"])
        elif k == "credit.return":
            check(
                credit_due.pop(cause, None) == (n, peer, t) and used[n, peer] > 0,
                "credit returned early, twice or to wrong channel",
            )
            used[n, peer] -= 1
        elif k == "credit.withhold":
            amount = int(r["detail"])
            check(
                variant == "credit"
                and s
                and origin is not None
                and t == origin + s.cycle
                and (n, src, seq, peer)
                == (m.case["router"], m.target, m.case["flit"], m.case["port"])
                and 0 < amount == m.arch["ingress_depth"] - used[n, peer]
                and not held,
                "credit withholding does not match available tokens or case",
            )
            held[n, peer] = rid, amount
        elif k == "credit.restore":
            check(
                held.pop((n, peer), None) == (cause, int(r["detail"]))
                and fault is not None
                and t == fault["cycle"] + 1,
                "withheld credit not restored",
            )
        elif k == "fault":
            check(
                variant == "credit"
                and fault is None
                and origin is not None
                and s
                and (n, src, seq, peer)
                == (m.case["router"], m.target, m.case["flit"], m.case["port"])
                and t == origin + s.cycle
                and r["detail"] == "no-credit"
                and (n, peer) in held
                and not published,
                "unexpected fault site, reason or cycle",
            )
            check(s.ingress == -1 or active.get(key) == cause, "fault lacks an accepted input")
            fault = r
        elif k in ("local.write", "shared.ack"):
            if k == "shared.ack":
                check(pending.pop(key, None) == (cause, t), "ack has no matching due Shared write")
                addr, payload = fragment[src, seq]
                result = accounts[n].deliver(
                    Segment(SegmentId(src, seq), r["opcode"], r["epoch"], addr, len(payload))
                )
                check(result == DeliveryOutcome.COUNTED, "ack did not add unique remote bytes")
                milestones[n]["last_shared_ack"] = t
            else:
                check(
                    n == src and "resource.reserve" in setup[n] and origin is None,
                    "unexpected local segment write",
                )
            start = r["address"] - m.base
            data = bytes.fromhex(r["payload"])
            span = range(start, start + len(data))
            check(coverage[n].isdisjoint(span), "visible ranges overlap")
            coverage[n].update(span)
            tiles[n][start : start + len(data)] = data
        elif k == "tile.publish":
            check(
                fault is None
                and n not in published
                and origin is not None
                and t > origin + m.slots[-1].cycle
                and released == slots.keys()
                and accounts[n].complete
                and len(coverage[n]) == len(m.tile)
                and t > milestones[n]["last_shared_ack"],
                "premature or repeated publication",
            )
            check(
                r["detail"] == tiles[n].hex() and tiles[n] == m.tile,
                "published Tile differs from confirmed writes or workload",
            )
            published[n] = rid
            milestones[n]["publication"] = t
        elif k == "consumer.read":
            check(
                n in m.nodes and n not in reads and published.get(n) == cause,
                "consumer lacks published Tile",
            )
            reads[n] = t
        elif k == "drain":
            check(
                not (
                    active
                    or sends
                    or credit_due
                    or pending
                    or held
                    or duplicate_due
                    or any(used.values())
                ),
                "drain with unfinished transactions",
            )
            check(r["detail"] == ("aborted" if fault else "success"), "wrong drain status")
            if not fault:
                check(
                    set(reads) == set(m.nodes) and t > max(reads.values()),
                    "drain precedes consumer completion",
                )
            drain = t
        else:
            check(False, f"unknown event {k}")

    check(drain is not None and origin is not None, "missing alignment or terminal drain")
    if variant == "credit":
        check(fault and not published and not reads, "failed generation published or fault missing")
        index = m.slots.index(slots[fault["node"], fault["source"], fault["flit"]])
        check(
            released == {(s.node, s.source, s.seq) for s in m.slots[:index]},
            "first violation differs from observed slot prefix",
        )
    else:
        check(released == slots.keys() and set(reads) == set(m.nodes), "missing transfers or Tiles")
    wanted = {(m.case["router"], m.target, m.case["flit"]): 1} if variant == "duplicate" else {}
    check(duplicate == wanted, "duplicate disturbance mismatch")
    highlights = []
    for r in records:
        if (
            r["kind"] in {"fault", "duplicate"}
            or (variant == "late" and r["kind"] == "bsp.report" and r["node"] == m.target)
            or (
                variant == "delay_ack"
                and r["kind"] == "shared.ack"
                and (r["node"], r["source"], r["flit"])
                == (m.case["router"], m.target, m.case["flit"])
            )
        ):
            event_id = f"e{r['id']}"
            highlights.append(
                {
                    "id": event_id,
                    "kind": r["kind"],
                    "cycle": r["cycle"],
                    "replay": f"replay.html#event={event_id}",
                }
            )
    return {
        "passed": True,
        "highlights": highlights,
        "outcome": "aborted" if fault else "published",
        "origin_cycle": origin,
        "last_reporters": [n for n, t in reported.items() if t == origin],
        "wait_for_group_cycles": {n: origin - t for n, t in reported.items()},
        "report_cycles": reported,
        "report_delay_cycles": {n: t - setup[n]["source.ready"] for n, t in reported.items()},
        "first_injection": next((r["cycle"] for r in records if r["kind"] == "inject"), None),
        "unique_remote_bytes": {n: a.counted_bytes for n, a in accounts.items()},
        "total_remote_bytes": sum(a.counted_bytes for a in accounts.values()),
        "milestones": dict(milestones),
        "drain_cycle": drain,
        "transactions": dict(accepted),
        "duplicate_events": [f"e{r['id']}" for r in records if r["kind"] == "duplicate"],
        "published_tiles": {n: tiles[n].hex() for n in published},
        "first_fault": None
        if not fault
        else {
            **fault,
            "id": f"e{fault['id']}",
            "relative_cycle": fault["cycle"] - origin,
            "participant": m.names[m.nodes.index(fault["source"])],
            "port": f"R{fault['node']}->R{fault['peer']}",
        },
    }
