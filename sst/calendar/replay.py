"""Convert SST observations and separately installed reservations for the offline viewer."""

import csv


def observations(path):
    with path.open() as stream:
        return [
            {k: v if k in {"kind", "detail", "payload"} else int(v) for k, v in row.items()}
            for row in csv.DictReader(stream, delimiter="\t")
        ]


def endpoint(node, peer):
    return f"R{node}.{'local' if peer < 0 else str(peer)}"


def link(node, peer):
    return f"L{min(node, peer)}-{max(node, peer)}"


def plan_id(node, peer, source, seq):
    return f"slot-{node}-{peer}-{source}-{seq}"


def convert(m, records):
    entities = [{"id": "BSP", "x": -1, "y": -1}]
    endpoints = []
    for n in m.mesh.nodes:
        y, x = m.mesh.coords(n)
        label = f"R{n}"
        if n in m.nodes:
            label += " / " + m.names[m.nodes.index(n)]
        entities.append({"id": f"R{n}", "label": label, "x": x, "y": y})
        endpoints.extend(
            {"id": endpoint(n, p), "entity": f"R{n}"} for p in (*m.mesh.neighbours(n), -1)
        )
    connections = [
        {"id": link(n, p), "endpoints": [endpoint(n, p), endpoint(p, n)]} for n, p in m.mesh.links()
    ]
    origin = next((r["cycle"] for r in records if r["kind"] == "bsp.align"), None)
    plans = (
        [
            {
                "id": plan_id(s.node, p, s.source, s.seq),
                "where": link(s.node, p) if p >= 0 else endpoint(s.node, p),
                "port": endpoint(s.node, p),
                "start": str(origin + s.cycle),
                "end": str(origin + s.cycle + 1),
                "opcode": m.opcode,
                "relative_cycle": s.cycle,
            }
            for s in m.slots
            for p in s.ports
        ]
        if origin is not None
        else []
    )
    names = dict(zip(m.nodes, m.names, strict=True))
    events = []
    for r in records:
        n, peer, src, seq, k = (r[key] for key in ("node", "peer", "source", "flit", "kind"))
        where = "BSP" if n == -1 else f"R{n}"
        e = {"id": f"e{r['id']}", "at": str(r["cycle"]), "kind": k, "where": where}
        if src >= 0:
            e.update(
                data=f"{names[src]}/f{seq}",
                address=r["address"],
                payload=r["payload"],
                epoch=r["epoch"],
                opcode=r["opcode"],
            )
        if r["cause"] >= 0:
            e["cause"] = f"e{r['cause']}"
        if k in ("send", "arrive"):
            sent = r["id"] if k == "send" else r["cause"]
            e.update(where=endpoint(n, peer), connection=link(n, peer), transfer_id=f"h{sent}")
        if k in ("send", "land", "fault"):
            e.update(where=endpoint(n, peer), plan_id=plan_id(n, peer, src, seq))
        if peer >= 0 and k in (
            "credit.return",
            "credit.withhold",
            "credit.restore",
            "drop",
            "release",
        ):
            e["where"] = endpoint(n, peer)
            e["connection"] = link(n, peer)
        if k == "fault" and peer >= 0:
            e["connection"] = link(n, peer)
        if k in ("tile.publish", "consumer.read"):
            e["data"] = f"Tile@R{n}"
        if r["detail"] != "-":
            e["reason" if k == "fault" else "detail"] = r["detail"]
        events.append(e)
    relations = [
        {
            "kind": "split",
            "inputs": [name],
            "outputs": [f"{name}/f{i}" for i in range(len(m.fragments[n]))],
        }
        for n, name in names.items()
    ]
    for n in m.nodes:
        if any(e.get("data") == f"Tile@R{n}" for e in events):
            relations.append(
                {
                    "kind": "assemble",
                    "inputs": [
                        f for rel in relations if rel["kind"] == "split" for f in rel["outputs"]
                    ],
                    "outputs": [f"Tile@R{n}"],
                }
            )
    return {
        "format": "network-trace",
        "version": "1.0",
        "title": f"Calendar AllGather · {m.mesh.rows}×{m.mesh.cols} · {len(m.tile)} B Tile · {m.case.get('kind', 'normal')}",
        "source": "SST wsecalendar.fabric",
        "coverage": "BSP, directed router ports, Shared and TMU",
        "timebase": {"tick_fs": str(1000 * m.arch["period_ps"])},
        "window": {"start": "0", "end": str(records[-1]["cycle"])},
        "topology": {"entities": entities, "endpoints": endpoints, "connections": connections},
        "plans": plans,
        "events": events,
        "relations": relations,
    }
