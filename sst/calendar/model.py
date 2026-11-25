"""Prepare a single-generation Calendar deployment and its workload oracle."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from wse_model.calendar.collective import Collective
from wse_model.calendar.route_bits import RouteBits
from wse_model.calendar.validate import validate_route_bits
from wse_model.noc.flit import FlitHeader, split_payload
from wse_model.noc.forward import simulate_multicast
from wse_model.topology import MeshTopology


def read(path):
    return json.loads(Path(path).read_text())


def require(test, message):
    if not test:
        raise ValueError(message)


def number(value, minimum=0):
    require(type(value) is int and minimum <= value <= 2**31 - 1, f"invalid integer {value!r}")
    return value


@dataclass(frozen=True)
class Slot:
    cycle: int
    node: int
    source: int
    seq: int
    ingress: int
    ports: tuple[int, ...]  # -1 is the local landing port


class Model:
    def __init__(self, workload, arch, deployment, case):
        self.arch = arch
        self.case = case
        self.mesh = MeshTopology(number(arch["rows"], 1), number(arch["columns"], 1))
        for key in (
            "period_ps",
            "width_bytes",
            "link_cycles",
            "credit_cycles",
            "ingress_depth",
            "landing_depth",
            "shared_cycles",
            "epoch_bits",
        ):
            number(arch[key], 1)
        require(arch["fork_policy"] == "atomic-all-ports", "unsupported fork policy")
        require(
            workload["operation"] == "AllGather" and workload["consume_after"] == "tile.publish",
            "unsupported operation or dependency",
        )
        parts = workload["participants"]
        self.names = workload["layout"]
        require(
            isinstance(self.names, list)
            and all(isinstance(name, str) and name for name in self.names)
            and len(self.names) >= 2
            and len(set(self.names)) == len(self.names)
            and set(self.names) == set(parts),
            "layout must contain each participant exactly once",
        )
        self.mapping = deployment["mapping"]
        require(
            set(self.mapping) == set(parts) == set(deployment["routes"]), "participant mismatch"
        )
        self.nodes = [number(self.mapping[name]) for name in self.names]
        require(
            len(set(self.nodes)) == len(self.nodes) and set(self.nodes) <= set(self.mesh.nodes),
            "each participant needs its own physical core",
        )
        self.base = number(deployment["shared_tile_base"])
        self.data = {}
        self.offset = {}
        self.fragments = {}
        self.routes = {}
        self.slots = []
        self.expected_rx = {}
        tile = bytearray()
        # The seed belongs to the logical participant and survives physical remapping.
        for name, node in zip(self.names, self.nodes, strict=True):
            part = parts[name]
            seed = number(part["seed"])
            size = number(part["bytes"], 1)
            require(seed <= 255, "seed must be a byte")
            self.data[node] = bytes((seed + i) % 256 for i in range(size))
            self.offset[node] = len(tile)
            tile.extend(self.data[node])
        self.tile = bytes(tile)
        number(self.base + len(tile))
        self.opcode = int(Collective.ALL_GATHER)
        for name, node in zip(self.names, self.nodes, strict=True):
            self.expected_rx[node] = len(tile) - len(self.data[node])
            self._route(node, deployment["routes"][name])
        self.slots.sort(key=lambda s: (s.cycle, s.node, s.source, s.seq))
        self._check_calendar()
        self._check_case()

    def _route(self, node, route):
        bits = RouteBits.from_hex(route["routeBits"], self.mesh.node_count)
        validate_route_bits(
            self.mesh,
            bits,
            source=node,
            expected_land_nodes=frozenset(self.nodes) - {node},
            self_delivery=False,
        ).raise_if_failed()
        require(number(route["opcode"]) == self.opcode, "AllGather requires opcode 1")
        self.routes[node] = bits
        header = FlitHeader(
            bits,
            self.opcode,
            link_width_bytes=self.arch["width_bytes"],
            epoch_tag_bits=self.arch["epoch_bits"],
        )
        fragments = split_payload(self.data[node], header)
        self.fragments[node] = [f.payload for f in fragments]
        hops = {h.node: h for h in simulate_multicast(self.mesh, bits, source=node).hops}
        for router, ports, cycles in route["reservations"]:
            router = number(router)
            require(router in hops, f"unknown or repeated reservation for router {router}")
            hop = hops.pop(router)
            outputs = tuple(-1 if p == "local" else number(p) for p in ports)
            required = (*hop.egress, -1) if hop.lands else hop.egress
            require(sorted(outputs) == sorted(required), f"wrong fork ports at router {router}")
            require(len(cycles) == len(fragments), "every flit needs one port reservation")
            for seq, cycle in enumerate(cycles):
                self.slots.append(
                    Slot(
                        number(cycle, 1),
                        router,
                        node,
                        seq,
                        -1 if hop.ingress is None else hop.ingress,
                        outputs,
                    )
                )
        require(not hops, f"missing router reservations {list(hops)}")

    def _check_calendar(self):
        occupied = set()
        inputs = defaultdict(list)
        returns = defaultdict(list)
        writes = defaultdict(list)
        times = {(s.node, s.source, s.seq): s.cycle for s in self.slots}
        for s in self.slots:
            for port in s.ports:
                key = (s.cycle, s.node, port)
                require(key not in occupied, f"overlapping port reservation {key}")
                occupied.add(key)
                if port == -1:
                    end = writes[s.node][-1] if writes[s.node] else 0
                    require(
                        sum(t > s.cycle for t in writes[s.node]) < self.arch["landing_depth"],
                        f"landing queue overbooked at {key}",
                    )
                    writes[s.node].append(max(end, s.cycle) + self.arch["shared_cycles"])
            if s.ingress == -1:
                prior = times.get((s.node, s.source, s.seq - 1), 0)
                require(prior < s.cycle, "source flit order")
                continue
            send = times[s.ingress, s.source, s.seq]
            require(send + self.arch["link_cycles"] <= s.cycle, f"slot precedes arrival {s}")
            channel = (s.ingress, s.node)
            inputs[channel].append(send)
            returns[channel].append((send, 1))
            returns[channel].append((s.cycle + self.arch["credit_cycles"], -1))
        for channel, order in inputs.items():
            require(order == sorted(order), f"reservation violates ingress FIFO on {channel}")
            used = 0
            for _, delta in sorted(returns[channel]):
                used += delta
                require(used <= self.arch["ingress_depth"], f"port credit overbooked on {channel}")

    def _check_case(self):
        kind = self.case.get("kind", "normal")
        require(kind in {"normal", "late", "credit", "duplicate", "delay_ack"}, "unknown case")
        if kind == "normal":
            require(set(self.case) <= {"kind"}, "unexpected case parameter")
            return
        fields = {"kind", "participant"}
        if kind != "late":
            fields.update(("router", "flit"))
        if kind in ("late", "delay_ack"):
            fields.add("cycles")
        if kind == "credit":
            fields.add("port")
        require(set(self.case) == fields, f"incorrect fields for {kind} case")
        self.target = self.mapping[self.case["participant"]]
        if kind == "late":
            number(self.case["cycles"], 1)
            return
        node, seq = number(self.case["router"]), number(self.case["flit"])
        target = next(
            (s for s in self.slots if (s.source, s.node, s.seq) == (self.target, node, seq)), None
        )
        require(target is not None, "perturbation has no matching reservation")
        if kind == "credit":
            require(number(self.case["port"]) in target.ports, "credit must target an egress")
        else:
            require(-1 in target.ports, "perturbation needs a landing")
            if kind == "delay_ack":
                number(self.case["cycles"], 1)

    def reference(self):
        return {
            "layout": [
                {
                    "participant": name,
                    "core": n,
                    "address": self.base + self.offset[n],
                    "bytes": len(self.data[n]),
                }
                for name, n in zip(self.names, self.nodes, strict=True)
            ],
            "expected_remote_bytes": self.expected_rx,
            "tile_hex": self.tile.hex(),
        }

    def write_image(self, path):
        # Numeric records form the private loader/element ABI. No event times or
        # expected Tile bytes are supplied to the element.
        a = self.arch
        lines = []

        def record(*fields):
            lines.append(" ".join(map(str, fields)))

        record(
            "mesh",
            self.mesh.rows,
            self.mesh.cols,
            a["link_cycles"],
            a["credit_cycles"],
            a["ingress_depth"],
            a["landing_depth"],
            a["shared_cycles"],
            self.base,
            len(self.tile),
        )
        for n in self.nodes:
            bits = "".join(str(int(p)) for p in self.routes[n].pairs())
            record("source", n, self.opcode, bits, self.offset[n], self.expected_rx[n])
            for seq, frag in enumerate(self.fragments[n]):
                record("flit", n, seq, frag.hex())
        for s in self.slots:
            record("slot", s.cycle, s.node, self.opcode, s.source, s.seq, len(s.ports), *s.ports)
        c = self.case
        record(
            "case",
            c.get("kind", "normal"),
            getattr(self, "target", -1),
            c.get("router", -1),
            c.get("flit", -1),
            c.get("port", -1),
            c.get("cycles", 0),
        )
        path.write_text("\n".join(lines) + "\n")
