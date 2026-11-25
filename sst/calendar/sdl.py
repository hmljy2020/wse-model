"""One SST component per physical tile, one bidirectional link per mesh edge."""

import json
import sys
from pathlib import Path

import sst
from wse_model.topology import MeshTopology

arch = json.loads(Path(sys.argv[1]).read_text())
mesh = MeshTopology(arch["rows"], arch["columns"])
period = arch["period_ps"]
# SST's link latency is common to all message types. Each sender adds the
# remaining flit or credit delay in units of the node period.
latency = f"{min(arch['link_cycles'], arch['credit_cycles']) * period}ps"
nodes = []
sst.setProgramOption("partitioner", "self")
for n in mesh.nodes:
    node = sst.Component(f"R{n}", "wsecalendar.node")
    node.setRank(0, n % sst.getThreadCount())
    node.addParams(
        {
            "node": n,
            "rows": mesh.rows,
            "cols": mesh.cols,
            "clock": f"{period}ps",
            "link_cycles": arch["link_cycles"],
            "credit_cycles": arch["credit_cycles"],
            "image": f"R{n}.image",
            "events": f"R{n}.tsv",
        }
    )
    nodes.append(node)

for a, b in mesh.links():
    wire = sst.Link(f"L{a}_{b}")
    wire.connect((nodes[a], f"peer{b}", latency), (nodes[b], f"peer{a}", latency))
