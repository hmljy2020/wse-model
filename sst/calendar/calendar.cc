// Single clock domain for the reference Calendar fabric and its attached cores.
#include <sst/core/sst_config.h>
#include <sst/core/component.h>
#include <algorithm>
#include <cstdint>
#include <deque>
#include <fstream>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace WSE {
using Tick = int64_t;
using Channel = std::pair<int, int>;
constexpr int Local = -1;

struct Flit {
    int source, seq, opcode, address;
    std::string bits, payload; // wire order pairs, then valid payload in hex
};
struct Input {
    Flit flit;
    int peer, cause;
};
struct Flight {
    int from, to, send;
    Flit flit;
};
struct Reservation {
    int node, opcode, source, seq;
    std::set<int> ports;
};
struct Credit {
    Channel channel;
    int cause;
};
struct Write {
    Flit flit;
    Tick due;
    int cause;
};
struct Core {
    int opcode = 0, offset = 0, expected = 0, confirmed = 0;
    bool reported = false, published = false;
    Tick lastAck = -1, publication = -1;
    std::string bits, tile;
    std::vector<Flit> source;
    std::set<Channel> landed;
    std::vector<bool> visible;
    std::deque<Write> writes;
};
struct Experiment {
    std::string kind;
    int source, node, seq, port, cycles;
    bool matches(const std::string &k, int n, const Flit &f) const {
        return kind == k && node == n && source == f.source && seq == f.seq;
    }
};

class Calendar : public SST::Component {
  public:
    SST_ELI_REGISTER_COMPONENT(Calendar, "wsecalendar", "fabric", SST_ELI_ELEMENT_VERSION(1, 0, 0),
                               "Clocked Calendar AllGather fabric", COMPONENT_CATEGORY_NETWORK)
    SST_ELI_DOCUMENT_PARAMS({"image", "Validated deployment image", nullptr},
                            {"events", "Raw event output", nullptr},
                            {"clock", "Fabric clock period", nullptr})

    Calendar(SST::ComponentId_t id, SST::Params &params) : SST::Component(id) {
        load(params.find<std::string>("image"));
        log.exceptions(std::ios::failbit | std::ios::badbit);
        log.open(params.find<std::string>("events"));
        log << "id\tcycle\tkind\tnode\tpeer\tsource\tflit\topcode\tepoch\tcause\taddress\tpayload\tdetai"
               "l\n";
        registerAsPrimaryComponent();
        primaryComponentDoNotEndSim();
        registerClock(params.find<std::string>("clock"),
                      new SST::Clock::Handler<Calendar, &Calendar::clockTick>(this));
    }

  private:
    int rows{}, cols{}, latency{}, creditLatency{}, depth{}, landingDepth{}, service{}, base{}, size{};
    Tick now = 0, origin = -1, closed = -1;
    int nextId = 0;
    bool failed = false;
    int heldCredits = 0, holdEvent = -1;
    Experiment experiment;
    std::map<int, Core> cores;
    std::map<Tick, std::vector<Reservation>> calendar;
    std::map<Channel, int> credits;
    std::map<Channel, std::deque<Input>> queues; // (receiver, ingress peer)
    std::map<Tick, std::vector<Flight>> flights;
    std::map<Tick, std::vector<Credit>> returning;
    std::ofstream log;

    void load(const std::string &path) {
        std::ifstream in(path);
        if (!in)
            throw std::runtime_error("cannot open Calendar image");
        std::string tag;
        while (in >> tag) {
            if (tag == "mesh") {
                in >> rows >> cols >> latency >> creditLatency >> depth >> landingDepth >> service >>
                    base >> size;
                for (int n = 0; n < rows * cols; ++n)
                    for (int peer : neighbours(n))
                        credits[{n, peer}] = depth;
            } else if (tag == "source") {
                int n;
                in >> n;
                auto &c = cores[n];
                in >> c.opcode >> c.bits >> c.offset >> c.expected;
            } else if (tag == "flit") {
                int n, seq;
                std::string data;
                in >> n >> seq >> data;
                auto &c = cores.at(n);
                int address = base + c.offset;
                if (!c.source.empty()) {
                    const auto &last = c.source.back();
                    address = last.address + static_cast<int>(last.payload.size() / 2);
                }
                c.source.push_back({n, seq, c.opcode, address, c.bits, data});
            } else if (tag == "slot") {
                Tick cycle;
                Reservation s;
                int count;
                in >> cycle >> s.node >> s.opcode >> s.source >> s.seq >> count;
                for (int i = 0, port; i < count; ++i) {
                    in >> port;
                    s.ports.insert(port);
                }
                calendar[cycle].push_back(s);
            } else if (tag == "case") {
                in >> experiment.kind >> experiment.source >> experiment.node >> experiment.seq >>
                    experiment.port >> experiment.cycles;
            } else
                throw std::runtime_error("unknown Calendar image record: " + tag);
            if (!in)
                throw std::runtime_error("incomplete Calendar image record: " + tag);
        }
    }

    std::vector<int> neighbours(int n) const {
        std::vector<int> out;
        if (n >= cols)
            out.push_back(n - cols);
        if (n % cols > 0)
            out.push_back(n - 1);
        if (n % cols + 1 < cols)
            out.push_back(n + 1);
        if (n + cols < rows * cols)
            out.push_back(n + cols);
        return out;
    }

    int emit(std::string kind, int n, int peer = Local, const Flit *f = nullptr, int cause = -1,
             std::string detail = "-") {
        log << ++nextId << '\t' << now << '\t' << kind << '\t' << n << '\t' << peer << '\t'
            << (f ? f->source : -1) << '\t' << (f ? f->seq : -1) << '\t' << (f ? f->opcode : -1) << '\t'
            << (f ? 1 : -1) << '\t' << cause << '\t' << (f ? f->address : -1) << '\t'
            << (f ? f->payload : "-") << '\t' << detail << '\n';
        return nextId;
    }

    void writeBytes(Core &core, const Flit &f) {
        int start = f.address - base, length = static_cast<int>(f.payload.size() / 2);
        if (start < 0 || length > size - start)
            throw std::runtime_error("write outside Tile");
        for (int i = start; i < start + length; ++i) {
            if (core.visible.at(i))
                throw std::runtime_error("overlapping Tile write");
            core.visible[i] = true;
        }
        core.tile.replace(2ULL * start, f.payload.size(), f.payload);
    }

    void rendezvous() {
        for (auto &[n, c] : cores) {
            if (now == 1) {
                int ready = emit("source.ready", n);
                c.tile.assign(2ULL * size, '0');
                c.visible.assign(size, false);
                int reg = emit("range.register", n, Local, nullptr, ready, std::to_string(base));
                emit("resource.reserve", n, Local, nullptr, reg, std::to_string(size));
                for (const auto &f : c.source) {
                    writeBytes(c, f);
                    emit("local.write", n, Local, &f, reg);
                }
            }
            Tick delay = experiment.kind == "late" && experiment.source == n ? experiment.cycles : 0;
            if (!c.reported && now == 1 + delay) {
                c.reported = true;
                emit("bsp.report", n);
            }
        }
        if (std::all_of(cores.begin(), cores.end(), [](const auto &p) { return p.second.reported; })) {
            origin = now;
            emit("bsp.align", -1);
        }
    }

    void deliveries() {
        if (heldCredits) {
            Channel channel{experiment.node, experiment.port};
            credits.at(channel) += heldCredits;
            emit("credit.restore", channel.first, channel.second, nullptr, holdEvent,
                 std::to_string(heldCredits));
            heldCredits = 0;
        }
        for (const auto &ret : returning[now]) {
            auto [from, to] = ret.channel;
            ++credits.at(ret.channel);
            emit("credit.return", from, to, nullptr, ret.cause);
        }
        returning.erase(now);
        for (const auto &h : flights[now]) {
            int arrived = emit("arrive", h.to, h.from, &h.flit, h.send);
            queues[{h.to, h.from}].push_back({h.flit, h.from, arrived});
        }
        flights.erase(now);
        for (auto &[n, c] : cores) {
            if (c.writes.empty() || c.writes.front().due != now)
                continue;
            const auto &w = c.writes.front();
            writeBytes(c, w.flit);
            c.confirmed += static_cast<int>(w.flit.payload.size() / 2);
            emit("shared.ack", n, Local, &w.flit, w.cause);
            c.lastAck = now;
            c.writes.pop_front();
        }
    }

    void land(int n, const Flit &f, int cause) {
        auto &c = cores.at(n);
        int arrived = emit("land", n, Local, &f, cause);
        if (!c.landed.emplace(f.source, f.seq).second) {
            emit("duplicate", n, Local, &f, arrived);
            return;
        }
        Tick start = c.writes.empty() ? now : c.writes.back().due;
        Tick delay = experiment.matches("delay_ack", n, f) ? experiment.cycles : 0;
        c.writes.push_back({f, start + service + delay, arrived});
    }

    void release(int n, const Input &input, const std::string &kind) {
        int done = emit(kind, n, input.peer, &input.flit, input.cause);
        if (input.peer != Local) {
            returning[now + creditLatency].push_back({{input.peer, n}, done});
            queues.at({n, input.peer}).pop_front();
        }
    }

    void fault(const Reservation &s, int peer, const Flit &f, int cause, std::string reason) {
        emit("fault", s.node, peer, &f, cause, reason);
        failed = true;
        closed = now;
    }

    void serve(const Reservation &s) {
        const auto &requested = cores.at(s.source).source.at(s.seq);
        Input input{requested, Local, -1};
        if (s.node != s.source) {
            bool found = false;
            for (int peer : neighbours(s.node)) {
                auto &q = queues[{s.node, peer}];
                if (!q.empty() && q.front().flit.source == s.source && q.front().flit.seq == s.seq) {
                    input = q.front();
                    found = true;
                    break;
                }
            }
            if (!found)
                return fault(s, Local, requested, -1, "input-unavailable");
        }
        const auto &f = input.flit;
        std::set<int> ports;
        for (int peer : neighbours(s.node))
            if (peer != input.peer && ((f.bits.at(peer) - '0') & 2))
                ports.insert(peer);
        if ((f.bits.at(s.node) - '0') & 1)
            ports.insert(Local);
        if (s.opcode != f.opcode || ports != s.ports)
            return fault(s, Local, f, input.cause, "unreserved-fork");
        for (int port : ports) {
            if (port == Local) {
                if (cores.at(s.node).writes.size() >= static_cast<size_t>(landingDepth))
                    return fault(s, port, f, input.cause, "landing-full");
            } else {
                auto &available = credits.at({s.node, port});
                if (experiment.matches("credit", s.node, f) && port == experiment.port) {
                    heldCredits = available;
                    available = 0;
                    holdEvent = emit("credit.withhold", s.node, port, &f, input.cause,
                                     std::to_string(heldCredits));
                }
                if (available == 0)
                    return fault(s, port, f, input.cause, "no-credit");
            }
        }
        if (input.peer == Local)
            input.cause = emit("inject", s.node, Local, &f);
        for (int port : ports) {
            if (port == Local) {
                land(s.node, f, input.cause);
                if (experiment.matches("duplicate", s.node, f))
                    land(s.node, f, input.cause);
            } else {
                --credits.at({s.node, port});
                int sent = emit("send", s.node, port, &f, input.cause);
                flights[now + latency].push_back({s.node, port, sent, f});
            }
        }
        release(s.node, input, "release");
    }

    bool clockTick(SST::Cycle_t cycle) {
        now = static_cast<Tick>(cycle);
        if (origin < 0)
            rendezvous();
        deliveries();
        if (origin < 0)
            return false;
        auto due = calendar.find(now - origin);
        if (!failed && due != calendar.end()) {
            for (const auto &s : due->second) {
                serve(s);
                if (failed)
                    break;
            }
            if (!failed && due == std::prev(calendar.end()))
                closed = now;
        }
        if (failed) {
            for (auto &[port, q] : queues)
                while (!q.empty())
                    release(port.first, q.front(), "drop");
        }
        if (closed < 0 || now <= closed)
            return false;
        bool drained = flights.empty() && returning.empty();
        for (auto &[n, c] : cores) {
            if (!failed && !c.published && now > c.lastAck && c.confirmed == c.expected &&
                std::all_of(c.visible.begin(), c.visible.end(), [](bool v) { return v; })) {
                c.published = true;
                c.publication = now;
                int pub = emit("tile.publish", n, Local, nullptr, -1, c.tile);
                emit("consumer.read", n, Local, nullptr, pub);
            }
            if (!failed && c.writes.empty() && !c.published && now > c.lastAck)
                throw std::runtime_error("incomplete Tile after final accepted write");
            drained &= c.writes.empty() && (failed || (c.published && now > c.publication));
        }
        if (!drained)
            return false;
        for (const auto &p : queues)
            if (!p.second.empty())
                throw std::runtime_error("undischarged input at drain");
        for (const auto &p : credits)
            if (p.second != depth)
                throw std::runtime_error("missing credit at drain");
        emit("drain", -1, Local, nullptr, -1, failed ? "aborted" : "success");
        log.flush();
        primaryComponentOKToEndSim();
        return true;
    }
};
} // namespace WSE
