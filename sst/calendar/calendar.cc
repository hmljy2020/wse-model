// A physical Calendar mesh node. All cross-node state travels on SST links.
#include <sst/core/sst_config.h>
#include <sst/core/component.h>
#include <sst/core/link.h>

#include <algorithm>
#include <cstdint>
#include <deque>
#include <fstream>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace WSE {
using Tick = int64_t;
using Observation = int64_t;
constexpr int Local = -1;
constexpr int ReservationExit = 42;

struct Flit {
    int source = -1, seq = -1, opcode = -1, epoch = 1, address = -1;
    std::string bits, payload;
};

enum class Kind { Prepare, Report, Ready, Start, Align, Slot, Data, Credit, Ack, Check, Done };

class Message : public SST::Event {
  public:
    Kind kind{};
    Observation cause = -1;
    Tick origin = -1;
    Flit flit;

    Message() = default;
    explicit Message(Kind k) : kind(k) {
        // Arrivals, returned credits and Shared acknowledgements precede a hard slot.
        setPriority(k == Kind::Slot ? EVENTPRIORITY + 1 : EVENTPRIORITY);
    }
    void serialize_order(SST::Core::Serialization::serializer &ser) override {
        Event::serialize_order(ser);
        SST_SER(kind);
        SST_SER(cause);
        SST_SER(origin);
        SST_SER(flit.source);
        SST_SER(flit.seq);
        SST_SER(flit.opcode);
        SST_SER(flit.epoch);
        SST_SER(flit.address);
        SST_SER(flit.bits);
        SST_SER(flit.payload);
    }
    ImplementSerializable(WSE::Message);
};

struct Port {
    SST::Link *link;
    int credit;
    std::deque<std::pair<Flit, Observation>> fifo;
};
struct Reservation {
    int opcode, source, seq, ingress;
    std::set<int> ports;
};
struct Core {
    int expected = 0, confirmed = 0;
    Tick lastAck = -1, publication = -1;
    std::string tile;
    std::vector<Flit> source;
    std::set<std::pair<int, int>> landed;
    std::vector<bool> visible;
    std::deque<std::pair<Flit, Observation>> writes;
    Tick tailDue = 0;
};
struct Experiment {
    std::string kind;
    int source, node, seq, port, cycles;
    bool matches(const std::string &k, int n, const Flit &f) const {
        return kind == k && node == n && source == f.source && seq == f.seq;
    }
};

class Node : public SST::Component {
  public:
    SST_ELI_REGISTER_COMPONENT(Node, "wsecalendar", "node", SST_ELI_ELEMENT_VERSION(1, 0, 0),
                               "Calendar mesh router and core", COMPONENT_CATEGORY_NETWORK)
    SST_ELI_DOCUMENT_PARAMS({"node", "Row-major node index", nullptr}, {"rows", "Physical rows", nullptr},
                            {"cols", "Physical columns", nullptr},
                            {"image", "Node deployment image", nullptr},
                            {"events", "Node observations", nullptr}, {"clock", "Node period", nullptr},
                            {"link_cycles", "Flit and control latency", nullptr},
                            {"credit_cycles", "Credit latency", nullptr})
    SST_ELI_DOCUMENT_PORTS({"peer%d", "Bidirectional neighbour port", {"WSE::Message"}})

    Node(SST::ComponentId_t id, SST::Params &params) : SST::Component(id) {
        n = params.find<int>("node");
        rows = params.find<int>("rows");
        cols = params.find<int>("cols");
        latency = params.find<int>("link_cycles");
        creditLatency = params.find<int>("credit_cycles");
        load(params.find<std::string>("image"));
        log.exceptions(std::ios::failbit | std::ios::badbit);
        log.open(params.find<std::string>("events"));
        log << "id\tcycle\trelative_cycle\tkind\tnode\tpeer\tsource\tflit\topcode\tepoch\tcause\t"
               "address\tpayload\tdetail\n";
        auto period = registerTimeBase(params.find<std::string>("clock"));
        timer = configureSelfLink<&Node::handle>("timer", period, this, Local);
        // Tree paths from R0 go down the first column and then across each row.
        parent = parentOf(n);
        if (n >= cols)
            connect(n - cols, period);
        if (n % cols)
            connect(n - 1, period);
        if (n % cols + 1 < cols)
            connect(n + 1, period);
        if (n + cols < rows * cols)
            connect(n + cols, period);
        if (n == 0) {
            registerAsPrimaryComponent();
            primaryComponentDoNotEndSim();
        }
    }

    void setup() override { later(1, Kind::Prepare); }

  private:
    int n{}, rows{}, cols{}, latency{}, creditLatency{}, depth{}, landingDepth{}, service{}, base{}, size{};
    int parent{};
    std::vector<int> children;
    size_t ready = 0, done = 0;
    Tick now = 0, origin = -1, finalSlot{};
    Observation sequence = 0;
    bool reported = false, completed = false;
    Experiment experiment;
    Core core;
    SST::Link *timer{};
    std::map<int, Port> ports;
    std::map<Tick, std::vector<Reservation>> calendar;
    std::ofstream log;

    int parentOf(int node) const { return node == 0 ? Local : node - (node % cols ? 1 : cols); }

    void connect(int peer, SST::TimeConverter period) {
        auto *wire = configureLink<&Node::handle>("peer" + std::to_string(peer), period, this, peer);
        if (!wire)
            throw std::runtime_error("missing mesh link");
        ports.emplace(peer, Port{wire, depth, {}});
        if (parentOf(peer) == n)
            children.push_back(peer);
    }

    void load(const std::string &path) {
        std::ifstream in(path);
        if (!in)
            throw std::runtime_error("cannot open " + path);
        std::string tag;
        Flit source;
        source.source = n;
        while (in >> tag) {
            if (tag == "resources") {
                in >> depth >> landingDepth >> service >> base >> size >> finalSlot;
            } else if (tag == "source") {
                int offset;
                in >> source.opcode >> source.bits >> offset >> core.expected;
                source.address = base + offset;
            } else if (tag == "flit") {
                in >> source.seq >> source.payload;
                core.source.push_back(source);
                source.address += static_cast<int>(source.payload.size() / 2);
            } else if (tag == "slot") {
                Tick cycle;
                Reservation s;
                int count;
                in >> cycle >> s.opcode >> s.source >> s.seq >> s.ingress >> count;
                for (int i = 0, port; i < count; ++i) {
                    in >> port;
                    s.ports.insert(port);
                }
                calendar[cycle].push_back(std::move(s));
            } else if (tag == "case") {
                in >> experiment.kind >> experiment.source >> experiment.node >> experiment.seq >>
                    experiment.port >> experiment.cycles;
            } else
                throw std::runtime_error("unknown image record: " + tag);
            if (!in)
                throw std::runtime_error("incomplete image record: " + tag);
        }
    }

    Observation emit(const char *kind, int peer = Local, const Flit *f = nullptr, Observation cause = -1,
                     const std::string &detail = "-") {
        Observation id = ++sequence * rows * cols + n;
        log << id << '\t' << now << '\t' << (origin >= 0 ? now - origin : -1) << '\t' << kind << '\t' << n
            << '\t' << peer << '\t' << (f ? f->source : -1) << '\t' << (f ? f->seq : -1) << '\t'
            << (f ? f->opcode : -1) << '\t' << (f ? f->epoch : -1) << '\t' << cause << '\t'
            << (f ? f->address : -1) << '\t' << (f && !f->payload.empty() ? f->payload : "-") << '\t'
            << detail << std::endl;
        return id;
    }

    void later(Tick delay, Kind kind) { timer->send(delay, new Message(kind)); }

    void transmit(int peer, Kind kind, Observation cause = -1, const Flit *flit = nullptr) {
        auto *msg = new Message(kind);
        msg->cause = cause;
        msg->origin = origin;
        if (flit)
            msg->flit = *flit;
        int cycles = kind == Kind::Credit ? creditLatency : latency;
        ports.at(peer).link->send(cycles - std::min(latency, creditLatency), msg);
    }

    void writeBytes(const Flit &f) {
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

    void prepare() {
        if (!core.source.empty()) {
            auto bound = emit("source.ready");
            core.tile.assign(2ULL * size, '0');
            core.visible.assign(size, false);
            auto reg = emit("range.register", Local, nullptr, bound, std::to_string(base));
            emit("resource.reserve", Local, nullptr, reg, std::to_string(size));
            for (const auto &f : core.source) {
                writeBytes(f);
                emit("local.write", Local, &f, reg);
            }
        }
        later(experiment.kind == "late" && experiment.source == n ? experiment.cycles : 0, Kind::Report);
    }

    void gatherReady() {
        if (!reported || ready != children.size())
            return;
        emit("node.ready", parent);
        if (parent != Local)
            transmit(parent, Kind::Ready);
        else
            start(now + (Tick(rows) + cols - 2) * latency);
    }

    void start(Tick common) {
        origin = common;
        for (int child : children)
            transmit(child, Kind::Start);
        later(origin - now, Kind::Align);
        for (const auto &entry : calendar)
            later(origin - now + entry.first, Kind::Slot);
        later(origin - now + finalSlot + 1, Kind::Check);
    }

    void land(const Flit &f, Observation cause) {
        auto arrived = emit("land", Local, &f, cause);
        if (!core.landed.emplace(f.source, f.seq).second) {
            emit("duplicate", Local, &f, arrived);
            return;
        }
        Tick delay = experiment.matches("delay_ack", n, f) ? experiment.cycles : 0;
        core.tailDue = std::max(now, core.tailDue) + service + delay;
        core.writes.emplace_back(f, arrived);
        later(core.tailDue - now, Kind::Ack);
    }

    void fault(int peer, const Flit &f, Observation cause, const char *reason) {
        emit("fault", peer, &f, cause, reason);
        fatal(CALL_INFO, ReservationExit,
              "Calendar reservation fatal epoch=%d relative=%lld node=%d port=%d source=%d flit=%d "
              "reason=%s\n",
              f.epoch, static_cast<long long>(now - origin), n, peer, f.source, f.seq, reason);
    }

    void serve(const Reservation &s) {
        Observation cause = -1;
        Flit f;
        if (s.ingress == Local)
            f = core.source.at(s.seq);
        else {
            auto &q = ports.at(s.ingress).fifo;
            if (q.empty() || q.front().first.source != s.source || q.front().first.seq != s.seq) {
                f.source = s.source;
                f.seq = s.seq;
                f.opcode = s.opcode;
                return fault(s.ingress, f, cause, "input-unavailable");
            }
            f = q.front().first;
            cause = q.front().second;
        }
        std::set<int> outputs;
        for (const auto &[peer, port] : ports)
            if (peer != s.ingress && ((f.bits.at(peer) - '0') & 2))
                outputs.insert(peer);
        if ((f.bits.at(n) - '0') & 1)
            outputs.insert(Local);
        if (s.opcode != f.opcode || outputs != s.ports)
            return fault(Local, f, cause, "unreserved-fork");
        for (int peer : outputs) {
            if (peer == Local) {
                if (core.writes.size() >= static_cast<size_t>(landingDepth))
                    return fault(peer, f, cause, "landing-full");
            } else {
                auto &credit = ports.at(peer).credit;
                if (experiment.matches("credit", n, f) && peer == experiment.port) {
                    emit("credit.withhold", peer, &f, cause, std::to_string(credit));
                    credit = 0;
                }
                if (!credit)
                    return fault(peer, f, cause, "no-credit");
            }
        }
        if (s.ingress == Local)
            cause = emit("inject", Local, &f);
        for (int peer : outputs) {
            if (peer == Local) {
                land(f, cause);
                if (experiment.matches("duplicate", n, f))
                    land(f, cause);
            } else {
                --ports.at(peer).credit;
                transmit(peer, Kind::Data, emit("send", peer, &f, cause), &f);
            }
        }
        auto freed = emit("release", s.ingress, &f, cause);
        if (s.ingress != Local) {
            ports.at(s.ingress).fifo.pop_front();
            transmit(s.ingress, Kind::Credit, freed);
        }
    }

    void checkCompletion() {
        if (completed || origin < 0 || now <= origin + finalSlot || !core.writes.empty())
            return;
        if (!core.source.empty()) {
            if (now <= core.lastAck)
                return;
            if (core.publication < 0) {
                if (core.confirmed != core.expected ||
                    !std::all_of(core.visible.begin(), core.visible.end(), [](bool v) { return v; }))
                    throw std::runtime_error("incomplete Tile after final accepted write");
                core.publication = now;
                auto pub = emit("tile.publish", Local, nullptr, -1, core.tile);
                emit("consumer.read", Local, nullptr, pub);
                later(1, Kind::Check);
            }
            if (now <= core.publication)
                return;
        }
        if (done != children.size())
            return;
        for (const auto &[peer, port] : ports)
            if (!port.fifo.empty() || port.credit != depth)
                return;
        completed = true;
        emit("node.done", parent);
        if (parent != Local)
            transmit(parent, Kind::Done);
        else {
            emit("drain", Local, nullptr, -1, "success");
            primaryComponentOKToEndSim();
        }
    }

    void handle(SST::Event *event, int peer) {
        std::unique_ptr<Message> msg(static_cast<Message *>(event));
        now = getCurrentSimTime();
        switch (msg->kind) {
        case Kind::Prepare:
            prepare();
            break;
        case Kind::Report:
            if (!core.source.empty())
                emit("bsp.report");
            reported = true;
            gatherReady();
            break;
        case Kind::Ready:
            ++ready;
            gatherReady();
            break;
        case Kind::Start:
            start(msg->origin);
            break;
        case Kind::Align:
            emit(n == 0 ? "bsp.align" : "node.align");
            break;
        case Kind::Slot:
            for (const auto &s : calendar.at(now - origin))
                serve(s);
            break;
        case Kind::Data: {
            auto &fifo = ports.at(peer).fifo;
            if (fifo.size() >= static_cast<size_t>(depth))
                throw std::runtime_error("ingress overflow");
            fifo.emplace_back(msg->flit, emit("arrive", peer, &msg->flit, msg->cause));
            break;
        }
        case Kind::Credit:
            if (++ports.at(peer).credit > depth)
                throw std::runtime_error("credit overflow");
            emit("credit.return", peer, nullptr, msg->cause);
            checkCompletion();
            break;
        case Kind::Ack: {
            const auto &[f, cause] = core.writes.front();
            writeBytes(f);
            core.confirmed += static_cast<int>(f.payload.size() / 2);
            emit("shared.ack", Local, &f, cause);
            core.lastAck = now;
            core.writes.pop_front();
            if (now >= origin + finalSlot)
                later(1, Kind::Check);
            break;
        }
        case Kind::Check:
            checkCompletion();
            break;
        case Kind::Done:
            ++done;
            checkCompletion();
            break;
        }
    }
};
} // namespace WSE
