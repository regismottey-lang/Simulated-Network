// Raft consensus (leader election + log replication + commit) running inside a
// deterministic simulated network with partitions, drops, delays and crash/restart.
// Safety invariants are checked after every simulated tick.
// Build: g++ -std=c++20 -O2 -Wall -Wextra project12_raft.cpp -o raft
// Next steps for the resume: snapshots/log compaction, membership changes, real RPC transport
// (gRPC) in place of the simulator, and a linearizability checker over client histories.
#include <algorithm>
#include <cstdint>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <queue>
#include <random>
#include <string>
#include <vector>

constexpr int kNoop = -1;

struct Entry { int term; int cmd; };
enum class Role { Follower, Candidate, Leader };
enum class MsgType { RequestVote, VoteReply, AppendEntries, AppendReply };

struct Msg {
    explicit Msg(MsgType t = MsgType::RequestVote) : type(t) {}
    MsgType type;
    int from = 0, to = 0, term = 0;
    // RequestVote
    int last_log_index = 0, last_log_term = 0;
    // VoteReply
    bool granted = false;
    // AppendEntries
    int prev_index = 0, prev_term = 0, leader_commit = 0;
    std::vector<Entry> entries;
    // AppendReply
    bool success = false;
    int match_index = 0;
};

class Node {
public:
    Node(int id, int n, uint32_t seed, std::function<void(Msg)> send)
        : id(id), n_(n), rng_(seed), send_(std::move(send)) { reset_timer(); }

    // ---- persistent state (survives crash) ----
    int id;
    int term = 0;
    int voted_for = -1;
    std::vector<Entry> log{{0, 0}};  // index 0 is a sentinel; real entries start at 1
    // ---- volatile state ----
    Role role = Role::Follower;
    int commit = 0, last_applied = 0;
    std::vector<int> applied;  // the replicated state machine: list of applied client commands
    bool up = true;

    int last_index() const { return static_cast<int>(log.size()) - 1; }

    void tick() {
        if (!up) return;
        if (role == Role::Leader) {
            if (++heartbeat_ >= 3) { heartbeat_ = 0; broadcast_append(); }
        } else if (++elapsed_ >= timeout_) {
            start_election();
        }
        apply();
    }

    bool propose(int cmd) {
        if (!up || role != Role::Leader) return false;
        log.push_back({term, cmd});
        match_[id] = last_index();
        broadcast_append();
        return true;
    }

    void crash() { up = false; }
    void restart() {  // keep persistent state, rebuild volatile state
        up = true;
        role = Role::Follower;
        commit = last_applied = 0;
        applied.clear();
        reset_timer();
    }

    void on_message(const Msg& m) {
        if (!up) return;
        if (m.term > term) become_follower(m.term);
        switch (m.type) {
        case MsgType::RequestVote: on_request_vote(m); break;
        case MsgType::VoteReply: on_vote_reply(m); break;
        case MsgType::AppendEntries: on_append(m); break;
        case MsgType::AppendReply: on_append_reply(m); break;
        }
        apply();
    }

private:
    void reset_timer() { elapsed_ = 0; timeout_ = 12 + static_cast<int>(rng_() % 12); }

    void become_follower(int new_term) {
        if (new_term > term) { term = new_term; voted_for = -1; }
        role = Role::Follower;
    }

    void start_election() {
        role = Role::Candidate;
        ++term;
        voted_for = id;
        votes_ = 1;
        reset_timer();
        for (int p = 0; p < n_; ++p) {
            if (p == id) continue;
            Msg m{MsgType::RequestVote};
            m.from = id; m.to = p; m.term = term;
            m.last_log_index = last_index();
            m.last_log_term = log.back().term;
            send_(m);
        }
    }

    void on_request_vote(const Msg& m) {
        Msg r{MsgType::VoteReply};
        r.from = id; r.to = m.from; r.term = term;
        if (m.term >= term) {
            // Candidate's log must be at least as up-to-date as ours (Raft section 5.4.1).
            bool up_to_date = m.last_log_term > log.back().term ||
                              (m.last_log_term == log.back().term && m.last_log_index >= last_index());
            if ((voted_for == -1 || voted_for == m.from) && up_to_date) {
                voted_for = m.from;
                r.granted = true;
                reset_timer();
            }
        }
        send_(r);
    }

    void on_vote_reply(const Msg& m) {
        if (role != Role::Candidate || m.term != term || !m.granted) return;
        if (++votes_ > n_ / 2) become_leader();
    }

    void become_leader() {
        role = Role::Leader;
        heartbeat_ = 0;
        next_.assign(n_, last_index() + 1);
        match_.assign(n_, 0);
        log.push_back({term, kNoop});  // no-op lets the new leader commit entries from older terms
        match_[id] = last_index();
        broadcast_append();
    }

    void broadcast_append() {
        for (int p = 0; p < n_; ++p) if (p != id) send_append(p);
    }

    void send_append(int p) {
        Msg m{MsgType::AppendEntries};
        m.from = id; m.to = p; m.term = term;
        m.prev_index = next_[p] - 1;
        m.prev_term = log[m.prev_index].term;
        m.entries.assign(log.begin() + next_[p], log.end());
        m.leader_commit = commit;
        send_(m);
    }

    void on_append(const Msg& m) {
        Msg r{MsgType::AppendReply};
        r.from = id; r.to = m.from; r.term = term;
        if (m.term < term) { send_(r); return; }  // stale leader
        role = Role::Follower;                    // a valid leader exists for this term
        reset_timer();
        if (m.prev_index > last_index() || log[m.prev_index].term != m.prev_term) {
            send_(r);  // log inconsistency: leader will back up next_index
            return;
        }
        for (size_t i = 0; i < m.entries.size(); ++i) {
            size_t idx = static_cast<size_t>(m.prev_index) + 1 + i;
            if (idx < log.size()) {
                if (log[idx].term != m.entries[i].term) {  // conflict: drop our suffix
                    log.resize(idx);
                    log.push_back(m.entries[i]);
                }
            } else {
                log.push_back(m.entries[i]);
            }
        }
        int last_new = m.prev_index + static_cast<int>(m.entries.size());
        if (m.leader_commit > commit) commit = std::min(m.leader_commit, last_new);
        r.success = true;
        r.match_index = last_new;
        send_(r);
    }

    void on_append_reply(const Msg& m) {
        if (role != Role::Leader || m.term != term) return;
        if (m.success) {
            match_[m.from] = std::max(match_[m.from], m.match_index);
            next_[m.from] = match_[m.from] + 1;
            advance_commit();
        } else {
            next_[m.from] = std::max(1, next_[m.from] - 1);
            send_append(m.from);
        }
    }

    void advance_commit() {
        for (int idx = last_index(); idx > commit; --idx) {
            if (log[idx].term != term) break;  // only commit current-term entries by counting
            int count = 0;
            for (int p = 0; p < n_; ++p) count += match_[p] >= idx;
            if (count > n_ / 2) { commit = idx; break; }
        }
    }

    void apply() {
        while (last_applied < commit) {
            ++last_applied;
            if (log[last_applied].cmd != kNoop) applied.push_back(log[last_applied].cmd);
        }
    }

    int n_;
    std::mt19937 rng_;
    std::function<void(Msg)> send_;
    int elapsed_ = 0, timeout_ = 0, heartbeat_ = 0, votes_ = 0;
    std::vector<int> next_, match_;
};

// Deterministic simulated cluster.
class Cluster {
public:
    Cluster(int n, uint32_t seed) : rng_(seed), group_(n, 0) {
        nodes_.reserve(n);
        for (int i = 0; i < n; ++i)
            nodes_.push_back(std::make_unique<Node>(i, n, seed * 1000 + i, [this](Msg m) { enqueue(std::move(m)); }));
    }

    Node& node(int i) { return *nodes_[i]; }
    int size() const { return static_cast<int>(nodes_.size()); }
    void set_drop_rate(double p) { drop_ = p; }
    void partition(const std::vector<int>& group_ids) { group_ = group_ids; }
    void heal() { std::fill(group_.begin(), group_.end(), 0); }
    const std::string& violation() const { return violation_; }

    Node* leader() {  // live leader with the highest term
        Node* best = nullptr;
        for (auto& n : nodes_)
            if (n->up && n->role == Role::Leader && (!best || n->term > best->term)) best = n.get();
        return best;
    }

    void step() {
        ++now_;
        while (!q_.empty() && q_.top().at <= now_) {
            Msg m = q_.top().msg;
            q_.pop();
            if (group_[m.from] == group_[m.to]) nodes_[m.to]->on_message(m);
        }
        for (auto& n : nodes_) n->tick();
        check_invariants();
    }
    void run(int ticks) { for (int i = 0; i < ticks && violation_.empty(); ++i) step(); }

private:
    struct Pending {
        int64_t at; uint64_t seq; Msg msg;
        bool operator<(const Pending& o) const { return at != o.at ? at > o.at : seq > o.seq; }
    };

    void enqueue(Msg m) {
        if (group_[m.from] != group_[m.to]) return;               // partitioned
        if (std::uniform_real_distribution<>(0, 1)(rng_) < drop_) return;  // dropped
        int64_t delay = 1 + static_cast<int64_t>(rng_() % 3);
        q_.push({now_ + delay, seq_++, std::move(m)});
    }

    void check_invariants() {
        // 1. Election safety: at most one leader per term.
        for (auto& n : nodes_) {
            if (!n->up || n->role != Role::Leader) continue;
            auto [it, inserted] = leader_of_term_.emplace(n->term, n->id);
            if (!inserted && it->second != n->id)
                violation_ = "two leaders in term " + std::to_string(n->term);
        }
        // 2. State machine safety: every committed entry is identical on all nodes, forever.
        for (auto& n : nodes_) {
            for (int i = 1; i <= n->commit && i <= n->last_index(); ++i) {
                if (i > static_cast<int>(committed_.size())) {
                    if (i == static_cast<int>(committed_.size()) + 1) committed_.push_back(n->log[i]);
                } else if (committed_[i - 1].term != n->log[i].term || committed_[i - 1].cmd != n->log[i].cmd) {
                    violation_ = "committed entry " + std::to_string(i) + " diverged on node " + std::to_string(n->id);
                }
            }
        }
    }

    std::vector<std::unique_ptr<Node>> nodes_;
    std::mt19937_64 rng_;
    std::vector<int> group_;
    std::priority_queue<Pending> q_;
    int64_t now_ = 0;
    uint64_t seq_ = 0;
    double drop_ = 0;
    std::map<int, int> leader_of_term_;
    std::vector<Entry> committed_;
    std::string violation_;
};

static std::string show(const std::vector<int>& v) {
    std::string s = "[";
    for (size_t i = 0; i < v.size(); ++i) s += (i ? " " : "") + std::to_string(v[i]);
    return s + "]";
}

static bool scenario() {
    std::cout << "== Scenario: partition, failover, heal ==\n";
    Cluster c(5, 7);
    c.run(100);
    Node* l1 = c.leader();
    if (!l1) { std::cout << "no leader elected\n"; return false; }
    std::cout << "leader elected: node " << l1->id << " (term " << l1->term << ")\n";
    for (int cmd = 1; cmd <= 5; ++cmd) l1->propose(cmd);
    c.run(60);

    int old = l1->id, buddy = (old + 1) % 5;  // isolate old leader + one follower (minority)
    std::vector<int> groups(5, 1);
    groups[old] = groups[buddy] = 0;
    c.partition(groups);
    l1->propose(99);  // accepted by the isolated leader but can never reach a majority
    c.run(120);
    Node* l2 = nullptr;
    for (int i = 0; i < 5; ++i)
        if (groups[i] == 1 && c.node(i).role == Role::Leader) l2 = &c.node(i);
    if (!l2) { std::cout << "majority side failed to elect a leader\n"; return false; }
    std::cout << "partitioned old leader " << old << "; new leader: node " << l2->id << " (term " << l2->term << ")\n";
    for (int cmd = 6; cmd <= 8; ++cmd) l2->propose(cmd);
    c.run(60);

    c.heal();
    c.run(150);
    std::vector<int> expect{1, 2, 3, 4, 5, 6, 7, 8};
    bool ok = c.violation().empty();
    for (int i = 0; i < 5; ++i) {
        std::cout << "node " << i << " applied " << show(c.node(i).applied) << "\n";
        ok = ok && c.node(i).applied == expect;
    }
    std::cout << "uncommitted entry 99 was discarded on healing; all nodes converged: " << (ok ? "PASS" : "FAIL") << "\n";
    return ok;
}

static bool chaos(int seeds) {
    std::cout << "== Chaos: " << seeds << " seeds, random partitions/crashes/drops ==\n";
    int total_committed = 0;
    for (int seed = 1; seed <= seeds; ++seed) {
        Cluster c(5, seed);
        c.set_drop_rate(0.1);
        std::mt19937 rng(seed * 31 + 5);
        int next_cmd = 1;
        for (int step = 0; step < 4000 && c.violation().empty(); ++step) {
            if (step % 150 == 0) {
                switch (rng() % 4) {
                case 0: { std::vector<int> g(5); for (auto& x : g) x = static_cast<int>(rng() % 2); c.partition(g); break; }
                case 1: c.heal(); break;
                case 2: c.node(static_cast<int>(rng() % 5)).crash(); break;
                case 3: c.node(static_cast<int>(rng() % 5)).restart(); break;
                }
            }
            if (step % 20 == 0)
                if (Node* l = c.leader()) l->propose(next_cmd++);
            c.step();
        }
        if (!c.violation().empty()) {
            std::cout << "seed " << seed << " VIOLATION: " << c.violation() << "\n";
            return false;
        }
        for (int i = 0; i < 5; ++i) total_committed = std::max(total_committed, c.node(i).commit);
    }
    std::cout << "no safety violations (max commit index seen: " << total_committed << "): PASS\n";
    return true;
}

int main() {
    bool ok = scenario();
    ok = chaos(50) && ok;
    return ok ? 0 : 1;
}

