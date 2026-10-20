# Miniconsensus

A small, leaderless, Avalanche-style consensus exercise using Python 3 and UDP unicast. Five independent processes choose one candidate data block and each writes its own `ledger.json`. Runtime and tests use only the Python standard library.

## How to run the 5-node demo

Requirements: Python 3.9 or newer, with `python3` available. No packages to install. Stop any old nodes using ports 8001-8005 before starting.

```bash
cd miniconsensus
python3 run.py --demo
```

The launcher starts five separate processes, waits for their `READY` messages, and sends `/start` to each process. Each node proposes its own block. Normally, the demo finishes in about 30 seconds. The launcher prints every final result, verifies the five local ledgers, and stops its child processes. Exit code 0 and `PASS` mean all launched nodes agreed and their saved blocks passed the hash check. Ctrl+C stops the launcher and its children early.

Outputs:

- `data/8001/ledger.json` through `data/8005/ledger.json`: one accepted block per node.
- `logs/run-*/command-1.log` through `command-5.log`: complete output of each process.
- A fresh run replaces the previous ledger for each participating port after finalization. Existing ledgers are not used as input or as proof that the new run passed.

## Interactive mode and editable commands

Edit `COMMANDS` at the top of `run.py`, then run:

```bash
python3 run.py
```

Launcher commands:

```text
all /start
all /status
1 /peers
status
/quit
```

`all /start` begins the decision. `all /status` writes each node's consensus status to its own log. `status` prints process liveness in the launcher. `1 /peers` sends a command to the first process. `/quit` terminates all launched children.

Command strings are split into executable arguments; shell operators such as pipes, `&&`, or redirection are not interpreted. The launcher starts all commands without waiting for the previous command to exit. The OS still schedules process creation sequentially; perfectly simultaneous execution is neither required nor promised.

To run manually, use one terminal per command, then enter `/start` in each after all are ready:

```bash
python3 peer.py 8001
python3 peer.py 8002
python3 peer.py 8003
python3 peer.py 8004
python3 peer.py 8005
```

The only node argument is its port. Each node reads `peers.json` from the working directory and writes to `data/<port>/ledger.json`. Node commands: `/start`, `/status`, `/peers`, `/quit`. One process run performs at most one consensus decision.

## One node absent at startup

Remove or comment out the command for port 8005 in `run.py`; **keep all five entries in `peers.json`**. Run `python3 run.py --demo` again. Only four nodes start; the fifth remains a possible sampling target. Restore the command afterwards for the five-node demo.

Each sampling round waits 1.5 seconds, then counts the responses it actually received. It never waits forever for an absent peer. With K=3 and alpha=2, a sample containing one absent node can still succeed if the two responding nodes agree. The threshold is not lowered when a node fails. A failed round resets confidence to zero. With insufficient agreement, the nodes keep trying; the demo launcher has a separate 120-second observation limit and reports failure rather than claiming consensus.

## Architecture and protocol

Each process has one UDP socket bound to `127.0.0.1:<port>`, a listener thread, a sampling thread, and private in-memory state. Membership comes from `peers.json`; IDs are explicit `host:port` addresses. No discovery socket, broadcast, multicast, or shared discovery port remains. Every outgoing datagram targets one configured address.

JSON messages:

| Type | Fields | Purpose |
|---|---|---|
| `BLOCK_PROPOSAL` | `type`, `block` | Announce the complete candidate block |
| `BLOCK_QUERY` | `type`, `query_id`, `block` | Ask a sampled neighbor for its preference |
| `BLOCK_RESPONSE` | `type`, `query_id`, `block` | Return the full currently preferred block |
| `BLOCK_FINALIZED` | `type`, `block` | Announce a local decision; never force another node to finalize |

The block has `index`, `timestamp`, `proposer`, `prev_hash`, `transactions`, and `block_hash`. The only supported index is 1, with an all-zero previous hash. Transactions are generated at startup; no CSV file was supplied with the starter.

Hash calculation: remove `block_hash`, serialize the remaining fields with `json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=False)`, encode as UTF-8, and compute `hashlib.sha256(...).hexdigest()`. Transaction order remains significant. Recipients validate the shape and recompute the hash before caching or counting a block. Invalid blocks are logged and ignored. A sender outside the configured membership is ignored.

After `/start`, each node announces its proposal four times over two seconds. It chooses the smallest hash among valid proposals observed during that period as its **initial preference**. This is a deterministic bootstrap tie-break, not consensus. It helps avoid a stalled initial state with five unique preferences and no repeated response. It assumes reasonably coordinated local startup; delivery to all nodes is not guaranteed.

Then each node repeatedly samples K=3 random distinct neighbors. At least alpha=2 must return the same block hash. If that hash matches the current preference, confidence increases; if it differs, the node adopts it with confidence 1. Without the threshold, confidence resets to 0. After beta=10 consecutive successful rounds for the preference, the node writes its validated block to its own ledger and announces finalization. A finalized node keeps responding while its process remains alive.

Each query has a unique ID. Only configured, sampled senders with a matching active ID are counted, once per sender. Late responses cannot enter a later round. A received finalization notice only supplies a validated block; the recipient must reach its own beta threshold.

Ledger output is a one-element JSON array. Writing uses a temporary file and rename to avoid exposing a partially written JSON file. This is not a multi-block chain, and no disk-fsync durability guarantee is claimed.

## Why this is real networking

Nodes have separate processes, sockets, address spaces, block stores, confidence counters, and ledger files. All inter-node protocol traffic uses the OS UDP stack over loopback. Threads only handle concurrent work inside a node. `run.py` is a launcher and observer: stdin starts the experiment, and logs/ledgers are read to check results. Nodes never use these files or launcher pipes to exchange votes or blocks with one another.

## Verification and evidence

```bash
python3 -B -m unittest -v test_peer
```

Eight tests cover canonical hashing, tampered payloads, malformed block structures, stale/unsampled/duplicate responses, invalid received blocks, independent finalization, five real node processes, and a run with one of five configured nodes absent. The two integration tests take approximately one minute in total and allocate temporary loopback ports.

`evidence/` contains actual recorded outputs, saved ledgers, the test result, and screenshots of the recorded run. `evidence/five-nodes/` is the default-port demo; `evidence/four-nodes/` is the absent-node experiment. Each run had a distinct proposal from every running node and finished at round 10 on every running node. These are observed results, not a bound on every future run.

## Limitations

This is a classroom, Avalanche-inspired implementation, not production Snowball or a proof of Byzantine safety. The initial minimum-hash rule is predictable and can be manipulated by a malicious proposer. Membership is static. There is no authentication, ledger recovery, reliable delivery, transaction balance validation, or arbitrary-partition safety guarantee. A hash detects an inconsistent payload; it does not establish who created a block or whether a payment is legitimate. UDP loss or delayed startup can hurt convergence, and a sufficiently long partition can lead to conflicting local decisions.

The next hardening step would be authenticated messages plus an explicit, retried proposal/bootstrap phase. A production protocol would also need a defensible quorum/finality model and failure analysis rather than the starter README's unsupported percentage guarantees.

## Submission contents

- `peer.py`, `run.py`, `peers.json`, `test_peer.py`
- This README
- `report.pdf`: four-page technical report
- `evidence/`: screenshots, run logs, ledgers, test output
