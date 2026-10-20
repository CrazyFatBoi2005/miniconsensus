# Recorded evidence

- `demo.txt`: stdout/stderr of `python3 run.py --demo` on ports 8001-8005.
- `five-nodes/command-*.log`: original per-process logs from that run.
- `five-nodes/800*/ledger.json`: copies of all five accepted blocks from that run.
- `four-nodes/peers.json`: five configured addresses, 19001-19005.
- `four-nodes/19001.log` through `19004.log`: logs from four running processes; 19005 was never started.
- `four-nodes/1900*/ledger.json`: accepted blocks from the four-node experiment.
- `tests.txt`: actual output of the eight-test suite, including real subprocess networking tests.
- `five-nodes.jpg`: browser screenshot of a display built from the saved five-node outputs and ledgers. It is a presentation of recorded evidence, not a screenshot of a live terminal or a new simulation.

Each recorded running node had a distinct proposed block, finalized at round 10, and reached its own confidence threshold. Hashes from different experiments may differ because timestamps and proposer addresses are part of the payload.

The paths inside the raw logs identify their original recording locations. Use the accompanying relative paths in this directory to inspect the archived copies.

`clean-install-check.txt` records a further check of the extracted submission ZIP in a fresh temporary directory. That run took 10-11 rounds per node and ended with five matching, validated ledgers. This is a different run from the fixed screenshot above.
