import hashlib
import json
from pathlib import Path
import random
import socket
import sys
import threading
import time
import uuid


K = 3
ALPHA = 2
BETA = 10
QUERY_INTERVAL = 1.0
QUERY_TIMEOUT = 1.5


class AvalanchePeer:
    def __init__(self, port):
        self.port = port
        self.host = "127.0.0.1"
        self.peer_id = f"{self.host}:{port}"
        self.known_peers = self._parse_json("peers.json")
        self.lock = threading.RLock()
        self.blocks = {}
        self.proposal = self._create_block()
        self.blocks[self.proposal["block_hash"]] = self.proposal
        self.my_vote = None
        self.my_confidence = 0
        self.finalized = False
        self.round = 0
        self.pending_queries = {}
        self.started = False
        self.running = True
        self.ledger_path = Path("data") / str(port) / "ledger.json"
        self.consensus_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.consensus_sock.bind((self.host, self.port))
        self.consensus_sock.settimeout(0.5)

    def _parse_json(self, path):
        with open(path, encoding="utf-8") as f:
            peers = json.load(f)
        known_peers = {}
        for peer in peers:
            peer_id = f"{peer['host']}:{peer['port']}"
            if peer_id != self.peer_id:
                known_peers[peer_id] = peer
        return known_peers

    def _block_hash(self, block):
        fields = {key: value for key, value in block.items() if key != "block_hash"}
        data = json.dumps(fields, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(data.encode("utf-8")).hexdigest()

    def _create_block(self):
        block = {
            "index": 1,
            "timestamp": int(time.time()),
            "proposer": self.peer_id,
            "prev_hash": "0" * 64,
            "transactions": ["Alice pays Bob 10", f"Bob pays Carol {self.port % 10 + 1}"],
        }
        block["block_hash"] = self._block_hash(block)
        return block

    def _accept_block(self, block):
        fields = {"index", "timestamp", "proposer", "prev_hash", "transactions", "block_hash"}
        if not isinstance(block, dict) or set(block) != fields:
            return False
        if type(block["index"]) is not int or block["index"] != 1:
            return False
        if type(block["timestamp"]) is not int or block["timestamp"] < 0:
            return False
        if not isinstance(block["proposer"], str):
            return False
        if block["proposer"] not in {*self.known_peers, self.peer_id}:
            return False
        if block["prev_hash"] != "0" * 64:
            return False
        transactions = block["transactions"]
        if not isinstance(transactions, list) or not 1 <= len(transactions) <= 20:
            return False
        if any(not isinstance(tx, str) or not 1 <= len(tx) <= 1000 for tx in transactions):
            return False
        if block["block_hash"] != self._block_hash(block):
            return False
        self.blocks[block["block_hash"]] = block
        return True

    def _parse_peer_id(self, peer_id):
        host, port = peer_id.rsplit(":", 1)
        return host, int(port)

    def _send_udp(self, message, host, port):
        data = json.dumps(message, ensure_ascii=False).encode("utf-8")
        if len(data) > 60000:
            raise ValueError("Message is too large for UDP")
        try:
            self.consensus_sock.sendto(data, (host, port))
        except OSError as error:
            if self.running:
                print(f"Send failed: {error}", flush=True)

    def _send_to_all(self, message):
        for peer in self.known_peers:
            host, port = self._parse_peer_id(peer)
            self._send_udp(message, host, port)

    def _select_random_peers(self, n):
        peers = list(self.known_peers)
        return random.sample(peers, min(n, len(peers)))

    def _start_udp_listener(self):
        def consensus_loop():
            while self.running:
                try:
                    data, addr = self.consensus_sock.recvfrom(65535)
                except socket.timeout:
                    continue
                except OSError:
                    break
                try:
                    message = json.loads(data.decode("utf-8"))
                except (UnicodeError, ValueError):
                    print("Rejected malformed message", flush=True)
                    continue
                self._handle_udp(message, addr[0], addr[1])

        threading.Thread(target=consensus_loop, daemon=True).start()

    def _handle_udp(self, message, sender_ip, sender_port):
        sender = f"{sender_ip}:{sender_port}"
        if sender not in self.known_peers or not isinstance(message, dict):
            return
        msg_type = message.get("type")
        if msg_type not in ("BLOCK_PROPOSAL", "BLOCK_QUERY", "BLOCK_RESPONSE", "BLOCK_FINALIZED"):
            return
        with self.lock:
            block = message.get("block")
            if not self._accept_block(block):
                print(f"Rejected invalid block from {sender}", flush=True)
                return
            if msg_type == "BLOCK_QUERY":
                query_id = message.get("query_id")
                if not isinstance(query_id, str):
                    return
                preferred = self.blocks[self.my_vote] if self.my_vote else self.proposal
                response = {"type": "BLOCK_RESPONSE", "query_id": query_id, "block": preferred}
                self._send_udp(response, sender_ip, sender_port)
            elif msg_type == "BLOCK_RESPONSE":
                query_id = message.get("query_id")
                if not isinstance(query_id, str):
                    return
                pending = self.pending_queries.get(query_id)
                if pending and sender in pending["targets"]:
                    pending["responses"][sender] = block["block_hash"]

    def _finalize(self):
        block = self.blocks[self.my_vote]
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.ledger_path.with_suffix(".tmp")
        with temporary.open("w", encoding="utf-8") as f:
            json.dump([block], f, indent=2, ensure_ascii=False)
        temporary.replace(self.ledger_path)
        self.finalized = True
        print(f"FINALIZED {self.my_vote} rounds={self.round} ledger={self.ledger_path}", flush=True)
        self._send_to_all({"type": "BLOCK_FINALIZED", "block": block})

    def _avalanche_loop(self):
        try:
            for _ in range(4):
                if not self.running:
                    return
                self._send_to_all({"type": "BLOCK_PROPOSAL", "block": self.proposal})
                time.sleep(0.5)
            with self.lock:
                self.my_vote = min(self.blocks)
            while self.running and not self.finalized:
                targets = self._select_random_peers(K)
                query_id = uuid.uuid4().hex
                with self.lock:
                    self.pending_queries[query_id] = {"targets": set(targets), "responses": {}}
                    block = self.blocks[self.my_vote]
                for peer in targets:
                    host, port = self._parse_peer_id(peer)
                    self._send_udp({"type": "BLOCK_QUERY", "query_id": query_id, "block": block}, host, port)
                time.sleep(QUERY_TIMEOUT)
                if not self.running:
                    return
                with self.lock:
                    responses = self.pending_queries.pop(query_id)["responses"]
                    counts = {}
                    for block_hash in responses.values():
                        counts[block_hash] = counts.get(block_hash, 0) + 1
                    self.round += 1
                    winner = max(counts, key=counts.get) if counts else None
                    if winner is not None and counts[winner] >= ALPHA:
                        self.my_confidence = self.my_confidence + 1 if winner == self.my_vote else 1
                        self.my_vote = winner
                    else:
                        self.my_confidence = 0
                    self._status()
                    if self.my_confidence >= BETA:
                        self._finalize()
                time.sleep(QUERY_INTERVAL)
        except (OSError, ValueError) as error:
            print(f"Consensus stopped: {error}", flush=True)

    def _status(self):
        with self.lock:
            print(f"{self.peer_id} round={self.round} vote={self.my_vote} confidence={self.my_confidence}/{BETA} finalized={self.finalized}", flush=True)

    def _start_consensus(self):
        if not self.started:
            self.started = True
            threading.Thread(target=self._avalanche_loop, daemon=True).start()

    def start(self):
        self._start_udp_listener()
        print(f"READY {self.peer_id} proposal={self.proposal['block_hash']}", flush=True)
        print("Commands: /start, /status, /peers, /quit", flush=True)
        try:
            while self.running:
                cmd = input().strip().lower()
                if cmd == "/start":
                    self._start_consensus()
                elif cmd == "/status":
                    self._status()
                elif cmd == "/peers":
                    print(list(self.known_peers), flush=True)
                elif cmd == "/quit":
                    break
                else:
                    print("Commands: /start, /status, /peers, /quit", flush=True)
        except (KeyboardInterrupt, EOFError):
            pass
        finally:
            self.running = False
            self.consensus_sock.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 peer.py <PORT>")
        sys.exit(1)

    peer = AvalanchePeer(int(sys.argv[1]))
    peer.start()
