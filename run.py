import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import time

from peer import AvalanchePeer


COMMANDS = [
    "python3 -u peer.py 8001",
    "python3 -u peer.py 8002",
    "python3 -u peer.py 8003",
    "python3 -u peer.py 8004",
    "python3 -u peer.py 8005",
]


def demo(processes, log_paths, root):
    deadline = time.monotonic() + 10
    while not all("READY " in path.read_text(encoding="utf-8") for path in log_paths):
        if any(process.poll() is not None for process in processes):
            raise RuntimeError("Узел завершился при запуске. Проверь логи и свободные порты.")
        if time.monotonic() >= deadline:
            raise RuntimeError("Узлы не готовы за 10 секунд.")
        time.sleep(0.1)
    for process in processes:
        process.stdin.write("/start\n")
        process.stdin.flush()
    print("Все узлы готовы. Начинаем опросы.", flush=True)
    deadline = time.monotonic() + 120
    results = {}
    while len(results) < len(processes):
        for number, (process, path) in enumerate(zip(processes, log_paths), start=1):
            if process.poll() is not None:
                raise RuntimeError(f"Процесс {number} завершился. См. {path}")
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.startswith("FINALIZED ") and number not in results:
                    block_hash = line.split()[1]
                    ledger_path = root / line.split("ledger=", 1)[1]
                    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
                    if len(ledger) != 1 or AvalanchePeer._block_hash(ledger[0]) != block_hash or ledger[0]["block_hash"] != block_hash:
                        raise RuntimeError(f"Некорректный ledger: {ledger_path}")
                    results[number] = block_hash
                    print(f"{number}: {line}", flush=True)
        if time.monotonic() >= deadline:
            raise RuntimeError("Нет результата за 120 секунд. Проверь логи.")
        time.sleep(0.2)
    if len(set(results.values())) != 1:
        raise RuntimeError("Узлы выбрали разные блоки.")
    print(f"PASS: {len(processes)} узлов, одинаковые хеши, ledger проверены.", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--demo", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    (root / "logs").mkdir(exist_ok=True)
    log_dir = Path(tempfile.mkdtemp(prefix="run-", dir=root / "logs"))
    processes = []
    log_paths = []
    print(f"Логи: {log_dir}", flush=True)

    try:
        for number, command in enumerate(COMMANDS, start=1):
            log_path = log_dir / f"command-{number}.log"
            with log_path.open("w", encoding="utf-8") as log:
                process = subprocess.Popen(
                    shlex.split(command),
                    cwd=root,
                    stdin=subprocess.PIPE,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    text=True,
                    env={**os.environ, "TERM": "xterm"},
                    start_new_session=True,
                )
            processes.append(process)
            log_paths.append(log_path)
            print(f"{number}: {command} → {log_path}", flush=True)

        if args.demo:
            if not processes:
                raise RuntimeError("Список COMMANDS пуст.")
            demo(processes, log_paths, root)
            return 0

        print("Ввод: '1 /status' — первому процессу, 'all /start' — всем.")
        print("'status' — состояние процессов, '/quit' или Ctrl+C — остановить всех.")
        while processes:
            line = input("> ").strip()
            if line == "/quit":
                break
            if line == "status":
                for number, process in enumerate(processes, start=1):
                    code = process.poll()
                    state = "работает" if code is None else f"завершён, код {code}"
                    print(f"{number}: {state}")
                continue

            target, separator, message = line.partition(" ")
            if not separator or not message:
                print("Формат: номер сообщение, например '1 /status' или 'all /start'.")
                continue
            if target == "all":
                targets = list(enumerate(processes, start=1))
            elif target.isdigit() and 1 <= int(target) <= len(processes):
                number = int(target)
                targets = [(number, processes[number - 1])]
            else:
                print("Неизвестный номер процесса.")
                continue

            for number, process in targets:
                try:
                    if process.poll() is not None:
                        raise BrokenPipeError
                    process.stdin.write(message + "\n")
                    process.stdin.flush()
                except BrokenPipeError:
                    print(f"Процесс {number} уже завершён. Проверь его лог.")
    except (KeyboardInterrupt, EOFError):
        print("\nОстанавливаем процессы…")
    except (OSError, ValueError, RuntimeError) as error:
        print(f"Ошибка запуска: {error}", file=sys.stderr)
        return 1
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            if process.stdin:
                try:
                    process.stdin.close()
                except BrokenPipeError:
                    pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
