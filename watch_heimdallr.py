#!/usr/bin/env python3
# Copyright (c) 2026 Rodrigo Americo
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import subprocess
from pathlib import Path
from typing import Dict, Optional

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent

WATCH_DIR = Path.home() / "Temp" / "HEIMDALLR"
PYTHON = Path.home() / "Heimdallr" / "venv" / "bin" / "python"
UPLOADER = Path.home() / "Heimdallr" / "uploader.py"

QUIET_SECONDS = 3
POLL_SECONDS = 1

LOG_FILE = Path("/tmp/watch_heimdallr_py.log")


def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a") as f:
        f.write(f"[{ts()}] {msg}\n")


def top_folder_from_event_path(p: Path) -> Optional[Path]:
    """
    retorna a subpasta direta dentro de WATCH_DIR associada ao evento.
    ex: WATCH_DIR/ABC/arquivo.txt -> WATCH_DIR/ABC
    """
    try:
        rel = p.resolve().relative_to(WATCH_DIR.resolve())
    except Exception:
        return None

    if not rel.parts:
        return None

    return (WATCH_DIR / rel.parts[0])


class Handler(FileSystemEventHandler):
    def __init__(self, last_event: Dict[Path, float]):
        self.last_event = last_event

    def on_any_event(self, event: FileSystemEvent):
        p = Path(event.src_path)
        
        # ignora arquivos do macOS
        if p.name == ".DS_Store":
            return
        folder = top_folder_from_event_path(p)
        if folder is None:
            return

        # marca timestamp para debounce; não exige folder existir já
        self.last_event[folder] = time.time()
        log(f"evento: {event.event_type} -> {folder}")


def main():
    if not WATCH_DIR.exists():
        raise SystemExit(f"watch_dir não existe: {WATCH_DIR}")
    if not PYTHON.exists():
        raise SystemExit(f"python não encontrado: {PYTHON}")
    if not UPLOADER.exists():
        raise SystemExit(f"uploader não encontrado: {UPLOADER}")

    last_event: Dict[Path, float] = {}
    running: Dict[Path, subprocess.Popen] = {}

    log(f"iniciando watchdog | watch_dir={WATCH_DIR} | quiet={QUIET_SECONDS}s")
    print(f"monitorando: {WATCH_DIR}")
    print(f"log: {LOG_FILE}")

    observer = Observer()
    observer.schedule(Handler(last_event), str(WATCH_DIR), recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(POLL_SECONDS)
            now = time.time()

            # limpa processos finalizados
            for folder, proc in list(running.items()):
                rc = proc.poll()
                if rc is not None:
                    log(f"uploader terminou | rc={rc} | {folder}")
                    running.pop(folder, None)

            # dispara uploads após inatividade
            for folder, t0 in list(last_event.items()):
                if folder in running:
                    continue
                if now - t0 < QUIET_SECONDS:
                    continue

                if not folder.exists():
                    log(f"pasta não existe mais (provável limpeza do uploader): {folder}")
                    last_event.pop(folder, None)
                    continue

                log(f"inatividade >= {QUIET_SECONDS}s | executando: {folder}")
                print(f"inatividade detectada: executando uploader em {folder}")

                # mantém stdout/stderr em log para diagnóstico
                proc = subprocess.Popen(
                    [str(PYTHON), str(UPLOADER), str(folder)],
                    stdout=LOG_FILE.open("a"),
                    stderr=LOG_FILE.open("a"),
                )
                running[folder] = proc
                last_event.pop(folder, None)

    except KeyboardInterrupt:
        log("interrompido pelo usuário (ctrl+c)")
    finally:
        observer.stop()
        observer.join()


if __name__ == "__main__":
    main()