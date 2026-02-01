#!/usr/bin/env python3
import time
import subprocess
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

WATCH_DIR = Path.home() / "Temp" / "HEIMDALLR"
PYTHON = Path.home() / "Heimdallr" / "venv" / "bin" / "python"
UPLOADER = Path.home() / "Heimdallr" / "uploader.py"

QUIET_SECONDS = 3

# estado: última modificação por pasta
last_event = {}
running = set()

def now():
    return time.time()

class Handler(FileSystemEventHandler):
    def on_any_event(self, event):
        path = Path(event.src_path)
        # subir até a subpasta direta de WATCH_DIR
        try:
            while path.parent != WATCH_DIR:
                path = path.parent
        except Exception:
            return
        if path.parent == WATCH_DIR and path.is_dir():
            last_event[path] = now()

def main():
    print(f"monitorando: {WATCH_DIR}")
    observer = Observer()
    observer.schedule(Handler(), str(WATCH_DIR), recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
            t = now()
            for folder, ts in list(last_event.items()):
                if t - ts >= QUIET_SECONDS and folder not in running and folder.exists():
                    print(f"inatividade detectada: executando uploader em {folder}")
                    running.add(folder)
                    subprocess.Popen(
                        [str(PYTHON), str(UPLOADER), str(folder)],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                    last_event.pop(folder, None)
                    running.discard(folder)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()