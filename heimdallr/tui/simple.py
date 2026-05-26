"""Simple Rich TUI for Heimdallr case queue monitoring."""

from __future__ import annotations

import argparse
import sqlite3
import time
from pathlib import Path

from rich.align import Align
from rich.box import HEAVY_HEAD, SIMPLE_HEAVY
from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from textual.app import App, ComposeResult
from textual.events import Key
from textual.widgets import Static

from heimdallr.shared import settings
from heimdallr.shared import store
from heimdallr.tui.i18n import queue_status_label, stage_label, tui
from heimdallr.tui.snapshot import CaseOverview, DashboardSnapshot, build_snapshot


BACKLOG_STAGES = {"queued", "prepared", "segmentation", "metrics", "ineligible"}
ACTIVE_QUEUE_STATUSES = {"pending", "claimed"}
COMMAND_NAMES = {"x": "cancel", "p": "priority"}


class SimpleQueueTextualApp(App[None]):
    """Textual shell for the simple Rich queue renderer."""

    BINDINGS = [
        ("q", "quit_tui", "Sair"),
        ("r", "refresh", "Atualizar"),
    ]
    CSS = """
    Screen {
        background: transparent;
        color: auto;
    }

    #queue {
        width: 100%;
        height: 1fr;
        background: transparent;
        color: auto;
    }
    """

    def __init__(self, renderer: "SimpleQueueTui") -> None:
        super().__init__(ansi_color=True)
        self.renderer = renderer
        self._root: Static | None = None
        self._command_buffer = ""

    def compose(self) -> ComposeResult:
        self._root = Static(self.renderer.render(), id="queue")
        yield self._root

    def on_mount(self) -> None:
        self.title = tui("simple.title")
        self.sub_title = tui("simple.subtitle")
        self.set_interval(self.renderer.refresh_seconds, self.refresh_render)

    def refresh_render(self) -> None:
        if self._root is not None:
            self._root.update(self.renderer.render())

    def action_refresh(self) -> None:
        self.refresh_render()

    def action_quit_tui(self) -> None:
        self.exit()

    def on_key(self, event: Key) -> None:
        key = event.key.lower()
        character = event.character or ""
        if key == "escape":
            self._command_buffer = ""
            self.renderer.command_message = tui("simple.command.cleared")
            self.refresh_render()
            event.stop()
            return
        if key == "enter" and len(self._command_buffer) > 1:
            self._execute_buffered_command()
            self.refresh_render()
            event.stop()
            return
        command_character = character.lower() or key
        if command_character in COMMAND_NAMES:
            command = command_character
            self._command_buffer = command
            self.renderer.command_message = tui(f"simple.command.{COMMAND_NAMES[command]}_waiting")
            self.refresh_render()
            event.stop()
            return
        digit = _key_digit(key, character)
        if self._command_buffer and digit is not None:
            self._command_buffer += digit
            event.stop()
            if len(self._command_buffer) >= 3:
                self._execute_buffered_command()
                self.refresh_render()
            return
        if self._command_buffer:
            self._command_buffer = ""

    def _execute_buffered_command(self) -> None:
        command = self._command_buffer[0]
        slot_text = self._command_buffer[1:]
        self._command_buffer = ""
        if command not in COMMAND_NAMES or not slot_text.isdigit():
            self.renderer.command_message = tui("simple.command.cleared")
            return
        slot = int(slot_text[:2])
        if command == "x":
            self.renderer.cancel_visible_case(slot)
        elif command == "p":
            self.renderer.prioritize_visible_case(slot)


class SimpleQueueTui:
    """Small, queue-first Rich dashboard inspired by WebRISAhead."""

    def __init__(
        self,
        *,
        refresh_seconds: float = 2.0,
        db_path: Path | None = None,
        limit: int = 20,
        processed_limit: int = 12,
    ) -> None:
        self.refresh_seconds = max(float(refresh_seconds), 0.5)
        self.db_path = db_path
        self.limit = max(int(limit), 1)
        self.processed_limit = max(int(processed_limit), 1)
        self.started_at = time.time()
        self.snapshot: DashboardSnapshot | None = None
        self.command_message = ""

    def load(self) -> DashboardSnapshot:
        self.snapshot = build_snapshot(db_path=self.db_path)
        return self.snapshot

    def run(self) -> None:
        SimpleQueueTextualApp(self).run()

    def cancel_visible_case(self, slot: int) -> None:
        case = self._visible_case(slot)
        if case is None:
            self.command_message = tui("simple.command.cancel_invalid", slot=f"{slot:02d}")
            return
        result = cancel_case_from_pipeline(self.db_path, case.case_id)
        accession = _case_accession(case)
        if result:
            self.command_message = tui(
                "simple.command.cancel_done",
                slot=f"{slot:02d}",
                accession=accession,
                count=result,
            )
        else:
            self.command_message = tui(
                "simple.command.cancel_noop",
                slot=f"{slot:02d}",
                accession=accession,
            )

    def prioritize_visible_case(self, slot: int) -> None:
        case = self._visible_case(slot)
        if case is None:
            self.command_message = tui("simple.command.priority_invalid", slot=f"{slot:02d}")
            return
        result = prioritize_case_in_pipeline(self.db_path, case.case_id)
        accession = _case_accession(case)
        if result:
            self.command_message = tui(
                "simple.command.priority_done",
                slot=f"{slot:02d}",
                accession=accession,
                count=result,
            )
        else:
            self.command_message = tui(
                "simple.command.priority_noop",
                slot=f"{slot:02d}",
                accession=accession,
            )

    def _visible_case(self, slot: int) -> CaseOverview | None:
        snapshot = self.snapshot or self.load()
        cases = _backlog_cases(snapshot)[: self.limit]
        index = int(slot) - 1
        if index < 0 or index >= len(cases):
            return None
        return cases[index]

    def render(self):
        snapshot = self.load()
        return Group(
            self._render_header(snapshot),
            self._render_summary(snapshot),
            self._render_backlog(snapshot),
            self._render_processed(snapshot),
            self._render_footer(snapshot),
        )

    def _render_header(self, snapshot: DashboardSnapshot) -> Panel:
        uptime = _format_duration(int(time.time() - self.started_at))
        title = Text()
        title.append(tui("simple.title"), style="bold black on green3")
        title.append(f"  {tui('simple.subtitle')}", style="bold bright_cyan")
        title.append(f"  {tui('simple.updated_at')} {snapshot.generated_at.strftime('%H:%M:%S')}", style="khaki1")
        title.append(f"  uptime {uptime}", style="grey62")
        return Panel(Align.left(title), box=SIMPLE_HEAVY, border_style="green4")

    def _render_summary(self, snapshot: DashboardSnapshot) -> Panel:
        active = sum(1 for case in snapshot.cases if _is_active_case(case))
        queued = sum(1 for case in snapshot.cases if _is_queued_case(case))
        processed = len(_processed_cases(snapshot))
        failed = len(_failed_cases(snapshot))

        line = Text()
        line.append(f"{tui('simple.summary.active')} ", style="grey85")
        line.append(str(active), style="bold bright_cyan")
        line.append(f"  {tui('simple.summary.queue')} ", style="grey85")
        line.append(str(queued), style="bold khaki1")
        line.append(f"  {tui('simple.summary.processed')} ", style="grey85")
        line.append(str(processed), style="bold green3")
        line.append(f"  {tui('simple.summary.failures')} ", style="grey85")
        line.append(str(failed), style="bold red3")
        line.append(f"  {tui('simple.summary.total')} ", style="grey85")
        line.append(str(snapshot.total_cases), style="grey85")
        line.append("  Refresh ", style="grey85")
        line.append(f"{self.refresh_seconds:g}s", style="deep_sky_blue1")
        return Panel(Align.left(line), border_style="green3")

    def _render_backlog(self, snapshot: DashboardSnapshot) -> Panel:
        table = Table(box=HEAVY_HEAD, expand=True, row_styles=["", "dim"])
        table.add_column(tui("simple.table.slot"), justify="right", no_wrap=True)
        table.add_column(tui("app.table.accession"), style="bold cyan", no_wrap=True)
        table.add_column(tui("app.table.stage"), no_wrap=True)
        table.add_column(tui("app.table.queue"), no_wrap=True)
        table.add_column(tui("simple.table.prepare_short"), justify="right", no_wrap=True)
        table.add_column(tui("simple.table.segment_short"), justify="right", no_wrap=True)

        cases = _backlog_cases(snapshot)[: self.limit]
        if not cases:
            table.add_row("--", "-", tui("simple.empty.backlog"), "-", "-", "-")
        for index, case in enumerate(cases, start=1):
            table.add_row(
                f"{index:02d}",
                _case_accession(case),
                _stage_text(case),
                _queue_text(case),
                case.prepare_elapsed or "-",
                case.segmentation_elapsed or "-",
            )
        return Panel(table, title=f"[bold spring_green3]{tui('simple.panel.backlog')}[/]", border_style="green4")

    def _render_processed(self, snapshot: DashboardSnapshot) -> Panel:
        table = Table(box=HEAVY_HEAD, expand=True, row_styles=["", "dim"])
        table.add_column(tui("app.table.accession"), style="bold cyan", no_wrap=True)
        table.add_column(tui("simple.table.status"), no_wrap=True)
        table.add_column(tui("simple.table.pipeline"), justify="right", no_wrap=True)
        table.add_column(tui("simple.table.duration"), justify="right", no_wrap=True)
        table.add_column(tui("simple.table.finished"), justify="right", no_wrap=True)

        cases = _processed_cases(snapshot)[: self.processed_limit]
        if not cases:
            table.add_row("-", tui("simple.empty.processed"), "-", "-", "-")
        for case in cases:
            table.add_row(
                _case_accession(case),
                Text(tui("simple.status.ok"), style="bold green3"),
                _pipeline_elapsed(case),
                case.total_elapsed or "-",
                _finished_time(case),
            )
        return Panel(table, title=f"[bold spring_green3]{tui('simple.panel.processed')}[/]", border_style="green4")

    def _render_footer(self, snapshot: DashboardSnapshot) -> Panel:
        lines: list[Text] = []
        stage_line = Text()
        for stage in snapshot.stages:
            stage_line.append(stage.label, style="grey85")
            stage_line.append(" ")
            stage_line.append(f"{stage.queued}/{stage.active}", style="bold bright_cyan")
            stage_line.append("  ")
        lines.append(stage_line)

        shortcut_line = Text()
        shortcut_line.append(tui("simple.shortcuts"), style="grey85")
        shortcut_line.append("  q", style="bold spring_green3")
        shortcut_line.append(f" {tui('simple.shortcut.quit')}  ", style="grey85")
        shortcut_line.append("r", style="bold spring_green3")
        shortcut_line.append(f" {tui('simple.shortcut.refresh')}  ", style="grey85")
        shortcut_line.append("pNN", style="bold bright_cyan")
        shortcut_line.append(f" {tui('simple.shortcut.priority')}  ", style="grey85")
        shortcut_line.append("xNN", style="bold orange3")
        shortcut_line.append(f" {tui('simple.shortcut.cancel')}", style="grey85")
        lines.append(shortcut_line)
        if self.command_message:
            lines.append(Text(self.command_message, style="khaki1"))
        failed = _failed_cases(snapshot)[:2]
        if failed:
            for case in failed:
                line = Text()
                line.append(_case_accession(case), style="bold red3")
                line.append("  ")
                line.append(case.error or case.signal or "falha sem detalhe", style="grey85")
                lines.append(line)
        else:
            lines.append(Text(tui("simple.empty.failures"), style="dark_sea_green2"))
        return Panel(Group(*lines), title=f"[bold khaki1]{tui('simple.panel.signals')}[/]", border_style="dark_olive_green3")


def _backlog_cases(snapshot: DashboardSnapshot) -> list[CaseOverview]:
    return _recent_cases(
        case
        for case in snapshot.cases
        if case.stage_key in BACKLOG_STAGES or case.queue_status_key in ACTIVE_QUEUE_STATUSES
    )


def _processed_cases(snapshot: DashboardSnapshot) -> list[CaseOverview]:
    return _recent_cases(case for case in snapshot.cases if case.stage_key == "processed")


def _failed_cases(snapshot: DashboardSnapshot) -> list[CaseOverview]:
    return _recent_cases(
        case for case in snapshot.cases if case.stage_key == "failed" or case.queue_status_key == "error"
    )


def _recent_cases(cases) -> list[CaseOverview]:
    return sorted(cases, key=lambda case: case.sort_timestamp, reverse=True)


def cancel_case_from_pipeline(db_path: Path | None, case_id: str) -> int:
    db_file = Path(db_path) if db_path is not None else settings.DB_PATH
    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    try:
        return store.mark_case_pipeline_canceled(conn, case_id)
    finally:
        conn.close()


def prioritize_case_in_pipeline(db_path: Path | None, case_id: str) -> int:
    db_file = Path(db_path) if db_path is not None else settings.DB_PATH
    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    try:
        return store.prioritize_case_pipeline(conn, case_id)
    finally:
        conn.close()


def _key_digit(key: str, character: str) -> str | None:
    if character.isdigit():
        return character
    if key.isdigit():
        return key
    if key.startswith("digit") and key[5:].isdigit():
        return key[5:]
    if key.startswith("number") and key[6:].isdigit():
        return key[6:]
    return None


def _is_active_case(case: CaseOverview) -> bool:
    return case.queue_status_key == "claimed" or case.stage_key in {"segmentation", "metrics"}


def _is_queued_case(case: CaseOverview) -> bool:
    return case.queue_status_key == "pending" or case.stage_key in {"queued", "prepared"}


def _case_accession(case: CaseOverview) -> str:
    return case.accession_number or case.case_id.split("_", 1)[0] or "-"


def _stage_text(case: CaseOverview) -> Text:
    style = {
        "prepared": "khaki1",
        "queued": "khaki1",
        "segmentation": "bold bright_cyan",
        "metrics": "bold deep_sky_blue1",
        "ineligible": "bold orange3",
        "failed": "bold red3",
    }.get(case.stage_key, "grey85")
    return Text(stage_label(case.stage_key) if case.stage_key else "-", style=style)


def _queue_text(case: CaseOverview) -> Text:
    style = {
        "pending": "khaki1",
        "claimed": "bold bright_cyan",
        "done": "green3",
        "error": "bold red3",
    }.get(case.queue_status_key, "grey70")
    return Text(queue_status_label(case.queue_status_key), style=style)


def _updated(case: CaseOverview) -> str:
    if case.updated_at is None:
        return "-"
    return case.updated_at.strftime("%d/%m %H:%M")


def _finished_time(case: CaseOverview) -> str:
    if case.updated_at is None:
        return "-"
    return case.updated_at.strftime("%H:%M")


def _pipeline_elapsed(case: CaseOverview) -> str:
    total = 0.0
    seen = False
    for value in (case.prepare_elapsed, case.segmentation_elapsed, case.metrics_elapsed):
        seconds = _duration_text_to_seconds(value)
        if seconds is None:
            continue
        total += seconds
        seen = True
    if not seen:
        return "-"
    return _format_colon_duration(int(round(total)))


def _duration_text_to_seconds(value: str | None) -> float | None:
    if not value:
        return None
    text = str(value).strip()
    if not text or text == "-":
        return None
    if text.startswith("reuso (") or text.startswith("duplicata ("):
        inner = text.split("(", 1)[1].rsplit(")", 1)[0]
        return _duration_text_to_seconds(inner)
    if "h" in text and ":" not in text:
        hours, _, minutes = text.partition("h")
        try:
            return int(hours) * 3600 + int(minutes or 0) * 60
        except ValueError:
            return None
    parts = text.split(":")
    try:
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
        if len(parts) == 2:
            return int(parts[0]) * 60 + float(parts[1])
    except ValueError:
        return None
    return None


def _format_colon_duration(seconds: int) -> str:
    hours, remainder = divmod(max(seconds, 0), 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}"


def _format_duration(seconds: int) -> str:
    hours, remainder = divmod(max(seconds, 0), 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}"
    return f"{minutes:02d}:{secs:02d}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Simple Heimdallr case queue TUI")
    parser.add_argument("--refresh-seconds", type=float, default=2.0)
    parser.add_argument("--db-path", default=str(settings.DB_PATH))
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--processed-limit", type=int, default=12)
    parser.add_argument("--once", action="store_true", help="Render one frame and exit")
    args = parser.parse_args(argv)

    app = SimpleQueueTui(
        refresh_seconds=args.refresh_seconds,
        db_path=Path(args.db_path),
        limit=args.limit,
        processed_limit=args.processed_limit,
    )
    if args.once:
        from rich.console import Console

        Console().print(app.render())
        return 0
    app.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
