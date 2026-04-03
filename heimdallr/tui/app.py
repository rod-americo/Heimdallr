"""Textual app for Heimdallr operational monitoring."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from rich.align import Align
from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.reactive import reactive
from textual.widgets import DataTable, Footer, Header, Static

from heimdallr.shared import settings
from .snapshot import CaseOverview, DashboardSnapshot, RuntimeLayout, build_snapshot


class RichPanel(Static):
    """Simple Static wrapper that updates with Rich renderables."""

    def set_renderable(self, renderable) -> None:
        self.update(renderable)


class HeimdallrDashboardApp(App[None]):
    """Operational cockpit for intake, prepare, and processing flow."""

    AUTO_FOCUS = "#cases"
    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh_now", "Refresh"),
        Binding("p", "toggle_pause", "Pause"),
    ]
    CSS_PATH = str(Path(__file__).with_name("dashboard.tcss"))

    paused = reactive(False)

    def __init__(
        self,
        *,
        refresh_seconds: float = 2.0,
        layout: RuntimeLayout | None = None,
        db_path: Path | None = None,
    ) -> None:
        super().__init__()
        self.refresh_seconds = refresh_seconds
        self.layout = layout
        self.db_path = db_path
        self.snapshot: DashboardSnapshot | None = None
        self.selected_case_id: str | None = None
        self._refresh_timer = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Container(id="shell"):
            yield RichPanel(id="radar")
            yield RichPanel(id="flow")
            with Horizontal(id="signal-row"):
                yield RichPanel(id="pulse")
                yield RichPanel(id="spotlight")
                yield RichPanel(id="alerts")
            with Vertical(id="table-wrap"):
                yield DataTable(id="cases", zebra_stripes=True, cursor_type="row")
        yield Footer()

    def on_mount(self) -> None:
        self.title = "Heimdallr TUI"
        self.sub_title = "Intake · Prepare · Process"
        table = self.query_one("#cases", DataTable)
        table.add_columns(
            "Patient",
            "Stage",
            "Queue",
            "Prepare",
            "Process",
            "Updated",
            "Signal",
        )
        self._refresh_timer = self.set_interval(self.refresh_seconds, self._tick_refresh)
        self.refresh_snapshot()

    def action_refresh_now(self) -> None:
        self.refresh_snapshot()

    def action_toggle_pause(self) -> None:
        self.paused = not self.paused
        self.notify("Auto-refresh paused." if self.paused else "Auto-refresh resumed.", timeout=1.5)
        self.refresh_snapshot()

    def _tick_refresh(self) -> None:
        if not self.paused:
            self.refresh_snapshot()

    def refresh_snapshot(self) -> None:
        self.snapshot = build_snapshot(layout=self.layout, db_path=self.db_path)
        if self.selected_case_id is None and self.snapshot.cases:
            self.selected_case_id = self.snapshot.cases[0].case_id
        self._render()

    @on(DataTable.RowHighlighted, "#cases")
    def handle_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        row_key = getattr(event.row_key, "value", event.row_key)
        row_key = str(row_key)
        self.selected_case_id = row_key
        self._render_spotlight()

    def _render(self) -> None:
        self._render_radar()
        self._render_pulse()
        self._render_alerts()
        self._render_flow()
        self._render_cases_table()
        self._render_spotlight()

    def _render_radar(self) -> None:
        assert self.snapshot is not None
        stage_by_slug = {stage.slug: stage for stage in self.snapshot.stages}
        table = Table(expand=True, box=None, padding=(0, 1))
        table.add_column("Service", style="bold #e2e8f0", no_wrap=True)
        table.add_column("State", no_wrap=True)
        table.add_column("Workers", justify="right", no_wrap=True)
        table.add_column("Q", justify="right", no_wrap=True)
        table.add_column("A", justify="right", no_wrap=True)
        table.add_column("Signal", style="#94a3b8")
        for service in self.snapshot.services:
            stage = stage_by_slug[service.slug]
            signal = _compact_signal(service)
            status = "[#4ade80]LIVE[/]" if service.running else "[#f97316]OFF[/]"
            table.add_row(
                service.label,
                status,
                str(service.instances),
                str(stage.queued),
                str(stage.active),
                signal,
            )
        self.query_one("#radar", RichPanel).set_renderable(
            Panel(
                table,
                border_style="#0f766e",
                title="Workers",
                subtitle="state + queue pressure",
            )
        )

    def _render_pulse(self) -> None:
        assert self.snapshot is not None
        stats = Table.grid(expand=True)
        stats.add_column(ratio=1)
        stats.add_column(ratio=1)
        stats.add_column(ratio=1)
        stats.add_row(
            self._metric_panel("Cases", str(self.snapshot.total_cases), "#38bdf8", "seen"),
            self._metric_panel("Backlog", str(self.snapshot.backlog_cases), "#f59e0b", "queue"),
            self._metric_panel("Failures", str(self.snapshot.failed_cases), "#ef4444", "attention"),
        )
        avg_prepare = _seconds_to_clock(self.snapshot.avg_prepare_seconds)
        avg_processing = _seconds_to_clock(self.snapshot.avg_processing_seconds)
        meta = Table.grid(expand=True)
        meta.add_column()
        meta.add_column()
        meta.add_row(
            Text(f"refresh {self.refresh_seconds:.1f}s", style="#94a3b8"),
            Text("paused" if self.paused else "live", style="bold #fb7185" if self.paused else "bold #22c55e", justify="right"),
        )
        meta.add_row(
            Text(f"avg prepare {avg_prepare}", style="#94a3b8"),
            Text(f"avg process {avg_processing}", style="#94a3b8", justify="right"),
        )
        self.query_one("#pulse", RichPanel).set_renderable(
            Panel(
                Group(
                    stats,
                    meta,
                ),
                border_style="#1e3a8a",
                title="Pulse",
            )
        )

    def _render_alerts(self) -> None:
        assert self.snapshot is not None
        lines = [Text(alert, style="#fef3c7" if "No blocking" not in alert else "#86efac") for alert in self.snapshot.alerts[:6]]
        if not lines:
            lines = [Text("No alerts.", style="#86efac")]
        self.query_one("#alerts", RichPanel).set_renderable(
            Panel(
                Group(
                    Text("Blocking, stale backlog, worker mismatch.", style="#94a3b8"),
                    *lines,
                ),
                border_style="#7c2d12",
                title="Alerts",
                subtitle=settings.local_timestamp("%H:%M:%S"),
            )
        )

    def _render_flow(self) -> None:
        assert self.snapshot is not None
        table = Table(expand=True, box=None, padding=(0, 1))
        table.add_column("Stage", style="bold #e2e8f0", no_wrap=True)
        table.add_column("State", no_wrap=True)
        table.add_column("Q", justify="right", no_wrap=True)
        table.add_column("A", justify="right", no_wrap=True)
        table.add_column("Done", justify="right", no_wrap=True)
        table.add_column("Fail", justify="right", no_wrap=True)
        table.add_column("Oldest", no_wrap=True)
        table.add_column("Notes", style="#94a3b8")
        for stage in self.snapshot.stages:
            state_color = {
                "flow": "#10b981",
                "warning": "#f59e0b",
                "blocked": "#ef4444",
            }.get(stage.state, "#38bdf8")
            oldest = _short_age(stage.oldest_age_seconds)
            notes = " | ".join(stage.notes[:2])
            table.add_row(
                stage.label,
                Text(stage.state.upper(), style=f"bold {state_color}"),
                str(stage.queued),
                str(stage.active),
                str(stage.completed),
                str(stage.failed),
                oldest,
                notes,
            )
        self.query_one("#flow", RichPanel).set_renderable(
            Panel(
                table,
                border_style="#334155",
                title="Pipeline Lanes",
                subtitle="filesystem + sqlite + ps",
            )
        )

    def _render_cases_table(self) -> None:
        assert self.snapshot is not None
        table = self.query_one("#cases", DataTable)
        table.clear(columns=False)
        for case in self.snapshot.cases[:20]:
            updated = case.updated_at.strftime("%d/%m %H:%M") if case.updated_at else "-"
            table.add_row(
                case.patient_name,
                case.stage_label,
                case.queue_status,
                case.prepare_elapsed,
                case.processing_elapsed,
                updated,
                case.signal,
                key=case.case_id,
            )

    def _render_spotlight(self) -> None:
        assert self.snapshot is not None
        case = self._selected_case()
        if case is None:
            self.query_one("#spotlight", RichPanel).set_renderable(
                Panel(Text("No cases discovered yet.", style="#94a3b8"), title="Spotlight", border_style="#475569")
            )
            return
        timeline = Table.grid(padding=(0, 1))
        timeline.add_row(Text("Stage", style="#94a3b8"), Text(case.stage_label, style="bold #f8fafc"))
        timeline.add_row(Text("Queue", style="#94a3b8"), Text(case.queue_status, style="#e2e8f0"))
        timeline.add_row(Text("Prepare", style="#94a3b8"), Text(case.prepare_elapsed, style="#e2e8f0"))
        timeline.add_row(Text("Process", style="#94a3b8"), Text(case.processing_elapsed, style="#e2e8f0"))
        timeline.add_row(Text("Total", style="#94a3b8"), Text(case.total_elapsed, style="#e2e8f0"))
        timeline.add_row(Text("Series", style="#94a3b8"), Text(f"{case.selected_series} selected / {case.discarded_series} discarded", style="#e2e8f0"))
        timeline.add_row(Text("Signal", style="#94a3b8"), Text(case.signal, style="#e2e8f0"))
        timeline.add_row(Text("Path", style="#94a3b8"), Text(_relative_study_path(case.path), style="#64748b"))
        if case.error:
            timeline.add_row(Text("Error", style="#fb7185"), Text(case.error, style="#fecaca"))
        title = f"{case.patient_name} · AN {case.accession_number}"
        self.query_one("#spotlight", RichPanel).set_renderable(
            Panel(timeline, title=title, border_style="#0f766e")
        )

    def _selected_case(self) -> CaseOverview | None:
        assert self.snapshot is not None
        if self.selected_case_id is not None:
            for case in self.snapshot.cases:
                if case.case_id == self.selected_case_id:
                    return case
        return self.snapshot.cases[0] if self.snapshot.cases else None

    def _metric_panel(self, label: str, value: str, color: str, footnote: str) -> Panel:
        return Panel(
            Group(
                Align.center(Text(value, style=f"bold {color}")),
                Align.center(Text(label, style="bold #e2e8f0")),
                Align.center(Text(footnote, style="#94a3b8")),
            ),
            border_style=color,
            padding=(0, 1),
        )


def _seconds_to_clock(value: float | None) -> str:
    if value is None:
        return "n/a"
    total = int(value)
    hours, remainder = divmod(total, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes}:{seconds:02d}"


def _short_age(value: float | None) -> str:
    if value is None:
        return "-"
    total_minutes = int(value // 60)
    hours, minutes = divmod(total_minutes, 60)
    if hours:
        return f"{hours}h{minutes:02d}"
    return f"{minutes}m"


def _compact_signal(service) -> str:
    if not service.details:
        return "-"
    detail = service.details[0]
    if "•" in detail:
        parts = [part.strip() for part in detail.split("•")]
        if len(parts) >= 2:
            return _truncate_text(f"{parts[0]} • {parts[1]}", 28)
    return _truncate_text(detail, 28)


def _truncate_text(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "…"


def _relative_study_path(path: Path | None) -> str:
    if path is None:
        return "-"
    try:
        return str(path.relative_to(settings.STUDIES_DIR))
    except ValueError:
        return path.name
