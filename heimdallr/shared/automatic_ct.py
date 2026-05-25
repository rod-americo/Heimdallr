"""Automatic CT job planning from segmentation inventory."""

from __future__ import annotations

from typing import Any

from heimdallr.shared.segmentation_inventory import (
    inventory_satisfies,
    normalize_inventory_requirements,
)


def normalize_job_needs(job: dict[str, Any]) -> list[str]:
    raw_needs = job.get("needs", [])
    if raw_needs in (None, ""):
        return []
    if not isinstance(raw_needs, list):
        raise RuntimeError(f"Metrics job '{job.get('name', '<unknown>')}' needs must be a list")
    normalized: list[str] = []
    seen: set[str] = set()
    for item in raw_needs:
        need = str(item or "").strip()
        if not need or need in seen:
            continue
        normalized.append(need)
        seen.add(need)
    return normalized


def normalize_required_segmentation_tasks(job: dict[str, Any]) -> list[str] | None:
    raw_tasks = job.get("requires_segmentation_tasks")
    if raw_tasks is None:
        raw_tasks = job.get("segmentation_tasks")
    if raw_tasks is None:
        return None
    if not isinstance(raw_tasks, list):
        raise RuntimeError(
            f"Metrics job '{job.get('name', '<unknown>')}' requires_segmentation_tasks must be a list"
        )
    normalized: list[str] = []
    seen: set[str] = set()
    for item in raw_tasks:
        task_name = str(item or "").strip()
        if not task_name or task_name in seen:
            continue
        normalized.append(task_name)
        seen.add(task_name)
    return normalized


def automatic_ct_planning_enabled(profile: dict[str, Any]) -> bool:
    planning = profile.get("planning", {})
    if not isinstance(planning, dict):
        return False
    return str(planning.get("mode") or "").strip() == "automatic_ct"


def enabled_metrics_jobs(profile: dict[str, Any]) -> list[dict[str, Any]]:
    jobs = [dict(job) for job in profile.get("jobs", []) if job.get("enabled", True)]
    seen_names: set[str] = set()
    for job in jobs:
        name = str(job.get("name", "") or "").strip()
        if not name:
            raise RuntimeError("Metrics job is missing a name")
        if name in seen_names:
            raise RuntimeError(f"Metrics profile contains duplicate job '{name}'")
        seen_names.add(name)
        job["name"] = name
        job["needs"] = normalize_job_needs(job)
    return jobs


def resolve_requested_metrics_jobs(
    profile: dict[str, Any],
    requested_job_names: list[str] | None = None,
) -> list[dict[str, Any]]:
    jobs = enabled_metrics_jobs(profile)
    if not requested_job_names:
        return jobs

    jobs_by_name = {job["name"]: job for job in jobs}
    unknown = [name for name in requested_job_names if name not in jobs_by_name]
    if unknown:
        raise RuntimeError(f"Requested metrics job(s) not found in profile: {', '.join(unknown)}")

    resolved_names: list[str] = []
    seen_resolved: set[str] = set()

    def include_job(name: str) -> None:
        if name in seen_resolved:
            return
        for need in jobs_by_name[name]["needs"]:
            include_job(need)
        resolved_names.append(name)
        seen_resolved.add(name)

    for name in requested_job_names:
        include_job(name)
    for name, job in jobs_by_name.items():
        if bool(job.get("automatic", False)):
            include_job(name)

    return [jobs_by_name[name] for name in resolved_names]


def filter_jobs_by_inventory(
    jobs: list[dict[str, Any]],
    inventory: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Filter jobs whose declared inventory requirements are not satisfied."""
    remaining: dict[str, dict[str, Any]] = {job["name"]: job for job in jobs}
    skipped: list[dict[str, Any]] = []

    for job in jobs:
        requirements = normalize_inventory_requirements(job)
        failed = [
            requirement
            for requirement in requirements
            if not inventory_satisfies(inventory, requirement)
        ]
        if failed:
            remaining.pop(job["name"], None)
            skipped.append(
                {
                    "name": job["name"],
                    "reason": "inventory_requirements_not_met",
                    "requires_inventory": requirements,
                    "failed_requirements": failed,
                }
            )

    changed = True
    while changed:
        changed = False
        for name, job in list(remaining.items()):
            missing_needs = [need for need in normalize_job_needs(job) if need not in remaining]
            if missing_needs:
                remaining.pop(name, None)
                skipped.append(
                    {
                        "name": name,
                        "reason": "dependency_not_selected",
                        "missing_needs": missing_needs,
                    }
                )
                changed = True

    selected = [job for job in jobs if job["name"] in remaining]
    return selected, skipped


def required_segmentation_tasks_for_jobs(jobs: list[dict[str, Any]]) -> set[str] | None:
    required_tasks: set[str] = set()
    for job in jobs:
        required = normalize_required_segmentation_tasks(job)
        if required is None:
            return None
        required_tasks.update(required)
    return required_tasks
