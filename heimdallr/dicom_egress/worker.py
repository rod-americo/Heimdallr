#!/usr/bin/env python3
"""Resident worker for outbound DICOM artifact delivery."""

from __future__ import annotations

import copy
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import pydicom
from pynetdicom import AE, ALL_TRANSFER_SYNTAXES
from pydicom.uid import JPEGLosslessSV1, UID

from heimdallr.dicom_egress.config import (
    dicom_egress_connect_timeout_seconds,
    dicom_egress_dimse_timeout_seconds,
    dicom_egress_local_ae_title,
    dicom_egress_retry_attempts,
    dicom_egress_retry_backoff_seconds,
    load_dicom_egress_config,
)
from heimdallr.shared import settings, store
from heimdallr.shared.paths import study_dir
from heimdallr.shared.sqlite import connect as db_connect

settings.configure_service_stdio()


def ensure_dicom_egress_queue_table() -> None:
    conn = db_connect()
    try:
        store.ensure_schema(conn)
    finally:
        conn.close()


def claim_next_pending_dicom_egress_queue_item():
    conn = db_connect()
    try:
        return store.claim_next_pending_dicom_egress_queue_item(conn)
    finally:
        conn.close()


def mark_dicom_egress_queue_item_done(queue_id: int) -> None:
    conn = db_connect()
    try:
        store.mark_dicom_egress_queue_item_done(conn, queue_id)
    finally:
        conn.close()


def retry_dicom_egress_queue_item(queue_id: int, error_message: str, *, backoff_seconds: int) -> None:
    conn = db_connect()
    try:
        store.retry_dicom_egress_queue_item(
            conn,
            queue_id,
            error_message,
            backoff_seconds=backoff_seconds,
        )
    finally:
        conn.close()


def mark_dicom_egress_queue_item_error(queue_id: int, error_message: str) -> None:
    conn = db_connect()
    try:
        store.mark_dicom_egress_queue_item_error(conn, queue_id, error_message)
    finally:
        conn.close()


def _artifact_abspath(case_id: str, artifact_path: str) -> Path:
    return study_dir(case_id) / artifact_path


def _accepted_context_for_sop_class(assoc, sop_class_uid: str):
    for context in assoc.accepted_contexts:
        if str(context.abstract_syntax) == str(sop_class_uid):
            return context
    return None


def _resolve_dcmcjpeg_bin() -> str | None:
    configured = settings.DCMCJPEG_BIN
    if not configured:
        return None

    configured_path = Path(configured)
    if configured_path.is_absolute() or "/" in configured:
        return str(configured_path) if configured_path.exists() else None

    return shutil.which(configured)


def _transcode_with_dcmcjpeg(source_path: Path, accepted_transfer_syntax: UID) -> pydicom.Dataset:
    dcmcjpeg_bin = _resolve_dcmcjpeg_bin()
    if not dcmcjpeg_bin:
        raise RuntimeError(
            "No dcmcjpeg binary available. Configure HEIMDALLR_DCMCJPEG_BIN or bundle "
            "bin/linux-amd64/dcmcjpeg."
        )

    with tempfile.TemporaryDirectory(prefix="heimdallr-dcmcjpeg-") as tmpdir:
        output_path = Path(tmpdir) / "transcoded.dcm"
        result = subprocess.run(
            [dcmcjpeg_bin, str(source_path), str(output_path)],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            stderr = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(f"dcmcjpeg failed with exit code {result.returncode}: {stderr}")

        transcoded = pydicom.dcmread(str(output_path))
        transcoded_transfer_syntax = UID(transcoded.file_meta.TransferSyntaxUID)
        if transcoded_transfer_syntax != accepted_transfer_syntax:
            raise RuntimeError(
                "dcmcjpeg produced unexpected transfer syntax "
                f"'{transcoded_transfer_syntax.name}' instead of '{accepted_transfer_syntax.name}'"
            )
        return transcoded


def _prepare_dataset_for_peer(
    ds: pydicom.Dataset,
    accepted_transfer_syntax: UID,
    *,
    source_path: Path | None = None,
) -> pydicom.Dataset:
    current_transfer_syntax = UID(ds.file_meta.TransferSyntaxUID)
    if current_transfer_syntax == accepted_transfer_syntax:
        return ds

    if not accepted_transfer_syntax.is_compressed:
        return ds

    prepared = copy.deepcopy(ds)
    try:
        prepared.compress(str(accepted_transfer_syntax), generate_instance_uid=False)
    except Exception as exc:
        if accepted_transfer_syntax == JPEGLosslessSV1 and source_path is not None:
            try:
                return _transcode_with_dcmcjpeg(source_path, accepted_transfer_syntax)
            except Exception as external_exc:
                raise RuntimeError(
                    "Peer only accepted transfer syntax "
                    f"'{accepted_transfer_syntax.name}' for SOP Class '{ds.SOPClassUID.name}', "
                    "but local runtime could not transcode the dataset with pydicom or dcmcjpeg: "
                    f"{external_exc}"
                ) from external_exc
        raise RuntimeError(
            "Peer only accepted transfer syntax "
            f"'{accepted_transfer_syntax.name}' for SOP Class '{ds.SOPClassUID.name}', "
            f"but local runtime could not transcode the dataset: {exc}"
        ) from exc

    return prepared


def send_dicom_export(
    *,
    case_id: str,
    artifact_path: str,
    destination_host: str,
    destination_port: int,
    destination_called_aet: str,
) -> None:
    config = load_dicom_egress_config()
    local_ae_title = dicom_egress_local_ae_title(config)
    connect_timeout_seconds = dicom_egress_connect_timeout_seconds(config)
    dimse_timeout_seconds = dicom_egress_dimse_timeout_seconds(config)

    artifact_abspath = _artifact_abspath(case_id, artifact_path)
    if not artifact_abspath.exists():
        raise RuntimeError(f"DICOM artifact not found: {artifact_abspath}")

    ds = pydicom.dcmread(str(artifact_abspath))
    sop_class_uid = getattr(ds, "SOPClassUID", None)
    if not sop_class_uid:
        raise RuntimeError(f"DICOM artifact missing SOPClassUID: {artifact_abspath}")

    ae = AE(ae_title=local_ae_title)
    ae.add_requested_context(sop_class_uid, ALL_TRANSFER_SYNTAXES)
    ae.acse_timeout = connect_timeout_seconds
    ae.dimse_timeout = dimse_timeout_seconds
    ae.network_timeout = connect_timeout_seconds

    assoc = ae.associate(
        destination_host,
        int(destination_port),
        ae_title=str(destination_called_aet),
    )
    if not assoc.is_established:
        raise RuntimeError(
            f"Association failed to {destination_called_aet}@{destination_host}:{destination_port}"
        )

    try:
        accepted_context = _accepted_context_for_sop_class(assoc, sop_class_uid)
        if accepted_context is None:
            raise RuntimeError(
                f"Peer accepted no presentation context for SOP Class '{UID(sop_class_uid).name}' "
                f"at {destination_called_aet}@{destination_host}:{destination_port}"
            )

        prepared_ds = _prepare_dataset_for_peer(
            ds,
            accepted_context.transfer_syntax[0],
            source_path=artifact_abspath,
        )
        status = assoc.send_c_store(prepared_ds)
    finally:
        assoc.release()

    status_code = getattr(status, "Status", None)
    if status_code is None:
        raise RuntimeError(
            f"C-STORE returned no status for {destination_called_aet}@{destination_host}:{destination_port}"
        )
    if int(status_code) not in {0x0000}:
        raise RuntimeError(
            f"C-STORE failed with status 0x{int(status_code):04X} "
            f"for {destination_called_aet}@{destination_host}:{destination_port}"
        )


def main() -> int:
    print("Starting DICOM egress queue monitoring...")
    ensure_dicom_egress_queue_table()

    try:
        while True:
            try:
                queue_item = claim_next_pending_dicom_egress_queue_item()
                if not queue_item:
                    time.sleep(settings.DICOM_EGRESS_SCAN_INTERVAL)
                    continue

                (
                    queue_id,
                    case_id,
                    _study_uid,
                    artifact_path,
                    _artifact_type,
                    destination_name,
                    destination_host,
                    destination_port,
                    destination_called_aet,
                    attempts_before_claim,
                ) = queue_item

                print(
                    f"[DICOM Egress] Claimed {case_id}: {artifact_path} -> "
                    f"{destination_name} ({destination_called_aet}@{destination_host}:{destination_port})"
                )
                try:
                    send_dicom_export(
                        case_id=case_id,
                        artifact_path=artifact_path,
                        destination_host=destination_host,
                        destination_port=int(destination_port),
                        destination_called_aet=destination_called_aet,
                    )
                    mark_dicom_egress_queue_item_done(queue_id)
                    print(
                        f"[DICOM Egress] ✓ {case_id} -> {destination_name} "
                        f"({destination_called_aet}@{destination_host}:{destination_port})"
                    )
                except Exception as exc:
                    config = load_dicom_egress_config()
                    retry_attempts = dicom_egress_retry_attempts(config)
                    retry_backoff_seconds = dicom_egress_retry_backoff_seconds(config)
                    claimed_attempt = int(attempts_before_claim) + 1
                    if claimed_attempt < max(retry_attempts, 1):
                        retry_dicom_egress_queue_item(
                            queue_id,
                            str(exc),
                            backoff_seconds=retry_backoff_seconds,
                        )
                        print(
                            f"[DICOM Egress] Retry {claimed_attempt}/{retry_attempts} for "
                            f"{case_id} -> {destination_name}: {exc}"
                        )
                    else:
                        mark_dicom_egress_queue_item_error(queue_id, str(exc))
                        print(
                            f"[DICOM Egress] Error for {case_id} -> {destination_name}: {exc}"
                        )
            except Exception as exc:
                print(f"Error in DICOM egress main loop: {exc}")
                time.sleep(settings.DICOM_EGRESS_SCAN_INTERVAL)
    except KeyboardInterrupt:
        print("\nStopping DICOM egress monitoring...")
        return 0
