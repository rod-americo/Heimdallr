# Container API Stack

This document describes the current container experiment for the Heimdallr API integration scope. It is not a replacement for the full DICOM intake and egress deployment model described in `docs/OPERATIONS.md`.

## Scope

The API container stack is intended for an external application that submits an image package and receives requested outputs through Heimdallr's integration delivery contract.

Included services:

- `heimdallr.control_plane`
- `heimdallr.prepare`
- `heimdallr.segmentation`
- `heimdallr.metrics`
- `heimdallr.integration.delivery`
- optional `heimdallr.space_manager`
- optional `heimdallr.resource_monitor`

Excluded services:

- `heimdallr.intake`
- `heimdallr.dicom_egress`
- `heimdallr.integration.dispatch`

DICOM C-STORE intake and outbound DICOM egress remain part of the broader Heimdallr stack, but they are not necessary for this API-scoped container.

## Image

`Dockerfile.api` builds a Python 3.12 image, installs `requirements.txt`, installs the local Heimdallr package, copies only tracked defaults/examples, and can bake TotalSegmentator weights into the image.

The API image currently carries an experimental TotalSegmentator dependency patch for Thor validation: the internal single-thread saving guard is raised from `512*512*1000` voxels to `512*512*3000` voxels. This is controlled by the `HEIMDALLR_TOTALSEG_BIG_SHAPE_SLICES` build arg and defaults to `3000`.

The default baked TotalSegmentator task set is:

```text total_fast
```

That matches the current Thor POC GPU segmentation profile, where `total` is
run with `--fast`. Additional tasks can be requested at build time:

```bash
HEIMDALLR_TOTALSEG_WEIGHTS_TASKS=total_fast,tissue_types \
  docker compose -f docker-compose.api.yml build
```

The dependency guard patch can be changed at build time:

```bash HEIMDALLR_TOTALSEG_BIG_SHAPE_SLICES=3000 \ docker compose -f docker-compose.api.yml build
```

Licensed TotalSegmentator tasks still require a valid TotalSegmentator license
and should not be treated as available just because the container build exists.

## Build on Thor

Thor is the preferred host for this image build because it has Docker and the
GPU environment used by the POC stack.

Before building or validating the image on Thor, local and `thor` must be on the
same branch and commit. Keep the Thor checkout synchronized with a fast-forward
pull after pushing local changes.

```bash
git status --short --branch
git rev-parse --short HEAD
ssh thor 'cd ~/Heimdallr && git status --short --branch && git rev-parse --short HEAD'
ssh thor 'cd ~/Heimdallr && DOCKER_BUILDKIT=1 docker compose -f docker-compose.api.yml build'
```

To disable baked weights for a fast syntax/build-context check:

```bash ssh thor 'cd ~/Heimdallr && HEIMDALLR_BAKE_TOTALSEG_WEIGHTS=false docker compose -f docker-compose.api.yml build'
```

## Host-Local Runtime Requirements

The compose stack mounts these directories from the host:

- `./runtime:/app/runtime`
- `./database:/app/database`
- `./config:/app/config:ro`

The image does not include mutable runtime data, SQLite databases, PHI-bearing
test packages, or concrete host-local config files. `.dockerignore` excludes
those paths from the Docker build context.

On Thor, Docker is not currently configured with the NVIDIA container runtime or
CDI GPU vendor discovery. The compose file therefore exposes GPU to the
segmentation service by binding:

- `/dev/nvidia0`
- `/dev/nvidiactl`
- `/dev/nvidia-uvm`
- `/usr/lib/x86_64-linux-gnu/libcuda.so.1`
- `/usr/lib/x86_64-linux-gnu/libnvidia-ml.so.1`

If this stack is moved to a host with a properly configured NVIDIA container
runtime, the segmentation service can instead be overridden to use the native
Compose GPU reservation model.

Before starting the API stack, the host must provide the concrete ignored config
files used by the included services:

```bash
cp config/segmentation_pipeline.gpu.example.json config/segmentation_pipeline.json
cp config/metrics_pipeline.example.json config/metrics_pipeline.json
cp config/integration_delivery.example.json config/integration_delivery.json
cp config/presentation.example.json config/presentation.json
```

Use `config/segmentation_pipeline.gpu.example.json` for Thor GPU validation.

## Run

Start the API scope:

```bash docker compose -f docker-compose.api.yml up -d \ control-plane prepare segmentation metrics integration-delivery
```

Optional maintenance and monitoring services:

```bash
docker compose -f docker-compose.api.yml --profile maintenance up -d space-manager
docker compose -f docker-compose.api.yml --profile monitoring up -d resource-monitor
```

Stop the API scope:

```bash docker compose -f docker-compose.api.yml down
```

## Validation

Control-plane smoke:

```bash
curl -fsS http://127.0.0.1:8001/docs >/dev/null
```

Container Python and CUDA visibility:

```bash docker compose -f docker-compose.api.yml exec segmentation python --version docker compose -f docker-compose.api.yml exec segmentation python - <<'PY' import torch print(torch.__version__) print(torch.cuda.is_available()) print(torch.cuda.get_device_name(0) if torch.cuda.is_available() else "no cuda") PY
```

Single-container Thor GPU smoke without starting the stack:

```bash
docker run --rm \
  --device /dev/nvidia0 \
  --device /dev/nvidiactl \
  --device /dev/nvidia-uvm \
  -v /usr/lib/x86_64-linux-gnu/libcuda.so.1:/usr/lib/x86_64-linux-gnu/libcuda.so.1:ro \
  -v /usr/lib/x86_64-linux-gnu/libnvidia-ml.so.1:/usr/lib/x86_64-linux-gnu/libnvidia-ml.so.1:ro \
  heimdallr-api:local \
  python -c 'import torch; print(torch.cuda.is_available()); print(torch.cuda.get_device_name(0))'
```

TotalSegmentator CLI visibility:

```bash docker compose -f docker-compose.api.yml exec segmentation TotalSegmentator --version docker compose -f docker-compose.api.yml exec segmentation totalseg_download_weights --help
```

End-to-end validation still requires a non-PHI test package and the integration
submission contract documented under `heimdallr/integration/docs/`.

## Operational Notes

- The compose file intentionally does not start DICOM intake or DICOM egress.
- The stack exposes only the control plane on port `8001` by default.
- All workers share the same mounted runtime directory and SQLite database.
- TotalSegmentator config is stored in the named volume
  `heimdallr-totalseg-config`.
- Baked weights live in the image under `/opt/totalsegmentator/weights`.
- The Thor GPU segmentation example keeps `--nr_thr_resamp 14` and
  `--nr_thr_saving 14` by default. A supervised direct test with the dependency
  guard raised to `512*512*3000` and `--nr_thr_saving 30` avoided
  TotalSegmentator's single-thread downgrade, but exhausted RAM and swap while
  saving the full `total` atlas for a 512x512x1767 case. Prefer
  `--roi_subset` or `--ml` before raising full-atlas saving concurrency.
- Runtime output packages, logs, SQLite state, and host-local config remain
  outside the image.
- Thor GPU access currently depends on direct device and driver-library bind
  mounts, not on Docker `--gpus all`.
