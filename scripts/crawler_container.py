#!/usr/bin/env python3
"""Run RERA crawler invocations inside Docker containers.

This wrapper keeps crawler CLI arguments unchanged. Wrapper flags are parsed
first; everything else is passed through to ``run_crawlers.py`` inside the
image.
"""
from __future__ import annotations

import argparse
import os
import re
import shlex
import subprocess
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_IMAGE = "rera-crawlers:latest"
ROLE_LABEL = "com.primenumbers.rera.role"
LABEL_PREFIX = "com.primenumbers.rera"


def _slug(value: str, *, limit: int = 80) -> str:
    value = re.sub(r"[^a-zA-Z0-9_.-]+", "-", value.strip()).strip("-").lower()
    return (value or "run")[:limit]


def _arg_values(args: list[str], names: set[str]) -> list[str]:
    values: list[str] = []
    i = 0
    while i < len(args):
        token = args[i]
        if token in names and i + 1 < len(args):
            values.append(args[i + 1])
            i += 2
            continue
        matched = False
        for name in names:
            prefix = f"{name}="
            if token.startswith(prefix):
                values.append(token[len(prefix):])
                matched = True
                break
        i += 1 if matched else 1
    return values


def infer_mode(crawler_args: list[str]) -> str:
    values = _arg_values(crawler_args, {"--mode"})
    return values[-1] if values else "weekly_deep"


def infer_sites(crawler_args: list[str]) -> str:
    selected: list[str] = []
    for raw in _arg_values(crawler_args, {"--site", "--sites"}):
        for site_id in raw.split(","):
            site_id = site_id.strip()
            if site_id and site_id not in selected:
                selected.append(site_id)
    return ",".join(selected)


def is_tester(crawler_args: list[str]) -> bool:
    return "--tester" in crawler_args


def _running_normal_crawler_containers() -> list[str]:
    try:
        result = subprocess.run(
            [
                "docker", "ps",
                "--filter", f"label={ROLE_LABEL}=crawler",
                "--filter", f"label={LABEL_PREFIX}.tester=false",
                "--format", "{{.ID}} {{.Names}}",
            ],
            cwd=str(PROJECT_ROOT),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
    except Exception:
        return []
    if result.returncode != 0:
        return []
    return [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]


def assert_no_duplicate_normal_run(crawler_args: list[str], *, allow_concurrent: bool = False) -> None:
    if allow_concurrent or is_tester(crawler_args):
        return
    running = _running_normal_crawler_containers()
    if not running:
        return
    raise RuntimeError(
        "A normal crawler container is already running. Stop it first with "
        "`docker stop $(docker ps -q --filter label=com.primenumbers.rera.role=crawler "
        "--filter label=com.primenumbers.rera.tester=false)` or pass --allow-concurrent.\n"
        "Running containers:\n" + "\n".join(running)
    )


def default_container_name(crawler_args: list[str], *, prefix: str = "rera-crawler") -> str:
    mode = _slug(infer_mode(crawler_args), limit=32)
    sites = infer_sites(crawler_args)
    site_part = _slug(sites.replace(",", "-"), limit=48) if sites else "all"
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"{prefix}-{mode}-{site_part}-{ts}-{os.getpid()}"


def build_docker_run_command(
    crawler_args: list[str],
    *,
    image: str | None = None,
    detach: bool = False,
    remove: bool | None = None,
    name: str | None = None,
    network: str | None = None,
    env_file: Path | None = None,
    logs_dir: Path | None = None,
    shm_size: str = "1g",
) -> list[str]:
    image = image or os.environ.get("RERA_CRAWLER_IMAGE") or DEFAULT_IMAGE
    network = network if network is not None else os.environ.get("RERA_DOCKER_NETWORK", "host")
    env_file = env_file if env_file is not None else PROJECT_ROOT / ".env"
    logs_dir = logs_dir if logs_dir is not None else PROJECT_ROOT / "logs"
    remove = (not detach) if remove is None else remove
    name = name or default_container_name(crawler_args)

    labels = {
        ROLE_LABEL: "crawler",
        f"{LABEL_PREFIX}.mode": infer_mode(crawler_args),
        f"{LABEL_PREFIX}.sites": infer_sites(crawler_args),
        f"{LABEL_PREFIX}.tester": "true" if is_tester(crawler_args) else "false",
        f"{LABEL_PREFIX}.started_at": datetime.now(timezone.utc).isoformat(),
        f"{LABEL_PREFIX}.cmd": " ".join(shlex.quote(part) for part in crawler_args),
    }

    logs_dir.mkdir(parents=True, exist_ok=True)

    cmd = ["docker", "run", "--init"]
    if detach:
        cmd.append("--detach")
    if remove:
        cmd.append("--rm")
    cmd.extend(["--name", name, "--shm-size", shm_size])
    if network and network.lower() != "default":
        cmd.extend(["--network", network])
    if env_file.exists():
        cmd.extend(["--env-file", str(env_file)])
    cmd.extend([
        "-e", "PYTHONHASHSEED=0",
        "-e", "PYTHONUNBUFFERED=1",
        "-e", "RERA_IN_DOCKER=true",
        "-e", "CHROME_BIN=/usr/bin/chromium",
        "-e", "CHROMEDRIVER_BIN=/usr/bin/chromedriver",
        "--pids-limit", "512",
        "--tmpfs", "/tmp:rw,nosuid,nodev,size=1g",
        "-v", f"{logs_dir.resolve()}:/app/logs",
    ])
    for key, value in labels.items():
        cmd.extend(["--label", f"{key}={value}"])
    cmd.append(image)
    cmd.extend(crawler_args)
    return cmd


def run_container(crawler_args: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
    assert_no_duplicate_normal_run(
        crawler_args,
        allow_concurrent=bool(kwargs.pop("allow_concurrent", False)),
    )
    cmd = build_docker_run_command(crawler_args, **kwargs)
    return subprocess.run(cmd, cwd=str(PROJECT_ROOT), text=True)


def start_detached(crawler_args: list[str], **kwargs) -> dict[str, str]:
    assert_no_duplicate_normal_run(
        crawler_args,
        allow_concurrent=bool(kwargs.pop("allow_concurrent", False)),
    )
    cmd = build_docker_run_command(crawler_args, detach=True, **kwargs)
    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout or "docker run failed").strip())
    container_id = (result.stdout or "").strip()
    return {
        "container_id": container_id,
        "container": container_id[:12],
        "cmd": " ".join(shlex.quote(part) for part in cmd),
    }


def parse_args(argv: list[str] | None = None) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(
        description="Run run_crawlers.py inside the RERA Docker image",
        add_help=True,
    )
    parser.add_argument("--image", default=os.environ.get("RERA_CRAWLER_IMAGE", DEFAULT_IMAGE))
    parser.add_argument("--detach", action="store_true", help="Start the container in the background")
    parser.add_argument("--name", default=None, help="Docker container name")
    parser.add_argument("--keep", action="store_true", help="Do not pass --rm")
    parser.add_argument("--network", default=os.environ.get("RERA_DOCKER_NETWORK", "host"))
    parser.add_argument("--shm-size", default="1g")
    parser.add_argument(
        "--allow-concurrent",
        action="store_true",
        help="Allow starting another normal crawler while one is already running",
    )
    return parser.parse_known_args(argv)


def main(argv: list[str] | None = None) -> int:
    args, crawler_args = parse_args(argv)
    remove = False if args.keep else None
    if args.detach:
        info = start_detached(
            crawler_args,
            image=args.image,
            name=args.name,
            network=args.network,
            remove=remove,
            shm_size=args.shm_size,
            allow_concurrent=args.allow_concurrent,
        )
        print(info["container_id"])
        return 0
    result = run_container(
        crawler_args,
        image=args.image,
        name=args.name,
        network=args.network,
        remove=remove,
        shm_size=args.shm_size,
        allow_concurrent=args.allow_concurrent,
    )
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
