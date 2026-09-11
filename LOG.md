# LOG

Append-only. Newest at the bottom. Each entry: date · what got done · (weekend) prediction vs actual · **next command to run**.

This file is how you resume cold. If you sit down and don't know where you were, read the last entry.

---

## 2026-09-11 — Phase 1 kicked off

- Roadmap spec approved; `telemetry-lakehouse` repo created.
- Week-1 teaching plan written: `docs/plans/2026-09-11-phase1-week1.md`.
- **Blocker:** Docker not yet installed.

**Next command to run:**

```
winget install -e --id Docker.DockerDesktop
```

Then Task 0 in the week-1 plan (`docker run --rm hello-world`).

## 2026-09-11 — Docker installed (the hard way)

Docker Desktop 4.90 crash-looped on startup: `rename <socket> <socket>.stale: The file cannot be accessed by the system`. Cause: orphaned AF_UNIX socket files in a broken state (one, `docker-secrets-engine\engine.sock`, dated back to March) that no user-space tool could delete. Fix: killed all Docker procs + `wsl --shutdown`, then **renamed the parent directories aside** (`Docker\run`, `docker-secrets-engine`) so Docker recreated them clean. Engine came up: server 29.7.2, 16 CPUs, ~7.5 GB VM RAM. Compose v5.5.1.

**Task 0 done** — `docker run --rm hello-world` prints the greeting.

**Next command to run:** Task 1, Step 1 — `docker run -it --name box1 ubuntu:24.04 bash`

## 2026-09-11 — Tasks 1 & 2 done

- **Task 1** (image vs container): file in a container's writable layer survived stop→start but died on `docker rm`. Bonus lesson: `docker` is not available *inside* a container — proved isolation by accident.
- **Task 2** (layers + cache): first build 21.0s, rebuild after editing app.txt 3.6s. Two `RUN` layers `CACHED`, only `COPY` re-ran. `docker history` timestamps show reused vs rebuilt layers; `<missing>` = unnamed intermediate layers, not an error.

**Next command to run:** Task 3, Step 1 — `docker volume create mydata`
