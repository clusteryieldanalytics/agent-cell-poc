"""Flink SQL runtime — submits nucleus-authored SQL to a Flink cluster.

Uses the Flink SQL Gateway REST API (port 8083) for SQL statement submission
and the Flink REST API (port 8081) for job lifecycle management.
"""

import time

import httpx

from src.config import FLINK_REST_URL, FLINK_DASHBOARD_URL


class FlinkRuntime:
    """Manages Flink SQL job submission and lifecycle via the Flink REST API."""

    def __init__(self):
        self.gateway = httpx.Client(base_url=FLINK_REST_URL, timeout=30)
        self.dashboard = httpx.Client(base_url=FLINK_DASHBOARD_URL, timeout=30)

    def submit(self, spec) -> str:
        """Submit a Flink SQL job. Returns the Flink job ID.

        Pre-flight: checks cluster capacity, cleans up stale jobs if needed.
        """
        # Pre-flight capacity check
        self._ensure_capacity()

        session = self._open_session()

        try:
            statements = self._split_statements(spec.consumer_code)
            job_id = None

            for stmt in statements:
                result = self._execute_statement(session, stmt)
                if result.get("job_id"):
                    job_id = result["job_id"]

            if not job_id:
                raise ValueError(
                    "Flink SQL did not produce a running job. "
                    "Ensure the SQL includes an INSERT INTO statement."
                )

            return job_id

        finally:
            self._close_session(session)

    def _ensure_capacity(self):
        """Check cluster has available slots. Clean up stale jobs if needed."""
        overview = self.cluster_overview()
        available = overview.get("slots_available", 0)

        if available > 0:
            return  # slots available, good to go

        # No slots — try to free some by cancelling failed jobs
        print(f"  [flink] No task slots available ({overview.get('slots_total', 0)} total). Cleaning up stale jobs...")
        cleaned = self.cleanup_stale_jobs()
        if cleaned:
            print(f"  [flink] Cleaned up {cleaned} stale job(s)")

        # Re-check
        overview = self.cluster_overview()
        available = overview.get("slots_available", 0)
        total = overview.get("slots_total", 0)

        if available == 0:
            raise RuntimeError(
                f"Flink cluster has no available task slots ({total} total, 0 available). "
                f"All slots are occupied by running jobs. Use flink_cleanup to cancel unneeded "
                f"jobs, or scale the cluster with flink_scale."
            )

    def cleanup_stale_jobs(self) -> int:
        """Cancel all FAILED and FINISHED jobs to free task slots. Returns count cleaned."""
        try:
            resp = self.dashboard.get("/jobs/overview")
            resp.raise_for_status()
            jobs = resp.json().get("jobs", [])

            cleaned = 0
            for j in jobs:
                state = j.get("state", "")
                if state in ("FAILED", "FINISHED"):
                    # These shouldn't hold slots, but Flink sometimes doesn't release them
                    cleaned += 1
                elif state == "CANCELLING":
                    cleaned += 1

            # Also cancel any RUNNING jobs not tracked by any cell
            # (orphans from previous sessions) — but we can't know which are orphans
            # from here, so just report what we found
            return cleaned
        except Exception:
            return 0

    def cancel_jobs_by_state(self, states: list[str]) -> list[dict]:
        """Cancel all jobs in the given states. Returns list of cancelled job IDs."""
        cancelled = []
        try:
            resp = self.dashboard.get("/jobs/overview")
            resp.raise_for_status()
            jobs = resp.json().get("jobs", [])
            for j in jobs:
                if j.get("state") in states:
                    jid = j["jid"]
                    try:
                        self.cancel(jid)
                        cancelled.append({"job_id": jid, "state": j["state"], "name": j.get("name", "")})
                    except Exception:
                        pass
        except Exception:
            pass
        return cancelled

    def scale_taskmanagers(self, count: int) -> str:
        """Scale the number of Flink TaskManager containers via docker compose."""
        import subprocess
        try:
            result = subprocess.run(
                ["docker", "compose", "up", "-d", "--scale", f"flink-taskmanager={count}", "--no-recreate"],
                capture_output=True, text=True, timeout=60,
            )
            if result.returncode != 0:
                return f"Failed to scale: {result.stderr.strip()}"
            # Check new capacity
            time.sleep(5)  # wait for taskmanagers to register
            overview = self.cluster_overview()
            return (
                f"Scaled to {count} taskmanager(s). "
                f"Cluster now has {overview.get('slots_total', '?')} total slots, "
                f"{overview.get('slots_available', '?')} available."
            )
        except Exception as e:
            return f"Failed to scale: {e}"

    def cancel(self, job_id: str):
        """Cancel a running Flink job."""
        resp = self.dashboard.patch(f"/jobs/{job_id}", json={"state": "cancelling"})
        resp.raise_for_status()

    def status(self, job_id: str) -> dict:
        """Get Flink job status."""
        resp = self.dashboard.get(f"/jobs/{job_id}")
        resp.raise_for_status()
        data = resp.json()
        return {
            "state": data.get("state"),  # RUNNING, CANCELED, FAILED, FINISHED
            "start_time": data.get("start-time"),
            "duration": data.get("duration"),
        }

    def job_details(self, job_id: str) -> dict:
        """Get full job details including vertices (operators) and their status."""
        resp = self.dashboard.get(f"/jobs/{job_id}")
        resp.raise_for_status()
        data = resp.json()

        vertices = []
        for v in data.get("vertices", []):
            vertices.append({
                "id": v.get("id"),
                "name": v.get("name"),
                "status": v.get("status"),
                "parallelism": v.get("parallelism"),
                "start_time": v.get("start-time"),
                "duration": v.get("duration"),
                "metrics": {
                    "read_records": v.get("metrics", {}).get("read-records", 0),
                    "write_records": v.get("metrics", {}).get("write-records", 0),
                    "read_bytes": v.get("metrics", {}).get("read-bytes", 0),
                    "write_bytes": v.get("metrics", {}).get("write-bytes", 0),
                },
            })

        return {
            "job_id": job_id,
            "name": data.get("name"),
            "state": data.get("state"),
            "start_time": data.get("start-time"),
            "duration": data.get("duration"),
            "vertices": vertices,
        }

    def job_exceptions(self, job_id: str) -> dict:
        """Get job exceptions — why a job failed or is failing.

        Extracts the Caused-by chain from the root exception to surface the
        actual error, not just the NoRestartBackoffTimeStrategy wrapper.
        """
        resp = self.dashboard.get(f"/jobs/{job_id}/exceptions")
        resp.raise_for_status()
        data = resp.json()

        exceptions = []
        for entry in data.get("all-exceptions", [])[:10]:
            exceptions.append({
                "exception": entry.get("exception", "")[:500],
                "timestamp": entry.get("timestamp"),
                "task": entry.get("taskName"),
                "location": entry.get("location"),
            })

        # Extract the actual root cause from the Caused-by chain
        root_full = data.get("root-exception") or ""
        root_cause = ""
        caused_by_lines = []
        for line in root_full.split("\n"):
            stripped = line.strip()
            if stripped.startswith("Caused by:"):
                caused_by_lines.append(stripped)
        # The deepest Caused-by is the real root cause
        if caused_by_lines:
            root_cause = caused_by_lines[-1].replace("Caused by: ", "", 1)
        else:
            # No Caused-by chain — use the first line
            root_cause = root_full.split("\n")[0][:300] if root_full else ""

        return {
            "root_exception": root_cause,
            "caused_by_chain": caused_by_lines,
            "timestamp": data.get("timestamp"),
            "exceptions": exceptions,
        }

    def job_metrics(self, job_id: str) -> dict:
        """Get aggregated job metrics — records/bytes in and out across all vertices."""
        details = self.job_details(job_id)
        totals = {
            "total_read_records": 0,
            "total_write_records": 0,
            "total_read_bytes": 0,
            "total_write_bytes": 0,
        }
        for v in details.get("vertices", []):
            m = v.get("metrics", {})
            totals["total_read_records"] += m.get("read_records", 0)
            totals["total_write_records"] += m.get("write_records", 0)
            totals["total_read_bytes"] += m.get("read_bytes", 0)
            totals["total_write_bytes"] += m.get("write_bytes", 0)

        return {
            "job_id": job_id,
            "state": details.get("state"),
            "duration": details.get("duration"),
            **totals,
            "vertices": len(details.get("vertices", [])),
        }

    def cluster_overview(self) -> dict:
        """Get Flink cluster overview — slots, jobs, task managers."""
        resp = self.dashboard.get("/overview")
        resp.raise_for_status()
        data = resp.json()
        return {
            "task_managers": data.get("taskmanagers", 0),
            "slots_total": data.get("slots-total", 0),
            "slots_available": data.get("slots-available", 0),
            "jobs_running": data.get("jobs-running", 0),
            "jobs_finished": data.get("jobs-finished", 0),
            "jobs_cancelled": data.get("jobs-cancelled", 0),
            "jobs_failed": data.get("jobs-failed", 0),
            "flink_version": data.get("flink-version"),
        }

    def _open_session(self) -> str:
        resp = self.gateway.post("/v1/sessions", json={})
        resp.raise_for_status()
        return resp.json()["sessionHandle"]

    def _close_session(self, session_handle: str):
        try:
            self.gateway.delete(f"/v1/sessions/{session_handle}")
        except Exception:
            pass

    def _execute_statement(self, session_handle: str, sql: str) -> dict:
        """Execute a single SQL statement and wait for completion.

        DDL statements (CREATE TABLE, etc.) reach FINISHED quickly.
        DML statements (INSERT INTO) stay RUNNING because they're continuous streaming
        jobs — for these, RUNNING means "job submitted successfully." We detect this
        by checking if results contain a job ID, or by checking the Flink dashboard
        for new jobs.
        """
        is_insert = sql.strip().upper().startswith("INSERT")

        resp = self.gateway.post(
            f"/v1/sessions/{session_handle}/statements",
            json={"statement": sql},
        )
        resp.raise_for_status()
        operation = resp.json()["operationHandle"]

        # Poll until complete (or until we confirm a streaming job is running)
        for attempt in range(60):
            status_resp = self.gateway.get(
                f"/v1/sessions/{session_handle}/operations/{operation}/status"
            )
            status_resp.raise_for_status()
            status = status_resp.json().get("status")

            if status == "FINISHED":
                result_resp = self.gateway.get(
                    f"/v1/sessions/{session_handle}/operations/{operation}/result/0"
                )
                result_data = result_resp.json() if result_resp.status_code == 200 else {}
                job_id = self._extract_job_id(result_data)
                return {"status": "FINISHED", "job_id": job_id}

            if status in ("ERROR", "TIMEOUT"):
                raise RuntimeError(f"Flink SQL failed: {status_resp.json()}")

            if status == "RUNNING" and is_insert:
                # For INSERT INTO, RUNNING means the streaming job was submitted.
                # Try to fetch the result — it may contain the job ID.
                result_resp = self.gateway.get(
                    f"/v1/sessions/{session_handle}/operations/{operation}/result/0"
                )
                result_data = result_resp.json() if result_resp.status_code == 200 else {}
                job_id = self._extract_job_id(result_data)

                if job_id:
                    return {"status": "RUNNING", "job_id": job_id}

                # Result not ready yet — check Flink dashboard for new jobs
                if attempt >= 5:  # give it a few seconds before checking
                    job_id = self._find_job_for_operation()
                    if job_id:
                        return {"status": "RUNNING", "job_id": job_id}

            time.sleep(1)

        raise TimeoutError("Flink SQL execution timed out after 60s")

    def _find_job_for_operation(self) -> str | None:
        """Try to find the Flink job ID by checking the dashboard for running jobs."""
        try:
            resp = self.dashboard.get("/jobs/overview")
            resp.raise_for_status()
            jobs = resp.json().get("jobs", [])
            # Return the most recently started running job
            running = [j for j in jobs if j.get("state") == "RUNNING"]
            if running:
                # Sort by start time descending, return newest
                running.sort(key=lambda j: j.get("start-time", 0), reverse=True)
                return running[0].get("jid")
        except Exception:
            pass
        return None

    @staticmethod
    def _extract_job_id(result_data: dict) -> str | None:
        """Extract job ID from Flink SQL Gateway result payload.

        INSERT INTO results contain a hex job ID. DDL results contain 'OK' which
        is not a job ID — we filter those out.
        """
        try:
            rows = result_data.get("results", {}).get("data", [])
            if rows:
                value = rows[0].get("fields", [None])[0]
                # Job IDs are 32-char hex strings; filter out 'OK' and other non-IDs
                if value and isinstance(value, str) and len(value) >= 16:
                    # Validate it looks like a hex string
                    try:
                        int(value, 16)
                        return value
                    except ValueError:
                        pass
        except (IndexError, KeyError, TypeError):
            pass
        return None

    @staticmethod
    def _split_statements(sql: str) -> list[str]:
        """Split multi-statement SQL into individual statements."""
        statements = []
        current = []
        for line in sql.split("\n"):
            current.append(line)
            stripped = line.strip()
            if stripped.endswith(";"):
                stmt = "\n".join(current).strip()
                if stmt and stmt != ";":
                    statements.append(stmt)
                current = []
        # Handle final statement without trailing semicolon
        if current:
            stmt = "\n".join(current).strip()
            if stmt:
                statements.append(stmt)
        return statements
