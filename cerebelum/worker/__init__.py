"""Cerebelum Worker — Generic poll loop for distributed execution.

Usage:
    python -m cerebelum.worker

Auto-discovers @step functions from workflow.py in the current directory,
connects to the engine via gRPC mTLS, and executes steps on demand.
"""

from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
from pathlib import Path
from typing import Any, Dict

import grpc

try:
    from .proto.worker_service_pb2 import (
        Ack,
        ErrorInfo,
        PollRequest,
        RegisterRequest,
        TaskResult,
        TaskStatus,
        UnregisterRequest,
    )
    from .proto.worker_service_pb2_grpc import WorkerServiceStub
    from .dsl.registry import StepRegistry
except ImportError:
    # Fallback for direct execution
    from cerebelum.proto.worker_service_pb2 import (
        Ack,
        ErrorInfo,
        PollRequest,
        RegisterRequest,
        TaskResult,
        TaskStatus,
        UnregisterRequest,
    )
    from cerebelum.proto.worker_service_pb2_grpc import WorkerServiceStub
    from cerebelum.dsl.registry import StepRegistry

# ── Configuration ───────────────────────────────────────────

ENGINE_HOST = os.environ.get("CEREBELUM_ENGINE", "cerebelum.zea.cl")
ENGINE_PORT = int(os.environ.get("CEREBELUM_GRPC_PORT", "50051"))
CERTS_DIR = Path.home() / ".cerebelum" / "certs"


# ── Step Discovery ──────────────────────────────────────────

def discover_workflow_file() -> Path | None:
    """Find workflow.py in the current directory."""
    cwd = Path.cwd()
    for name in ("workflow.py", "main.py"):
        path = cwd / name
        if path.exists():
            return path
    return None


def load_workflow_steps(path: Path) -> Dict[str, Any]:
    """Import workflow.py and return registered @step functions.

    The @step decorator in cerebelum.dsl.decorators automatically
    registers functions in StepRegistry when the module is imported.
    """
    spec = importlib.util.spec_from_file_location("workflow", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules["workflow"] = module
    spec.loader.exec_module(module)

    steps = StepRegistry.all()
    if not steps:
        raise RuntimeError(f"No @step functions found in {path}")

    return {name: meta.function for name, meta in steps.items()}


# ── gRPC Connection ─────────────────────────────────────────

def create_channel() -> grpc.Channel:
    """Create a secure gRPC channel with mTLS."""
    ca_path = CERTS_DIR / "ca.crt"
    cert_path = CERTS_DIR / "client.crt"
    key_path = CERTS_DIR / "client.key"

    if not ca_path.exists():
        raise RuntimeError(
            f"Certificates not found in {CERTS_DIR}.\n"
            "Run: cerebelum login && cerebelum run first to generate them."
        )

    with open(ca_path, "rb") as f:
        ca = f.read()
    with open(cert_path, "rb") as f:
        cert = f.read()
    with open(key_path, "rb") as f:
        key = f.read()

    creds = grpc.ssl_channel_credentials(
        root_certificates=ca,
        private_key=key,
        certificate_chain=cert,
    )

    return grpc.secure_channel(f"{ENGINE_HOST}:{ENGINE_PORT}", creds)


# ── Main Loop ───────────────────────────────────────────────

async def main_worker() -> None:
    """Register worker, poll for tasks, execute steps, submit results."""

    # 1. Discover steps
    wf_path = discover_workflow_file()
    if wf_path is None:
        print("❌ No workflow.py found in current directory.")
        print("   Create one with: npx @zea.cl/create-cerebelum my-project")
        sys.exit(1)

    print(f"📦 Loading steps from {wf_path}...")
    step_functions = load_workflow_steps(wf_path)
    print(f"   Steps found: {', '.join(step_functions.keys())}")

    # 2. Connect to engine
    worker_id = f"worker-{os.getpid()}"
    print(f"🔌 Connecting to {ENGINE_HOST}:{ENGINE_PORT} (mTLS)...")

    try:
        channel = create_channel()
        stub = WorkerServiceStub(channel)
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        sys.exit(1)

    # 3. Register worker
    capabilities = list(step_functions.keys())
    req = RegisterRequest(
        worker_id=worker_id,
        language="python",
        capabilities=capabilities,
        version="1.0",
        metadata={},
    )

    try:
        resp = stub.Register(req, timeout=10)
        if not resp.success:
            print(f"❌ Registration failed: {resp.message}")
            sys.exit(1)
        print(f"✅ Worker registered ({worker_id})")
    except Exception as e:
        print(f"❌ Registration error: {e}")
        channel.close()
        sys.exit(1)

    # 4. Poll loop
    print("🔄 Waiting for tasks... (Ctrl+C to stop)")
    try:
        while True:
            poll_req = PollRequest(
                worker_id=worker_id,
                timeout_ms=30_000,
            )

            try:
                task = stub.PollForTask(poll_req, timeout=35)
            except grpc.RpcError:
                continue

            if not task.task_id:
                continue

            step_name = task.step_name
            print(f"\n📋 Task: {step_name} (exec: {task.execution_id})")

            # Execute step
            step_func = step_functions.get(step_name)
            if step_func is None:
                error = ErrorInfo(
                    kind="not_found",
                    message=f"Step '{step_name}' not found",
                )
                result = TaskResult(
                    task_id=task.task_id,
                    execution_id=task.execution_id,
                    worker_id=worker_id,
                    status=TaskStatus.FAILED,
                    error=error,
                )
            else:
                try:
                    # Convert inputs from protobuf Struct to dict
                    from google.protobuf.json_format import MessageToDict

                    inputs = (
                        MessageToDict(task.step_inputs, preserving_proto_field_name=True)
                        if task.step_inputs
                        else {}
                    )

                    output = await step_func(None, **inputs)

                    result = TaskResult(
                        task_id=task.task_id,
                        execution_id=task.execution_id,
                        worker_id=worker_id,
                        status=TaskStatus.SUCCESS,
                    )
                    # Set result via result field
                    if output:
                        from google.protobuf import struct_pb2
                        result_struct = struct_pb2.Struct()
                        if isinstance(output, dict):
                            result_struct.update(output)
                        result.result.CopyFrom(result_struct)

                    print(f"   ✅ {step_name} → {output}")

                except Exception as e:
                    import traceback
                    error = ErrorInfo(
                        kind=type(e).__name__,
                        message=str(e),
                        stacktrace=traceback.format_exc(),
                    )
                    result = TaskResult(
                        task_id=task.task_id,
                        execution_id=task.execution_id,
                        worker_id=worker_id,
                        status=TaskStatus.FAILED,
                        error=error,
                    )
                    print(f"   ❌ {step_name}: {e}")

            # Submit result
            stub.SubmitResult(result, timeout=10)

    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
    finally:
        # Unregister
        try:
            unreg = UnregisterRequest(worker_id=worker_id, reason="shutdown")
            stub.Unregister(unreg, timeout=5)
        except Exception:
            pass
        channel.close()
        print("✅ Worker stopped")


if __name__ == "__main__":
    asyncio.run(main_worker())
