# Cerebelum Python SDK

Python SDK for **Cerebelum** — a deterministic workflow orchestration engine built on Elixir/OTP.

## Install

```bash
pip install cerebelum-sdk
```

Or from source:

```bash
git clone https://github.com/ZeaCl/cerebelum-python.git
cd cerebelum-python
pip install -e .
```

## Quick Start

```python
from cerebelum import step, workflow

@step
def hello(inputs):
    name = inputs.get("name", "World")
    return f"Hello, {name}!"

@workflow
def my_workflow(wf):
    wf.timeline(hello)

# Execute locally (no engine needed)
result = my_workflow.execute({"name": "Carlos"})
print(result.final_result)  # "Hello, Carlos!"
```

## Distributed Mode

Requires [Cerebelum Engine](https://github.com/ZeaCl/cerebelum) running with gRPC enabled.

```python
from cerebelum import DistributedExecutor

executor = DistributedExecutor(core_url="localhost:50051")
result = await executor.execute(my_workflow, {"name": "Carlos"})
```

## Examples

See `examples/` directory for complete tutorials.

## License

MIT — see [Cerebelum Engine](https://github.com/ZeaCl/cerebelum) for details.
