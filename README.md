# Synapsys Python SDK

Register a Python application as a Synapsys worker and let Synapsys Core start and
stop its background processes remotely.

Your application marks existing functions as either endless or progressive. The SDK
reports them to Core on an outbound heartbeat, applies start and stop commands, and
runs each process in an isolated thread. It never opens an inbound port and it is not
a scheduler.

## Requirements

- Python 3.9 or later
- A reachable Synapsys Core instance and a worker token

The SDK has no runtime dependencies.

## Install

```bash
pip install basamento-synapsys
```

The PyPI distribution is `basamento-synapsys`; the import package uses Python's
underscore convention:

```python
from basamento_synapsys import Synapsys
```

## Quick start

```python
from basamento_synapsys import StopSignal, Synapsys

synapsys = Synapsys(worker_name="billing-worker")


@synapsys.endless(name="invoice-listener")
def listen_for_invoices(stop: StopSignal) -> None:
    while not stop.requested:
        invoice = receive_invoice(timeout=1)
        if invoice is not None:
            process_invoice(invoice)


@synapsys.progressive(name="monthly-report")
def build_monthly_report(stop: StopSignal) -> None:
    for account in load_accounts():
        stop.raise_if_requested()
        add_account_to_report(account)


if __name__ == "__main__":
    synapsys.start()
```

Set connection details in the environment:

```bash
SYNAPSYS_CORE_URL=https://core.example.com
SYNAPSYS_CORE_TOKEN=syn_...
```

The token is sent as `Authorization: Bearer <token>`. Keep it in an environment
variable or secret, never in committed configuration.

## The two process types

An **endless** process owns a long-running body such as a listener or consumer. It
keeps running until it observes a stop request and returns:

```python
@synapsys.endless(name="queue-consumer")
def consume(stop: StopSignal) -> None:
    while not stop.requested:
        message = queue.get(timeout=1)
        handle(message)
```

A **progressive** process runs once and returns to `idle` when its function finishes:

```python
@synapsys.progressive(name="rebuild-index")
def rebuild_index() -> None:
    rebuild()
```

Synapsys does not repeatedly invoke either function on a timer. Core decides when a
process starts; the process type describes whether that invocation naturally ends.

### Optional `StopSignal`

Both decorators accept functions with either zero arguments or one `StopSignal`.
A zero-argument function is useful when existing logic needs no in-flight
cancellation:

```python
@synapsys.progressive(name="export")
def export() -> None:
    create_export()
```

Accept the signal when the function should stop cooperatively:

```python
@synapsys.progressive(name="export")
def export(stop: StopSignal) -> None:
    for record in records:
        stop.raise_if_requested()
        export_record(record)
```

`StopSignal` provides:

| Member | Meaning |
| --- | --- |
| `requested` | `True` once Core has requested a stop. |
| `event` | The underlying `threading.Event`, for compatible blocking APIs. |
| `wait(timeout)` | Wait until stopped; returns `True` when a stop caused the wake-up. |
| `wait_async(timeout)` | Async equivalent of `wait`. |
| `raise_if_requested()` | Raise the SDK's cooperative cancellation exception. |
| `is_stop_requested()` | Method form of `requested`. |

Python cannot safely kill an arbitrary running thread. A synchronous function that
does not observe the signal and cannot be unblocked by cleanup remains `stopping`
until it returns.

### Existing listeners and cleanup

An existing listener with its own shutdown method needs no internal Synapsys logic:

```python
listener = OrderListener()


@synapsys.endless(name="order-listener", on_stop=listener.close)
def run_listener() -> None:
    listener.run_forever()
```

On a stop command, `listener.close()` runs in an isolated cleanup thread and should
unblock `run_forever()`. Cleanup failures are reported but never abort the stop.

The equivalent explicit registration API is useful in application factories:

```python
synapsys.register_endless(
    listener.run_forever,
    name="order-listener",
    on_stop=listener.close,
)
```

Decorators return the original function unchanged, so normal direct unit testing
continues to work.

## Async functions

The same decorators accept `async def`:

```python
@synapsys.progressive(name="sync-orders")
async def sync_orders(stop: StopSignal) -> None:
    for order in await fetch_orders():
        stop.raise_if_requested()
        await save_order(order)
```

A stop cancels the async task at its next cancellation point. With normal `start()`,
the vanilla SDK gives each async invocation an isolated event loop. With
`await start_async()`, async processes run on the calling application's loop, so
they can safely reuse loop-bound resources created by an async framework. Sync
processes always remain on dedicated threads.

## Configuration

Constructor arguments override environment variables.

| Argument | Environment variable | Default | Description |
| --- | --- | --- | --- |
| `enabled` | `SYNAPSYS_ENABLED` | `True` | `False` makes the SDK a complete no-op. |
| `core_url` | `SYNAPSYS_CORE_URL` | — | **Required.** Synapsys Core base URL. |
| `core_token` | `SYNAPSYS_CORE_TOKEN` | — | Bearer token. Missing tokens warn; Core rejects the heartbeat. |
| `worker_name` | `SYNAPSYS_WORKER_NAME` | — | **Required.** Stable worker identity. |
| `host` | `SYNAPSYS_HOST` | machine hostname | Host reported to Core. |
| `heartbeat_interval` | `SYNAPSYS_HEARTBEAT_INTERVAL` | `"5s"` | Positive whole-second interval. |
| `fail_fast` | `SYNAPSYS_FAIL_FAST` | `False` | Fail startup when Core is unreachable. |
| `connect_timeout` | `SYNAPSYS_CONNECT_TIMEOUT` | `"2s"` | TCP/TLS connection timeout. |
| `request_timeout` | `SYNAPSYS_REQUEST_TIMEOUT` | `"5s"` | HTTP request I/O timeout. |
| `capture_console` | `SYNAPSYS_CAPTURE_CONSOLE` | `True` | Send process stdout/stderr to Core. |
| `log_level` | `SYNAPSYS_LOG_LEVEL` | `"info"` | `debug`, `info`, `warning`, `error`, or `silent`. |
| `logger` | — | `logging.getLogger("basamento_synapsys")` | Application logger to use. |

Durations carry units: `"30s"`, `"5m"`, `"250ms"`, or `"2h"`. A numeric value is
accepted as milliseconds. Unitless strings are rejected.

Unknown constructor arguments fail immediately, including when `enabled=False`, so
a typo cannot hide in a disabled test run.

## Logging

The SDK uses Python's `logging` framework and prefixes every message with
`[Synapsys] `. Configure it like any other library logger:

```python
import logging

logging.basicConfig(level=logging.INFO)
```

Alternatively, pass an existing logger:

```python
synapsys = Synapsys(worker_name="billing-worker", logger=application_logger)
```

Healthy heartbeats are silent. Connection failures are logged on transitions rather
than on every retry. The Core token is never exposed through `config` or logs.

## Console capture

While a process runs, complete lines it writes to stdout or stderr are sent to Core
against the exact execution. Output still reaches the application's real streams in
order: capture is a tee, not a redirect. Output outside a process is not captured,
and concurrent process output is attributed with Python context variables.

This transmits anything the process prints, potentially including sensitive data.
Disable it independently while keeping remote control active:

```python
synapsys = Synapsys(worker_name="billing-worker", capture_console=False)
```

## Lifecycle and framework reuse

Plain Python has no universal application lifecycle, so it calls `start()` once.
`stop()` is idempotent, restores stdout/stderr, stops heartbeats, and requests that
running processes stop. The SDK does not install operating-system signal handlers;
the host application retains control:

```python
try:
    synapsys.start()
    run_application()
finally:
    synapsys.stop()
```

It can also be used as a context manager:

```python
with synapsys:
    run_application()
```

Async framework lifecycles can call `await start_async()` and `await stop_async()`,
or use `async with synapsys`. These methods keep startup HTTP checks and shutdown
waits off the framework event loop and bind async process bodies to that application
loop. This is the supported seam for future FastAPI adapters; synchronous lifecycle
methods provide the corresponding seam for Django.

## Failure behavior

- Core being unavailable never stops user work; heartbeats keep retrying.
- Startup succeeds while Core is down unless `fail_fast=True`.
- A heartbeat exception cannot terminate the heartbeat loop.
- One process failure cannot prevent commands reaching another.
- User exceptions are captured, reported as `failed`, and never propagate into the
  host application.
- Duplicate commands are ignored through acknowledged command tokens.
- Core's 64-bit run IDs remain exact because Python integers have arbitrary precision.

## Testing applications

Set `enabled=False` or `SYNAPSYS_ENABLED=false`. The SDK then performs no validation,
network calls, heartbeat work, or console interception. Decorated functions remain
ordinary functions and can be called directly.

## License

Apache License 2.0. See [LICENSE](LICENSE).
