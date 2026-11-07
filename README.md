# Agent deadline ledger

A small, archived Python utility for monotonic request admission and evidence-preserving cancellation accounting. Extracted from an experimental developer-agent project. It uses the Python standard library and does not import an SDK, start a model, manage processes, or contact a service.

## What it does

- Caps request timeouts by remaining monotonic budget.
- Records preparation, dispatch, completion, cancellation and unknown outcomes in a JSONL ledger.
- Keeps cancellation distinct from proof that a remote operation stopped.
- Preserves explicitly acknowledged patch bytes and their SHA256.
- Rejects incompatible protocol identities and treats resumed ledgers as read-only.

## Run the tests

Python 3.11 or newer:

```sh
python -m unittest discover -s tests -v
```

Tests use synthetic payloads, temporary local files and asyncio fixtures. They do not run an Agent, GPU, shell tool, competition task or remote API.

## Limits

This is archived experimental code, not a production sandbox or a safety certification. `safe_to_advance` describes the ledger's local request evidence only: it does not prove remote quiescence, absence of tool descendants, or cross-session isolation. `asyncio.wait_for` requests cooperative cancellation; a sender that suppresses cancellation or performs detached/remote work may outlive the deadline. The caller must enforce those boundaries separately.

Default policy constants reflect the original diagnostic setup, not a recommended competition budget. No score improvement or generalization claim is made. Ledger timestamps from a previous process are not permission to retry unknown side effects.

## Provenance and scope

This public snapshot contains only project-owned deadline accounting and two synthetic test suites. The implementation is unchanged from the archived v1.2 module; test import paths were adjusted to this repository. New documentation and an MIT license were added. AI-assisted development was used. No third-party SDK, model, frozen submission, task dataset, reference patch, private trace, account credential or infrastructure configuration is distributed.

The source project has been discontinued. This repository is provided for inspection and reuse, without a maintenance commitment.
