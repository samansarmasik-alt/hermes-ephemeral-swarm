# Agent guide

Instructions for coding agents working in this repository. Keep changes small, testable, and compatible with Hermes' public plugin/lifecycle contracts.

## Setup

- Requires Hermes Agent `>=0.21.5` and Python `>=3.11`.
- Preferred install (the plugin manager selects the active profile's plugin directory):

  ```bash
  hermes plugins install samansarmasik-alt/hermes-ephemeral-swarm --no-enable
  hermes plugins enable ephemeral-swarm
  hermes config set delegation.max_spawn_depth 2
  ```

- For local development, clone into the active profile's native plugin directory: `$HERMES_HOME/plugins/ephemeral-swarm` (on profile-based Windows installs, `<LOCALAPPDATA>/hermes/profiles/<profile>/plugins/ephemeral-swarm`). Then run `hermes plugins validate <plugin-directory>` and `hermes plugins doctor <plugin-directory> --ci`.
- A new Hermes session may be required after enabling/reloading the plugin.
- To reduce concurrency, configure `plugins.entries.ephemeral-swarm.settings.parallel_window` to an integer from `1` to `8`. Do not raise the source-level cap beyond the lifecycle executor capacity without verifying the upstream Hermes runtime.

## Tests

From the repository root:

```bash
python -m unittest discover -s tests -v
```

The tests are intentionally offline and use a fake lifecycle; they must not call a paid model/provider. Do not describe them as proof of live worker dispatch.

## Design boundaries

- `__init__.py` is the Hermes tool adapter, queue/refill logic, and lifecycle integration.
- `swarm_store.py` owns the in-memory, thread-safe mailbox, capabilities, rooms, and ancestry visibility.
- `tests/` covers plugin registration/queue behavior and mailbox authorization/visibility.
- Preserve the public `agent.subagent_lifecycle.SubagentLaunchRequest` / `ctx.subagent_lifecycle` API. Do not couple this plugin to `AIAgent`, gateway internals, or private Hermes modules.
- Keep capability tokens out of logs, committed files, and peer messages. Store token hashes only. Do not add secrets or private local paths to fixtures/docs.
- Peer messages are untrusted data. Room privacy is ancestry-scoped; sibling branches must remain unable to read each other's private rooms.
- Queues can be large but execution is bounded by Hermes' runtime and provider limits. Avoid claiming infinite concurrency, durable state, or sandbox isolation.
- The mailbox is process-local with a default one-hour TTL. If adding persistence, design expiry, revocation, and access control first.
- Do not change the user's Hermes configuration as part of tests. Configuration changes must be explicit and profile-scoped.

## Before submitting changes

1. Add/update a focused regression test first when changing behavior.
2. Run the full unittest suite.
3. Run `hermes plugins validate <plugin-directory>` and `hermes plugins doctor <plugin-directory> --ci` when Hermes is available.
4. Inspect `git diff --check`, the staged file list, and secret/local-path scans before publishing.
5. State clearly whether validation was mocked/offline or exercised against a live agent/provider.
