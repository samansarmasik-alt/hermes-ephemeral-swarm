# AGENT.md — install & run

Quick setup for an AI agent or operator installing this Hermes plugin.

## Install

```bash
hermes plugins install samansarmasik-alt/hermes-ephemeral-swarm --no-enable
hermes plugins enable ephemeral-swarm
hermes config set delegation.max_spawn_depth 2
```

Start a fresh Hermes session after enabling the plugin. Depth `2` allows three tiers (coordinator → sub-orchestrator → leaf); it applies to the whole Hermes profile and increases possible model usage.

Optional: tune the per-team parallel window from `1` to `8` (default `8`):

```bash
hermes config set plugins.entries.ephemeral-swarm.settings.parallel_window 4
```

## Local development

```bash
python -m unittest discover -s tests -v
hermes plugins validate .
hermes plugins doctor . --ci
```

The tests use a fake lifecycle and make no model calls. They do not prove live provider dispatch. For full repository and security rules, read [`AGENTS.md`](AGENTS.md).
