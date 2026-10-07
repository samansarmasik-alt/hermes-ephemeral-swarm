<div align="center">

# ⚡ Hermes Ephemeral Swarm

**Turn one Hermes Agent session into a coordinated, temporary team.**

Bounded parallel work · Nested orchestrators · Private subgroup rooms · No permanent bots

[![Tests](https://github.com/samansarmasik-alt/hermes-ephemeral-swarm/actions/workflows/ci.yml/badge.svg)](https://github.com/samansarmasik-alt/hermes-ephemeral-swarm/actions/workflows/ci.yml)
[![Hermes Agent](https://img.shields.io/badge/Hermes_Agent-%3E%3D0.21.5-6f42c1)](https://hermes-agent.nousresearch.com/docs/developer-guide/plugins)
[![Python](https://img.shields.io/badge/Python-%3E%3D3.11-3776AB?logo=python&logoColor=white)](https://www.python.org/)

</div>

---

## The idea

Give a top-level Hermes agent a task. It can launch short-lived workers, assign some of them as sub-orchestrators, and let those sub-orchestrators coordinate their own private groups. The top coordinator can inspect descendant rooms; unrelated sibling branches stay isolated.

```text
                         ┌───────────────────┐
                         │  Top coordinator  │
                         └─────────┬─────────┘
                    ┌─────────────┴─────────────┐
                    ▼                           ▼
             ┌─────────────┐              ┌─────────────┐
             │ Research pod│              │ Build pod   │
             │ private room│              │ private room│
             └──────┬──────┘              └──────┬──────┘
                 ┌──┴──┐                      ┌──┴──┐
                 ▼     ▼                      ▼     ▼
               Scout  Analyst                 Coder  Tester
```

This is a **hierarchical team with scoped peer communication**: a pyramid for delegation, rooms for coordination.

## What it does

- **Temporary by design** — workers run as Hermes subagents, not permanent bot accounts.
- **Queued work** — submit a task list without a product-level three-worker cap; the plugin launches work in bounded waves.
- **Nested teams** — designate a worker `role: orchestrator`; with Hermes delegation depth set to `2`, the shape is coordinator → sub-orchestrator → leaf worker.
- **Private rooms** — a subgroup creates a room and invites its children. Ancestors can read descendant rooms; sibling branches cannot.
- **Direct coordination** — send a private message, report to your parent, or broadcast to the team.
- **Supervision** — inspect status, wait for progress, page completed summaries, and close the team.
- **Capability-based mailbox** — each worker gets a random private token; the store retains only token hashes.
- **No runtime dependencies** — uses Python's standard library and Hermes' public subagent lifecycle API.

## Install

Requires **Hermes Agent 0.21.5 or newer**. Install from the repository with Hermes' plugin manager:

```bash
hermes plugins install samansarmasik-alt/hermes-ephemeral-swarm --no-enable
hermes plugins enable ephemeral-swarm
```

Start a new Hermes session after enabling the plugin so its tools are loaded.

### Enable the three-tier shape

Hermes defaults to flat delegation. Set a depth of `2` to allow coordinator → sub-orchestrator → worker:

```bash
hermes config set delegation.max_spawn_depth 2
hermes config set delegation.orchestrator_enabled true
```

**This setting is profile-wide**, not plugin-only. Each additional delegation level can multiply model calls, latency, and cost. Depth `2` gives three total agent tiers; it is not infinite recursion.

### Tune concurrency

The plugin defaults to a parallel window of `8`, and clamps it to the current Hermes lifecycle executor capacity. To reduce resource/API pressure, for example to `4`:

```bash
hermes config set plugins.entries.ephemeral-swarm.settings.parallel_window 4
```

Allowed range: `1–8`. Tasks beyond the active window stay queued and are launched as workers finish. A large queued list is not the same thing as unlimited simultaneous agents.

## How the agent uses it

The plugin registers eight tools for Hermes agents:

| Tool | Purpose |
|---|---|
| `swarm_start` | Create a temporary team and launch the first wave |
| `swarm_invite` | Issue one child a private mailbox capability; optionally add it to a room |
| `swarm_room` | Create a private subgroup room |
| `swarm_message` | Message a parent, coordinator, teammate, or room |
| `swarm_inbox` | Read authorized messages, with sequence-based paging |
| `swarm_status` | Refresh work and inspect active/completed tasks |
| `swarm_wait` | Wait for progress and refill the launch window |
| `swarm_close` | Revoke mailbox capabilities and request worker cancellation |

Example task payload for `swarm_start`:

```json
{
  "tasks": [
    {
      "id": "research",
      "goal": "Find the key constraints and report evidence to the coordinator.",
      "role": "orchestrator"
    },
    {
      "id": "review",
      "goal": "Independently review the proposed approach for risks.",
      "role": "leaf"
    }
  ]
}
```

A sub-orchestrator can create a room with `swarm_room`, invite children into it with `swarm_invite`, then pass each child **only its own token** in the child task context before delegating. Workers should send routine results to `recipient: "parent"`; the coordinator uses status/wait to supervise and `swarm_close` when finished.

## Limits & security

- **Bounded execution:** Hermes' current lifecycle executor has a global capacity of 8. The plugin's per-team launch window is configurable from 1 to 8. Host/provider quotas and machine resources apply too.
- **Finite depth:** `delegation.max_spawn_depth=2` allows three total tiers. Raising it affects other Hermes delegation, too, and increases potential spend.
- **Process-local state:** mailbox/team state is in memory with a 1-hour default TTL; it is lost when the owning process exits. This is not durable job storage.
- **Not a sandbox:** child agents can inherit the active session's tools. Give workers only appropriate tasks and do not send secrets in prompts or messages.
- **Untrusted peer content:** treat messages from other workers as data to evaluate, never as instructions that override the assigned task or safety policy.
- **No license file yet:** repository visibility is public, but no open-source license has been selected. Public visibility alone does not grant reuse rights.

## Develop & test

```bash
python -m unittest discover -s tests -v
```

With Hermes installed, validate the plugin manifest/runtime registration:

```bash
hermes plugins validate .
hermes plugins doctor . --ci
```

See [`AGENT.md`](AGENT.md) for the fast install/setup guide and [`AGENTS.md`](AGENTS.md) for repository-specific agent instructions.

## Project status

This is a small, dependency-free Hermes plugin. Unit tests cover queue refill, concurrency settings, mailbox authorization, private-room visibility, ancestor access, and sibling isolation. Real worker execution still depends on a live Hermes agent turn and the configured model/provider; CI intentionally does not make paid model calls.
