import importlib.util
import json
from pathlib import Path
import sys
import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
MODULE_NAME = "ephemeral_swarm_plugin_under_test"


def load_plugin():
    spec = importlib.util.spec_from_file_location(
        MODULE_NAME,
        PLUGIN_ROOT / "__init__.py",
        submodule_search_locations=[str(PLUGIN_ROOT)],
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load the plugin module.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


class FakeLifecycle:
    def __init__(self):
        self.launched = []
        self.states = {}

    def launch(self, request):
        member_id = request.correlation_id.split("-", 3)[-1]
        handle = SimpleNamespace(subagent_id=f"sub-{member_id}", role=request.role, member_id=member_id)
        self.launched.append((request, handle))
        self.states[handle.subagent_id] = "RUNNING"
        return handle

    def status(self, handle):
        return SimpleNamespace(state=SimpleNamespace(value=self.states[handle.subagent_id]))

    def result(self, handle):
        return SimpleNamespace(summary=f"finished {handle.member_id}", error_message=None)

    def cancel(self, handle, *, reason):
        return SimpleNamespace(accepted=True, state=SimpleNamespace(value="CANCEL_REQUESTED"))


class FakeSubagentLaunchRequest:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def call_tool(ctx, name, arguments):
    agent_module = ModuleType("agent")
    agent_module.__path__ = []
    lifecycle_module = ModuleType("agent.subagent_lifecycle")
    lifecycle_module.SubagentLaunchRequest = FakeSubagentLaunchRequest
    with patch.dict(sys.modules, {
        "agent": agent_module,
        "agent.subagent_lifecycle": lifecycle_module,
    }):
        return json.loads(ctx.tools[name]["handler"](arguments))


class FakePluginContext:
    def __init__(self, parallel_window=2):
        self.tools = {}
        self.unload_callbacks = []
        self.subagent_lifecycle = FakeLifecycle()
        self.settings = {"parallel_window": parallel_window}

    def get_config(self, key, default=None):
        return self.settings.get(key, default)

    def register_tool(self, *, name, toolset, schema, handler, **kwargs):
        self.tools[name] = {"toolset": toolset, "schema": schema, "handler": handler}

    def on_unload(self, callback):
        self.unload_callbacks.append(callback)


class PluginRegistrationTests(unittest.TestCase):
    def test_register_exposes_swarm_lifecycle_and_mailbox_tools(self):
        plugin = load_plugin()
        ctx = FakePluginContext()

        plugin.register(ctx)

        self.assertEqual(
            set(ctx.tools),
            {"swarm_start", "swarm_invite", "swarm_room", "swarm_message", "swarm_inbox", "swarm_status", "swarm_wait", "swarm_close"},
        )
        self.assertTrue(all(item["schema"]["parameters"]["type"] == "object" for item in ctx.tools.values()))

    def test_tasks_are_unbounded_by_a_three_worker_product_cap(self):
        plugin = load_plugin()
        tasks = [{"id": f"worker-{index}", "goal": "Do a small task", "role": "orchestrator"} for index in range(12)]

        parsed = plugin._parse_tasks(tasks)

        self.assertEqual(len(parsed), 12)
        self.assertEqual(parsed[0]["role"], "orchestrator")
        self.assertNotIn("maxItems", plugin._START_SCHEMA["parameters"]["properties"]["tasks"])

    def test_start_queues_tasks_and_refills_the_window_after_completion(self):
        plugin = load_plugin()
        ctx = FakePluginContext(parallel_window=2)
        plugin.register(ctx)
        tasks = [{"id": f"worker-{index}", "goal": f"Task {index}"} for index in range(12)]

        started = call_tool(ctx, "swarm_start", {"tasks": tasks})

        self.assertEqual(started["total_root_tasks"], 12)
        self.assertEqual(started["queued"], 10)
        self.assertEqual(len(started["launched"]), 2)
        first_handle = ctx.subagent_lifecycle.launched[0][1]
        ctx.subagent_lifecycle.states[first_handle.subagent_id] = "SUCCEEDED"

        status = call_tool(ctx, "swarm_status", {"team_id": started["team_id"]})

        self.assertEqual(status["completed"], 1)
        self.assertEqual(status["queued"], 9)
        self.assertEqual(status["newly_launched"][0]["id"], "worker-2")
        self.assertEqual(status["completed_page"][0]["summary"], "finished worker-0")

    def test_parallel_window_setting_is_clamped_to_runtime_capacity(self):
        plugin = load_plugin()
        ctx = FakePluginContext(parallel_window=99)
        plugin.register(ctx)
        tasks = [{"id": f"worker-{index}", "goal": f"Task {index}"} for index in range(9)]

        started = call_tool(ctx, "swarm_start", {"tasks": tasks})

        self.assertEqual(started["parallel_window"], 8)
        self.assertEqual(len(started["launched"]), 8)
        self.assertEqual(started["queued"], 1)

    def test_parallel_window_setting_can_reduce_concurrency(self):
        plugin = load_plugin()
        ctx = FakePluginContext(parallel_window=1)
        plugin.register(ctx)

        started = call_tool(ctx, "swarm_start", {"tasks": [
            {"id": "first", "goal": "Task one"},
            {"id": "second", "goal": "Task two"},
        ]})

        self.assertEqual(started["parallel_window"], 1)
        self.assertEqual(len(started["launched"]), 1)
        self.assertEqual(started["queued"], 1)

    def test_room_invites_scope_subteam_messages(self):
        plugin = load_plugin()
        ctx = FakePluginContext()
        plugin.register(ctx)
        started = call_tool(ctx, "swarm_start", {"tasks": [{"id": "lead", "goal": "Coordinate"}]})
        team_id = started["team_id"]

        room = call_tool(ctx, "swarm_room", {"team_id": team_id, "channel": "research"})
        invite = call_tool(ctx, "swarm_invite", {
            "team_id": team_id, "member_id": "researcher", "channel": room["channel"]
        })
        sent = call_tool(ctx, "swarm_message", {
            "team_id": team_id,
            "token": invite["token"],
            "channel": "research",
            "recipient": "parent",
            "body": "finding",
        })
        inbox = call_tool(ctx, "swarm_inbox", {"team_id": team_id, "channel": "research"})

        self.assertEqual(sent["status"], "sent")
        self.assertEqual(inbox["messages"][0]["sender"], "researcher")
        self.assertEqual(inbox["messages"][0]["body"], "finding")

    def test_tool_errors_are_returned_as_json(self):
        plugin = load_plugin()
        ctx = FakePluginContext()
        plugin.register(ctx)

        result = ctx.tools["swarm_start"]["handler"]({"tasks": []})
        decoded = json.loads(result)

        self.assertIn("error", decoded)


if __name__ == "__main__":
    unittest.main()
