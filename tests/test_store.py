import sys
import unittest
from pathlib import Path

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN_ROOT))

from swarm_store import SwarmError, SwarmStore


class SwarmStoreTests(unittest.TestCase):
    def setUp(self):
        self.store = SwarmStore(ttl_seconds=60)
        self.team_id, self.owner_token, self.tokens = self.store.create_team(["alpha", "beta"])

    def test_member_message_is_visible_to_recipient_and_owner(self):
        seq = self.store.post(self.team_id, self.tokens["alpha"], "partial finding", recipient="beta")

        self.assertEqual(seq, 1)
        self.assertEqual(self.store.read(self.team_id, self.tokens["beta"])[0]["body"], "partial finding")
        self.assertEqual(self.store.read(self.team_id, self.owner_token)[0]["sender"], "alpha")
        self.assertEqual(self.store.read(self.team_id, self.tokens["alpha"]), [])

    def test_invalid_member_token_cannot_read_team_messages(self):
        with self.assertRaises(SwarmError):
            self.store.read(self.team_id, "not-a-capability")

    def test_closed_team_rejects_new_messages(self):
        self.store.close(self.team_id, self.owner_token)

        with self.assertRaises(SwarmError):
            self.store.post(self.team_id, self.tokens["alpha"], "too late")

    def test_team_size_has_no_product_cap_of_three(self):
        ids = [f"worker-{index}" for index in range(12)]
        _team_id, _owner_token, tokens = self.store.create_team(ids)

        self.assertEqual(set(tokens), set(ids))

    def test_message_body_has_a_hard_size_limit(self):
        with self.assertRaises(SwarmError):
            self.store.post(self.team_id, self.tokens["alpha"], "x" * 5000)

    def test_only_owner_capability_is_allowed_for_management(self):
        self.assertTrue(self.store.is_owner(self.team_id, self.owner_token))
        self.assertFalse(self.store.is_owner(self.team_id, self.tokens["beta"]))

    def test_multiple_teams_can_be_active_in_one_process(self):
        other_id, _owner_token, _tokens = self.store.create_team(["gamma"])

        self.assertNotEqual(self.team_id, other_id)

    def test_invited_child_can_send_to_its_parent(self):
        child_id, child_token = self.store.add_member(self.team_id, self.tokens["alpha"])

        self.store.post(self.team_id, child_token, "child update", recipient="parent")

        message = self.store.read(self.team_id, self.tokens["alpha"])[0]
        self.assertEqual(message["sender"], child_id)
        self.assertEqual(message["recipient"], "alpha")

    def test_private_room_messages_are_visible_only_to_room_members(self):
        self.store.create_channel(self.team_id, self.tokens["alpha"], "research")
        child_id, child_token = self.store.add_member(
            self.team_id, self.tokens["alpha"], "research-child", channel="research"
        )

        self.store.post(self.team_id, child_token, "private finding", channel="research")

        child_message = self.store.read(self.team_id, self.tokens["alpha"], channel="research")[0]
        self.assertEqual(child_message["sender"], child_id)
        self.assertEqual(child_message["body"], "private finding")
        self.assertEqual(self.store.read(self.team_id, self.tokens["beta"], channel="research"), [])
        self.assertEqual(self.store.read(self.team_id, self.owner_token)[0]["body"], "private finding")

    def test_non_member_cannot_post_to_a_private_room(self):
        self.store.create_channel(self.team_id, self.tokens["alpha"], "research")

        with self.assertRaises(SwarmError):
            self.store.post(self.team_id, self.tokens["beta"], "intrusion", channel="research")

    def test_private_room_recipient_must_be_a_room_member(self):
        self.store.create_channel(self.team_id, self.tokens["alpha"], "research")
        child_id, _child_token = self.store.add_member(self.team_id, self.tokens["alpha"])

        with self.assertRaises(SwarmError):
            self.store.post(self.team_id, self.tokens["alpha"], "not in this room", recipient=child_id, channel="research")

    def test_ancestors_can_read_descendant_rooms_but_sibling_branches_cannot(self):
        alpha_room = self.store.create_channel(self.team_id, self.tokens["alpha"], "alpha-team")
        child_id, child_token = self.store.add_member(
            self.team_id, self.tokens["alpha"], "child", channel=alpha_room
        )
        child_room = self.store.create_channel(self.team_id, child_token, "child-team")
        _grandchild_id, grandchild_token = self.store.add_member(
            self.team_id, child_token, "grandchild", channel=child_room
        )

        self.store.post(self.team_id, grandchild_token, "nested report", recipient="parent", channel=child_room)

        self.assertEqual(self.store.read(self.team_id, self.tokens["alpha"], channel=child_room)[0]["body"], "nested report")
        self.assertEqual(self.store.read(self.team_id, self.tokens["beta"], channel=child_room), [])
        self.assertEqual(child_id, "child")

    def test_malformed_recipient_is_rejected_as_a_swarm_error(self):
        with self.assertRaises(SwarmError):
            self.store.post(self.team_id, self.tokens["alpha"], "hello", recipient=[])


if __name__ == "__main__":
    unittest.main()
