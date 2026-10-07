"""Bounded, profile-process-local mailbox for temporary Hermes agent swarms."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import hmac
import re
import secrets
import threading
import time
from typing import Any


MAX_MESSAGE_CHARS = 2_000
MAX_MESSAGES_PER_TEAM = 10_000
DEFAULT_TTL_SECONDS = 3_600
_ID_RE = re.compile(r"[A-Za-z0-9_-]{1,32}\Z")


class SwarmError(ValueError):
    """A team request was invalid, expired, closed, or unauthorized."""


@dataclass
class _Team:
    team_id: str
    created_at: float
    expires_at: float
    token_members: dict[str, str]
    parents: dict[str, str]
    channels: dict[str, set[str]] = field(default_factory=dict)
    messages: list[dict[str, Any]] = field(default_factory=list)
    next_sequence: int = 1
    closed: bool = False


class SwarmStore:
    """Thread-safe, in-memory store. Only token hashes are retained."""

    def __init__(self, *, ttl_seconds: float = DEFAULT_TTL_SECONDS, clock=time.time):
        if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, (int, float)) or ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be a positive number")
        self._ttl_seconds = float(ttl_seconds)
        self._clock = clock
        self._lock = threading.RLock()
        self._teams: dict[str, _Team] = {}

    def create_team(self, worker_ids: list[str] | tuple[str, ...]) -> tuple[str, str, dict[str, str]]:
        if not isinstance(worker_ids, (list, tuple)) or not worker_ids:
            raise SwarmError("Provide at least one worker id.")
        if any(not isinstance(member, str) or not _ID_RE.fullmatch(member) for member in worker_ids):
            raise SwarmError("Worker ids must be 1-32 letters, digits, '_' or '-'.")
        if len(set(worker_ids)) != len(worker_ids):
            raise SwarmError("Worker ids must be unique.")
        if any(member.lower() in {"coordinator", "all", "parent"} for member in worker_ids):
            raise SwarmError("'coordinator', 'all', and 'parent' are reserved ids.")

        with self._lock:
            now = self._clock()
            self._prune_locked(now)
            team_id = secrets.token_hex(8)
            owner_token = secrets.token_urlsafe(32)
            worker_tokens = {member: secrets.token_urlsafe(32) for member in worker_ids}
            token_members = {_token_hash(owner_token): "coordinator"}
            token_members.update({_token_hash(token): member for member, token in worker_tokens.items()})
            parents = {member: "coordinator" for member in worker_ids}
            self._teams[team_id] = _Team(
                team_id=team_id,
                created_at=now,
                expires_at=now + self._ttl_seconds,
                token_members=token_members,
                parents=parents,
                channels={"general": {"coordinator", *worker_ids}},
            )
            return team_id, owner_token, worker_tokens

    def create_channel(self, team_id: str, token: str, channel_id: str) -> str:
        """Create a private room whose creator is its first member."""
        if not isinstance(channel_id, str) or not _ID_RE.fullmatch(channel_id) or channel_id == "general":
            raise SwarmError("Room ids must be 1-32 letters, digits, '_' or '-' and cannot be 'general'.")
        with self._lock:
            team, creator = self._authorized_team_locked(team_id, token)
            if channel_id in team.channels:
                raise SwarmError("A room with that id already exists.")
            members = {creator}
            ancestor = team.parents.get(creator)
            while ancestor is not None and ancestor not in members:
                members.add(ancestor)
                ancestor = team.parents.get(ancestor)
            team.channels[channel_id] = members
            return channel_id

    def add_member(
        self,
        team_id: str,
        inviter_token: str,
        member_id: str | None = None,
        *,
        channel: str | None = None,
    ) -> tuple[str, str]:
        """Issue a child capability scoped to the inviter's team and parent relation."""
        with self._lock:
            team, inviter = self._authorized_team_locked(team_id, inviter_token)
            if member_id is None:
                member_id = "m_" + secrets.token_hex(6)
            if not isinstance(member_id, str) or not _ID_RE.fullmatch(member_id):
                raise SwarmError("Member id must be 1-32 letters, digits, '_' or '-'.")
            if member_id.lower() in {"coordinator", "all", "parent"} or member_id in _worker_ids(team):
                raise SwarmError("Member id is reserved or already in use in this team.")
            if channel is not None:
                room = team.channels.get(channel) if isinstance(channel, str) else None
                if room is None or inviter not in room:
                    raise SwarmError("Inviter is not a member of that room, or the room does not exist.")
            token = secrets.token_urlsafe(32)
            team.token_members[_token_hash(token)] = member_id
            team.parents[member_id] = inviter
            team.channels["general"].add(member_id)
            if channel is not None:
                team.channels[channel].add(member_id)
            return member_id, token

    def post(
        self,
        team_id: str,
        token: str,
        body: str,
        *,
        recipient: str | None = None,
        channel: str = "general",
    ) -> int:
        if not isinstance(body, str) or not body.strip():
            raise SwarmError("Message body must be non-empty text.")
        if len(body) > MAX_MESSAGE_CHARS:
            raise SwarmError(f"Message exceeds the {MAX_MESSAGE_CHARS}-character limit.")
        with self._lock:
            team, sender = self._authorized_team_locked(team_id, token)
            room = team.channels.get(channel) if isinstance(channel, str) else None
            if room is None or sender not in room:
                raise SwarmError("Sender is not a member of that room, or the room does not exist.")
            if recipient is not None and not isinstance(recipient, str):
                raise SwarmError("Recipient must be a worker id, 'parent', 'coordinator', or 'all'.")
            if recipient == "parent":
                if sender == "coordinator":
                    raise SwarmError("The coordinator has no parent; use a worker id or 'all'.")
                recipient = team.parents[sender]
            if recipient not in (None, "all", "coordinator") and recipient not in _worker_ids(team):
                raise SwarmError("Recipient is not a member of this team.")
            if recipient not in (None, "all", "coordinator") and recipient not in room:
                raise SwarmError("Recipient is not a member of that room.")
            seq = team.next_sequence
            team.next_sequence += 1
            team.messages.append({
                "sequence": seq,
                "sender": sender,
                "recipient": recipient or "all",
                "channel": channel,
                "body": body,
                "created_at": self._clock(),
            })
            if len(team.messages) > MAX_MESSAGES_PER_TEAM:
                del team.messages[:len(team.messages) - MAX_MESSAGES_PER_TEAM]
            return seq

    def read(
        self,
        team_id: str,
        token: str,
        *,
        after_sequence: int = 0,
        limit: int = 50,
        channel: str | None = None,
    ) -> list[dict[str, Any]]:
        if type(after_sequence) is not int or after_sequence < 0:
            raise SwarmError("after_sequence must be a non-negative integer.")
        if type(limit) is not int or not 1 <= limit <= MAX_MESSAGES_PER_TEAM:
            raise SwarmError(f"limit must be between 1 and {MAX_MESSAGES_PER_TEAM}.")
        with self._lock:
            team, reader = self._authorized_team_locked(team_id, token)
            visible = [
                dict(message)
                for message in team.messages
                if message["sequence"] > after_sequence
                and (channel is None or message["channel"] == channel)
                and (reader == "coordinator" or reader in team.channels.get(message["channel"], set()))
                and (
                    reader == "coordinator"
                    or message["channel"] != "general"
                    or message["recipient"] == "all"
                    or message["recipient"] == reader
                )
            ]
            return visible[:limit]

    def is_owner(self, team_id: str, token: str) -> bool:
        with self._lock:
            _team, member = self._authorized_team_locked(team_id, token)
            return member == "coordinator"

    def close(self, team_id: str, owner_token: str) -> None:
        with self._lock:
            team, member = self._authorized_team_locked(team_id, owner_token)
            if member != "coordinator":
                raise SwarmError("Only the coordinator can close a team.")
            team.closed = True
            team.messages.clear()
            team.token_members.clear()
            team.parents.clear()
            team.channels.clear()

    def _authorized_team_locked(self, team_id: str, token: str) -> tuple[_Team, str]:
        if not isinstance(team_id, str) or not isinstance(token, str) or not token or len(token) > 256:
            raise SwarmError("Invalid team capability.")
        now = self._clock()
        self._prune_locked(now)
        team = self._teams.get(team_id)
        if team is None:
            raise SwarmError("Unknown or expired team.")
        if team.closed:
            raise SwarmError("Team is closed.")
        candidate = _token_hash(token)
        member = next(
            (member_id for digest, member_id in team.token_members.items() if hmac.compare_digest(candidate, digest)),
            None,
        )
        if member is None:
            raise SwarmError("Invalid team capability.")
        return team, member

    def _prune_locked(self, now: float) -> None:
        expired = [team_id for team_id, team in self._teams.items() if team.expires_at <= now]
        for team_id in expired:
            del self._teams[team_id]


def _worker_ids(team: _Team) -> set[str]:
    return {member for member in team.token_members.values() if member != "coordinator"}


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()
