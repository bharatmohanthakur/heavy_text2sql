"""Agentic layer — turns the existing pipeline into an LLM-callable tool surface.

Each tool here is a thin wrapper over a component built earlier (Components 2-10).
The wrappers preserve the original component's intent and behavior exactly;
they only translate the call shape to/from JSON-schema-tooling that an LLM
agent loop can drive.

Public surface:
  conversation_store : Postgres-backed multi-turn message log
  tools              : registry of LLM-callable tool definitions
  loop               : agent runner that drives tool calls until final_answer
"""

from text2sql.agent.conversation_store import (
    Conversation,
    ConversationMessage,
    ConversationStore,
)
from text2sql.agent.loop import AgentResult, AgentRunner, AgentStep
from text2sql.agent.tools import (
    CLASSIFY_DOMAINS,
    FINAL_ANSWER,
    FIND_JOIN_PATH,
    FIND_SIMILAR_QUERIES,
    INSPECT_TABLE,
    RESOLVE_ENTITY,
    RUN_SQL,
    SEARCH_TABLES,
    ToolContext,
    ToolDefinition,
    ToolRegistry,
    ToolResult,
    default_registry,
)

__all__ = [
    "AgentResult",
    "AgentRunner",
    "AgentStep",
    "Conversation",
    "ConversationMessage",
    "ConversationStore",
    "CLASSIFY_DOMAINS",
    "FINAL_ANSWER",
    "FIND_JOIN_PATH",
    "FIND_SIMILAR_QUERIES",
    "INSPECT_TABLE",
    "RESOLVE_ENTITY",
    "RUN_SQL",
    "SEARCH_TABLES",
    "ToolContext",
    "ToolDefinition",
    "ToolRegistry",
    "ToolResult",
    "default_registry",
]
