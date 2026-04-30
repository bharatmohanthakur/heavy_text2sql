"""FastAPI app factory.

Wires components 1-10 once at startup, then exposes them via REST + WebSocket.
The pipeline is shared across requests (it's stateless w.r.t. the request).
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import asdict, is_dataclass
from typing import Any, Callable
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from text2sql.agent import AgentRunner, ConversationStore
from text2sql.gold import GoldStore
from text2sql.pipeline import PipelineResult, Text2SqlPipeline
from text2sql.table_catalog import TableCatalog

log = logging.getLogger(__name__)


# ── Request / response schemas ───────────────────────────────────────────────


class QueryRequest(BaseModel):
    question: str
    execute: bool = True
    max_rows: int = 100


class GoldCreateRequest(BaseModel):
    nl_question: str
    sql: str
    tables_used: list[str] = Field(default_factory=list)
    author: str = ""
    note: str = ""


class GoldApproveRequest(BaseModel):
    reviewer: str = ""


class GoldRejectRequest(BaseModel):
    reviewer: str = ""
    reason: str = ""


class ChatRequest(BaseModel):
    message: str
    conversation_id: UUID | None = None


# ── Helpers ──────────────────────────────────────────────────────────────────


def _row_to_jsonable(value: Any) -> Any:
    """Postgres rows can carry datetimes / UUIDs / Decimals; coerce them."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [_row_to_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {k: _row_to_jsonable(v) for k, v in value.items()}
    return str(value)


def _serialize_pipeline(result: PipelineResult) -> dict[str, Any]:
    return {
        "nl_question": result.nl_question,
        "sql": result.sql,
        "rationale": result.rationale,
        "rows": [_row_to_jsonable(r) for r in result.rows],
        "row_count": result.row_count,
        "executed": result.executed,
        "validated": result.validated,
        "error": result.error,
        "description": result.description,
        "viz": asdict(result.viz) if result.viz is not None else None,
        "viz_vega_lite": result.viz_vega_lite,
        "domains": (
            {"domains": result.domains.domains, "reasoning": result.domains.reasoning}
            if result.domains else None
        ),
        "selected_tables": [h.fqn for h in result.retrieved_tables],
        "join_tree": (
            {"nodes": result.join_tree.nodes,
             "edge_count": len(result.join_tree.edges),
             "total_weight": result.join_tree.total_weight}
            if result.join_tree else None
        ),
        "resolved_entities": [
            asdict(p.chosen) for p in (result.resolved.phrases if result.resolved else [])
            if p.chosen
        ],
        "few_shot_count": len(result.few_shots),
        "repair_attempts": [asdict(a) for a in result.repair_attempts],
        "timings_ms": result.timings_ms,
    }


# ── App factory ──────────────────────────────────────────────────────────────


def build_app(
    *,
    pipeline: Text2SqlPipeline | None,
    catalog: TableCatalog | None,
    gold_store: GoldStore | None,
    agent_runner: AgentRunner | None = None,
    conv_store: ConversationStore | None = None,
    catalog_loader: Callable[[], TableCatalog | None] | None = None,
) -> FastAPI:
    app = FastAPI(title="Ed-Fi Text-to-SQL", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Mount the admin / settings router (read/write runtime config + DB pings).
    from text2sql.api.admin import router as _admin_router
    app.include_router(_admin_router)

    # Catalog resolver: when `catalog_loader` is supplied (cli serve, prod
    # path), each catalog-touching endpoint re-resolves per request so a
    # mid-flight `target_db.primary` switch via Settings → overlay is
    # picked up without a server restart. Tests that don't care about
    # provider switching just pass the static `catalog` and skip loader.
    def _catalog() -> TableCatalog | None:
        if catalog_loader is None:
            return catalog
        try:
            return catalog_loader()
        except Exception as e:
            log.warning("catalog_loader failed; falling back to startup catalog: %s", e)
            return catalog

    def _require_catalog() -> TableCatalog:
        cat = _catalog()
        if cat is None:
            raise HTTPException(
                503,
                detail="No catalog yet. Open Settings → Rebuild and run "
                       "ingest → classify → graph → catalog → index → gold-seed.",
            )
        return cat

    def _require_pipeline() -> Text2SqlPipeline:
        if pipeline is None:
            raise HTTPException(
                503,
                detail="Pipeline unavailable — artifacts haven't been built yet. "
                       "Open Settings → Rebuild to bootstrap, then restart the server.",
            )
        return pipeline

    # ── Health ──────────────────────────────────────────────────────────────
    @app.get("/health")
    def health() -> dict[str, Any]:
        cat = _catalog()
        return {
            "status": "ok",
            "tables": len(cat.entries) if cat else 0,
            "domains": len(cat.domain_counts()) if cat else 0,
            "gold_store": gold_store is not None,
            "provider_name": cat.provider_name if cat else "",
            "target_dialect": cat.target_dialect if cat else "",
            # Onboarding signals — frontend uses these to render a banner
            # when the operator hasn't bootstrapped artifacts yet.
            "catalog_loaded": cat is not None,
            "pipeline_ready": pipeline is not None,
        }

    # ── Query (sync) ────────────────────────────────────────────────────────
    @app.post("/query")
    async def query(req: QueryRequest) -> dict[str, Any]:
        pl = _require_pipeline()
        # The pipeline is sync; off-load to a worker so the event loop isn't
        # blocked while the LLM is thinking.
        result = await asyncio.to_thread(
            pl.answer, req.question, execute=req.execute, max_rows=req.max_rows,
        )
        return _serialize_pipeline(result)

    # ── Query (streaming) ───────────────────────────────────────────────────
    @app.websocket("/query/stream")
    async def query_stream(ws: WebSocket) -> None:
        await ws.accept()
        try:
            payload = await ws.receive_json()
            question = payload.get("question", "")
            if not question:
                await ws.send_json({"event": "error", "error": "empty question"})
                await ws.close()
                return
            if pipeline is None:
                await ws.send_json({
                    "event": "error",
                    "error": "Pipeline unavailable — run Settings → Rebuild first.",
                })
                await ws.close()
                return
            await ws.send_json({"event": "started", "question": question})
            # Pipeline is sync — run once and stream stage-result deltas.
            result = await asyncio.to_thread(pipeline.answer, question)
            for stage_event in _stream_stages(result):
                await ws.send_json(stage_event)
            await ws.send_json({"event": "done"})
        except WebSocketDisconnect:
            log.info("websocket disconnected")
        except Exception as e:
            log.exception("stream error")
            try:
                await ws.send_json({"event": "error", "error": str(e)})
            except Exception:
                pass
        finally:
            try:
                await ws.close()
            except Exception:
                pass

    # ── Catalog ─────────────────────────────────────────────────────────────
    @app.get("/tables")
    def list_tables(
        domain: str | None = Query(None),
        descriptors: bool = Query(True, description="Include descriptor tables"),
        limit: int = Query(200, ge=1, le=2000),
    ) -> dict[str, Any]:
        entries = _require_catalog().entries
        if domain:
            entries = [e for e in entries if e.has_domain(domain)]
        if not descriptors:
            entries = [e for e in entries if not e.is_descriptor]
        return {
            "total": len(entries),
            "tables": [
                {
                    "fqn": e.fqn,
                    "schema": e.schema,
                    "table": e.table,
                    "description": e.description,
                    "domains": list(e.domains),
                    "is_descriptor": e.is_descriptor,
                    "row_count": e.row_count,
                    "column_count": len(e.columns),
                }
                for e in entries[:limit]
            ],
        }

    @app.get("/tables/{fqn}")
    def get_table(fqn: str) -> dict[str, Any]:
        entry = _require_catalog().by_fqn().get(fqn)
        if not entry:
            raise HTTPException(404, detail=f"table not found: {fqn}")
        return {
            "fqn": entry.fqn,
            "schema": entry.schema,
            "table": entry.table,
            "description": entry.description,
            "description_source": entry.description_source,
            "domains": list(entry.domains),
            "is_descriptor": entry.is_descriptor,
            "is_association": entry.is_association,
            "is_extension": entry.is_extension,
            "primary_key": entry.primary_key,
            "parent_neighbors": entry.parent_neighbors,
            "child_neighbors": entry.child_neighbors,
            "aggregate_root": entry.aggregate_root,
            "row_count": entry.row_count,
            "columns": [asdict(c) for c in entry.columns],
            "sample_rows": [_row_to_jsonable(r) for r in entry.sample_rows[:5]],
        }

    @app.get("/domains")
    def list_domains() -> dict[str, Any]:
        counts = _require_catalog().domain_counts()
        return {
            "total": len(counts),
            "domains": [
                {"name": name, "table_count": n}
                for name, n in sorted(counts.items(), key=lambda kv: -kv[1])
            ],
        }

    # ── Gold ────────────────────────────────────────────────────────────────
    if gold_store is not None:
        @app.get("/gold")
        def list_gold(
            status: str | None = Query(None, pattern="^(pending|approved|rejected)$"),
            domain: str | None = Query(None),
            limit: int = Query(100, ge=1, le=500),
        ) -> dict[str, Any]:
            recs = gold_store.list(approval_status=status, domain=domain, limit=limit)
            return {"total": len(recs), "gold": [r.to_dict() for r in recs]}

        @app.post("/gold", status_code=201)
        def create_gold(req: GoldCreateRequest) -> dict[str, Any]:
            rec = gold_store.create(
                nl_question=req.nl_question,
                sql_text=req.sql,
                tables_used=req.tables_used,
                author=req.author,
                note=req.note,
                approval_status="pending",
            )
            return rec.to_dict()

        @app.post("/gold/{gold_id}/approve")
        def approve_gold(gold_id: UUID, req: GoldApproveRequest) -> dict[str, Any]:
            rec = gold_store.approve(gold_id, reviewer=req.reviewer)
            if rec is None:
                raise HTTPException(404, detail="gold record not found")
            return rec.to_dict()

        @app.post("/gold/{gold_id}/reject")
        def reject_gold(gold_id: UUID, req: GoldRejectRequest) -> dict[str, Any]:
            rec = gold_store.reject(gold_id, reviewer=req.reviewer, reason=req.reason)
            if rec is None:
                raise HTTPException(404, detail="gold record not found")
            return rec.to_dict()

    # ── Agentic chat (multi-turn, streaming) ────────────────────────────────
    if agent_runner is not None and conv_store is not None:
        @app.get("/conversations")
        def list_conversations(limit: int = Query(50, ge=1, le=200)) -> dict[str, Any]:
            convs = conv_store.list_conversations(limit=limit)
            return {
                "total": len(convs),
                "conversations": [
                    {
                        "id": str(c.id),
                        "title": c.title,
                        "dialect": c.dialect,
                        "created_at": c.created_at.isoformat(),
                        "last_active": c.last_active.isoformat(),
                    }
                    for c in convs
                ],
            }

        @app.get("/conversations/{conv_id}")
        def get_conversation(conv_id: UUID) -> dict[str, Any]:
            c = conv_store.get_conversation(conv_id)
            if c is None:
                raise HTTPException(404, detail="conversation not found")
            msgs = conv_store.history(conv_id)
            return {
                "id": str(c.id),
                "title": c.title,
                "dialect": c.dialect,
                "created_at": c.created_at.isoformat(),
                "last_active": c.last_active.isoformat(),
                "messages": [
                    {
                        "seq": m.seq,
                        "role": m.role,
                        "content": m.content,
                        "tool_calls": m.tool_calls,
                        "tool_call_id": m.tool_call_id,
                        "tool_name": m.tool_name,
                        "created_at": m.created_at.isoformat(),
                    }
                    for m in msgs
                ],
            }

        @app.delete("/conversations/{conv_id}", status_code=204)
        def delete_conversation(conv_id: UUID) -> None:
            if not conv_store.delete_conversation(conv_id):
                raise HTTPException(404, detail="conversation not found")

        @app.post("/chat")
        async def chat(req: ChatRequest) -> dict[str, Any]:
            result = await asyncio.to_thread(
                agent_runner.run, req.conversation_id, req.message,
            )
            return {
                "conversation_id": str(result.conversation_id),
                "summary": result.final_summary,
                "sql": result.final_sql,
                "row_count": result.final_row_count,
                "aborted": result.aborted,
                "abort_reason": result.abort_reason,
                "total_ms": result.total_ms,
                "rows": [_row_to_jsonable(r) for r in result.final_rows],
                "viz": result.viz,
                "vega_lite": result.vega_lite,
                "description": result.description,
                "steps": [
                    {
                        "kind": s.kind, "name": s.name,
                        "arguments": s.arguments,
                        "tool_call_id": s.tool_call_id,
                        "result": s.result, "error": s.error,
                        "elapsed_ms": s.elapsed_ms,
                    }
                    for s in result.steps
                ],
            }

        @app.post("/chat/stream")
        async def chat_stream(req: ChatRequest) -> StreamingResponse:
            """Stream agent events as Server-Sent Events.

            Each event is a single line `data: <json>\\n\\n` per the SSE spec.
            The agent loop is a synchronous generator, so we wrap it with
            asyncio.to_thread per yield to avoid blocking the event loop.
            """
            queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()

            def _producer() -> None:
                try:
                    for ev in agent_runner.run_stream(req.conversation_id, req.message):
                        asyncio.run_coroutine_threadsafe(queue.put(ev), loop).result()
                except Exception as e:
                    asyncio.run_coroutine_threadsafe(
                        queue.put({"kind": "error", "error": str(e)}),
                        loop,
                    ).result()
                finally:
                    asyncio.run_coroutine_threadsafe(queue.put(None), loop).result()

            loop = asyncio.get_running_loop()
            loop.run_in_executor(None, _producer)

            async def _gen():
                while True:
                    ev = await queue.get()
                    if ev is None:
                        break
                    yield f"data: {json.dumps(ev, default=str)}\n\n"

            return StreamingResponse(
                _gen(),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )

    return app


# ── WS streaming helpers ─────────────────────────────────────────────────────


def _stream_stages(result: PipelineResult) -> list[dict[str, Any]]:
    """Convert a finished PipelineResult into a sequence of stage events.

    These mirror the pipeline phases so the frontend can render stage-by-stage
    even though the underlying call ran synchronously.
    """
    events: list[dict[str, Any]] = []
    if result.domains is not None:
        events.append({
            "event": "domains",
            "domains": result.domains.domains,
            "reasoning": result.domains.reasoning,
        })
    if result.retrieved_tables:
        events.append({
            "event": "tables",
            "tables": [
                {"fqn": h.fqn, "score": h.score, "domains": h.domains}
                for h in result.retrieved_tables
            ],
        })
    if result.resolved is not None:
        events.append({
            "event": "entities",
            "resolved": [
                asdict(p.chosen) for p in result.resolved.phrases if p.chosen
            ],
        })
    if result.join_tree is not None:
        events.append({
            "event": "join_tree",
            "nodes": result.join_tree.nodes,
            "edge_count": len(result.join_tree.edges),
        })
    if result.repair_attempts:
        events.append({
            "event": "repair_attempts",
            "attempts": [asdict(a) for a in result.repair_attempts],
        })
    events.append({"event": "sql", "sql": result.sql, "validated": result.validated})
    if result.executed:
        events.append({
            "event": "rows",
            "rows": [_row_to_jsonable(r) for r in result.rows],
            "row_count": result.row_count,
        })
    if result.description:
        events.append({"event": "description", "summary": result.description})
    if result.viz is not None:
        events.append({
            "event": "viz",
            "spec": asdict(result.viz),
            "vega_lite": result.viz_vega_lite,
        })
    if result.error:
        events.append({"event": "error", "error": result.error})
    return events
