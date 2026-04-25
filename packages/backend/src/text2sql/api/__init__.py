"""Component 11: FastAPI app — thin HTTP/WS surface over the pipeline.

No auth, no RBAC at this stage. Endpoints:

  GET  /health
  POST /query                 → full sync pipeline result
  WS   /query/stream          → streamed stages
  GET  /tables                → catalog list (filter by domain)
  GET  /tables/{fqn}          → one table's metadata
  GET  /domains               → domains + table counts
  GET  /gold                  → list gold pairs
  POST /gold                  → create pending pair
  POST /gold/{id}/approve     → approve
  POST /gold/{id}/reject      → reject
"""

from text2sql.api.app import build_app

__all__ = ["build_app"]
