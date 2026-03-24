"""Per-cell pgvector knowledge store.

Each cell gets its own Postgres schema. Supports:
- Vector embeddings via sentence-transformers (384-dim, HNSW indexed)
- Full-text search via tsvector/GIN indexes
- Hybrid search merging vector + FTS via Reciprocal Rank Fusion
- Custom tables created dynamically by nucleus-authored consumer code
"""

import json

import numpy as np
import psycopg
from pgvector.psycopg import register_vector

from src.config import POSTGRES_URL
from src.embeddings import embed_one, chunk_text, EMBEDDING_DIM


class KnowledgeStore:
    """Manages a cell's pgvector knowledge base in its own schema."""

    def __init__(self, cell_id: str):
        self.cell_id = cell_id
        self.schema = f"cell_{cell_id.replace('-', '_')}"

    def initialize(self):
        """Create the cell's schema with base tables, HNSW index, and FTS."""
        with psycopg.connect(POSTGRES_URL) as conn:
            register_vector(conn)
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")

            # Knowledge embeddings with FTS support
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.knowledge (
                    id SERIAL PRIMARY KEY,
                    content TEXT NOT NULL,
                    embedding vector({EMBEDDING_DIM}) NOT NULL,
                    category VARCHAR(50),
                    metadata JSONB,
                    content_tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', content)) STORED,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    expires_at TIMESTAMPTZ
                )
            """)

            # HNSW index for fast approximate nearest neighbor search
            conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self.schema}_knowledge_hnsw
                ON {self.schema}.knowledge
                USING hnsw (embedding vector_cosine_ops)
                WITH (m = 16, ef_construction = 64)
            """)

            # GIN index for full-text search
            conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self.schema}_knowledge_fts
                ON {self.schema}.knowledge
                USING GIN (content_tsv)
            """)

            # Key-value state
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.state (
                    key VARCHAR(255) PRIMARY KEY,
                    value JSONB NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.commit()

    def destroy(self):
        """Drop the cell's schema entirely."""
        with psycopg.connect(POSTGRES_URL) as conn:
            conn.execute(f"DROP SCHEMA IF EXISTS {self.schema} CASCADE")
            conn.commit()

    def store(self, content: str, category: str, metadata: dict | None = None):
        """Store a knowledge entry — chunks long content and embeds each chunk."""
        chunks = chunk_text(content)
        with psycopg.connect(POSTGRES_URL) as conn:
            register_vector(conn)
            for chunk in chunks:
                vec = np.array(embed_one(chunk), dtype=np.float32)
                conn.execute(
                    f"INSERT INTO {self.schema}.knowledge (content, embedding, category, metadata) VALUES (%s, %s, %s, %s)",
                    (chunk, vec, category, json.dumps(metadata or {})),
                )
            conn.commit()

    def semantic_search(self, query: str, limit: int = 5, category: str | None = None) -> list[dict]:
        """Pure vector similarity search."""
        vec = np.array(embed_one(query), dtype=np.float32)
        cat_filter = f"AND category = '{category}'" if category else ""

        with psycopg.connect(POSTGRES_URL) as conn:
            register_vector(conn)
            rows = conn.execute(
                f"""SELECT content, category, metadata,
                           1 - (embedding <=> %s) AS similarity
                    FROM {self.schema}.knowledge
                    WHERE 1=1 {cat_filter}
                    ORDER BY embedding <=> %s
                    LIMIT %s""",
                (vec, vec, limit),
            ).fetchall()
            return [
                {"content": r[0], "category": r[1], "metadata": r[2], "similarity": float(r[3])}
                for r in rows
            ]

    def fts_search(self, query: str, limit: int = 5) -> list[dict]:
        """Full-text search using tsvector."""
        with psycopg.connect(POSTGRES_URL) as conn:
            rows = conn.execute(
                f"""SELECT content, category, metadata,
                           ts_rank(content_tsv, plainto_tsquery('english', %s)) AS rank
                    FROM {self.schema}.knowledge
                    WHERE content_tsv @@ plainto_tsquery('english', %s)
                    ORDER BY rank DESC
                    LIMIT %s""",
                (query, query, limit),
            ).fetchall()
            return [
                {"content": r[0], "category": r[1], "metadata": r[2], "rank": float(r[3])}
                for r in rows
            ]

    def hybrid_search(self, query: str, limit: int = 5, keyword: str | None = None) -> list[dict]:
        """Hybrid search: merge vector similarity + FTS via Reciprocal Rank Fusion.

        If keyword is provided, FTS uses the keyword; otherwise uses the query.
        """
        n_candidates = max(limit * 4, 20)

        # Vector results
        vector_results = self.semantic_search(query, limit=n_candidates)

        # FTS results
        fts_query = keyword or query
        fts_results = self.fts_search(fts_query, limit=n_candidates)

        # Build rank maps (content -> rank position)
        vector_ranks = {r["content"]: i for i, r in enumerate(vector_results)}
        fts_ranks = {r["content"]: i for i, r in enumerate(fts_results)}

        # Merge all unique results
        all_contents = set(vector_ranks.keys()) | set(fts_ranks.keys())

        scored = []
        for content in all_contents:
            ranks = []
            if content in vector_ranks:
                ranks.append(vector_ranks[content])
            if content in fts_ranks:
                ranks.append(fts_ranks[content])
            rrf = _rrf_score(ranks)

            # Find the original result dict
            result = None
            for r in vector_results:
                if r["content"] == content:
                    result = r
                    break
            if result is None:
                for r in fts_results:
                    if r["content"] == content:
                        result = dict(r)
                        break

            if result:
                result["rrf_score"] = rrf
                result["search_mode"] = "hybrid"
                scored.append(result)

        scored.sort(key=lambda x: x["rrf_score"], reverse=True)
        return scored[:limit]

    def execute(self, sql: str, params: tuple = ()) -> list[tuple]:
        """Execute arbitrary SQL scoped to this cell's schema."""
        with psycopg.connect(POSTGRES_URL) as conn:
            register_vector(conn)
            conn.execute(f"SET search_path TO {self.schema}, public")
            result = conn.execute(sql, params)
            rows = result.fetchall() if result.description else []
            conn.commit()
            return rows

    def get_state(self, key: str) -> dict | None:
        with psycopg.connect(POSTGRES_URL) as conn:
            row = conn.execute(
                f"SELECT value FROM {self.schema}.state WHERE key = %s", (key,)
            ).fetchone()
            return row[0] if row else None

    def set_state(self, key: str, value: dict):
        with psycopg.connect(POSTGRES_URL) as conn:
            conn.execute(
                f"""INSERT INTO {self.schema}.state (key, value, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()""",
                (key, json.dumps(value)),
            )
            conn.commit()

    def stats(self) -> dict:
        with psycopg.connect(POSTGRES_URL) as conn:
            knowledge_count = conn.execute(f"SELECT COUNT(*) FROM {self.schema}.knowledge").fetchone()[0]
            state_count = conn.execute(f"SELECT COUNT(*) FROM {self.schema}.state").fetchone()[0]
            categories = conn.execute(
                f"SELECT category, COUNT(*) FROM {self.schema}.knowledge GROUP BY category"
            ).fetchall()

            # All tables in the schema with sizes
            all_tables = conn.execute(
                """SELECT t.table_name,
                          pg_total_relation_size(quote_ident(t.table_schema) || '.' || quote_ident(t.table_name)) as size_bytes
                   FROM information_schema.tables t
                   WHERE t.table_schema = %s
                   ORDER BY t.table_name""",
                (self.schema,),
            ).fetchall()

            # Get all indexes in the schema
            indexes = conn.execute(
                """SELECT tablename, indexname, indexdef
                   FROM pg_indexes
                   WHERE schemaname = %s""",
                (self.schema,),
            ).fetchall()

            # Build per-table index info
            table_indexes: dict[str, list[str]] = {}
            for tbl, idx_name, idx_def in indexes:
                if tbl not in table_indexes:
                    table_indexes[tbl] = []
                idx_def_lower = idx_def.lower()
                if "hnsw" in idx_def_lower or "ivfflat" in idx_def_lower:
                    table_indexes[tbl].append("vector")
                elif "gin" in idx_def_lower and "tsvector" in idx_def_lower:
                    table_indexes[tbl].append("fts")
                elif "gin" in idx_def_lower:
                    table_indexes[tbl].append("gin")
                elif "btree" in idx_def_lower or "pkey" in idx_name:
                    pass  # skip default btree/pkey
                else:
                    table_indexes[tbl].append("idx")

            tables = {}
            custom_tables = []
            for tname, size_bytes in all_tables:
                row_count = conn.execute(
                    f"SELECT COUNT(*) FROM {self.schema}.{tname}"
                ).fetchone()[0]

                # Check for vector and tsvector columns
                col_types = conn.execute(
                    "SELECT column_name, data_type, udt_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s",
                    (self.schema, tname),
                ).fetchall()
                has_vector = any(udt == "vector" for _, _, udt in col_types)
                has_tsvector = any(udt == "tsvector" for _, _, udt in col_types)

                # Determine index types present
                idx_types = set(table_indexes.get(tname, []))
                if has_vector and "vector" not in idx_types:
                    idx_types.add("vector*")  # has column but no index
                if has_tsvector and "fts" not in idx_types:
                    idx_types.add("fts*")  # has column but no index

                tables[tname] = {
                    "rows": row_count,
                    "size": _human_size(size_bytes),
                    "size_bytes": size_bytes,
                    "indexes": sorted(idx_types) if idx_types else [],
                }
                if tname not in ("knowledge", "state"):
                    custom_tables.append(tname)

            return {
                "knowledge_entries": knowledge_count,
                "state_entries": state_count,
                "categories": {r[0]: r[1] for r in categories},
                "tables": tables,
                "custom_tables": custom_tables,
            }


def _human_size(nbytes: int) -> str:
    """Convert bytes to human-readable size."""
    for unit in ("B", "KB", "MB", "GB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} TB"


def _rrf_score(ranks: list[int], k: int = 60) -> float:
    """Reciprocal Rank Fusion score."""
    return sum(1.0 / (k + r) for r in ranks)
