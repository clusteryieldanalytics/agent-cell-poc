"""Embedding and re-ranking module using sentence-transformers.

Bi-encoder: all-MiniLM-L6-v2 (384-dim, L2-normalized) for fast similarity search.
Cross-encoder: ms-marco-MiniLM-L-6-v2 for accurate re-ranking of search results.

Both models are loaded lazily and cached for the process lifetime.
"""

from functools import lru_cache

import numpy as np
from sentence_transformers import SentenceTransformer, CrossEncoder

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
RERANKER_MODEL_NAME = "cross-encoder/ms-marco-MiniLM-L-6-v2"
EMBEDDING_DIM = 384


@lru_cache(maxsize=1)
def _load_model() -> SentenceTransformer:
    return SentenceTransformer(MODEL_NAME)


@lru_cache(maxsize=1)
def _load_reranker() -> CrossEncoder:
    return CrossEncoder(RERANKER_MODEL_NAME)


def embed(texts: str | list[str]) -> np.ndarray:
    """Embed one or more texts. Returns array of shape (n, 384), L2-normalized."""
    model = _load_model()
    if isinstance(texts, str):
        texts = [texts]
    return model.encode(texts, normalize_embeddings=True, show_progress_bar=False)


def embed_one(text: str) -> list[float]:
    """Embed a single text, return as list of floats."""
    return embed(text)[0].tolist()


def rerank(query: str, documents: list[str], top_k: int | None = None) -> list[tuple[int, float]]:
    """Re-rank documents against a query using a cross-encoder.

    Returns list of (original_index, score) sorted by score descending.
    Much more accurate than bi-encoder similarity for determining relevance,
    but too expensive to run against the full corpus — use after initial retrieval.
    """
    if not documents:
        return []
    model = _load_reranker()
    pairs = [(query, doc) for doc in documents]
    scores = model.predict(pairs)
    indexed = [(i, float(s)) for i, s in enumerate(scores)]
    indexed.sort(key=lambda x: x[1], reverse=True)
    if top_k:
        indexed = indexed[:top_k]
    return indexed


def chunk_text(text: str, max_tokens: int = 250, overlap_tokens: int = 50) -> list[str]:
    """Split text into chunks with overlap for embedding.

    Uses a hierarchical split strategy:
    1. Section headers (##)
    2. Double newlines (paragraphs)
    3. Single newlines
    4. Sentence boundaries
    """
    approx_tokens = len(text) // 4
    if approx_tokens <= max_tokens:
        return [text]

    # Try splitting by section headers first
    import re
    sections = re.split(r'\n(?=##)', text)
    if len(sections) > 1:
        return _rechunk_with_overlap(sections, max_tokens, overlap_tokens)

    # Try paragraphs
    paragraphs = text.split('\n\n')
    if len(paragraphs) > 1:
        return _rechunk_with_overlap(paragraphs, max_tokens, overlap_tokens)

    # Try lines
    lines = text.split('\n')
    if len(lines) > 1:
        return _rechunk_with_overlap(lines, max_tokens, overlap_tokens)

    # Fall back to sentence splitting
    sentences = re.split(r'(?<=[.!?])\s+', text)
    return _rechunk_with_overlap(sentences, max_tokens, overlap_tokens)


def _rechunk_with_overlap(pieces: list[str], max_tokens: int, overlap_tokens: int) -> list[str]:
    """Merge small pieces into chunks of ~max_tokens, with overlap between chunks."""
    chunks = []
    current = []
    current_len = 0

    for piece in pieces:
        piece_len = len(piece) // 4
        if current_len + piece_len > max_tokens and current:
            chunk_text = '\n'.join(current)
            chunks.append(chunk_text)
            # Overlap: keep last ~overlap_tokens worth of words
            overlap_words = chunk_text.split()[-overlap_tokens:]
            current = [' '.join(overlap_words)]
            current_len = overlap_tokens
        current.append(piece)
        current_len += piece_len

    if current:
        chunks.append('\n'.join(current))

    return chunks
