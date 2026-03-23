-- Enable extensions
CREATE EXTENSION IF NOT EXISTS vector;

-- Cell registry (shared, managed by orchestrator)
CREATE TABLE public.cells (
    cell_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    directive TEXT NOT NULL,
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    consumers JSONB DEFAULT '[]',
    topics_subscribed JSONB DEFAULT '[]',
    topics_produced JSONB DEFAULT '[]',
    config JSONB DEFAULT '{}'
);
