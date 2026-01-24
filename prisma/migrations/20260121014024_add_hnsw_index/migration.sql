-- Add dimensions to embedding
ALTER TABLE email_embedding
ALTER COLUMN embedding TYPE halfvec(1024);

-- HNSW index for cosine similarity
CREATE INDEX email_embedding_embedding_hnsw
ON email_embedding
USING hnsw (embedding halfvec_cosine_ops)
WITH (
  m = 16,
  ef_construction = 200
);
