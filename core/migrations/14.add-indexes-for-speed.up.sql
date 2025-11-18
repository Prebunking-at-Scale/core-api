-- Normally I'd prefer to do this in a transaction, but that gets really slow for
-- index creation like this. These should all be idempotent, though, so I'm comfortable
-- this is safe without one.

CREATE INDEX IF NOT EXISTS transcript_sentences_video_id
ON transcript_sentences (video_id);

CREATE INDEX IF NOT EXISTS video_claims_video_id
ON video_claims (video_id);

CREATE INDEX IF NOT EXISTS videos_metadata_idx
ON videos USING GIN (metadata jsonb_ops);

CREATE INDEX IF NOT EXISTS transcript_sentences_metadata_idx
ON transcript_sentences USING GIN (metadata jsonb_ops);

CREATE INDEX IF NOT EXISTS video_claims_metadata_idx
ON video_claims USING GIN(metadata jsonb_ops);