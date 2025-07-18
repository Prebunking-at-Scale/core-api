BEGIN;

-- We're also inserting the ID here because those are hardcoded in our narratives system for the MVP
INSERT INTO topics (id, topic, metadata) VALUES
    ('db3d996b-e691-4ce5-8c46-e35a82a9b28c', 'Climate', '{}'),
    ('bb52f622-b9ee-4d5b-9b70-5fd05046528b', 'Health', '{}'),
    ('3cd4a9cd-5906-4b0b-9167-57ff22c2345a', 'Migration', '{}'),
    ('0d7aaf8d-5b7e-4c0c-b03a-28457e27ac7d', 'Conflicts', '{}'),
    ('ff1bedb9-43e4-49e6-9c3f-27babfb7bfa1', 'European Union', '{}')
ON CONFLICT (id) DO NOTHING;

COMMIT;