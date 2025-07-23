BEGIN;

CREATE TABLE IF NOT EXISTS users (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    display_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    password_hash BYTEA,
    password_last_updated TIMESTAMP,
    is_super_admin BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS organisations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    display_name TEXT NOT NULL,
    short_name TEXT NOT NULL UNIQUE,
    country_codes TEXT[] NOT NULL,
    deactivated TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS organisation_users (
    organisation_id uuid REFERENCES organisations (id),
    user_id uuid REFERENCES users (id),
    invited TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    accepted TIMESTAMP,
    deactivated TIMESTAMP,
    is_admin BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (organisation_id, user_id)
);

COMMIT;