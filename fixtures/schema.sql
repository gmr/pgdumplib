-- Fixture Schema For Testing

CREATE EXTENSION citext;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA test;
GRANT USAGE ON SCHEMA test TO PUBLIC;

SET search_path = test, public, pg_catalog;

CREATE TABLE empty_table(
    id               UUID                     NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_modified_at TIMESTAMP WITH TIME ZONE,
    column_name      TEXT
);

CREATE DOMAIN test.email_address AS citext
        CHECK ( value ~ '^[a-zA-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$' );

-- Simplified locale check, doesn't fully conform to BCP-47
CREATE DOMAIN test.bcp47_locale AS TEXT
        CHECK ( value ~ '^[a-z]{2}-[A-Z]{2,3}$' );

CREATE TYPE user_state AS ENUM ('unverified', 'verified', 'suspended');

CREATE TABLE users (
    id               UUID                     NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_modified_at TIMESTAMP WITH TIME ZONE,
    state            user_state               NOT NULL DEFAULT 'unverified',
    email            email_address            NOT NULL,
    name             TEXT                     NOT NULL,
    surname          TEXT                     NOT NULL,
    display_name     TEXT,
    locale           bcp47_locale             NOT NULL DEFAULT 'en-US',
    password_salt    TEXT                     NOT NULL,
    password         TEXT                     NOT NULL,
    signup_ip        INET                     NOT NULL,
    icon             OID
);

CREATE UNIQUE INDEX users_unique_email ON users (email);

CREATE TYPE address_type AS ENUM ('billing', 'delivery');

CREATE TABLE addresses (
    id               UUID                     NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_modified_at TIMESTAMP WITH TIME ZONE,
    user_id          UUID                     NOT NULL REFERENCES users (id) ON DELETE CASCADE ON UPDATE CASCADE,
    type             address_type             NOT NULL,
    address1         TEXT                     NOT NULL,
    address2         TEXT,
    address3         TEXT,
    locality         TEXT                     NOT NULL,
    region           TEXT,
    postal_code      TEXT                     NOT NULL,
    country          TEXT                     NOT NULL
);

