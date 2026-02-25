-- SeedKit Test Fixture: Edge Cases
-- Tests: composite keys, composite FKs, unusual types, self-references

CREATE TABLE tags (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    color VARCHAR(7) -- hex color
);

CREATE TABLE posts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title VARCHAR(200) NOT NULL,
    slug VARCHAR(200) UNIQUE,
    body TEXT,
    author_email VARCHAR(255),
    status VARCHAR(20) DEFAULT 'draft',
    view_count INTEGER DEFAULT 0 CHECK (view_count >= 0),
    metadata JSONB DEFAULT '{}',
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Composite PK junction table
CREATE TABLE post_tags (
    post_id UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (post_id, tag_id)
);

-- Table with composite unique constraint
CREATE TABLE user_settings (
    id SERIAL PRIMARY KEY,
    user_email VARCHAR(255) NOT NULL,
    setting_key VARCHAR(100) NOT NULL,
    setting_value TEXT,
    UNIQUE(user_email, setting_key)
);

-- Self-referencing comments (threaded)
CREATE TABLE comments (
    id SERIAL PRIMARY KEY,
    post_id UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    parent_id INTEGER REFERENCES comments(id) ON DELETE CASCADE,
    author_name VARCHAR(100),
    body TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
