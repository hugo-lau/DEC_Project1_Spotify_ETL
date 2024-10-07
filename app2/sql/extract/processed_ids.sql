CREATE TABLE IF NOT EXISTS processed_ids (
    id VARCHAR(255) PRIMARY KEY,  -- This will store the track IDs
    last_processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Timestamp for when the ID was processed
);