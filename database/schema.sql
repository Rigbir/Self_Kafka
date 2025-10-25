-- SelfKafka Consumer Groups Database Schema

-- Create database (run this manually first)
-- CREATE DATABASE selfkafka;

-- Connect to selfkafka database
-- \c selfkafka;

-- Consumer Groups table
CREATE TABLE IF NOT EXISTS consumer_groups (
    group_id VARCHAR(255) PRIMARY KEY,
    topic_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Consumers table
CREATE TABLE IF NOT EXISTS consumers (
    consumer_id VARCHAR(255) PRIMARY KEY,
    group_id VARCHAR(255) NOT NULL,
    last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES consumer_groups(group_id) ON DELETE CASCADE
);

-- Partition assignments table
CREATE TABLE IF NOT EXISTS partition_assignments (
    id SERIAL PRIMARY KEY,
    group_id VARCHAR(255) NOT NULL,
    consumer_id VARCHAR(255) NOT NULL,
    partition_id INTEGER NOT NULL,
    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES consumer_groups(group_id) ON DELETE CASCADE,
    FOREIGN KEY (consumer_id) REFERENCES consumers(consumer_id) ON DELETE CASCADE,
    UNIQUE(group_id, partition_id)  -- Each partition can only be assigned to one consumer in a group
);

-- Indexes for better performance
CREATE INDEX IF NOT EXISTS idx_consumers_group_id ON consumers(group_id);
CREATE INDEX IF NOT EXISTS idx_consumers_heartbeat ON consumers(last_heartbeat);
CREATE INDEX IF NOT EXISTS idx_assignments_group_id ON partition_assignments(group_id);
CREATE INDEX IF NOT EXISTS idx_assignments_consumer_id ON partition_assignments(consumer_id);

-- Update trigger for consumer_groups
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_consumer_groups_updated_at 
    BEFORE UPDATE ON consumer_groups 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
