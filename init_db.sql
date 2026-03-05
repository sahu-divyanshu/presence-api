-- Create Table
CREATE TABLE IF NOT EXISTS user_activity_logs (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    event VARCHAR(50) NOT NULL,
    logged_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create Indexes for performance
CREATE INDEX IF NOT EXISTS idx_user_id ON user_activity_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_logged_at ON user_activity_logs(logged_at);

-- Grant permissions (if you created a specific user, otherwise postgres superuser owns it)
GRANT ALL PRIVILEGES ON TABLE user_activity_logs TO postgres;
