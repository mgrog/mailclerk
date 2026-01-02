-- Create the trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    IF row(NEW.*) IS DISTINCT FROM row(OLD.*) THEN
        NEW.updated_at = CURRENT_TIMESTAMP;
    END IF;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create the triggers
CREATE TRIGGER update_user_account_access_updated_at
    BEFORE UPDATE ON user_account_access
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_user_updated_at
    BEFORE UPDATE ON "user"
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_auto_cleanup_setting_updated_at
    BEFORE UPDATE ON auto_cleanup_setting
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_default_email_rule_override_updated_at
    BEFORE UPDATE ON default_email_rule_override
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_custom_email_rules_updated_at
    BEFORE UPDATE ON custom_email_rule
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_user_token_usage_stat_updated_at
    BEFORE UPDATE ON user_token_usage_stat
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();