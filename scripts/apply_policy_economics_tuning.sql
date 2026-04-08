BEGIN;

-- Seed keys in case they do not exist yet.
INSERT INTO app_settings (key, value) VALUES
    ('curtailment_penalty_multiplier', '2.0'),
    ('policy_reward_per_th_hour_usd', '0.0022916667'),
    ('policy_failure_cost_usd', '300')
ON CONFLICT (key) DO NOTHING;

-- Update only legacy defaults so custom operator values are preserved.
UPDATE app_settings
SET value = '2.0'
WHERE key = 'curtailment_penalty_multiplier'
  AND COALESCE(value, '') IN ('', '1.5');

UPDATE app_settings
SET value = '0.0022916667'
WHERE key = 'policy_reward_per_th_hour_usd'
  AND COALESCE(value, '') IN ('', '0.0025');

UPDATE app_settings
SET value = '300'
WHERE key = 'policy_failure_cost_usd'
  AND COALESCE(value, '') IN ('', '120');

COMMIT;
