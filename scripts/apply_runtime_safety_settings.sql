BEGIN;

-- Seed runtime controls for production-safe defaults.
INSERT INTO app_settings (key, value) VALUES
    ('inference_lookback_hours', '24'),
    ('cooling_power_ratio', '0.24'),
    ('control_mode', 'advisory')
ON CONFLICT (key) DO NOTHING;

-- Fill missing legacy values without overriding explicit operator settings.
UPDATE app_settings
SET value = '24'
WHERE key = 'inference_lookback_hours'
  AND COALESCE(value, '') = '';

UPDATE app_settings
SET value = '0.24'
WHERE key = 'cooling_power_ratio'
  AND COALESCE(value, '') = '';

UPDATE app_settings
SET value = 'advisory'
WHERE key = 'control_mode'
  AND COALESCE(value, '') NOT IN ('advisory', 'actuation');

COMMIT;
