-- eft_config: tunable business parameters, keyed per environment so the
-- same physical database (or a copy of this DDL per environment) can hold
-- different values without a code change. Read via FraudConfigRead, cached
-- with a 30s refresh so a change takes effect without a restart.

CREATE TABLE eft_config (
    config_key    VARCHAR(64) NOT NULL,
    environment   VARCHAR(16) NOT NULL,
    config_value  VARCHAR(64) NOT NULL,
    updated_at    DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_eft_config PRIMARY KEY (config_key, environment)
);
GO

INSERT INTO eft_config (config_key, environment, config_value)
VALUES ('auto_approve_timeout_minutes', 'local', '10'),
       ('auto_approve_timeout_minutes', 'ist', '10'),
       ('auto_approve_timeout_minutes', 'uat', '10'),
       ('auto_approve_timeout_minutes', 'prd', '10');
GO
