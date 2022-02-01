CREATE TABLE studies(
       id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
       created TIMESTAMPTZ NOT NULL DEFAULT current_timestamp(),
       seed INT NOT NULL DEFAULT FLOOR(RANDOM() * 1000000)::INT,
       strategy VARCHAR NOT NULL,
       config JSONB NOT NULL
);

CREATE TABLE treatments(
       created TIMESTAMPTZ NOT NULL DEFAULT current_timestamp(),
       batch INT NOT NULL,
       study_id VARCHAR NOT NULL,
       user_id VARCHAR NOT NULL,
       treatment VARCHAR NOT NULL,
       covariates JSONB,
       PRIMARY KEY(study_id, user_id)
);
