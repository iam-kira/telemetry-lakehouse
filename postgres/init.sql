-- Device registry: which freezer sits at which site, for which customer,
-- with what temperature threshold. Debezium captures every change to this table.
CREATE TABLE devices (
    device_id   text PRIMARY KEY,
    site_id     text NOT NULL,
    customer    text NOT NULL,
    threshold_c double precision NOT NULL DEFAULT -15.0,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

INSERT INTO devices (device_id, site_id, customer, threshold_c) VALUES
    ('freezer-00', 'site-A', 'Acme Foods',  -15.0),
    ('freezer-01', 'site-A', 'Acme Foods',  -15.0),
    ('freezer-02', 'site-B', 'Globex Cold', -12.0);

-- REPLICA IDENTITY FULL makes Postgres log the *before* image on UPDATE/DELETE,
-- so CDC events carry both old and new rows — needed to track history correctly.
ALTER TABLE devices REPLICA IDENTITY FULL;
