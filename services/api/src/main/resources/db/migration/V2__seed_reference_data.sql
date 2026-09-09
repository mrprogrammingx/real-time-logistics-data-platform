-- ============================================================================
-- Seed data: enough reference rows to create orders/drivers via the API and to
-- give the Phase 4 geofencing job real polygons to test against.
--
-- All coordinates are in central Yerevan so the simulator has a compact map.
-- ============================================================================

INSERT INTO customers (name, email, phone) VALUES
    ('Ani Grigoryan',   'ani@example.com',   '+37491000001'),
    ('Davit Petrosyan', 'davit@example.com', '+37491000002'),
    ('Mariam Sargsyan', 'mariam@example.com','+37491000003');

INSERT INTO vehicles (type, plate) VALUES
    ('BICYCLE',    '00 AA 001'),
    ('SCOOTER',    '00 AA 002'),
    ('MOTORCYCLE', '00 AA 003'),
    ('CAR',        '00 AA 004');

INSERT INTO restaurants (name, location) VALUES
    ('Lavash Republic Square', ST_SetSRID(ST_MakePoint(44.5133, 40.1776), 4326)),
    ('Ponchik Opera',          ST_SetSRID(ST_MakePoint(44.5153, 40.1872), 4326)),
    ('Tashir Pizza Cascade',   ST_SetSRID(ST_MakePoint(44.5194, 40.1907), 4326));

INSERT INTO drivers (name, status, vehicle_id) VALUES
    ('Karen Hakobyan', 'OFFLINE', 1),
    ('Nare Avetisyan', 'OFFLINE', 2);

-- Geofences: ~120 m boxes around the three restaurants plus one delivery zone.
INSERT INTO geofences (name, type, geom) VALUES
    ('Republic Square pickup', 'RESTAURANT', ST_SetSRID(ST_MakeEnvelope(44.5126, 40.1770, 44.5140, 40.1782), 4326)),
    ('Opera pickup',           'RESTAURANT', ST_SetSRID(ST_MakeEnvelope(44.5146, 40.1866, 44.5160, 40.1878), 4326)),
    ('Cascade pickup',         'RESTAURANT', ST_SetSRID(ST_MakeEnvelope(44.5187, 40.1901, 44.5201, 40.1913), 4326)),
    ('Kentron delivery zone',  'ZONE',       ST_SetSRID(ST_MakeEnvelope(44.4980, 40.1700, 44.5300, 40.1950), 4326));
