-- ============================================================
-- SCHÉMA PostgreSQL — Crimes et Délits 2012-2021 (PN + GN)
-- ============================================================

-- Nettoyage
DROP TABLE IF EXISTS statistique  CASCADE;
DROP TABLE IF EXISTS service      CASCADE;
DROP TABLE IF EXISTS type_fait    CASCADE;
DROP TABLE IF EXISTS departement  CASCADE;
DROP TABLE IF EXISTS direction    CASCADE;
DROP TABLE IF EXISTS force        CASCADE;

-- ── Tables ───────────────────────────────────────────────

CREATE TABLE force (
    id_force  SERIAL      PRIMARY KEY,
    code      VARCHAR(10) NOT NULL UNIQUE,
    libelle   TEXT        NOT NULL
);

CREATE TABLE direction (
    id_direction SERIAL      PRIMARY KEY,
    code         VARCHAR(100) NOT NULL UNIQUE,
    libelle      TEXT
);

CREATE TABLE departement (
    code_dept VARCHAR(3) PRIMARY KEY,
    nom       TEXT
);

CREATE TABLE service (
    id_service   SERIAL      PRIMARY KEY,
    nom          TEXT        NOT NULL,
    code_dept    VARCHAR(3)  NOT NULL REFERENCES departement(code_dept),
    id_force     INTEGER     NOT NULL REFERENCES force(id_force),
    id_direction INTEGER              REFERENCES direction(id_direction)
);

CREATE TABLE type_fait (
    code_index INTEGER PRIMARY KEY,
    libelle    TEXT    NOT NULL
);

CREATE TABLE statistique (
    id_service  INTEGER NOT NULL REFERENCES service(id_service)   ON DELETE CASCADE,
    annee       SMALLINT NOT NULL CHECK (annee BETWEEN 2012 AND 2021),
    code_index  INTEGER NOT NULL REFERENCES type_fait(code_index) ON DELETE RESTRICT,
    nombre      INTEGER NOT NULL DEFAULT 0 CHECK (nombre >= 0),
    PRIMARY KEY (id_service, annee, code_index)
);

-- ── Index ─────────────────────────────────────────────────

CREATE INDEX idx_stat_annee       ON statistique(annee);
CREATE INDEX idx_stat_code_index  ON statistique(code_index);
CREATE INDEX idx_service_dept     ON service(code_dept);
CREATE INDEX idx_service_force    ON service(id_force);
CREATE INDEX idx_service_dir      ON service(id_direction);
