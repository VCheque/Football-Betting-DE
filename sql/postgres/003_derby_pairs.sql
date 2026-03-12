-- ═══════════════════════════════════════════════════════════════════════════
-- 003_derby_pairs.sql  —  Local / same-city rivalry reference table
-- ═══════════════════════════════════════════════════════════════════════════
-- Purpose: store known derby pairs across the six supported leagues so that
-- the Streamlit ML model can load them from the database rather than relying
-- on a hardcoded Python frozenset.
--
-- Each row stores the pair in canonical (team_a < team_b alphabetically) form
-- to enforce uniqueness regardless of home / away assignment.
--
-- To add a new rivalry:
--   INSERT INTO dim_derby_pairs (league_code, team_a, team_b, rivalry_name)
--   VALUES ('E0', 'Brentford', 'Fulham', 'West London derby');
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS dim_derby_pairs (
    derby_pair_id   SERIAL      PRIMARY KEY,
    league_code     TEXT        NOT NULL,               -- football-data.co.uk code
    team_a          TEXT        NOT NULL,               -- alphabetically first
    team_b          TEXT        NOT NULL,               -- alphabetically second
    rivalry_name    TEXT,                               -- human-readable label
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,  -- set FALSE to disable
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_derby_pair UNIQUE (league_code, team_a, team_b)
);

COMMENT ON TABLE  dim_derby_pairs              IS 'Known same-city / local rivalry pairs per league. Team names use football-data.co.uk format.';
COMMENT ON COLUMN dim_derby_pairs.team_a       IS 'Alphabetically lower team name (ensures bidirectional uniqueness).';
COMMENT ON COLUMN dim_derby_pairs.team_b       IS 'Alphabetically higher team name.';
COMMENT ON COLUMN dim_derby_pairs.is_active    IS 'FALSE = pair is excluded from derby_flag feature (e.g. team relegated out of dataset).';

-- ── Seed data ────────────────────────────────────────────────────────────────
INSERT INTO dim_derby_pairs (league_code, team_a, team_b, rivalry_name) VALUES

    -- England – Premier League (E0)
    ('E0', 'Arsenal',     'Chelsea',     'North vs West London'),
    ('E0', 'Arsenal',     'Tottenham',   'North London derby'),
    ('E0', 'Chelsea',     'Fulham',      'West London derby'),
    ('E0', 'Chelsea',     'Tottenham',   'London derby'),
    ('E0', 'Leeds',       'Man United',  'War of the Roses'),
    ('E0', 'Liverpool',   'Everton',     'Merseyside derby'),
    ('E0', 'Man City',    'Man United',  'Manchester derby'),
    ('E0', 'Newcastle',   'Sunderland',  'Tyne–Wear derby'),
    ('E0', 'Tottenham',   'West Ham',    'North-East London derby'),

    -- Spain – La Liga (SP1)
    ('SP1', 'Athletic Club', 'Sociedad',        'Basque derby'),
    ('SP1', 'Atletico Madrid','Real Madrid',     'Madrid derby'),
    ('SP1', 'Barcelona',   'Espanyol',           'Derbi Barceloní'),
    ('SP1', 'Betis',       'Sevilla',            'Derbi sevillano'),
    ('SP1', 'Getafe',      'Real Madrid',        'South Madrid derby'),
    ('SP1', 'Valencia',    'Villarreal',          'Derbi de la Comunitat'),

    -- Italy – Serie A (I1)
    ('I1', 'AC Milan',    'Inter',       'Derby della Madonnina'),
    ('I1', 'Lazio',       'Roma',        'Derby della Capitale'),
    ('I1', 'Juventus',    'Torino',      'Derby della Mole'),

    -- Germany – Bundesliga (D1)
    ('D1', 'Cologne',     'Leverkusen',  'Rheinisches Derby'),
    ('D1', 'Dortmund',    'Cologne',     'Westfälisches Derby'),
    ('D1', 'Dortmund',    'Schalke 04',  'Revierderby'),
    ('D1', 'Hamburg',     'St Pauli',    'Hamburger Stadtderby'),

    -- France – Ligue 1 (F1)
    ('F1', 'Lens',        'Lille',       'Derby du Nord'),
    ('F1', 'Lens',        'Paris SG',    'Northern rivalry'),
    ('F1', 'Lyon',        'Marseille',   'Clasico français'),
    ('F1', 'Marseille',   'Nice',        'Côte d''Azur rivalry'),

    -- Portugal – Primeira Liga (P1)
    ('P1', 'Benfica',     'Belenenses',  'Lisbon derby'),
    ('P1', 'Benfica',     'Porto',       'O Clássico'),
    ('P1', 'Benfica',     'Sporting CP', 'Derby de Lisboa'),
    ('P1', 'Boavista',    'Porto',       'Derby do Porto'),
    ('P1', 'Porto',       'Sporting CP', 'Derby dos Grandes')

ON CONFLICT (league_code, team_a, team_b) DO NOTHING;

-- ── Index ────────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_derby_pairs_league
    ON dim_derby_pairs (league_code)
    WHERE is_active = TRUE;
