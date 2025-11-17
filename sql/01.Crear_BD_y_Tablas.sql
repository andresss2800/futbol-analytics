
--------------------------------------------------------------------------------------

--BASE DE DATOS

-------------------------------------------------------------------------------------

-- Crear base de datos

CREATE DATABASE IF NOT EXISTS futbol_analytics
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE futbol_analytics;


-----------------------------------------------------------
-- DIMENSIONES
-----------------------------------------------------------

-- Jugadores
CREATE TABLE IF NOT EXISTS dim_player (
    player_id INT AUTO_INCREMENT PRIMARY KEY,
    player_name VARCHAR(100) NOT NULL,
    age TINYINT,
    height_m DECIMAL(4,2),
    foot VARCHAR(10),                      -- Perfil (izquierdo, derecho)
    position VARCHAR(50),                  -- Posición
    field_role VARCHAR(50),                -- Posicion en campo
    injuries_last_year TINYINT,            -- Lesiones Ultimo Año (número o nivel)
    attack_rating TINYINT,                 -- Atacante
    creativity_rating TINYINT,             -- Creatividad
    defense_rating TINYINT,                -- Defensa
    tactical_rating TINYINT,               -- Táctica
    technical_rating TINYINT,              -- Técnica
    matches INT,                              -- Partidos Jugados Selección
    national_team_goals INT,               -- Goles con la selección
    season_matches INT,                    -- Partidos Temporada
    season_starts INT,                     -- Partidos Titular
    season_minutes INT,                    -- Total Minutos
    season_rating DECIMAL(3,1),            -- Valoracion Temporada
    market_value_millions DECIMAL(6,2)     -- Valor de mercado (en millones)
) ENGINE=InnoDB;


-- Partidos (Fechas)
CREATE TABLE IF NOT EXISTS dim_match (
    match_id INT PRIMARY KEY,              -- Fecha de la eliminatoria o partido amistoso
    match_date DATE NOT NULL,              -- 'Fecha Compromiso'
    opponent VARCHAR(100),                 -- 'Rival'
    home_away VARCHAR(20),                 -- 'Condicion' (Local/Visitante)
    result VARCHAR(10)                     -- 'Resultado' (ej: '1-0')
) ENGINE=InnoDB;

-- Clubes
CREATE TABLE IF NOT EXISTS dim_club (
    club_id INT AUTO_INCREMENT PRIMARY KEY,
    club_name VARCHAR(120) NOT NULL,       -- 'Equipo'
    country_code CHAR(3)                   -- 'Pais' (COL, MEX, etc.)
) ENGINE=InnoDB;



-----------------------------------------------------------
-- HECHOS
-----------------------------------------------------------

-- Convocatorias
CREATE TABLE IF NOT EXISTS fact_callup (
    callup_id INT AUTO_INCREMENT PRIMARY KEY,
    callup_date DATE NOT NULL,             -- 'Fecha Convocatoria'
    qualifier_round VARCHAR(10),           -- 'Número de Fecha Clasificación' 
    match_id INT NULL,                     -- 'Id de partido'
    player_id INT NOT NULL,
    club_id INT,
    FOREIGN KEY (player_id) REFERENCES dim_player(player_id),
    FOREIGN KEY (club_id) REFERENCES dim_club(club_id),
    FOREIGN KEY (match_id) REFERENCES dim_match(match_id)
) ENGINE=InnoDB;


-- Estadísticas por partido y jugador

CREATE TABLE IF NOT EXISTS fact_match_stats (
    match_id INT NOT NULL,                 -- Número de 'Fecha' del partido  (clave a dim_match)
    player_id INT NOT NULL,                -- 'Jugador' (clave a dim_player)
    rating DECIMAL(3,1),                   -- Calificacion del partido
    minutes_played SMALLINT,               -- Minutos Jugados
    goals TINYINT,                         -- Goles marcados
    total_shots TINYINT,                   -- Tiros Totales
    shots_on_target TINYINT,               -- Remates a Puerta
    shots_off_target TINYINT,              -- Remates Fuera
    shots_hit_post TINYINT,                -- Remates al palo
    big_chances_missed TINYINT,            -- Ocasiones Claras Falladas
    offsides TINYINT,                      -- Fueras de Juego
    assists TINYINT,                       -- Asistencias
    big_chances_created TINYINT,           -- Ocasiones Claras Creadas
    key_passes TINYINT,                    -- Pases Clave
    accurate_crosses TINYINT,              -- Centros efectivos
    total_crosses TINYINT,                 -- Centros Totales
    accurate_passes INT,                   -- Pases Precisos
    attempted_passes INT,                  -- Pases Intentados
    accurate_passes_opponent_half INT,     -- Pases precisos en campo rival
    passes_opponent_half INT,              -- Pases en campo rival
    accurate_passes_own_half INT,          -- Pases precisos en campo propio
    passes_own_half INT,                   -- Pases en campo propio
    long_balls_completed TINYINT,          -- Pases Largos completados
    long_balls_attempted TINYINT,          -- Pases Largos intentados
    touches INT,                           -- Toques
    miscontrols TINYINT,                   -- Toques Fallidos
    dribbles_won TINYINT,                  -- Regates Ganados
    dribbles_attempted TINYINT,            -- Regates Intentados
    fouls_won TINYINT,                     -- Faltas Recibidas
    possessions_lost TINYINT,              -- Posesiones Perdidas
    tackles_won TINYINT,                   -- Entradas ganadas
    tackles_total TINYINT,                 -- Entradas Totales
    interceptions TINYINT,                 -- Interceptaciones
    clearances TINYINT,                    -- Despejes
    recoveries TINYINT,                    -- Recuperaciones
    ground_duels_won TINYINT,              -- Duelos en el suelo ganados
    ground_duels_total TINYINT,            -- Duelos en el suelo
    aerial_duels_won TINYINT,              -- Duelos Aereos Ganados
    aerial_duels_total TINYINT,            -- Duelos Aereos
    fouls_committed TINYINT,               -- Faltas Cometidas
    times_dribbled_past TINYINT,           -- Regateado
    penalties_committed TINYINT,           -- Penalti cometido
    penalties_won TINYINT,                 -- Penalti Provocado
    penalties_received TINYINT,            -- Penalti Recibido
    saves TINYINT,                         -- Salvadas
    own_goals TINYINT,                     -- Autogoles
    PRIMARY KEY (match_id, player_id),
    FOREIGN KEY (match_id) REFERENCES dim_match(match_id),
    FOREIGN KEY (player_id) REFERENCES dim_player(player_id)
) ENGINE=InnoDB;

-- Rendimiento mensual por jugador

CREATE TABLE IF NOT EXISTS fact_monthly_player_perf (
    perf_id INT AUTO_INCREMENT PRIMARY KEY,
    player_id INT NOT NULL,
    year SMALLINT NULL,                    -- año del datos
    month TINYINT NOT NULL,                -- 1=Enero ... 12=Diciembre
    avg_rating DECIMAL(3,1),               -- 'Promedio mensual'
    matches_played TINYINT,                -- 'Partidos jugados en el mes'
    FOREIGN KEY (player_id) REFERENCES dim_player(player_id)
) ENGINE=InnoDB;