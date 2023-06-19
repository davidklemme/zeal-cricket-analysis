CREATE TABLE IF NOT EXISTS meta (
  data_version VARCHAR(10),
  created DATE,
  revision INT
);
CREATE TABLE IF NOT EXISTS officials (
  id SERIAL PRIMARY KEY,
  name VARCHAR(150)
);
CREATE TABLE IF NOT EXISTS teams (
  id SERIAL PRIMARY KEY,
  name VARCHAR(150),
  matches_id INT,
  team_type VARCHAR(50)
--   CONSTRAINT unique_team_names UNIQUE (name)
);

CREATE TABLE IF NOT EXISTS players (
  id SERIAL PRIMARY KEY,
  name VARCHAR(150),
  team_id INT,
  FOREIGN KEY (team_id) REFERENCES teams(id)
);
CREATE TABLE IF NOT EXISTS registry (
  id SERIAL PRIMARY KEY,
  person_id VARCHAR(150),
  player_id INT,
  FOREIGN KEY (player_id) REFERENCES players(id)
);

CREATE TABLE IF NOT EXISTS matches (
  id SERIAL PRIMARY KEY,
  balls_per_over INT,
  city VARCHAR(150),
  event_name VARCHAR(100),
  match_date DATE,
  match_number INT,
  match_group VARCHAR(10),
  gender VARCHAR(10),
  match_type VARCHAR(10),
  match_type_number INT,
  team_type VARCHAR(100),
  overs INT,
  winner_id INT,
  outcome_by INT,
  player_of_match_id INT,
  toss_decision VARCHAR(10),
  toss_winner_id INT,
  venue VARCHAR(100),
  FOREIGN KEY (player_of_match_id) REFERENCES players(id),
  FOREIGN KEY (toss_winner_id) REFERENCES teams(id)
);
CREATE TABLE IF NOT EXISTS innings (
  id SERIAL PRIMARY KEY,
  matches_id INT,
  FOREIGN KEY (matches_id) REFERENCES matches(id),
  team_id INT,
  FOREIGN KEY (team_id) REFERENCES teams(id)
);
CREATE TABLE IF NOT EXISTS overs (
  id SERIAL PRIMARY KEY,
  inning_id INT,
  over_number INT,
  FOREIGN KEY (inning_id) REFERENCES innings(id)
);
CREATE TABLE IF NOT EXISTS deliveries (
  id SERIAL PRIMARY KEY,
  over_id INT,
  batter_id INT,
  bowler_id INT,
  non_striker_id INT,
  runs_batter INT,
  runs_extras INT,
  total_runs INT,
  FOREIGN KEY (over_id) REFERENCES overs(id),
  FOREIGN KEY (batter_id) REFERENCES players(id),
  FOREIGN KEY (bowler_id) REFERENCES players(id),
  FOREIGN KEY (non_striker_id) REFERENCES players(id)
);
CREATE TABLE IF NOT EXISTS wickets (
  id SERIAL PRIMARY KEY,
  delivery_id INT,
  player_out_id INT,
  kind VARCHAR(150),
  FOREIGN KEY (delivery_id) REFERENCES deliveries(id),
  FOREIGN KEY (player_out_id) REFERENCES players(id)
);
CREATE TABLE IF NOT EXISTS outcome (
  id SERIAL PRIMARY KEY,
  matches_id INT,
  winner_id INT,
  by_wickets INT,
  FOREIGN KEY (matches_id) REFERENCES matches(id),
  FOREIGN KEY (winner_id) REFERENCES teams(id)
);
CREATE TABLE IF NOT EXISTS extras (
    id SERIAL PRIMARY KEY,
    delivery_id INT, 
    extra_type VARCHAR(50), 
    count INT,
    FOREIGN KEY (delivery_id) REFERENCES deliveries(id)
);