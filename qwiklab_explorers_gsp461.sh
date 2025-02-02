bq mk --dataset $DEVSHELL_PROJECT_ID:bracketology

bq query --use_legacy_sql=false \
"
SELECT
  season,
  COUNT(*) as games_per_tournament
  FROM
 \`bigquery-public-data.ncaa_basketball.mbb_historical_tournament_games\`
 GROUP BY season
 ORDER BY season # default is Ascending (low to high)
"

bq query --use_legacy_sql=false \
"
# create a row for the winning team
SELECT
  # features
  season, # ex: 2015 season has March 2016 tournament games
  round, # sweet 16
  days_from_epoch, # how old is the game
  game_date,
  day, # Friday
  'win' AS label, # our label
  win_seed AS seed, # ranking
  win_market AS market,
  win_name AS name,
  win_alias AS alias,
  win_school_ncaa AS school_ncaa,
  # win_pts AS points,
  lose_seed AS opponent_seed, # ranking
  lose_market AS opponent_market,
  lose_name AS opponent_name,
  lose_alias AS opponent_alias,
  lose_school_ncaa AS opponent_school_ncaa
  # lose_pts AS opponent_points
FROM \`bigquery-public-data.ncaa_basketball.mbb_historical_tournament_games\`
UNION ALL
# create a separate row for the losing team
SELECT
# features
  season,
  round,
  days_from_epoch,
  game_date,
  day,
  'loss' AS label, # our label
  lose_seed AS seed, # ranking
  lose_market AS market,
  lose_name AS name,
  lose_alias AS alias,
  lose_school_ncaa AS school_ncaa,
  # lose_pts AS points,
  win_seed AS opponent_seed, # ranking
  win_market AS opponent_market,
  win_name AS opponent_name,
  win_alias AS opponent_alias,
  win_school_ncaa AS opponent_school_ncaa
  # win_pts AS opponent_points
FROM
\`bigquery-public-data.ncaa_basketball.mbb_historical_tournament_games\`;
"

bq query --use_legacy_sql=false \
"
CREATE OR REPLACE MODEL
  \`bracketology.ncaa_model\`
OPTIONS
  ( model_type='logistic_reg') AS
# create a row for the winning team
SELECT
  # features
  season,
  'win' AS label, # our label
  win_seed AS seed, # ranking
  win_school_ncaa AS school_ncaa,
  lose_seed AS opponent_seed, # ranking
  lose_school_ncaa AS opponent_school_ncaa
FROM \`bigquery-public-data.ncaa_basketball.mbb_historical_tournament_games\`
WHERE season <= 2017
UNION ALL
# create a separate row for the losing team
SELECT
# features
  season,
  'loss' AS label, # our label
  lose_seed AS seed, # ranking
  lose_school_ncaa AS school_ncaa,
  win_seed AS opponent_seed, # ranking
  win_school_ncaa AS opponent_school_ncaa
FROM
\`bigquery-public-data.ncaa_basketball.mbb_historical_tournament_games\`
# now we split our dataset with a WHERE clause so we can train on a subset of data and then evaluate and test the model's performance against a reserved subset so the model doesn't memorize or overfit to the training data.
# tournament season information from 1985 - 2017
# here we'll train on 1985 - 2017 and predict for 2018
WHERE season <= 2017
"

bq query --use_legacy_sql=false \
"
SELECT
  category,
  weight
FROM
  UNNEST((
    SELECT
      category_weights
    FROM
      ML.WEIGHTS(MODEL \`bracketology.ncaa_model\`)
    WHERE
      processed_input = 'seed')) # try other features like 'school_ncaa'
      ORDER BY weight DESC;
  team.pace_rating,
  # new efficiency metrics (scoring over time)
  team.efficiency_rank,
  team.pts_100poss,
  team.efficiency_rating,

# opposing team
  opponent_seed,
  opponent_school_ncaa,
  # new pace metrics (basketball possession)
  opp.pace_rank AS opp_pace_rank,
  opp.poss_40min AS opp_poss_40min,
  opp.pace_rating AS opp_pace_rating,
  # new efficiency metrics (scoring over time)
  opp.efficiency_rank AS opp_efficiency_rank,
  opp.pts_100poss AS opp_pts_100poss,
  opp.efficiency_rating AS opp_efficiency_rating,

# a little feature engineering (take the difference in stats)

  # new pace metrics (basketball possession)
  opp.pace_rank - team.pace_rank AS pace_rank_diff,
  opp.poss_40min - team.poss_40min AS pace_stat_diff,
  opp.pace_rating - team.pace_rating AS pace_rating_diff,
  # new efficiency metrics (scoring over time)
  opp.efficiency_rank - team.efficiency_rank AS eff_rank_diff,
  opp.pts_100poss - team.pts_100poss AS eff_stat_diff,
  opp.efficiency_rating - team.efficiency_rating AS eff_rating_diff

FROM outcomes AS o
LEFT JOIN \`data-to-insights.ncaa.feature_engineering\` AS team
ON o.school_ncaa = team.team AND o.season = team.season
LEFT JOIN \`data-to-insights.ncaa.feature_engineering\` AS opp
ON o.opponent_school_ncaa = opp.team AND o.season = opp.season
"

bq query --use_legacy_sql=false \
"
CREATE OR REPLACE TABLE \`bracketology.ncaa_2018_predictions\` AS

# let's add back our other data columns for context
SELECT
  *
FROM
  ML.PREDICT(MODEL     \`bracketology.ncaa_model_updated\`, (

SELECT
* # include all columns now (the model has already been trained)
FROM \`bracketology.training_new_features\`

WHERE season = 2018

))
"

bq query --use_legacy_sql=false \
"
CREATE OR REPLACE TABLE \`bracketology.ncaa_2019_tournament_predictions\` AS

SELECT
  *
FROM
  # let's predicted using the newer model
  ML.PREDICT(MODEL     \`bracketology.ncaa_model_updated\`, (

# let's predict on March 2019 tournament games:
SELECT * FROM \`bracketology.ncaa_2019_tournament\`
))

"
