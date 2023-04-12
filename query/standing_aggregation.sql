WITH
  # 常に30チームが順位表に表示されるように全てのチーム名の情報を含むテーブルを作成
  teams AS (
    SELECT * FROM UNNEST([
      STRUCT('Marlins' AS teamName),
      STRUCT('Braves' AS teamName),
      STRUCT('Blue Jays' AS teamName),
      STRUCT('Phillies' AS teamName),
      STRUCT('Diamondbacks' AS teamName),
      STRUCT('Athletics' AS teamName),
      STRUCT('Rockies' AS teamName),
      STRUCT('Mariners' AS teamName),
      STRUCT('Cardinals' AS teamName),
      STRUCT('White Sox' AS teamName),
      STRUCT('Pirates' AS teamName),
      STRUCT('Angels' AS teamName),
      STRUCT('Cubs' AS teamName),
      STRUCT('Tigers' AS teamName),
      STRUCT('Orioles' AS teamName),
      STRUCT('Indians' AS teamName),
      STRUCT('Royals' AS teamName),
      STRUCT('Red Sox' AS teamName),
      STRUCT('Yankees' AS teamName),
      STRUCT('Giants' AS teamName),
      STRUCT('Twins' AS teamName),
      STRUCT('Rays' AS teamName),
      STRUCT('Reds' AS teamName),
      STRUCT('Padres' AS teamName),
      STRUCT('Nationals' AS teamName),
      STRUCT('Rangers' AS teamName),
      STRUCT('Brewers' AS teamName),
      STRUCT('Astros' AS teamName),
      STRUCT('Dodgers' AS teamName),
      STRUCT('Mets' AS teamName)])),
  # データセットから順位テーブルの作成に必要なカラムだけを抽出したresultsテーブルを作成
  results AS (
    SELECT
      gameId,
      DATE(startTime) AS game_date,
      homeTeamName,
      homeFinalRuns,
      awayTeamName,
      awayFinalRuns,
      ROW_NUMBER() OVER (PARTITION BY gameId) AS row_num
    FROM
      `bigquery-public-data.baseball.games_wide`
    UNION ALL
    SELECT
      concat(gameId, '_reverse') AS gameId,
      DATE(startTime) AS game_date,
      awayTeamName AS homeTeamName,
      awayFinalRuns AS homeFinalRuns,
      homeTeamName AS awayTeamName,
      homeFinalRuns AS awayFinalRuns,
      ROW_NUMBER() OVER (PARTITION BY gameId) AS row_num
    FROM
      `bigquery-public-data.baseball.games_wide`),
  # teamsテーブルにresultsテーブルを外部結合し、NULLを許容することで全チームの試合情報を持たせたgamesテーブルを作成
  games AS (
    SELECT
      results.gameId AS gameId,
      results.game_date AS game_date,
      teams.teamName AS homeTeamName,
      results.homeFinalRuns AS homeFinalRuns,
      results.awayTeamName AS awayTeamName,
      results.awayFinalRuns AS awayFinalRuns,
      results.row_num AS row_num
    FROM
      teams
    LEFT JOIN (
      SELECT
        *
      FROM
        results
      WHERE row_num =1 AND homeTeamName != 'National League' AND homeTeamName != 'American League') AS results ON teams.teamName = results.homeTeamName
  ),
  # gamesテーブルの値を基に勝利数、敗北数、勝率を集計したstandingsテーブルを作成
  standings AS (
    SELECT
      CURRENT_DATE() AS standing_date,
      homeTeamName AS teamName,
      IFNULL(COUNT(gameId), 0) AS game_played,
      IFNULL(COUNTIF(homeFinalRuns > awayFinalRuns), 0) AS wins,
      IFNULL(COUNTIF(homeFinalRuns < awayFinalRuns), 0) AS loses,
      IFNULL(COUNTIF(homeFinalRuns = awayFinalRuns), 0) AS ties,
      TRUNC(SAFE_DIVIDE(COUNTIF(homeFinalRuns > awayFinalRuns), COUNT(gameId)) * 100, 2) AS winning_percentage
    FROM games
    WHERE game_date < CURRENT_DATE()
    GROUP BY teamName) 
SELECT
  standing_date,
  standings.teamName,
  RANK() OVER (ORDER BY winning_percentage DESC) AS standing,
  wins,
  loses,
  winning_percentage
FROM standings
ORDER BY standing