WITH
  # データセットから順位テーブルの作成に必要なカラムだけを抽出したgamesテーブルを作成
  games AS (
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
    # ホームチーム名でグループ化して勝利数、敗北数、勝率を計算するために、
    # ホームチーム名とアウェイチーム名、ホーム得点数とアウェイ得点数をそれぞれ入れ替えたテーブルを結合。
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
  # gamesテーブルの値を用いて順位テーブルを作成
  # 今日までの試合の集計をするためCURRENT_DATE()をstanding_dateにするのが正しいが、デモのため今日の日付を2016-07-07とする。
  standings AS (
    SELECT
      '2016-07-07' AS standing_date,
      homeTeamName AS teamName,
      COUNT(*) AS game_played,
      COUNTIF(homeFinalRuns > awayFinalRuns) AS wins,
      COUNTIF(homeFinalRuns < awayFinalRuns) AS loses,
      COUNTIF(homeFinalRuns = awayFinalRuns) AS ties,
      TRUNC(COUNTIF(homeFinalRuns > awayFinalRuns) / COUNT(*) * 100, 2) AS winning_percentage
    FROM
      (
        SELECT
          *
        FROM games
        WHERE row_num = 1 AND homeTeamName != 'National League' AND homeTeamName != 'American League' AND game_date < '2016-04-04'
      )
    GROUP BY
      teamName)
SELECT
  standing_date,
  standings.teamName,
  RANK() OVER (ORDER BY winning_percentage DESC) AS standing,
  wins,
  loses,
  winning_percentage
FROM standings
ORDER BY standing