/*
Original table : 

Actor           | Actor ID   | Film                          | Year  | Votes  | Rating | Film ID
------------------------------------------------------------------------------------------------
Fred Astaire     | nm0000001  | On the Beach                  | 1959  | 12066  | 7.2    | tt0053137
Lauren Bacall    | nm0000002  | North West Frontier           | 1959  | 2312   | 7.1    | tt0053126
Brigitte Bardot  | nm0000003  | Come Dance with Me!           | 1959  | 551    | 6.3    | tt0053428
Brigitte Bardot  | nm0000003  | Babette Goes to War           | 1959  | 352    | 6.1    | tt0052595
Richard Burton   | nm0000009  | Look Back in Anger            | 1959  | 3518   | 7.0    | tt0051879
Richard Burton   | nm0000009  | A Midsummer Night's Dream     | 1959  | 326    | 7.2    | tt0053261
James Cagney     | nm0000010  | Shake Hands with the Devil    | 1959  | 11497  | 7.1    | tt0053272
James Cagney     | nm0000010  | Never Steal Anything Small    | 1959  | 277    | 6.1    | tt0053109
Gary Cooper      | nm0000011  | The Hanging Tree              | 1959  | 3721   | 7.1    | tt0052876
Gary Cooper      | nm0000011  | The Wreck of the Mary Deare   | 1959  | 2327   | 6.8    | tt0053455

*/

-- DDL command 

CREATE TABLE agupta93.actors
             (
                          actor    VARCHAR,
                          actor_id VARCHAR,
                          films ARRAY( row( film VARCHAR, votes INTEGER, rating DOUBLE, film_id VARCHAR ) ),
                          is_active     boolean,
                          quality_class VARCHAR,
                          current_year  INTEGER
             )
             WITH
             (
                          format = 'PARQUET',
                          partitioning = array['current_year']
             )

-- inserting data into the above table 

INSERT INTO agupta93.actors
WITH last_year
     AS (SELECT *
         FROM   agupta93.actors
         WHERE  current_year = 1917),
     this_year
     AS (SELECT actor,
                actor_id,
                Array_agg(ROW(film, votes, rating, film_id)) AS films,
                CASE
                  WHEN Avg(rating) >= 8 THEN 'star'
                  WHEN Avg(rating) > 7 THEN 'good'
                  WHEN Avg(rating) > 6 THEN 'average'
                  ELSE 'bad'
                END                                          AS quality_class,
                Max(YEAR)                                    AS current_year
         FROM   bootcamp.actor_films
         WHERE  YEAR = 1918
         GROUP  BY actor,
                   actor_id)
SELECT Coalesce(ly.actor, ty.actor)                   AS actor,
       Coalesce(ly.actor_id, ty.actor_id)             AS actor_id,
       CASE
         WHEN ty.actor IS NULL THEN ly.films
         WHEN ty.actor IS NOT NULL
              AND ly.actor IS NULL THEN ty.films
         WHEN ty.actor IS NOT NULL
              AND ly.actor IS NOT NULL THEN ty.films
                                            || ly.films
       END                                            AS films,
       CASE
         WHEN ty.actor IS NULL THEN false
         ELSE true
       END                                            AS is_active,
       Coalesce(ty.quality_class, ly.quality_class)   AS quality_class,
       Coalesce(ty.current_year, ly.current_year + 1) AS current_year
FROM   last_year ly
       FULL OUTER JOIN this_year ty
                    ON ly.actor_id = ty.actor_id 


-- DDL command for creating the slowly changing dimensions type 2 table 

CREATE TABLE agupta93.actors_history_scd
  (
     actor         VARCHAR,
     quality_class VARCHAR,
     is_active     BOOLEAN,
     start_date    INTEGER,
     end_date      INTEGER,
     current_year  INTEGER
  )

with ( format = 'PARQUET', partitioning = array['current_year'] )

-- Batch insert into the table created above for scd type 2

INSERT INTO agupta93.actors_history_scd
WITH lagged
     AS (SELECT actor,
                quality_class,
                CASE
                  WHEN is_active THEN 1
                  ELSE 0
                END AS is_active,
                CASE
                  WHEN ( Lag(is_active, 1)
                           over (
                             PARTITION BY actor
                             ORDER BY current_year) ) THEN 1
                  ELSE 0
                END AS is_active_last_year,
                current_year
         FROM   agupta93.actors
         WHERE  current_year <= 1917),
     streaked
     AS (SELECT *,
                SUM(CASE
                      WHEN is_active <> is_active_last_year THEN 1
                      ELSE 0
                    END)
                  over (
                    PARTITION BY actor
                    ORDER BY current_year) AS streak_identifier
         FROM   lagged)
SELECT actor,
       Max(quality_class) AS quality_class,
       Max(is_active) = 1 AS is_active,
       Min(current_year)  AS start_date,
       Max(current_year)  AS end_date,
       1917               AS current_year
FROM   streaked
GROUP  BY actor,
          streak_identifier
ORDER  BY actor,
          start_date 

  /*
  
  Sample output from above 
  
Actor           | Quality Class | Is Active | Start Date | End Date | Current Year
---------------------------------------------------------------------------------
Charles Chaplin | Average       | True      | 1914       | 1914     | 1918
Charles Chaplin | Average       | False     | 1915       | 1918     | 1918
Harold Lloyd    | Bad           | True      | 1914       | 1914     | 1918
Harold Lloyd    | Bad           | False     | 1915       | 1918     | 1918
Lillian Gish    | Good          | True      | 1914       | 1916     | 1918
Lillian Gish    | Good          | False     | 1917       | 1917     | 1918
Lillian Gish    | Good          | True      | 1918       | 1918     | 1918
Milton Berle    | Average       | True      | 1917       | 1917     | 1918
Milton Berle    | Average       | False     | 1918       | 1918     | 1918
Gloria Swanson  | Bad           | True      | 1918       | 1918     | 1918

*/
-- Incremental insert into the scd type 2 table 

INSERT INTO agupta93.actors_history_scd with last_year_scd AS
            (
                   SELECT *
                   FROM   agupta93.actors_history_scd
                   WHERE  current_year = 1917
            )
            ,
            curren_year_scd AS
            (
                   SELECT *
                   FROM   agupta93.actors
                   WHERE  current_year = 1918
            )
            ,
            combined AS
            (
                            SELECT          coalesce(ls.actor, cs.actor)                 AS actor,
                                            coalesce(ls.start_date, cs.current_year)     AS start_date,
                                            coalesce(ls.end_date, cs.current_year)       AS end_date,
                                            coalesce(ls.quality_class, cs.quality_class) AS quality_class,
                                            CASE
                                                            WHEN ls.is_active <> cs.is_active THEN 1
                                                            WHEN ls.is_active = cs.is_active THEN 0
                                            end          AS did_change,
                                            ls.is_active AS is_active_last_year,
                                            cs.is_active AS is_active_this_year,
                                            1918         AS current_year
                            FROM            last_year_scd ls
                            full OUTER JOIN curren_year_scd cs
                            ON              ls.actor = cs.actor
                            AND             ls.end_date + 1 = cs.current_year
            )
            ,
            changes AS
            (
                   SELECT actor,
                          current_year,
                          CASE
                                 WHEN did_change = 0 THEN array[CAST(ROW(quality_class, is_active_last_year, start_date, end_date + 1) AS ROW(quality_class                                VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))]
                                 WHEN did_change = 1 THEN array[CAST(ROW(quality_class, is_active_last_year, start_date, end_date) AS ROW(quality_class                                    VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER)),                                         CAST(ROW(quality_class, is_active_this_year, current_year, current_year) AS ROW(quality_class                                                      VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))]
                                 WHEN did_change IS NULL THEN array[CAST(ROW(quality_class, COALESCE(is_active_last_year, is_active_this_year), start_date, end_date) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))]
                          end AS change_array
                   FROM   combined
            )
     SELECT     actor,
                arr.quality_class,
                arr.is_active,
                arr.start_date,
                arr.end_date,
                current_year
     FROM       changes
     CROSS JOIN unnest(change_array) AS arr
