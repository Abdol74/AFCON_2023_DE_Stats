{{
    config(
        materialized='view'
    )
}}

with 

source as (

    select * from {{ source('reporting_layer', 'match_fact') }}

),

match_fact as (

    select
        match_id,
        home_team_id,
        away_team_id,
        home_manager_id,
        away_manager_id,
        stadium_id,
        referee_id,
        home_score,
        away_score,
        goals_scored,
        penalties_finished

    from source

)
,

team_dim as (

    select 
        team_id,
        team
    from {{ source('reporting_layer', 'team_dim') }}
),

manager_dim as (

    select 
        manager_id,
        manager
    from {{ source('reporting_layer', 'manager_dim') }}
),

stadium_dim as (

    select 
        stadium_id,
        stadium
    from {{ source('reporting_layer', 'stadium_dim') }}
),

referee_dim as (
    select 
        referee_id,
        referee
    from {{ source('reporting_layer', 'referee_dim') }}
)

select 
        M.match_id,
        H_T.team as home_team,
        A_T.team as away_team,
        H_M.manager as home_manager,
        A_M.manager as away_manager,
        S.stadium,
        R.referee,
        M.home_score,
        M.away_score,
        M.goals_scored,
        M.penalties_finished,
        CASE 
            WHEN M.home_score > M.away_score THEN 'Win'
            WHEN M.home_score < M.away_score THEN 'Loss'
            ELSE 'Draw'
        END AS match_result
from 
match_fact as M
LEFT JOIN 
team_dim as H_T
ON 
M.home_team_id = H_T.team_id
LEFT JOIN 
team_dim as A_T 
ON 
M.away_team_id = A_T.team_id

LEFT JOIN 
manager_dim as H_M 
ON 
M.home_manager_id = H_M.manager_id
LEFT JOIN 
manager_dim as A_M 
ON 
M.away_manager_id = A_M.manager_id

LEFT JOIN 
stadium_dim as S 
ON 
M.stadium_id = S.stadium_id

LEFT JOIN 
referee_dim as R 
ON 
M.referee_id = R.referee_id