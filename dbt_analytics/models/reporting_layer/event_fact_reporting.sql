{{
    config(
        materialized='table'
    )
}}

with 

source as (

    select * from {{ source('reporting_layer', 'event_fact') }}

),

event_fact as (

    select
        event_id,
        player_id,
        match_id,
        team_id,
        event_type,
        event_timestamp,
        event_minute,
        event_location,
        play_pattern,
        position,
        pass_outcome,
        pass_cross,
        pass_goal_assist,
        pass_shot_assist,
        foul_committed_type,
        foul_committed_card,
        foul_committed_offensive,
        foul_won_defensive,
        foul_committed_penalty,
        foul_won_penalty,
        shot_outcome,
        interception_outcome,
        possession,
        possession_team_id,
        zone

    from source

),

match_dim as (

    select 
        match_id,
        match_date, 
        match_week,
        kick_off,
        competition_stage
    from {{ source('reporting_layer', 'match_dim') }}
 ),

player_dim as (
    select 
        player_id,
        player
    from {{ source('reporting_layer', 'player_dim') }}
), 

team_dim as (
    select
        team_id,
        team
    from {{ source('reporting_layer', 'team_dim') }}
)

select 
        E.event_id,
        P.player,
        M.match_date,
        M.match_week,
        M.kick_off,
        M.competition_stage,
        T.team,
        E.event_type,
        E.event_timestamp,
        E.event_minute,
        E.event_location,
        E.play_pattern,
        E.position,
        E.pass_outcome,
        E.pass_cross,
        E.pass_goal_assist,
        E.pass_shot_assist,
        E.foul_committed_type,
        E.foul_committed_card,
        E.foul_committed_offensive,
        E.foul_won_defensive,
        E.foul_committed_penalty,
        E.foul_won_penalty,
        E.shot_outcome,
        E.interception_outcome,
        E.possession,
        E.possession_team_id,
        E.zone
from event_fact as E

LEFT JOIN match_dim as M 

ON E.match_id = M.match_id

LEFT JOIN player_dim as P 

ON E.player_id = P.player_id

LEFT JOIN team_dim as T 

ON E.team_id = T.team_id