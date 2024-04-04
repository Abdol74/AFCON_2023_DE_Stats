import io
import requests
from statsbombpy import sb
import pandas as pd
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test



@data_loader
def load_data_from_api(*args, **kwargs):

    # url = ''
    # response = requests.get(url)

    competitions_df = sb.competitions()
    competitions_df['source'] = 'competitions'
    
    acfon_matches_df = sb.matches(competition_id=1267, season_id=107)[['match_id',
                                                                      'match_date',
                                                                      'kick_off',
                                                                      'competition',
                                                                      'season',
                                                                      'home_team',
                                                                      'away_team',
                                                                      'home_score',
                                                                      'away_score',
                                                                      'match_week',
                                                                      'competition_stage',
                                                                      'stadium',
                                                                      'referee',
                                                                      'home_managers',
                                                                      'away_managers']]
    acfon_matches_df['source'] = 'acfon_matches'
    afcon_2023_matches_list = sb.matches(competition_id=1267, season_id=107)['match_id'].to_list()


    afcon_events_df = pd.DataFrame()
    match_event_df = pd.DataFrame()
    for match_id in afcon_2023_matches_list:
        try:
            match_event_df = sb.events(match_id=match_id)[['player',
                                                                'player_id',
                                                                'match_id',
                                                                'team',
                                                                'team_id',
                                                                'timestamp',
                                                                'type',
                                                                'location',
                                                                'play_pattern',
                                                                'foul_committed_card',
                                                                'foul_committed_offensive',
                                                                'foul_committed_penalty',
                                                                'foul_committed_type',
                                                                'foul_won_defensive',
                                                                'foul_won_penalty',
                                                                'minute',
                                                                'pass_goal_assist',
                                                                'pass_shot_assist',
                                                                'pass_outcome',
                                                                'pass_cross',
                                                                'position',
                                                                'possession',
                                                                'possession_team',
                                                                'interception_outcome',
                                                                'shot_outcome' ]] 

            afcon_events_df = pd.concat([afcon_events_df, match_event_df], ignore_index=True)
        except KeyError as e:
        # Handle missing columns
            missing_cols = ['foul_committed_penalty', 'foul_won_penalty']
            for col in missing_cols:
                match_event_df[col] = 0  
                afcon_events_df = pd.concat([afcon_events_df, match_event_df], ignore_index=True)

    afcon_events_df['source'] = 'match_events'
    

    return [competitions_df, acfon_matches_df, afcon_events_df]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
