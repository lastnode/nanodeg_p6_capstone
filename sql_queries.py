"""
Support module for the ETL scripts.
Contains the queries used by the following functions:
  
"""


games_table_chessdotcom = ("""

    select
        from_unixtime(end_time, 'yyyy-MM-dd hh:mm:ss') as game_end_time,
        from_unixtime(end_time, 'yyyy-MM-dd') as game_end_date,
        time_class,
        rated,
        white_username,
        white_rating,
        black_username,
        black_rating,
        case
            when white_result = "win" then "white"
            when black_result = "win" then "black"
            when (white_result = "agree" or black_result = "agree") then "draw"
            when (white_result = "stalemate" or black_result = "stalemate") then "stalemate"
        end as winner,
        get_termination_from_pgn(pgn) as termination,
        get_eco_from_pgn(pgn) as opening,
        get_moves_from_pgn(pgn) as moves,
        'chessdotcom' as platform
    from chessdotcom_staging
    order by game_end_time
                    
""")


games_table_lichess = ("""

	select
	    from_unixtime(lastMoveAt/1000, 'yyyy-MM-dd hh:mm:ss') as game_end_time,
	    from_unixtime(lastMoveAt/1000, 'yyyy-MM-dd') as game_end_date,
	    speed as time_class,
	    players_white_user_name as white_username,
	    players_white_rating as white_rating,
	    players_black_user_name as black_username,
	    players_black_rating as black_rating,
	    case
	        when winner = "black" then "black"
	        when winner = "white" then "white"
	        when winner = "" then status
	    end as winner,
	    status as termination,
	    opening_name as opening,
	    moves,
	    'lichess' as platform
	from lichess_staging

	order by game_end_time
                    
""")


