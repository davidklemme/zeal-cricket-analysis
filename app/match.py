import logging


logging.basicConfig(level=logging.DEBUG)
'''
Batch size number for postgres executions
'''
pageSize = 100

def persistData(data, connection, extras):
    '''
    Function will iterate over the data blocks and add it to the database.
    Order is loosely based on dependency, using single statements. 
    In production environments, the more performant approach would be to batch as much possible.
    '''
    cursor = connection.cursor()
  # Meta data
    matches = data['info']
    innings = data['innings']
    

    # MATCH
    # TODO add rest of data to insert
    extras.execute_batch(cursor,cursor.mogrify("""
        INSERT INTO 
            matches 
        (
            event_name, 
            match_number, 
            match_type, 
            venue, 
            city, 
            match_date, 
            gender,
            overs, 
            team_type, 
            winner_id
        )
        VALUES 
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, (SELECT id from teams where name = %s limit 1))
        RETURNING id;
        """, (
            matches.get('event').get('name'), 
            matches.get('event').get('match_number'), 
            matches.get('match_type'),
            matches['venue'], 
            matches['city'], 
            matches.get('dates')[0],
            matches.get('gender'),
            matches.get('overs'), 
            matches.get('team_type'), 
            matches.get('outcome').get('winner') 
        )
    ),pageSize)
    
    match_id = cursor.fetchone()
    connection.commit()
    
    # TEAMS    
    for team in matches['teams']:
        if isinstance(team, str):
            extras.execute_batch(cursor,"""
                        INSERT INTO teams (name, matches_id, team_type) VALUES (%s, %s, %s);
                    """, (team,match_id, matches.get('team_type')),pageSize)
            
            connection.commit()
            
    # OUTCOMES
    '''I miss Elvis :('''
    if matches.get('outcome').get('by') is None:
        extras.execute_batch(cursor,cursor.mogrify(
            """INSERT INTO 
                outcomes (
                    matches_id, 
                    bowl_out_id, 
                    eliminator_id, 
                    method, 
                    result, 
                    winner_id) 
                VALUES 
                (
                    %s,
                    (SELECT id from teams where name=%s),
                    (SELECT id from teams where name=%s),
                    %s,
                    %s,
                    (SELECT id from teams where name= %s)
                )
            """,
            (
                match_id, 
                matches.get('outcome').get('bowl_out'), 
                matches.get('outcome').get('eliminator'), 
                matches.get('outcome').get('method'), 
                matches.get('outcome').get('result'), 
                matches.get('team')
            )
        ),pageSize)
    else:
        extras.execute_batch(cursor,cursor.mogrify(
            """INSERT INTO 
                outcomes (
                    matches_id, 
                    bowl_out_id, 
                    eliminator_id, 
                    method, 
                    result, 
                    winner_id, 
                    by_runs, 
                    by_innings, 
                    by_wickets
                ) 
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                (SELECT id from teams where name= %s),
                %s,
                %s ,
                %s
                )
            """,
            (
                match_id, 
                matches.get('outcome').get('bowl_out'), 
                matches.get('outcome').get('eliminator'), 
                matches.get('outcome').get('method'), 
                matches.get('outcome').get('result'), 
                matches.get('team'),
                matches.get('outcome').get('by').get('runs'),
                matches.get('outcome').get('by').get('innings'),
                matches.get('outcome').get('by').get('wickets')
            )
        ),pageSize)

    connection.commit()
    
    # PLAYER INFO
    for team, players in matches['players'].items():
        for player in players:
            extras.execute_batch(cursor,cursor.mogrify("""
                INSERT INTO players (name, team_id)
                SELECT %s,id FROM teams WHERE name = %s;
            """, (player, team)),pageSize)
    
    # INNINGS
    for inning in innings:
        team_name = inning['team']
    
        extras.execute_batch(cursor,cursor.mogrify("""
            INSERT INTO innings (team_id, matches_id)
            VALUES ((SELECT id FROM teams WHERE name = %s limit 1),%s)
            RETURNING id;
        """, (team_name, match_id)),pageSize)
        inning_id = cursor.fetchone()
        connection.commit()
        
        
    # OVERS 
        for over in inning['overs']:
            extras.execute_batch(cursor,cursor.mogrify("""
                INSERT INTO overs (inning_id, over_number)
                VALUES (%s, %s)
                RETURNING id;
            """, (inning_id, over['over'])),pageSize)
            
            over_id = cursor.fetchone()  
            
    # DELIVERIES
            for delivery in over['deliveries']:
                batter = delivery['batter']
                bowler = delivery['bowler']
                non_striker = delivery['non_striker']
                runs = delivery['runs']
                wickets = delivery.get('wickets', [])
                extras = delivery.get('extras', {})
            #    cursor.execute_batch("""
            #         INSERT INTO deliveries (over_id, batter_id, bowler_id, non_striker_id, runs_batter, runs_extras)
            #         VALUES (
            #             %s, 
            #             (SELECT id from players where name = %s AND (SELECT team_type FROM teams  ) IN (SELECT team_type from matches m JOIN inngins i ON i.matches_id=m.id JOIN overs o ON o.id)=%s), 
            #             (SELECT id from players where name = %s AND IN (SELECT team_type from matches m JOIN inngins i ON i.matches_id=m.id JOIN overs o ON o.id)=%s),
            #             (SELECT id from players where name = %s AND IN (SELECT team_type from matches m JOIN inngins i ON i.matches_id=m.id JOIN overs o ON o.id)=%s),
            #             %s, 
            #             %s)
            #         RETURNING id;
            #     """, (over_id, batter,over_id, bowler,over_id, non_striker,over_id, runsBatter, runsExtras)) 

    # Using LIMIT 1 is a 'little' dirty, but time is running out. SELECT needs to be joined to make sure that the correct match/team (international vs club) is correctly depicted.
                extras.execute_batch(cursor,cursor.mogrify("""
                    INSERT INTO deliveries (over_id, batter_id, bowler_id, non_striker_id, runs_batter, runs_extras)
                    VALUES (
                        %s, 
                        (SELECT id from players where name = %s limit 1), 
                        (SELECT id from players where name = %s limit 1),
                        (SELECT id from players where name = %s limit 1), 
                        %s, 
                        %s)
                    RETURNING id;
                """, (over_id, batter, bowler, non_striker, runs['batter'], runs['extras'])),pageSize)
                
                delivery_id = cursor.fetchone() 
                
    # WICKET
                for wicket in wickets:
                    extras.execute_batch(cursor,cursor.mogrify("""
                        INSERT INTO wickets (delivery_id, player_out_id, kind)
                        VALUES (%s, (SELECT id from players where name = %s limit 1), %s);
                    """, (delivery_id, wicket['player_out'], wicket['kind'])),pageSize)
                
    # EXTRAS
                for extra_type, extra_count in extras.items():
                    extras.execute_batch(cursor,cursor.mogrify("""
                        INSERT INTO extras (delivery_id, extra_type, count)
                        VALUES (%s, %s, %s);
                    """, (delivery_id,extra_type, extra_count)),pageSize)