import logging


logging.basicConfig(
                    # filename='db_insert.log',
                    # filemode='a',
                    # format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    # datefmt='%H:%M:%S',
                    level=logging.DEBUG
                    )
'''
Batch size number for postgres executions
'''
pageSize = 1000

def persistData(data, connection, extras):
    '''
    Function will iterate over the data blocks and add it to the database.
    Order is loosely based on dependency, using single statements. 
    In production environments, the more performant approach would be to batch as much possible.
    '''
    try:
        cursor = connection.cursor()
        cursor.execute("SAVEPOINT za1")
    # Meta data
        matches = data['info']
        innings = data['innings']
        

        # MATCH
        # TODO add rest of data to insert
        extras.execute_batch(cursor,"""
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
            """, [
                (
                matches.get('event').get('name'), 
                matches.get('event').get('match_number'), 
                matches.get('match_type'),
                matches.get('venue'), 
                matches.get('city'), 
                matches.get('dates')[0],
                matches.get('gender'),
                matches.get('overs'), 
                matches.get('team_type'), 
                matches.get('outcome').get('winner') 
                )
            ]
        ,pageSize)
        
        match_id = cursor.fetchone()
        
        # TEAMS    
        for team in matches['teams']:
            if isinstance(team, str):
                extras.execute_batch(cursor,"""
                            INSERT INTO teams (name, matches_id, team_type) VALUES (%s, %s, %s);
                        """, [(team,match_id, matches.get('team_type'))],pageSize)
                
        
                
        # OUTCOMES
        '''I miss Elvis :('''
        if matches.get('outcome').get('by') is None:
            extras.execute_batch(cursor,
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
                        (SELECT id from teams where name=%s LIMIT 1),
                        (SELECT id from teams where name=%s LIMIT 1),
                        %s,
                        %s,
                        (SELECT id from teams where name= %s LIMIT 1)
                    )
                """,
                [(
                    match_id, 
                    matches.get('outcome').get('bowl_out'), 
                    matches.get('outcome').get('eliminator'), 
                    matches.get('outcome').get('method'), 
                    matches.get('outcome').get('result'), 
                    matches.get('team')
                )]
            ,pageSize)
        else:
            extras.execute_batch(cursor,
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
                    (SELECT id from teams where name= %s LIMIT 1),
                    %s,
                    %s,
                    %s
                    )
                """,
            [ (
                    match_id, 
                    matches.get('outcome').get('bowl_out'), 
                    matches.get('outcome').get('eliminator'), 
                    matches.get('outcome').get('method'), 
                    matches.get('outcome').get('result'), 
                    matches.get('team'),
                    matches.get('outcome').get('by').get('runs'),
                    matches.get('outcome').get('by').get('innings'),
                    matches.get('outcome').get('by').get('wickets')
                )]
            ,pageSize)

        
        # PLAYER INFO
        playerValue = []
        for team, players in matches['players'].items():
            for player in players:
                playerValue.append((player, team))
                extras.execute_batch(cursor,"""
                    INSERT INTO players (name, team_id)
                    SELECT %s,id FROM teams WHERE name = %s;
                """, playerValue,pageSize)
        
        # INNINGS
        inningsValue=[]
        for inning in innings:
            team_name = inning['team']
            inningsValue.append((team_name, match_id))
        extras.execute_batch(cursor,"""
                INSERT INTO innings (team_id, matches_id)
                VALUES ((SELECT id FROM teams WHERE name = %s limit 1),%s)
                RETURNING id;
            """, inningsValue,pageSize)
        inning_id = cursor.fetchone()
            
            
        # OVERS 
        overValues= []
        for over in inning['overs']:
                overValues.append((inning_id, over['over']))
        extras.execute_batch(cursor,"""
                    INSERT INTO overs (inning_id, over_number)
                    VALUES (%s, %s)
                    RETURNING id;
                """, overValues,pageSize)
                
        over_id = cursor.fetchone()  
                
        # DELIVERIES
        deliveryValues= []
        for delivery in over['deliveries']:
                    batter = delivery['batter']
                    bowler = delivery['bowler']
                    non_striker = delivery['non_striker']
                    runs = delivery['runs']
                    wickets = delivery.get('wickets', [])
                    extrass = delivery.get('extras', {})
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
                    deliveryValues.append((over_id, batter, bowler, non_striker, runs['batter'], runs['extras']))
        extras.execute_batch(cursor,"""
                        INSERT INTO deliveries (over_id, batter_id, bowler_id, non_striker_id, runs_batter, runs_extras)
                        VALUES (
                            %s, 
                            (SELECT id from players where name = %s limit 1), 
                            (SELECT id from players where name = %s limit 1),
                            (SELECT id from players where name = %s limit 1), 
                            %s, 
                            %s)
                        RETURNING id;
                    """, deliveryValues,pageSize)
                    
        delivery_id = cursor.fetchone() 
                    
        # WICKET    
        wicketsValues= []    
        for wicket in wickets:
                        wicketsValues.append((delivery_id, wicket['player_out'], wicket['kind']))
        extras.execute_batch(cursor,"""
                            INSERT INTO wickets (delivery_id, player_out_id, kind)
                            VALUES (%s, (SELECT id from players where name = %s limit 1), %s);
                        """, wicketsValues,pageSize)
                    
        # EXTRAS
        extrasValues= []
        for extra_type, extra_count in extrass.items():
                        extrasValues.append((delivery_id,extra_type, extra_count))
        extras.execute_batch(cursor,"""
                            INSERT INTO extras (delivery_id, extra_type, count)
                            VALUES (%s, %s, %s);
                        """, extrasValues,pageSize)
        
    except Exception as error:
        logging.error('DB INSERT ERROR %s',error)
        cursor.execute('ROLLBACK TO SAVEPOINT za1')
    else:
        cursor.execute('RELEASE SAVEPOINT za1')
        connection.commit()