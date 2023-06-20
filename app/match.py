import logging

logging.basicConfig(level=logging.DEBUG)

def persistData(data, connection):
    cursor = connection.cursor()
  # Meta data
    matches = data['info']
    innings = data['innings']
    

    # MATCH
    # TODO add rest of data to insert
    cursor.execute("""
        INSERT INTO matches (event_name, match_number, match_type, venue, city, match_date, gender, overs, team_type, winner_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, (SELECT id from teams where name = %s limit 1))
        RETURNING id;
    """, (matches.get('event').get('name'), matches.get('event').get('match_number'), matches.get('match_type'),
          matches['venue'], matches['city'], matches.get('dates')[0], matches.get('gender'),
          matches.get('overs'), matches.get('team_type'), matches.get('outcome').get('winner') ))
    match_id = cursor.fetchone()
    connection.commit()
    
    # TEAMS    
    for team in matches['teams']:
        if isinstance(team, str):
            cursor.execute("""
                        INSERT INTO teams (name, matches_id, team_type) VALUES (%s, %s, %s);
                    """, (team,match_id, matches.get('team_type')))
            
            connection.commit()
            
    # OUTCOMES
    if matches.get('outcome').get('by') is None:
        cursor.execute(
            """INSERT INTO outcomes (matches_id, bowl_out_id, eliminator_id, method, result, winner_id) VALUES (%s,(SELECT id from teams where name=%s),(SELECT id from teams where name=%s),%s,%s,(SELECT id from teams where name= %s))""",
            (
                match_id, matches.get('outcome').get('bowl_out'), matches.get('outcome').get('eliminator'), matches.get('outcome').get('method'), matches.get('outcome').get('result'), matches.get('team')
            )
        )
    else:
        cursor.execute(
            """INSERT INTO outcomes (matches_id, bowl_out_id, eliminator_id, method, result, winner_id, by_runs, by_innings, by_wickets) VALUES (%s,%s,%s,%s,%s,(SELECT id from teams where name= %s),%s,%s ,%s)""",
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
        )

    connection.commit()
    
    # PLAYER INFO
    for team, players in matches['players'].items():
        for player in players:
            cursor.execute("""
                INSERT INTO players (name, team_id)
                SELECT %s,id FROM teams WHERE name = %s;
            """, (player, team))
    
    # INNINGS
    for inning in innings:
        team_name = inning['team']
    
        cursor.execute("""
            INSERT INTO innings (team_id, matches_id)
            VALUES ((SELECT id FROM teams WHERE name = %s limit 1),%s)
            RETURNING id;
        """, (team_name, match_id))
        inning_id = cursor.fetchone()
        connection.commit()
        
        
    # OVERS 
        for over in inning['overs']:
            cursor.execute("""
                INSERT INTO overs (inning_id, over_number)
                VALUES (%s, %s)
                RETURNING id;
            """, (inning_id, over['over']))
            
            over_id = cursor.fetchone()  
            
    # DELIVERIES
            for delivery in over['deliveries']:
                batter = delivery['batter']
                bowler = delivery['bowler']
                non_striker = delivery['non_striker']
                runs = delivery['runs']
                wickets = delivery.get('wickets', [])
                extras = delivery.get('extras', {})
            #    cursor.execute("""
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
                cursor.execute("""
                    INSERT INTO deliveries (over_id, batter_id, bowler_id, non_striker_id, runs_batter, runs_extras)
                    VALUES (
                        %s, 
                        (SELECT id from players where name = %s limit 1), 
                        (SELECT id from players where name = %s limit 1),
                        (SELECT id from players where name = %s limit 1), 
                        %s, 
                        %s)
                    RETURNING id;
                """, (over_id, batter, bowler, non_striker, runs['batter'], runs['extras']))
                
                delivery_id = cursor.fetchone() 
                
    # WICKET
                for wicket in wickets:
                    cursor.execute("""
                        INSERT INTO wickets (delivery_id, player_out_id, kind)
                        VALUES (%s, (SELECT id from players where name = %s limit 1), %s);
                    """, (delivery_id, wicket['player_out'], wicket['kind']))
                
    # EXTRAS
                for extra_type, extra_count in extras.items():
                    cursor.execute("""
                        INSERT INTO extras (delivery_id, extra_type, count)
                        VALUES (%s, %s, %s);
                    """, (delivery_id,extra_type, extra_count))