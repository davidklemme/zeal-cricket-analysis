with 
  totals as 
  (
    select 
      name,
      m.gender,
      count(t.id) as games,
      EXTRACT('Year' FROM m.match_Date) as year  
    from teams t 
    join matches m on t.matches_id=m.id 
    join outcomes o on o.matches_id=m.id
    where m.id NOT IN (
      select ma.id from matches ma join outcomes o ON ma.id=o.matches_id WHERE o.method = 'D/L'
    )
    group by name, gender, year
  ),
  wins as 
  (
    select 
      t.name,
      m.gender,
      count(m.id) as wins,
      EXTRACT('Year' FROM m.match_Date) as year
    from matches m 
    join teams t on t.id=m.winner_id 
    join outcomes o on o.matches_id=m.id
    where m.id NOT IN (
      select ma.id from matches ma join outcomes o ON ma.id=o.matches_id WHERE o.method = 'D/L'
    )
    group by name, gender, year
  )
select 
  totals.name, 
  totals.year,
  wins.gender,
  games, 
  wins,
  ROUND(wins*100/nullif(games,0)::float) as winning_percentage
from 
  totals,wins,matches
where 
  totals.name=wins.name AND 
  wins.gender=totals.gender AND
  wins.year=totals.year AND 
  matches.winner_id IS NOT NULL
group by 
  totals.name,
  wins.name,
  wins,games,
  totals.year,
  wins.year,
  totals.gender,
  wins.gender
order by totals.name, totals.year, winning_percentage;