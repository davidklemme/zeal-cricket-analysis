select 
    name,
    EXTRACT('Year' FROM m.match_Date) as year,
    sum(runs_batter) as runs, 
    count(batter_id) as atbat, 
    (sum(runs_batter) * 100/(count(batter_id))::float) as strike_rate,
    rank() over(  order by  (sum(runs_batter) * 100/(count(batter_id))::float)  desc) as rank
from 
    deliveries d 
    join players p on p.id=d.batter_id 
    join overs o on d.over_id=o.id
    join innings i on i.id=o.inning_id
    join matches m on m.id=i.matches_id
where EXTRACT('Year' FROM m.match_Date) = '2019'
group by name,year order by strike_rate  desc limit 20;