
    
    

select
    standing_key as unique_field,
    count(*) as n_records

from "semantic"."gold_standings"
where standing_key is not null
group by standing_key
having count(*) > 1


