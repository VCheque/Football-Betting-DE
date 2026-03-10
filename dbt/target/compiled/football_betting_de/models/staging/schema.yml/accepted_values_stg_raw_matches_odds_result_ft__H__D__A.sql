
    
    

with all_values as (

    select
        result_ft as value_field,
        count(*) as n_records

    from "semantic"."stg_raw_matches_odds"
    group by result_ft

)

select *
from all_values
where value_field not in (
    'H','D','A'
)


