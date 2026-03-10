
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        result_ft as value_field,
        count(*) as n_records

    from "semantic"."silver_matches"
    group by result_ft

)

select *
from all_values
where value_field not in (
    'H','D','A'
)



  
  
      
    ) dbt_internal_test