
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select season_label
from "semantic"."gold_standings"
where season_label is null



  
  
      
    ) dbt_internal_test