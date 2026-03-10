
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select standing_key
from "semantic"."gold_standings"
where standing_key is null



  
  
      
    ) dbt_internal_test