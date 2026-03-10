
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select h2h_matches
from "semantic"."gold_h2h_context"
where h2h_matches is null



  
  
      
    ) dbt_internal_test