
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select result_ft
from "semantic"."silver_matches"
where result_ft is null



  
  
      
    ) dbt_internal_test