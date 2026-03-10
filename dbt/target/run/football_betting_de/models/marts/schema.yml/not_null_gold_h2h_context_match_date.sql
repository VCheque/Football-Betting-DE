
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select match_date
from "semantic"."gold_h2h_context"
where match_date is null



  
  
      
    ) dbt_internal_test