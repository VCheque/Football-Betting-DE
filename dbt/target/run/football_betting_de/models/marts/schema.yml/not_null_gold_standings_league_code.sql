
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select league_code
from "semantic"."gold_standings"
where league_code is null



  
  
      
    ) dbt_internal_test