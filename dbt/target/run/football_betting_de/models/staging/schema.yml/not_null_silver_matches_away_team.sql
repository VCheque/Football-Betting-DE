
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select away_team
from "semantic"."silver_matches"
where away_team is null



  
  
      
    ) dbt_internal_test