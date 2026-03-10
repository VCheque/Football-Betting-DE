
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select home_team
from "semantic"."stg_raw_matches_odds"
where home_team is null



  
  
      
    ) dbt_internal_test