
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select match_date
from "semantic"."stg_raw_matches_odds"
where match_date is null



  
  
      
    ) dbt_internal_test