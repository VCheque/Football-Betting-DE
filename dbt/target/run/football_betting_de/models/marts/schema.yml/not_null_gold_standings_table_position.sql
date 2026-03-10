
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select table_position
from "semantic"."gold_standings"
where table_position is null



  
  
      
    ) dbt_internal_test