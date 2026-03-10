select
    cast(run_id as varchar) as run_id,
    cast(source_name as varchar) as source_name,
    cast(entity_name as varchar) as entity_name,
    cast(status as varchar) as status,
    started_at,
    completed_at,
    row_count,
    cast(checksum as varchar) as checksum,
    cast(error_message as varchar) as error_message
from "semantic"."pipeline_run"