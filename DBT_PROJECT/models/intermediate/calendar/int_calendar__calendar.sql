select
    *,
    -- Flags période
    case when date_full >= date_trunc('year', current_date)  then true else false end as is_ytd,
    case when date_full >= date_trunc('month', current_date) then true else false end as is_mtd,
    case when date_full >= date_trunc('week', current_date)  then true else false end as is_wtd
from {{ ref('stg_calendar__calendar') }}