select *
from {{ source('calendar', 'CALENDAR') }}
WHERE date_full <= CURRENT_DATE