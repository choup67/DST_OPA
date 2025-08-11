select *
from {{ source('calendar', 'CALENDAR') }}