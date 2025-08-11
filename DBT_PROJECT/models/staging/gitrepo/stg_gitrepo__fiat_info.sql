select *
from {{ source('gitrepo', 'FIAT_INFO') }}