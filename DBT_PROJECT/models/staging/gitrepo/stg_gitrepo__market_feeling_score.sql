select
  symbol_type,
  market_feeling_label,
  market_feeling_score::INT as market_feeling_score
from {{ source('gitrepo', 'MARKET_FEELING_SCORE') }}