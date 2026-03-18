-- Staging: customer reviews classified by Mistral 7B in Phase 3
-- Only exposes columns needed by mart models
select
    review_id,
    order_id,
    review_score::integer   as review_score,
    sentiment,
    review_comment_message  as review_text

from {{ source('apex', 'sentiment') }}
where sentiment in ('Positive', 'Neutral', 'Negative')  -- enforce contract
