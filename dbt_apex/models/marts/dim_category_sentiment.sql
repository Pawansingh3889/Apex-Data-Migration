-- Mart: product category sentiment aggregation
-- Replaces the Polars groupby in Phase 3 / Notebook 2 — now a tested SQL artifact
-- Source: category_sentiment table (one row per review, joined with product category)
select
    category,
    count(*)                                                                    as review_count,
    round(avg(star_rating), 2)                                                  as avg_stars,

    -- Sentiment distribution
    round(sum(case when sentiment = 'Positive' then 1 else 0 end) * 100.0 / count(*), 1) as pct_positive,
    round(sum(case when sentiment = 'Negative' then 1 else 0 end) * 100.0 / count(*), 1) as pct_negative,
    round(sum(case when sentiment = 'Neutral'  then 1 else 0 end) * 100.0 / count(*), 1) as pct_neutral,

    -- Dominant sentiment (most frequent label)
    mode(sentiment)                                                             as dominant_sentiment,

    -- Complaint flag (useful for Power BI conditional formatting)
    case when sum(case when sentiment = 'Negative' then 1 else 0 end) * 100.0 / count(*) > 30
         then true else false end                                               as is_high_complaint_category

from {{ source('apex', 'category_sentiment') }}
where category != 'Unknown'
group by category
having count(*) >= 3
order by avg_stars desc
