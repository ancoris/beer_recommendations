{{
    config(
        materialized='table',
        schema='mat_fact_blog',
        tags=['post_mat_fact'],
        enabled = true
    )
}}
--NOTE this model may run into an error when run from command line
--but it completes when run from bigquery UI - weird!

-- this view predicts on ALL UNSEEN user-item pairs
-- then trims down to take X top recommended items per user
{% set desired_recommendation_no = 10 %} -- this variable defines the 'X'

with seen as (
select
  *,
  1 as seen_col
from {{ ref('beer_reviews_clean') }}
),

cte_one as (
select
  mm.user as review_profile_name,
  mm.item as beer_id,
  mm.predicted_rating,
  row_number() over (partition by mm.user order by mm.predicted_rating desc) as recommendation_rank,

  --additional pass through columns just to bump up the scan
  seen.brewery_id,
  seen.brewery_name,
  seen.review_time,
  seen.review_overall,
  seen.review_aroma,
  seen.review_appearance,
  seen.review_palate,
  seen.review_taste,
  seen.beer_style,
  seen.beer_name,
  seen.beer_abv
from
  ml.recommend(model {{ ref('ml_matrix_factorization') }}) as mm
left join seen
  on seen.review_profile_name = mm.user
  and seen.beer_id = mm.item
where seen_col is null -- null iff unseen
)

select
  co.review_profile_name,
  co.beer_id,
  co.predicted_rating,
  co.recommendation_rank,

  --beer info
  rb.beer_name,
  rb.beer_abv,
  rb.beer_style,

  --additional pass through columns just to bump up the scan
  co.brewery_id,
  co.brewery_name,
  co.review_time,
  co.review_overall,
  co.review_aroma,
  co.review_appearance,
  co.review_palate,
  co.review_taste,
from cte_one co
join {{ ref('ref_beer') }} rb
  using (beer_id)
where recommendation_rank <= {{desired_recommendation_no}}  -- now cap no. of recommendations passed to redis
