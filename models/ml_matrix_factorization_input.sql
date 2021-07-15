{{
    config(
        materialized='view',
        schema='mat_fact_blog',
        enabled = true
    )
}}


select
  review_profile_name as user,
  beer_id             as item,
  review_overall      as rating
from {{ ref('beer_reviews_clean') }}
