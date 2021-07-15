{{
    config(
        materialized='view',
        schema='mat_fact_blog',
        enabled = true
    )
}}


select
  review_profile_name as user_name,
  dense_rank() over (order by review_profile_name asc) as user_id
from {{ ref('beer_reviews_clean') }}
group by review_profile_name
