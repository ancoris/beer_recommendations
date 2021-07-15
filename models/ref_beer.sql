{{
    config(
        materialized='view',
        schema='mat_fact_blog',
        enabled = true
    )
}}


select distinct
  beer_name,
  beer_id,
  beer_abv,
  beer_style,
from {{ ref('beer_reviews_clean') }}
