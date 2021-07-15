{{
    config(
        materialized='table',
        schema='mat_fact_blog',
        enabled = true
    )
}}
with cte as (
select
  brewery_id,
  brewery_name,
  review_time,
  review_overall,
  review_aroma,
  review_appearance,
  review_palate,
  review_taste,
  review_profilename                          as review_profile_name,
  beer_style,
  beer_name,
  beer_abv,
  beer_beerid                                 as beer_id,

  case
    when review_profilename is null then 0 --null user name
    when row_number() over (partition by review_profilename, beer_name order by review_time desc) > 1 then 0 --dupes
    else 1
  end                                         as meta_is_valid,
from {{ source('mat_fact_blog', 'beer_reviews_raw') }}
)

select
  brewery_id,
  brewery_name,
  review_time,
  review_overall,
  review_aroma,
  review_appearance,
  review_palate,
  review_taste,
  review_profile_name,
  beer_style,
  beer_name,
  beer_abv,
  beer_id,
from cte
where meta_is_valid = 1
