{{
    config(
        materialized='ml_model_mat_fact',
        schema='mat_fact_blog',
        model_options='model_type="matrix_factorization", feedback_type = "explicit",
        user_col="user", item_col="item", rating_col="rating",
        l2_reg=0.2, data_split_method = "auto_split"',
        tags=['ml_model'],
        enabled = true
    )
}}

select
  user,
  item,
  rating
from
  {{ ref('ml_matrix_factorization_input') }}
