------------------------------------------------------------------------------------------------------------------------
-- Authors: BSL & JG
-- This materialisation is used to build a matrix factorization ml model
-- required arguments in model config are model_options & user_feature
------------------------------------------------------------------------------------------------------------------------
{% materialization ml_model_mat_fact, default %}

{% set target_relation = this %}
{% set invoke_time = meta_process_time() %}

-- 1. Reserve slots
{% set reserve_slots_sql %}
declare status string;
declare max_attempts int64;
declare attempts int64;

  {%- if adapter.get_relation(this.database, 'dw_utils', 'slot_requests') is none %} --hardcode
  create or replace table `dw_utils.slot_requests`
  as
    select
      {{- invoke_time }}                                 as request_time,
      1                                                  as request_no,
      500                                                as slot_count,
      360                                                as max_duration_mins,
      'Pending purchase'                                 as status,
      'reservation-' || cast(current_date as string)
      || '-t' || format_time("%H", current_time())
      || '-' || format_time("%M", current_time)
      || '-' || format_time("%S", current_time)          as reservation_name, --eg reservation-2020-07-03-t11-34-43
      ''                                                 as reservation_id, --updated by cloud function later
      ''                                                 as assignment_id,  --updated by cloud function later
      ''                                                 as commitment_id,  --updated by cloud function later
      timestamp_add(current_timestamp(), interval 60 minute) as cancel_time,
      null                                               as duration_mins;  --updated by cloud function later, is duration for which slots were open

  {%- else %}

  insert into `dw_utils.slot_requests`
    select
      {{- invoke_time }}                                 as request_time,
      (select ifnull(max(request_no),0)+1 from `dw_utils.slot_requests`) as request_no,
      500                                                as slot_count,
      240                                                as max_duration_mins,
      'Pending purchase'                                 as status,
      'reservation-' || cast(current_date as string)
      || '-t' || format_time("%H", current_time())
      || '-' || format_time("%M", current_time)
      || '-' || format_time("%S", current_time)          as reservation_name, --eg reservation-2020-07-03-t11-34-43
      ''                                                 as reservation_id, --updated by cloud function later
      ''                                                 as assignment_id,  --updated by cloud function later
      ''                                                 as commitment_id,  --updated by cloud function later
      timestamp_add(current_timestamp(), interval 60 minute) as cancel_time,
      null                                               as duration_mins;   --updated by cloud function later, is duration for which slots were open
  {% endif %}
{% endset %}


-- 2. Create stored procedure
{% set build_stored_procedure %}
  create procedure if not exists `dw_utils`.sleep(secs_wait int64)
    begin
    declare start timestamp;
    declare secs_elapsed int64;

    set secs_elapsed = 0;
    set start = current_timestamp();

    while secs_elapsed < secs_wait do
    set secs_elapsed = timestamp_diff(current_timestamp(), start, SECOND);
    end while;
  end;
{% endset %}


--3. Poll for Purchased status, then Create model

{% set build_model %}

set status = '';
set max_attempts = 24;
set attempts = 0;

while status != 'Purchased' and attempts <= max_attempts do

  call `dw_utils`.sleep(10);

  set status = (select ifnull(status,'') status
                from `dw_utils.slot_requests`
                where request_time = {{- invoke_time }} --ensure returned status is the one launched by this run
                order by request_time desc
                limit 1);
  set attempts = attempts + 1;
end while;

create or replace model {{this.schema}}.{{this.name}}
        options( {{ model.config.model_options }} ) as
      {{sql}};
{% endset %}

-- 4. Update slot requests to Pending cancellation
{% set update_status_to_pending_cancellation %}
  update `dw_utils.slot_requests`
    set status = 'Pending cancellation'
    where status = 'Purchased'
    and request_time = {{- invoke_time }};
{% endset %}




{% call statement("main") %}
    {{ reserve_slots_sql }}
    {{ build_stored_procedure }}
    {{ build_model }}
    {{ update_status_to_pending_cancellation }}
{% endcall %}

{{ run_hooks(post_hooks, inside_transaction=True) }}

{% do adapter.commit() %}

{{ run_hooks(post_hooks, inside_transaction=False) }}

{{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
