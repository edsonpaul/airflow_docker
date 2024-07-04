--create or replace table `salesforce_marketing_cloud_ods.send_jobs` as
insert into `salesforce_marketing_cloud_ods.send_jobs`
select generate_uuid() as optumlabs_load_id
    , a.*
    , current_timestamp() as optumlabs_load_utc_timestamp
from (
    select distinct
        cast(src.client_id as int64) as client_id
        , cast(src.send_id as int64) as send_id
        , cast(nullif(trim(src.from_name), '') as string) as from_name
        , cast(nullif(trim(src.from_email), '') as string) as from_email
        , parse_timestamp('%m/%d/%Y %I:%M:%S %p',  nullif(trim(src.sched_time), '') /*, 'UTC-6'*/) as sched_time
        , parse_timestamp('%m/%d/%Y %I:%M:%S %p',  nullif(trim(src.sent_time), '') /*, 'UTC-6'*/) as sent_time
        , cast(nullif(trim(src.subject), '') as string) as subject
        , cast(nullif(trim(src.email_name), '') as string) as email_name
        , cast(nullif(trim(src.triggered_send_external_key), '') as string) as triggered_send_external_key
        , cast(nullif(trim(src.send_definition_external_key), '') as string) as send_definition_external_key
        , cast(nullif(trim(src.job_status), '') as string) as job_status
        , cast(nullif(trim(src.preview_url), '') as string) as preview_url
        , cast(src.is_multipart as bool) as is_multipart
        , cast(nullif(trim(src.additional), '') as string) as additional
    from (
        select a.* from (
            select * from `salesforce_marketing_cloud_etl.all_bu_sendjobs`
            union all select * from `salesforce_marketing_cloud_etl.level2_sendjobs`
            union all select * from `salesforce_marketing_cloud_etl.pilots_sendjobs`
        ) as a
        left join `salesforce_marketing_cloud_ods.send_jobs` as b
            on cast(a.send_id as int64) = b.send_id
            and cast(a.client_id as int64) = b.client_id
        where b.optumlabs_load_id is null
    ) as src
) as a
;
