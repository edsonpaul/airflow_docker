--create or replace table `salesforce_marketing_cloud_ods.status_change` as
insert into `salesforce_marketing_cloud_ods.status_change`
select generate_uuid() as optumlabs_load_id
    , a.*
    , current_timestamp() as optumlabs_load_utc_timestamp
from (
    select distinct
        cast(src.client_id as int64) as client_id
        , cast(nullif(trim(src.subscriber_key), '') as string) as subscriber_key
        , cast(nullif(trim(src.email_address), '') as string) as email_address
        , cast(nullif(trim(src.subscriber_id), '') as string) as subscriber_id
        , cast(nullif(trim(src.old_status), '') as string) as old_status
        , cast(nullif(trim(src.new_status), '') as string) as new_status
        , parse_timestamp('%m/%d/%Y %I:%M:%S %p',  nullif(trim(src.date_changed), '') /*, 'UTC-6'*/) as date_changed
    from (
        select * from `salesforce_marketing_cloud_etl.all_bu_statuschange`
        union all select * from `salesforce_marketing_cloud_etl.level2_statuschange`
        union all select * from `salesforce_marketing_cloud_etl.pilots_statuschange`
    ) as src
) as a
;