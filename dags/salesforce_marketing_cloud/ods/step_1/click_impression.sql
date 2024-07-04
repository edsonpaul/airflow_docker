--create or replace table `salesforce_marketing_cloud_ods.click_impression` as
insert into `salesforce_marketing_cloud_ods.click_impression`
select generate_uuid() as optumlabs_load_id
    , a.*
    , current_timestamp() as optumlabs_load_utc_timestamp
from (
    select distinct
        cast(src.client_id as int64) as client_id
        , cast(src.send_id as int64) as send_id
        , cast(nullif(trim(src.subscriber_key), '') as string) as subscriber_key
        , cast(nullif(trim(src.email_address), '') as string) as email_address
        , cast(nullif(trim(src.subscriber_id), '') as string) as subscriber_id
        , cast(src.list_id as int64) as list_id
        , parse_timestamp('%m/%d/%Y %I:%M:%S %p',  nullif(trim(src.event_date), '') /*, 'UTC-6'*/) as event_date
        , cast(nullif(trim(src.event_type), '') as string) as event_type
        , cast(nullif(trim(src.send_urlid), '') as string) as send_urlid
        , cast(nullif(trim(src.urlid), '') as string) as urlid
        , cast(nullif(trim(src.url), '') as string) as url
        , cast(nullif(trim(src.alias), '') as string) as alias
        , cast(src.batch_id as int64) as batch_id
        , cast(nullif(trim(src.triggered_send_external_key), '') as string) as triggered_send_external_key
        , cast(nullif(trim(src.impression_region_name), '') as string) as impression_region_name
    from (
        select * from `salesforce_marketing_cloud_etl.level2_clickimpression`
        union all select * from `salesforce_marketing_cloud_etl.pilots_clickimpression`
    ) as src
) as a
;