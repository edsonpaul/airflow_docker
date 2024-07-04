--create or replace table `salesforce_marketing_cloud_ods.opens` as
insert into `salesforce_marketing_cloud_ods.opens`
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
        , cast(src.batch_id as int64) as batch_id
        , cast(nullif(trim(src.triggered_send_external_key), '') as string) as triggered_send_external_key
        , cast(src.is_unique as bool) as is_unique
        , cast(nullif(trim(src.ip_address), '') as string) as ip_address
        , cast(nullif(trim(src.country), '') as string) as country
        , cast(nullif(trim(src.region), '') as string) as region
        , cast(nullif(trim(src.city), '') as string) as city
        , cast(nullif(trim(src.latitude), '') as string) as latitude
        , cast(nullif(trim(src.longitude), '') as string) as longitude
        , cast(nullif(trim(src.metro_code), '') as string) as metro_code
        , cast(nullif(trim(src.area_code), '') as string) as area_code
        , cast(nullif(trim(src.browser), '') as string) as browser
        , cast(nullif(trim(src.email_client), '') as string) as email_client
        , cast(nullif(trim(src.operating_system), '') as string) as operating_system
        , cast(nullif(trim(src.device), '') as string) as device
    from (
        select * from `salesforce_marketing_cloud_etl.level2_opens`
        union all select * from `salesforce_marketing_cloud_etl.pilots_opens`
    ) as src
) as a
;