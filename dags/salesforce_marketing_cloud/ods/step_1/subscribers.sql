--create or replace table `salesforce_marketing_cloud_ods.subscribers` as
insert into `salesforce_marketing_cloud_ods.subscribers`
select generate_uuid() as optumlabs_load_id
    , a.*
    , current_timestamp() as optumlabs_load_utc_timestamp
from (
    select distinct
        cast(src.client_id as int64) as client_id
        , cast(nullif(trim(src.subscriber_key), '') as string) as subscriber_key
        , cast(nullif(trim(src.email_address), '') as string) as email_address
        , cast(nullif(trim(src.subscriber_id), '') as string) as subscriber_id
        , cast(nullif(trim(src.status), '') as string) as status
        , parse_timestamp('%m/%d/%Y %I:%M:%S %p',  nullif(trim(src.date_held), '') /*, 'UTC-6'*/) as date_held
        , parse_timestamp('%m/%d/%Y %I:%M:%S %p',  nullif(trim(src.date_created), '') /*, 'UTC-6'*/) as date_created
        , parse_timestamp('%m/%d/%Y %I:%M:%S %p',  nullif(trim(src.date_unsubscribed), '') /*, 'UTC-6'*/) as date_unsubscribed
    from (
        select a.* from (
            select * from `salesforce_marketing_cloud_etl.level2_subscribers`
            union all select * from `salesforce_marketing_cloud_etl.pilots_subscribers`
        ) as a
        left join `salesforce_marketing_cloud_ods.subscribers` as b
            on a.subscriber_key = nullif(trim(b.subscriber_key), '')
            and a.subscriber_id = nullif(trim(b.subscriber_id), '')
            and cast(a.client_id as int64) = b.client_id
        where b.optumlabs_load_id is null
    ) as src
) as a
;