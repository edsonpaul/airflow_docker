--create or replace table `salesforce_marketing_cloud_ods.attributes` as
insert into `salesforce_marketing_cloud_ods.attributes`
select generate_uuid() as optumlabs_load_id
    , a.*
    , current_timestamp() as optumlabs_load_utc_timestamp
from (
    select distinct
        cast(src.client_id as int64) as client_id
        , cast(nullif(trim(src.subscriber_key), '') as string) as subscriber_key
        , cast(nullif(trim(src.email_address), '') as string) as email_address
        , cast(nullif(trim(src.subscriber_id), '') as string) as subscriber_id
    from (
        select a.* from (
            select * from `salesforce_marketing_cloud_etl.level2_attributes`
            union distinct select * from `salesforce_marketing_cloud_etl.pilots_attributes`
        ) as a
        left join `salesforce_marketing_cloud_ods.attributes` as b
            on a.subscriber_key = nullif(trim(b.subscriber_key), '')
            and a.subscriber_id = nullif(trim(b.subscriber_id), '')
        where b.optumlabs_load_id is null
    ) as src
) as a
;