insert into `salesforce_marketing_cloud_ods.list_membership`
select generate_uuid() as optumlabs_load_id
    , a.*
    , current_timestamp() as optumlabs_load_utc_timestamp
from (
    select distinct
        cast(src.client_id as int64) as client_id
        , cast(nullif(trim(src.subscriber_key), '') as string) as subscriber_key
        , cast(nullif(trim(src.email_address), '') as string) as email_address
        , cast(nullif(trim(src.subscriber_id), '') as string) as subscriber_id
        , cast(src.list_id as int64) as list_id
        , cast(nullif(trim(src.list_name), '') as string) as list_name
        , parse_timestamp('%m/%d/%Y %I:%M:%S %p',  nullif(trim(src.date_joined), '') /*, 'UTC-6'*/) as date_joined
        , cast(nullif(trim(src.join_type), '') as string) as join_type
        , parse_timestamp('%m/%d/%Y %I:%M:%S %p',  nullif(trim(src.date_unsubscribed), '') /*, 'UTC-6'*/) as date_unsubscribed
        , cast(nullif(trim(src.unsubscribe_reason), '') as string) as unsubscribe_reason
    from(
        select a.* from (
            select * from `salesforce_marketing_cloud_etl.all_bu_listmembership`
            union all select * from `salesforce_marketing_cloud_etl.level2_listmembership`
            union all select * from `salesforce_marketing_cloud_etl.pilots_listmembership`
        ) as a
        left join `salesforce_marketing_cloud_ods.list_membership` as b
            on nullif(trim(a.subscriber_id), '') = b.subscriber_id
            and nullif(trim(a.subscriber_key), '') = b.subscriber_key
            and cast(a.list_id as int64) = b.list_id
            and cast(a.client_id as int64) = b.client_id
        where b.optumlabs_load_id is null
            or parse_timestamp('%m/%d/%Y %I:%M:%S %p',  nullif(trim(a.date_joined), '')) <> b.date_joined
            or parse_timestamp('%m/%d/%Y %I:%M:%S %p',  nullif(trim(a.date_unsubscribed), '')) <> b.date_unsubscribed
            or nullif(trim(a.join_type), '') <> b.join_type
            or nullif(trim(a.unsubscribe_reason), '')<> b.unsubscribe_reason
    )as src
) as a
;