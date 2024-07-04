insert into `salesforce_marketing_cloud_ods.multiple_data_extension_send_lists`
select generate_uuid() as optumlabs_load_id
    , a.*
    , current_timestamp() as optumlabs_load_utc_timestamp
from (
     select distinct
        cast(src.client_id as int64) as client_id
        , cast(src.send_id as int64) as send_id
        , cast(src.list_id as int64) as list_id
        , cast(nullif(trim(src.data_extension_name), '') as string) as data_extension_name
        , cast(nullif(trim(src.status), '') as string) as status
        , parse_timestamp('%m/%d/%Y %I:%M:%S %p',  nullif(trim(src.date_created), '') /*, 'UTC-6'*/) as date_created
        , cast(src.de_client_id as int64) as de_client_id
    from (
        select a.* from (
            select * from `salesforce_marketing_cloud_etl.all_bu_multipledataextensionsendlists`
            union all select * from `salesforce_marketing_cloud_etl.level2_multipledataextensionsendlists`
            union all select * from `salesforce_marketing_cloud_etl.pilots_multipledataextensionsendlists`
        ) as a
        left join `salesforce_marketing_cloud_ods.multiple_data_extension_send_lists` as b
            on cast(a.send_id as int64) = b.send_id
            and cast(a.list_id as int64) = b.list_id
            and cast(a.client_id as int64) = b.client_id
        where b.optumlabs_load_id is null
    ) as src
) as a
;