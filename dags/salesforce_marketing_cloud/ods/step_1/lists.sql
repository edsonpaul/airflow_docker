--create or replace table `salesforce_marketing_cloud_ods.lists` as
insert into `salesforce_marketing_cloud_ods.lists`
select generate_uuid() as optumlabs_load_id
    , a.*
    , current_timestamp() as optumlabs_load_utc_timestamp
from (
    select distinct
        cast(src.client_id as int64) as client_id
        , cast(src.list_id as int64) as list_id
        , cast(nullif(trim(src.name), '') as string) as name
        , cast(nullif(trim(src.description), '') as string) as description
        , parse_timestamp('%m/%d/%Y %I:%M:%S %p',  nullif(trim(src.date_created), '') /*, 'UTC-6'*/) as date_created
        , cast(nullif(trim(src.status), '') as string) as status
        , cast(nullif(trim(src.list_type), '') as string) as list_type
    from (
        select a.* from (
            select * from `salesforce_marketing_cloud_etl.all_bu_lists`
            union all select * from `salesforce_marketing_cloud_etl.level2_lists`
            union all select * from `salesforce_marketing_cloud_etl.pilots_lists`
        ) as a
        left join `salesforce_marketing_cloud_ods.lists` as b
            on cast(a.list_id as int64) = b.list_id
            and cast(a.client_id as int64) = b.client_id
        where b.optumlabs_load_id is null
    ) as src
) as a
;