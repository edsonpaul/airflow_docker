--create or replace table `salesforce_marketing_cloud_ods.send_job_impression` as
insert into `salesforce_marketing_cloud_ods.send_job_impression`
select generate_uuid() as optumlabs_load_id
    , a.*
    , current_timestamp() as optumlabs_load_utc_timestamp
from (
    select distinct
        cast(src.client_id as int64) as client_id
        , cast(src.send_id as int64) as send_id
        , cast(nullif(trim(src.impression_region_id), '') as string) as impression_region_id
        , cast(nullif(trim(src.impression_region_name), '') as string) as impression_region_name
        , cast(nullif(trim(src.fixedcontent), '') as string) as fixedcontent
        , parse_timestamp('%m/%d/%Y %I:%M:%S %p',  nullif(trim(src.event_date), '') /*, 'UTC-6'*/) as event_date
    from (
        select * from `salesforce_marketing_cloud_etl.level2_sendjobimpression`
        union all select * from `salesforce_marketing_cloud_etl.pilots_sendjobimpression`
    ) as src
) as a
;