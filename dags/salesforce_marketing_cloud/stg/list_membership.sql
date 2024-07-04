create or replace table `salesforce_marketing_cloud_stg.list_membership` as
select
    coalesce(mdm.savvy_pid, -1) as savvy_pid
	, coalesce(mdm.savvy_did, - 1) as savvy_did
	, coalesce(mdm.is_restricted, 0) as is_restricted
	, src.*
from `salesforce_marketing_cloud_ods.list_membership` as src
left join `salesforce_marketing_cloud_ods.sfmc_mdm_crosswalk` as mdm
    on src.subscriber_key = mdm.subscriber_key
;