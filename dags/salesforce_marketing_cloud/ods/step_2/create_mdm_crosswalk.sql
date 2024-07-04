create or replace table `salesforce_marketing_cloud_ods.sfmc_mdm_crosswalk` as
with subscribers as (
    select distinct 
        subscriber_key, subscriber_id
        , case when subscriber_key like '%\\_%' then split(subscriber_key,'_')[SAFE_OFFSET(0)] else null end as possible_pim
    from `ds-00-191017.salesforce_marketing_cloud_ods.subscribers`
)

select
    a.*
    , coalesce(zc.accountid, za.id, za.account_id__c) as account_id
    , max(coalesce(zmdm.savvy_pid, zmdm2.savvy_pid, lmdm.savvy_pid, lmdm2.savvy_pid, lmdm3.savvy_pid ,lmdm4.savvy_pid, -1)) as savvy_pid
    , max(coalesce(zmdm.savvy_did, zmdm2.savvy_did, lmdm.savvy_did, lmdm2.savvy_did, lmdm3.savvy_did ,lmdm4.savvy_did, -1)) as savvy_did
    , max(case when coalesce(zmdm2.savvy_pid, lmdm2.savvy_pid, lmdm4.savvy_pid) is not null then 1 else 0 end) as is_restricted

from subscribers as a 
left join `ds-00-191017.salesforce_zeus_final.Contact` as zc
    on a.subscriber_key = zc.id
left join `ds-00-191017.salesforce_zeus_final.Account` as za
    on a.subscriber_key = za.id
left join `ds-00-191017.salesforce_zeus_final.CareProgramEnrollee` as cpe
    on coalesce(zc.accountid, za.id, za.account_id__c) = coalesce(cpe.accountid, cpe.accountid__c)
    and (zc.id is not null or za.id is not null)
left join `ds-00-191017.df_mdm_crosswalk_r2.pim_mdm_crosswalk` as zmdm
    on coalesce(nullif(trim(cpe.srckeyid1__c), ''), a.possible_pim) = zmdm.src_key_1
    --on nullif(trim(cpe.srckeyid1__c), '') = zmdm.src_key_1
    and zmdm.src_key_label_1 = 'pim_id'
left join `ds-00-191017.df_mdm_crosswalk_restricted_r2.pim_mdm_crosswalk` as zmdm2
    on coalesce(nullif(trim(cpe.srckeyid1__c), ''), a.possible_pim) = zmdm2.src_key_1
    --on nullif(trim(cpe.srckeyid1__c), '') = zmdm2.src_key_1
    and zmdm2.src_key_label_1 = 'pim_id'

left join `ds-00-191017.df_mdm_crosswalk_r2.salesforce_mdm_crosswalk` as lmdm
    on a.subscriber_key = lmdm.src_key_1
    and lmdm.src_key_label_1 = 'candidate_id'
    and (zc.id is null and za.id is null)
left join `ds-00-191017.df_mdm_crosswalk_restricted_r2.salesforce_mdm_crosswalk` as lmdm2
    on a.subscriber_key = lmdm2.src_key_1
    and lmdm2.src_key_label_1 = 'candidate_id'
    and (zc.id is null and za.id is null)

left join `ds-00-191017.df_mdm_crosswalk_r2.salesforce_mdm_crosswalk` as lmdm3
    on a.possible_pim = lmdm3.src_key_1
    and lmdm3.src_key_label_1 = 'candidate_id'
    and (zc.id is null and za.id is null)
left join `ds-00-191017.df_mdm_crosswalk_restricted_r2.salesforce_mdm_crosswalk` as lmdm4
    on a.possible_pim = lmdm4.src_key_1
    and lmdm4.src_key_label_1 = 'candidate_id'
    and (zc.id is null and za.id is null)

group by 1, 2, 3, 4