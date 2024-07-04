create or replace table studybase_ods.studybase_participants_demographics as
select
  cast(pa.participantid as int) as participantid
  , cast(pa.cohortid as int) as cohortid
  , cast(pa.currentrefstatusid as int) as currentrefstatusid
  , cast(pa.memberid as int) as memberid
  , mrn.itemserialnumber as mrn_number
  , pa.salesforcecandidateid
  , nullif(trim(lower(pa.email)), '') as email
  , nullif(trim(lower(pa.firstname)), '') as firstname
  , nullif(trim(lower(pa.lastname)), '') as lastname
  , nullif(trim(lower(pa.birthdate)), '') as birthdate
  , nullif(trim(lower(pa.address_1)), '') as  address1
  , nullif(trim(lower(pa.address_2)), '') as  address2
  , nullif(trim(lower(pa.city)), '') as city
  , nullif(trim(lower(pa.statecode)), '') as statecode
  , nullif(trim(lower(pa.phonenumber)), '') as phonenumber
  , nullif(trim(lower(pa.gender)), '') as gender
  ,'AWS.prod-t2d-rds.t2d.studybase' as dataset_source
  , nullif(trim(lower(pa.zipcode)), '') as zipcode
from (
  select
    a.participantid
    , a.cohortid
    , a.currentrefstatusid
    , a.memberid
    , a.email
    , a.firstname
    , a.lastname
    , a.birthdate as birthdate
    , a.address_1
    , a.address_2
    , a.city
    , a.statecode
    , a.phonenumber
    , a.gender
    , a.salesforcecandidateid
    , a.zipcode
  from studybase_etl.participant a
  where lower(a.email) not like '%test%' 
    and lower(a.email) not like '%prego%'
    and lower(a.email) not like '%savvysherpa%'
    and lower(a.email) not like '%pgsfeasibility%'
    and lower(a.email) not like '%fitbit%'
    and lower(a.email) not like '%quimod%'
    and lower(a.email) not like '%base.study%'

    and lower(a.firstname) not like '%prego%'
    and lower(a.firstname) not like '%anon%'
    and lower(a.firstname) not like '%test%'
    and lower(a.firstname) not like '%fitbit%'
    and lower(a.firstname) not like '%admin%'
    and lower(a.firstname) not like '%quimod%'
    and lower(a.firstname) not like '%participant%'
    and lower(a.firstname) not like '%blossom%'
    and lower(a.firstname) not like '%demo%'
    and lower(a.firstname) not like '%user%'

    and lower(a.lastname) not like '%prego%'
    and lower(a.lastname) not like '%anon%'
    and lower(a.lastname) not like '%test%'
    and lower(a.lastname) not like '%fitbit%'
    and lower(a.lastname) not like '%admin%'
    and lower(a.lastname) not like '%quimod%'
    and lower(a.lastname) not like '%participant%'
    and lower(a.lastname) not like '%blossom%'
    and lower(a.lastname) not like '%demo%'
    and lower(a.lastname) not like '%user%'
    
    and a.birthdate is not null
  ) as pa
left join (
  select distinct a.participantid, itm.itemid, itm.itemserialnumber
  from studybase_etl.participant a
  left join studybase_etl.itemlocation loc
    on loc.participantid = a.participantid
  left join studybase_etl.item itm on itm.itemid = loc.itemid
  inner join studybase_etl.refitemtype r
    on r.refitemtypeid = itm.refitemtypeid and r.itemtype = 'MRN_Number'
  order by a.participantid
  )as mrn
  on pa.participantid = mrn.participantid
  ;