with base as (
  select distinct session_id, AH_Successful_Login, test_member_indicator, pe32_has_value
  from `r `
  where test_member_indicator is not null
    and session_id is not null
    and AH_Successful_Login is not null
    and pe32_has_value is not null 
),

step_1 as (
  select 'Step 1: All Visits' as step, count(*) as visit_cnt from base
),

step_2 as (
  select 'Step 2: Remove Impersonation' as step, count(*) as visit_cnt
  from base
  where test_member_indicator != 'IMPERSONATOR'
),

step_3 as (
  select 'Step 3: Member Field Cleaned' as step, count(*) as visit_cnt
  from base
  where test_member_indicator in ('Y', 'N')
),

step_4 as (
  select 'Step 4: Member Field Has Value' as step, count(*) as visit_cnt
  from base
  where test_member_indicator = 'Y'
),

step_5 as (
  select 'Step 5: Successful Logins' as step, count(*) as visit_cnt
  from base
  where AH_Successful_Login = 1
)

select * from step_1
union all
select * from step_2
union all
select * from step_3
union all
select * from step_4
union all
select * from step_5;
