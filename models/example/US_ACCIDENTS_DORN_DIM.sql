{{ config(materialized='incremental') }}

with source_data2 as (
select distinct SUNRISE_SUNSET,_MODIFIED,CIVIL_TWILIGHT,NAUTICAL_TWILIGHT,ASTRONOMICAL_TWILIGHT from {{source('DEVELOPER_DB','STAGE_US_DATA')}}
),
second_data2 as (
select   concat('A-', cast(seq_drn.nextval as varchar)) as D_ID,
SUNRISE_SUNSET, CIVIL_TWILIGHT, NAUTICAL_TWILIGHT, ASTRONOMICAL_TWILIGHT ,_MODIFIED as LOAD_TIME from source_data2
)

select  D_ID,
SUNRISE_SUNSET, CIVIL_TWILIGHT, NAUTICAL_TWILIGHT, ASTRONOMICAL_TWILIGHT, LOAD_TIME from second_data2
