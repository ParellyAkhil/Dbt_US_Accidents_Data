{{ config(materialized='incremental') }}

with source_data as (
select distinct CITY,_MODIFIED,COUNTRY,STATE,ZIPCODE,COUNTY,TIMEZONE,AIRPORT_CODE,STREET from {{source('DEVELOPER_DB','STAGE_US_DATA')}}
),
second_data as (
select concat('A-', cast(seq_add.nextval as varchar)) as ADD_ID,
CITY, COUNTRY, STATE, ZIPCODE, COUNTY, TIMEZONE, AIRPORT_CODE, STREET ,_MODIFIED as LOAD_TIME from source_data
)

select  ADD_ID,
CITY, COUNTRY, STATE, ZIPCODE, COUNTY, TIMEZONE, AIRPORT_CODE, STREET , LOAD_TIME from second_data