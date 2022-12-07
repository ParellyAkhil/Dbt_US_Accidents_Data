{{ config(materialized='incremental',unique_key='ADD_ID') }}

with source_data2 as

    (select distinct CITY,_MODIFIED,COUNTRY, STATE, ZIPCODE, COUNTY, TIMEZONE, AIRPORT_CODE, STREET from {{source('DEVELOPER_DB','STAGE_US_DATA')}}

),

 second_data as(

        select  concat('A-', cast(seq.nextval as varchar)) as ADD_ID,

        CITY, COUNTRY, STATE, ZIPCODE, COUNTY, TIMEZONE, AIRPORT_CODE, STREET , _MODIFIED as LOAD_TIME from source_data2 order by ADD_ID

     )



select ADD_ID,CITY, COUNTRY, STATE, ZIPCODE, COUNTY, TIMEZONE, AIRPORT_CODE, STREET, LOAD_TIME from second_data