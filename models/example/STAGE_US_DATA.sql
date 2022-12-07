{{ config(post_hook="truncate table {{source('DEVELOPER_DB','US_ACCIDENT_DATA_RAW_STAGE')}}",
materialized='table') }}
select to_number(to_char(START_TIME, 'yyyymmdd'))
 as date_id,* from {{source('DEVELOPER_DB','US_ACCIDENT_DATA_RAW_STAGE')}}