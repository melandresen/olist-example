with source as (
        select * from {{ source('olist', 'customers') }}
  ),
  renamed as (
      select
          {{ adapter.quote("CUSTOMER_ID") }},
        {{ adapter.quote("CUSTOMER_UNIQUE_ID") }},
        {{ adapter.quote("CUSTOMER_ZIP_CODE_PREFIX") }},
        {{ adapter.quote("CUSTOMER_CITY") }},
        {{ adapter.quote("CUSTOMER_STATE") }}

      from source
  )
  select * from renamed
    