with source as (

    select * from {{ source('raw_procurement', 'supplier_master') }}

),

renamed as (

    select
        supplier_id,
        supplier_name,
        supplier_category,
        country,
        region,
        city,
        preferred_currency,
        payment_terms_days,
        supplier_status,
        risk_tier,
        onboarded_date,
        primary_contact_email
    from source

)

select * from renamed
