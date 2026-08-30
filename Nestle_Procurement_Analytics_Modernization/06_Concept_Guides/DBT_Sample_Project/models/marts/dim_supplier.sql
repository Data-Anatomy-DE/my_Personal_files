select
    {{ surrogate_key(['supplier_id']) }} as supplier_key,
    supplier_id,
    supplier_name,
    supplier_category,
    country,
    region,
    city,
    preferred_currency,
    supplier_status,
    risk_tier,
    onboarded_date
from {{ ref('stg_suppliers') }}
