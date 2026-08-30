{% snapshot supplier_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='supplier_id',
        strategy='check',
        check_cols=['risk_tier', 'supplier_status', 'preferred_currency', 'payment_terms_days']
    )
}}

-- Snapshots read the RAW source directly, not a staging model — we want to capture
-- history of what actually changed upstream, before any cleanup logic runs.
select * from {{ source('raw_procurement', 'supplier_master') }}

{% endsnapshot %}
