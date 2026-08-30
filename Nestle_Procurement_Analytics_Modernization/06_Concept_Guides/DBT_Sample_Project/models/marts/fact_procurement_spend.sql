{{
    config(
        materialized='incremental',
        unique_key=['po_id', 'po_line_num'],
        on_schema_change='append_new_columns'
    )
}}

with po as (

    select * from {{ ref('stg_purchase_orders') }}

    {% if is_incremental() %}
    -- only look at PO lines newer than what's already in this table
    where po_date > (select max(po_date) from {{ this }})
    {% endif %}

),

supplier as (

    select * from {{ ref('dim_supplier') }}

),

final as (

    select
        po.po_id,
        po.po_line_num,
        po.po_date,
        po.supplier_id,
        supplier.supplier_name,
        supplier.region,
        supplier.risk_tier,
        po.material_id,
        po.plant_id,
        po.order_qty,
        po.unit_price,
        po.currency,
        po.line_amount,
        po.po_status,
        po.requested_delivery_date
    from po
    left join supplier
        on po.supplier_id = supplier.supplier_id

)

select * from final
