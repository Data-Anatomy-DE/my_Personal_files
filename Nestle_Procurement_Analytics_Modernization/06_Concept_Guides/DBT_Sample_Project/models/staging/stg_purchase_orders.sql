with source as (

    select * from {{ source('raw_procurement', 'purchase_orders') }}

),

renamed as (

    select
        po_id,
        po_line_num,
        po_date,
        supplier_id,
        plant_id,
        material_id,
        order_qty,
        uom,
        unit_price,
        currency,
        line_amount,
        requested_delivery_date,
        po_status,
        buyer_name,
        payment_terms_days
    from source

)

select * from renamed
