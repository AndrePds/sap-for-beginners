# silver_schemas.py  –  Reference column list for Silver L1 and L2 tables.
#
# Use this file to:
#   1. Validate Silver output columns match expected schema
#   2. Understand type conventions applied during Bronze -> Silver casting
#
# Silver type conventions (applied in silver_pipeline.py):
#   SAP DATS (YYYYMMDD string) -> DateType
#   SAP CURR / DEC             -> DecimalType(17, 2)
#   SAP QUAN                   -> DecimalType(17, 3)
#   SAP CHAR / NUMC / CUKY     -> StringType (unchanged from Bronze)

SILVER_L1_COLUMNS = {
    "silver_kna1":      ["kunnr", "name1", "mcod1", "cnpj_cpf", "ktokd",
                         "city", "state", "country"],
    "silver_lfa1":      ["lifnr", "name1", "mcod1", "cnpj_cpf",
                         "city", "state", "country"],
    "silver_mara":      ["matnr", "matkl", "meins", "gewei", "prdha",
                         "ntgew", "brgew", "bismt"],
    "silver_makt":      ["matnr", "material_desc"],
    "silver_pa0001":    ["pernr", "ename", "start_date", "end_date",
                         "plans", "orgeh"],
    "silver_vbak":      ["vbeln", "kunnr", "order_date", "created_date",
                         "requested_delivery_date", "auart", "vkorg",
                         "vtweg", "spart", "vbtyp", "knumv",
                         "netwr", "waerk", "augru", "faksk", "lifsk",
                         "gbstk", "vkgrp", "vkbur", "vsbed", "vgbel"],
    "silver_vbap":      ["vbeln", "posnr", "matnr", "werks", "lgort",
                         "meins", "vrkme", "netwr", "waerk",
                         "kwmeng", "kbmeng", "charg", "route", "matkl",
                         "requested_date", "fksta", "lfsta", "fkrel"],
    "silver_vbrk":      ["vbeln", "billing_date", "created_date", "fkart",
                         "vbtyp", "kunrg", "knumv", "netwr", "waerk",
                         "tax_amount", "cancelled_flag", "cancel_ref_doc", "zlsch"],
    "silver_vbrp":      ["vbeln", "posnr", "matnr", "meins",
                         "netwr", "waerk", "billed_qty", "item_tax",
                         "vgbel", "vgpos", "aubel", "aupos"],
    "silver_likp":      ["vbeln", "kunnr", "vkorg", "tdlnr",
                         "planned_delivery_date", "actual_goods_issue_date",
                         "created_date", "lfart", "trspg"],
    "silver_lips":      ["vbeln", "posnr", "matnr", "meins", "vrkme",
                         "lgort", "werks", "delivered_qty",
                         "vgbel", "vgpos", "export_date"],
    "silver_ekko":      ["ebeln", "bukrs", "lifnr", "ekgrp", "bsart",
                         "po_date", "last_change_date", "waers"],
    "silver_ekpo":      ["ebeln", "ebelp", "matnr", "werks", "meins",
                         "qty_ordered", "net_price", "price_unit", "waers"],
    "silver_ekbe":      ["ebeln", "ebelp", "zekkn", "belnr", "gjahr",
                         "vgabe", "qty", "amount", "waers"],
    "silver_j_1bnfdoc": ["docnum", "bukrs", "nfnum", "bupla", "serie",
                         "refkey", "stcd1", "direct", "docdat"],
    "silver_j_1bnflin": ["docnum", "itmnum", "matnr", "cfop",
                         "nf_qty", "nf_unit_price", "nf_value"],
    "silver_j_1bnfstx": ["docnum", "itmnum", "taxtyp", "tax_value", "tax_rate"],
    "silver_bkpf":      ["bukrs", "belnr", "gjahr", "posting_date",
                         "document_date", "waers", "awkey", "awtyp"],
}

SILVER_L2_COLUMNS = {
    "dim_customer":        ["kunnr", "name1", "mcod1", "cnpj_cpf", "ktokd",
                            "city", "state", "country"],
    "dim_material":        ["matnr", "matkl", "meins", "prdha",
                            "ntgew", "brgew", "material_desc"],
    "dim_vendor":          ["lifnr", "name1", "mcod1", "cnpj_cpf",
                            "city", "state", "country"],
    "fact_sales_order":    ["vbeln", "posnr", "kunnr", "order_date",
                            "order_type", "vkorg", "vtweg", "spart",
                            "overall_status", "matnr", "werks", "meins",
                            "qty_ordered", "item_net_value", "waerk",
                            "billing_status", "delivery_status"],
    "fact_billing":        ["vbeln", "posnr", "kunnr", "billing_date",
                            "billing_type", "waerk", "matnr", "meins",
                            "billed_qty", "item_net_value", "item_tax",
                            "doc_tax", "source_delivery", "source_order"],
    "fact_delivery":       ["vbeln", "posnr", "kunnr",
                            "planned_delivery_date", "actual_goods_issue_date",
                            "matnr", "meins", "delivered_qty",
                            "source_order", "source_order_item"],
    "fact_purchase_order": ["ebeln", "ebelp", "lifnr", "bukrs", "po_date",
                            "po_type", "matnr", "meins",
                            "qty_ordered", "net_price",
                            "qty_received", "amount_invoiced"],
    "fact_nota_fiscal":    ["docnum", "itmnum", "bukrs", "nfnum",
                            "bupla", "serie", "direct", "docdat",
                            "matnr", "cfop", "nf_qty", "nf_value",
                            "taxtyp", "tax_value", "tax_rate"],
}
