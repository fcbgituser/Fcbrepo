{{ config(materialized='view', tags=['raw']) }}

{%- set select_list = [
  "a.L_ORDERKEY AS ORDERKEY",
  "a.L_PARTKEY AS PARTKEY",
  "a.L_SUPPKEY AS SUPPLIERKEY",
  "a.L_LINENUMBER AS LINENUMBER",
  "a.L_QUANTITY AS QUANTITY",
  "a.L_EXTENDEDPRICE AS EXTENDEDPRICE",
  "a.L_DISCOUNT AS DISCOUNT",
  "a.L_TAX AS TAX",
  "a.L_RETURNFLAG AS RETURNFLAG",
  "a.L_LINESTATUS AS LINESTATUS",
  "a.L_SHIPDATE AS SHIPDATE",
  "a.L_COMMITDATE AS COMMITDATE",
  "a.L_RECEIPTDATE AS RECEIPTDATE",
  "a.L_SHIPINSTRUCT AS SHIPINSTRUCT",
  "a.L_SHIPMODE AS SHIPMODE",
  "a.L_COMMENT AS LINE_COMMENT",
  "b.O_CUSTKEY AS CUSTOMERKEY",
  "b.O_ORDERSTATUS AS ORDERSTATUS",
  "b.O_TOTALPRICE AS TOTALPRICE",
  "b.O_ORDERDATE AS ORDERDATE",
  "b.O_ORDERPRIORITY AS ORDERPRIORITY",
  "b.O_CLERK AS CLERK",
  "b.O_SHIPPRIORITY AS SHIPPRIORITY",
  "b.O_COMMENT AS ORDER_COMMENT",
  "c.C_NAME AS CUSTOMER_NAME",
  "c.C_ADDRESS AS CUSTOMER_ADDRESS",
  "c.C_NATIONKEY AS CUSTOMER_NATION_KEY",
  "c.C_PHONE AS CUSTOMER_PHONE",
  "c.C_ACCTBAL AS CUSTOMER_ACCBAL",
  "c.C_MKTSEGMENT AS CUSTOMER_MKTSEGMENT",
  "c.C_COMMENT AS CUSTOMER_COMMENT",
  "d.N_NAME AS CUSTOMER_NATION_NAME",
  "d.N_REGIONKEY AS CUSTOMER_REGION_KEY",
  "d.N_COMMENT AS CUSTOMER_NATION_COMMENT",
  "e.R_NAME AS CUSTOMER_REGION_NAME",
  "e.R_COMMENT AS CUSTOMER_REGION_COMMENT"
] -%}

{%- set joins = [
  { 'src_schema': 'tpch_sample', 'src_model': 'LINEITEM',  'alias': 'a', 'type': 'left', 'on': [['a.L_ORDERKEY','b.O_ORDERKEY']]},
  { 'src_schema': 'tpch_sample', 'src_model': 'CUSTOMER',  'alias': 'c', 'type': 'left', 'on': [['b.O_CUSTKEY','c.C_CUSTKEY']]},
  { 'src_schema': 'tpch_sample', 'src_model': 'NATION',    'alias': 'd', 'type': 'left', 'on': [['c.C_NATIONKEY','d.N_NATIONKEY']]},
  { 'src_schema': 'tpch_sample', 'src_model': 'REGION',    'alias': 'e', 'type': 'left', 'on': [['d.N_REGIONKEY','e.R_REGIONKEY']]},
  { 'src_schema': 'tpch_sample', 'src_model': 'PART',      'alias': 'g', 'type': 'left', 'on': [['a.L_PARTKEY','g.P_PARTKEY']]},
  { 'src_schema': 'tpch_sample', 'src_model': 'SUPPLIER',  'alias': 'h', 'type': 'left', 'on': [['a.L_SUPPKEY','h.S_SUPPKEY']]},
  { 'src_schema': 'tpch_sample', 'src_model': 'NATION',    'alias': 'j', 'type': 'left', 'on': [['h.S_NATIONKEY','j.N_NATIONKEY']]},
  { 'src_schema': 'tpch_sample', 'src_model': 'REGION',    'alias': 'k', 'type': 'left', 'on': [['j.N_REGIONKEY','k.R_REGIONKEY']]}
] -%}

{%- set where_clause = "b.O_ORDERDATE = TO_DATE('" ~ var('load_date') ~ "')" -%}

{{ raw_stage_table_generation('tpch_sample','ORDERS', select_list=select_list, joins=joins, where_clause=where_clause) }}
