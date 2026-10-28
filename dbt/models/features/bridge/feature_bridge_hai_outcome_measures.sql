{{ config(materialized='table') }}

select *
from {{ ref('bridge_measure_control') }}
where source_seed = 'hai_outcome_measures'
