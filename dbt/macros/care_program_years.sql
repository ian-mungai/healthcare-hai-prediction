{% macro care_program_rows(table, ccn, fiscal_year, columns) %}
{#- The rows of one Care Compare program table with their hospital and program fiscal year [514] [516]. -#}
select
    '{{ table }}' as bronze_table,
    nullif(trim({{ ccn }}), '') as ccn,
    {{ fiscal_year }} as fiscal_year,
    {%- for alias, expression in columns %}
    {{ expression }} as {{ alias }},
    {%- endfor %}
    stg._member_sha256 as member_sha256,
    stg._object_key as object_key,
    stg.is_label_held
from {{ ref('stg_' ~ table) }} as stg
{% endmacro %}

{% macro care_program_candidates(table, ccn, fiscal_year, columns) %}
{#- Usable rows with their file's latest publication date, ranked within each hospital and fiscal year [514]. -#}
with

program_rows as (
    {{ care_program_rows(table, ccn, fiscal_year, columns) }}
),

files as (
    select
        member_sha256,
        latest_publication_date
    from {{ ref('stg_bronze__files') }}
    where bronze_table = '{{ table }}'
),

dated as (
    select
        program_rows.*,
        files.latest_publication_date as release_date
    from program_rows
    inner join files on program_rows.member_sha256 = files.member_sha256
),

usable as (
    select *
    from dated
    where ccn is not null and fiscal_year is not null and not is_label_held and release_date is not null
),

latest as (
    select
        *,
        max(release_date) over (partition by ccn, fiscal_year) as latest_release_date,
        count(distinct member_sha256) over (partition by ccn, fiscal_year) as file_count
    from usable
)

select
    *,
    count(*) over (partition by ccn, fiscal_year) as latest_row_count,
    count(distinct member_sha256) over (partition by ccn, fiscal_year) as latest_file_count
from latest
where release_date = latest_release_date
{% endmacro %}

{% macro care_program_years(table, ccn, fiscal_year, columns) %}
{#- One row per hospital and program fiscal year from the latest dated file; conflicts are held instead [514]. -#}
with

candidates as (
    {{ care_program_candidates(table, ccn, fiscal_year, columns) }}
)

select
    ccn,
    fiscal_year,
    {%- for alias, expression in columns %}
    {{ alias }},
    {%- endfor %}
    member_sha256,
    object_key,
    release_date,
    file_count as release_file_count,
    ccn || ':' || fiscal_year as program_key
from candidates
where latest_row_count = 1
{% endmacro %}

{% macro care_program_holds(table, ccn, fiscal_year, columns) %}
{#- The rows a program-year model leaves out, with the reason and row count [514] [523]. -#}
with

program_rows as (
    {{ care_program_rows(table, ccn, fiscal_year, columns) }}
),

files as (
    select
        member_sha256,
        latest_publication_date
    from {{ ref('stg_bronze__files') }}
    where bronze_table = '{{ table }}'
),

reasons as (
    select
        program_rows.bronze_table,
        program_rows.ccn,
        program_rows.fiscal_year,
        case
            when program_rows.ccn is null then 'no_key'
            when program_rows.fiscal_year is null then 'no_fiscal_year'
            when program_rows.is_label_held then 'label_held'
            when files.latest_publication_date is null then 'undated_release'
        end as hold_reason
    from program_rows
    left join files on program_rows.member_sha256 = files.member_sha256
),

row_holds as (
    select
        bronze_table,
        hold_reason,
        case when hold_reason = 'no_key' then null else ccn end as ccn,
        cast(null as integer) as fiscal_year,
        count(*) as row_count
    from reasons
    where hold_reason is not null
    group by all
),

candidates as (
    {{ care_program_candidates(table, ccn, fiscal_year, columns) }}
),

year_holds as (
    select
        bronze_table,
        case when max(latest_file_count) > 1 then 'same_date_conflict' else 'repeated_in_file' end as hold_reason,
        ccn,
        fiscal_year,
        count(*) as row_count
    from candidates
    where latest_row_count > 1
    group by
        bronze_table,
        ccn,
        fiscal_year
)

select * from row_holds
union all by name
select * from year_holds
{% endmacro %}

{% macro hac_program_columns() %}
{#- The HAC columns staged: payment reduction, Total HAC Score, PSI-90 under either name and the domain z-scores; the
    republished HAI SIRs stay in bronze [516] [517] [518]. -#}
{{ return([
    ['payment_reduction_published', 'stg.payment_reduction'],
    ['is_payment_reduced', "case trim(stg.payment_reduction) when 'Yes' then true when 'No' then false end"],
    ['payment_reduction_footnote', 'stg.payment_reduction_footnote'],
    ['total_hac_score', 'stg.total_hac_score'],
    ['total_hac_score_footnote', 'coalesce(stg.total_hac_score_footnote, stg.total_hac_footnote)'],
    ['psi_90_value', 'coalesce(stg.psi_90_composite_value, stg.psi_90_composite)'],
    ['psi_90_w_z_score', 'stg.psi_90_w_z_score'],
    ['clabsi_w_z_score', 'stg.clabsi_w_z_score'],
    ['cauti_w_z_score', 'stg.cauti_w_z_score'],
    ['ssi_w_z_score', 'stg.ssi_w_z_score'],
    ['cdi_w_z_score', 'stg.cdi_w_z_score'],
    ['mrsa_w_z_score', 'stg.mrsa_w_z_score'],
    ['psi_90_start_date', 'stg.psi_90_start_date'],
    ['psi_90_end_date', 'stg.psi_90_end_date'],
    ['hai_measures_start_date', 'stg.hai_measures_start_date'],
    ['hai_measures_end_date', 'stg.hai_measures_end_date']
]) }}
{% endmacro %}

{% macro vbp_program_columns() %}
{#- The TPS columns the registry names [520]. -#}
{{ return([
    ['unweighted_normalized_safety_domain_score', 'stg.unweighted_normalized_safety_domain_score'],
    ['weighted_safety_domain_score', 'stg.weighted_safety_domain_score'],
    ['total_performance_score', 'stg.total_performance_score']
]) }}
{% endmacro %}

{% macro vbp_fiscal_year() %}
{#- The published fiscal_year, or the reviewed year of a file that publishes none [515]. -#}
coalesce(
    try_cast(trim(stg.fiscal_year) as integer),
    (select reviewed.fiscal_year from {{ ref('vbp_file_fiscal_years') }} as reviewed where reviewed.file_name = stg._member_path)
)
{% endmacro %}
