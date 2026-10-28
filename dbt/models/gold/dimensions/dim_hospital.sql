-- One row per hospital CCN any fact's hospital key carries, plus the unknown member. A hospital key is a CCN or an entity ID read as 6-character
-- text; a value outside that space takes the unknown member in every fact. first_seen and last_seen are the earliest and latest DATE
-- value of the source dates of the facts that carry the hospital's key, over the rows that carry it; they are null when none of those rows has a
-- typed date.
{%- set sources = [
    {'model': 'int_cc_complications_deaths_windows', 'column': 'entity_id', 'dates': ['window_start', 'window_end', 'release_date']},
    {'model': 'int_cc_hcahps_windows', 'column': 'entity_id', 'dates': ['window_start', 'window_end', 'release_date']},
    {'model': 'int_cc_hrrp_windows', 'column': 'entity_id', 'dates': ['window_start', 'window_end', 'release_date']},
    {'model': 'int_cc_maternal_windows', 'column': 'entity_id', 'dates': ['window_start', 'window_end', 'release_date']},
    {'model': 'int_cc_timely_effective_windows', 'column': 'entity_id', 'dates': ['window_start', 'window_end', 'release_date']},
    {'model': 'int_cc_unplanned_visits_windows', 'column': 'entity_id', 'dates': ['window_start', 'window_end', 'release_date']},
    {'model': 'int_cc_window_holds', 'column': 'entity_id', 'dates': ['window_start', 'window_end']},
    {'model': 'int_change_of_ownership_rows', 'column': 'ccn_buyer', 'dates': ['effective_date', 'period_start', 'period_end']},
    {'model': 'int_change_of_ownership_rows', 'column': 'ccn_seller', 'dates': ['effective_date', 'period_start', 'period_end']},
    {'model': 'int_cmi_hospital_rows', 'column': 'ccn', 'dates': []},
    {'model': 'int_cost_reports', 'column': 'ccn', 'dates': ['period_begin', 'period_end']},
    {
        'model': 'int_hac_program_years',
        'column': 'ccn',
        'dates': ['psi_90_start_date', 'psi_90_end_date', 'hai_measures_start_date', 'hai_measures_end_date', 'release_date'],
    },
    {'model': 'int_hai_hospital_windows', 'column': 'entity_id', 'dates': ['window_start', 'window_end', 'release_date']},
    {'model': 'int_hai_window_holds', 'column': 'entity_id', 'dates': ['window_start', 'window_end']},
    {'model': 'int_hgi_hospital_releases', 'column': 'ccn', 'dates': ['release_date']},
    {'model': 'int_hhs_capacity_weeks', 'column': 'ccn', 'dates': ['collection_week']},
    {'model': 'int_hospital_enrollment_rows', 'column': 'ccn', 'dates': ['incorporation_date', 'reh_conversion_date', 'period_start', 'period_end']},
    {'model': 'int_impact_holds', 'column': 'ccn', 'dates': []},
    {'model': 'int_impact_hospital_values', 'column': 'ccn', 'dates': []},
    {'model': 'int_mup_drg_discharges', 'column': 'ccn', 'dates': []},
    {'model': 'int_mup_providers', 'column': 'ccn', 'dates': []},
    {'model': 'int_occmix_survey_rows', 'column': 'ccn', 'dates': ['survey_start_date', 'survey_end_date']},
    {'model': 'int_onc_attestation_rows', 'column': 'ccn', 'dates': []},
    {'model': 'int_onc_chpl_linkage_rows', 'column': 'ccn', 'dates': ['start_date', 'end_date']},
    {
        'model': 'int_pos_hospital_snapshots',
        'column': 'ccn',
        'dates': ['original_participation_date', 'certification_date', 'termination_date', 'period_start', 'period_end'],
    },
    {'model': 'int_validation_program_holds', 'column': 'ccn', 'dates': []},
    {'model': 'int_validation_window_holds', 'column': 'entity_id', 'dates': ['window_start', 'window_end']},
    {'model': 'int_vbp_program_years', 'column': 'ccn', 'dates': ['release_date']},
] %}

with

appearances as (
    {%- set ns = namespace(first=true) %}
    {%- for source in sources %}
    {%- for source_date in (source.dates or [none]) %}
    {%- if not ns.first %}
    union all
    {%- endif %}
    {%- set ns.first = false %}
    select
        cast({{ source.column }} as varchar) as ccn,
        {{ source_date if source_date else 'cast(null as date)' }} as seen_date
    from {{ ref(source.model) }}
    {%- endfor %}
    {%- endfor %}
),

valid as (
    select
        ccn,
        min(seen_date) as first_seen,
        max(seen_date) as last_seen
    from appearances
    where length(ccn) = 6
    group by ccn
),

final as (
    select
        md5(ccn) as hospital_sk,
        ccn,
        first_seen,
        last_seen
    from valid
    union all
    select
        md5('<unknown>') as hospital_sk,
        '<unknown>' as ccn,
        cast(null as date) as first_seen,
        cast(null as date) as last_seen
)

select * from final
