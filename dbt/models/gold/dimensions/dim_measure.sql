-- One row per measure identity (fact, measure_key, value_column), plus the unknown member. The identities are the control bridge seed rows with a
-- non-empty measure key, plus, for each long fact, every distinct published measure key of its silver model paired with each value column the
-- fact's bridge rows name. A column no bridge row names is not an identity. measure_label is the silver model's measure_name where it has one;
-- unit is the value_unit of int_mmd_prevalence; value_kind is the type class of the silver column (number, text, boolean or date; a list of text
-- counts as text); the vintage range comes from the seeds. A measure that the identity_splits seed lists as split (its published meaning
-- changed) has one row per published name: edition_start and edition_end are the first and last release date that carry the name, and the identity text
-- and key add the edition start. Every other row has a null edition and keeps the identity text and key it had before editions existed.
{%- set bridge_seeds = [
    'registry_measure_sources_bridge',
    'cost_report_measures_bridge',
    'impact_measures_bridge',
    'mup_measures_bridge',
    'occmix_measures_bridge',
    'validation_measures_bridge',
    'hai_outcome_measures_bridge',
    'hhs_onc_measures_bridge',
    'ownership_measures_bridge',
    'county_health_measures_bridge',
    'acs_svi_measures_bridge',
    'income_labor_measures_bridge',
    'geography_measures_bridge',
    'mmd_conditions_bridge',
    'shortage_measures_bridge',
] %}
{%- set long_facts = [
    {'fact': 'fct_acs_county_value', 'model': 'int_acs_county_values', 'key': 'concept_id', 'named': false},
    {'fact': 'fct_bls_county_series', 'model': 'int_bls_county_series', 'key': 'measure', 'named': false},
    {'fact': 'fct_complications_deaths_window', 'model': 'int_cc_complications_deaths_windows', 'key': 'measure_id', 'named': true},
    {'fact': 'fct_gv_county_value', 'model': 'int_gv_county_values', 'key': 'field', 'named': false},
    {'fact': 'fct_hai_hospital_window', 'model': 'int_hai_hospital_windows', 'key': 'measure_id', 'named': true},
    {'fact': 'fct_hai_national_window', 'model': 'int_hai_national_windows', 'key': 'measure_id', 'named': true},
    {'fact': 'fct_hai_state_window', 'model': 'int_hai_state_windows', 'key': 'measure_id', 'named': true},
    {'fact': 'fct_hcahps_window', 'model': 'int_cc_hcahps_windows', 'key': 'measure_id', 'named': true},
    {'fact': 'fct_hrrp_window', 'model': 'int_cc_hrrp_windows', 'key': 'measure_id', 'named': false},
    {'fact': 'fct_ipps_impact_value', 'model': 'int_impact_hospital_values', 'key': 'field', 'named': false},
    {'fact': 'fct_maternal_health_window', 'model': 'int_cc_maternal_windows', 'key': 'measure_id', 'named': true},
    {'fact': 'fct_medicare_inpatient_drg', 'model': 'int_mup_drg_discharges', 'key': 'drg_cd', 'named': false},
    {'fact': 'fct_mmd_prevalence', 'model': 'int_mmd_prevalence', 'key': 'condition_label', 'named': false},
    {'fact': 'fct_places_county_value', 'model': 'int_places_county_values', 'key': 'measureid', 'named': false},
    {'fact': 'fct_svi_county_value', 'model': 'int_svi_county_values', 'key': 'field', 'named': false},
    {'fact': 'fct_timely_effective_care_window', 'model': 'int_cc_timely_effective_windows', 'key': 'measure_id', 'named': true},
    {'fact': 'fct_unplanned_visits_window', 'model': 'int_cc_unplanned_visits_windows', 'key': 'measure_id', 'named': true},
] %}
{%- set value_kinds = {
    'bridge_county_neighbor': {
        'text': ['county_fips', 'neighbor_fips', 'vintage'],
        'boolean': ['is_isolated', 'is_self_link'],
        'number': ['shared_border_length_m'],
    },
    'bridge_zip_county': {
        'number': ['bus_ratio', 'oth_ratio', 'res_ratio', 'tot_ratio'],
        'text': ['county_fips', 'quarter_label', 'zip_code'],
        'boolean': ['has_residential_addresses'],
    },
    'fct_acs_county_value': {
        'number': ['value_number'],
    },
    'fct_bls_county_series': {
        'text': ['footnote_codes'],
        'number': ['value_number'],
    },
    'fct_complications_deaths_window': {
        'number': ['score_number'],
    },
    'fct_cost_report': {
        'number': [
            'cash_on_hand_and_in_banks', 'contract_labor_direct_patient_care', 'cost_of_charity_care', 'cost_of_uncompensated_care',
            'cost_to_charge_ratio', 'depreciation_cost', 'fte_employees_on_payroll', 'health_information_technology_designated_assets',
            'less_total_operating_expense', 'net_income_from_service_to_patients', 'net_patient_revenue', 'net_revenue_from_medicaid',
            'number_of_beds', 'number_of_interns_and_residents_fte', 'total_assets', 'total_bad_debt_expense', 'total_bed_days_available',
            'total_costs', 'total_current_assets', 'total_current_liabilities', 'total_days_title_xix', 'total_days_title_xviii',
            'total_days_v_xviii_xix_unknown', 'total_discharges_v_xviii_xix_unknown', 'total_liabilities', 'total_salaries_adjusted',
            'total_salaries_from_worksheet_a',
        ],
        'text': ['provider_type', 'type_of_control'],
    },
    'fct_enrollment_release': {
        'text': ['associate_id', 'ccn', 'enrollment_id'],
    },
    'fct_gv_county_value': {
        'number': ['value_number'],
    },
    'fct_hac_program_year': {
        'text': ['payment_reduction_published'],
        'number': ['total_hac_score_number'],
    },
    'fct_hai_hospital_window': {
        'text': ['compared_to_national', 'score'],
    },
    'fct_hcahps_window': {
        'number': ['hcahps_answer_percent_number', 'number_of_completed_surveys_number', 'survey_response_rate_percent_number'],
    },
    'fct_hgi_release': {
        'number': ['overall_rating'],
    },
    'fct_hhs_capacity_week': {
        'number': [
            'all_adult_hospital_inpatient_bed_occupied_7_day_avg', 'all_adult_hospital_inpatient_beds_7_day_avg',
            'all_pediatric_inpatient_bed_occupied_7_day_avg', 'inpatient_beds_7_day_avg', 'inpatient_beds_used_7_day_avg',
            'inpatient_beds_used_covid_7_day_avg', 'previous_day_admission_adult_covid_confirmed_18_19_7_day_sum',
            'previous_day_admission_adult_covid_confirmed_20_29_7_day_sum', 'previous_day_admission_adult_covid_confirmed_30_39_7_day_sum',
            'previous_day_admission_adult_covid_confirmed_40_49_7_day_sum', 'previous_day_admission_adult_covid_confirmed_50_59_7_day_sum',
            'previous_day_admission_adult_covid_confirmed_60_69_7_day_sum', 'previous_day_admission_adult_covid_confirmed_70_79_7_day_sum',
            'previous_day_admission_adult_covid_confirmed_7_day_sum', 'previous_day_admission_adult_covid_confirmed_80_7_day_sum',
            'previous_day_admission_adult_covid_confirmed_unknown_7_day_sum', 'previous_day_admission_adult_covid_suspected_18_19_7_day_sum',
            'previous_day_admission_adult_covid_suspected_20_29_7_day_sum', 'previous_day_admission_adult_covid_suspected_30_39_7_day_sum',
            'previous_day_admission_adult_covid_suspected_40_49_7_day_sum', 'previous_day_admission_adult_covid_suspected_50_59_7_day_sum',
            'previous_day_admission_adult_covid_suspected_60_69_7_day_sum', 'previous_day_admission_adult_covid_suspected_70_79_7_day_sum',
            'previous_day_admission_adult_covid_suspected_7_day_sum', 'previous_day_admission_adult_covid_suspected_80_7_day_sum',
            'previous_day_admission_adult_covid_suspected_unknown_7_day_sum', 'previous_day_admission_influenza_confirmed_7_day_sum',
            'previous_day_admission_pediatric_covid_confirmed_0_4_7_day_sum', 'previous_day_admission_pediatric_covid_confirmed_12_17_7_day_sum',
            'previous_day_admission_pediatric_covid_confirmed_5_11_7_day_sum', 'previous_day_admission_pediatric_covid_confirmed_7_day_sum',
            'previous_day_admission_pediatric_covid_confirmed_unknown_7_day_sum', 'previous_day_admission_pediatric_covid_suspected_7_day_sum',
            'staffed_adult_icu_bed_occupancy_7_day_avg', 'staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_avg',
            'staffed_icu_adult_patients_confirmed_covid_7_day_avg', 'staffed_icu_pediatric_patients_confirmed_covid_7_day_avg',
            'staffed_pediatric_icu_bed_occupancy_7_day_avg', 'total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_avg',
            'total_adult_patients_hospitalized_confirmed_covid_7_day_avg',
            'total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_7_day_avg',
            'total_pediatric_patients_hospitalized_confirmed_covid_7_day_avg', 'total_staffed_adult_icu_beds_7_day_avg',
            'total_staffed_pediatric_icu_beds_7_day_avg',
        ],
    },
    'fct_hospital_owner_release': {
        'date': ['association_date'],
        'text': ['enrollment_id', 'role_code'],
        'boolean': ['flags_published', 'private_equity_company_owner', 'reit_owner'],
        'number': ['percentage_ownership'],
    },
    'fct_hpsa_component': {
        'date': ['designation_date', 'last_update_date', 'withdrawn_date'],
        'text': ['designation_type', 'hpsa_id', 'hpsa_status'],
        'number': ['hpsa_score'],
    },
    'fct_hrrp_window': {
        'number': ['excess_readmission_ratio_number'],
    },
    'fct_hsa_zip_cases': {
        'text': ['ccn_published', 'data_year', 'zip_code'],
        'boolean': ['is_cases_suppressed', 'is_zip_missing', 'is_zip_suppressed'],
        'number': ['total_cases'],
    },
    'fct_ipps_impact_value': {
        'number': ['value_number'],
        'text': ['value_text'],
    },
    'fct_maternal_health_window': {
        'number': ['score_number'],
    },
    'fct_medicare_inpatient_drg': {
        'number': ['tot_dschrgs'],
    },
    'fct_medicare_inpatient_provider': {
        'number': [
            'bene_age_65_74_cnt', 'bene_age_75_84_cnt', 'bene_age_gt_84_cnt', 'bene_age_lt_65_cnt', 'bene_avg_age', 'bene_avg_risk_scre',
            'bene_cc_bh_adhd_othcd_v1_pct', 'bene_cc_bh_alcohol_drug_v1_pct', 'bene_cc_bh_alz_nonalzdem_v2_pct', 'bene_cc_bh_anxiety_v1_pct',
            'bene_cc_bh_bipolar_v1_pct', 'bene_cc_bh_depress_v1_pct', 'bene_cc_bh_mood_v2_pct', 'bene_cc_bh_pd_v1_pct', 'bene_cc_bh_ptsd_v1_pct',
            'bene_cc_bh_schizo_othpsy_v1_pct', 'bene_cc_bh_tobacco_v1_pct', 'bene_cc_ph_afib_v2_pct', 'bene_cc_ph_arthritis_v2_pct',
            'bene_cc_ph_asthma_v2_pct', 'bene_cc_ph_cancer6_v2_pct', 'bene_cc_ph_ckd_v2_pct', 'bene_cc_ph_copd_v2_pct',
            'bene_cc_ph_diabetes_v2_pct', 'bene_cc_ph_hf_nonihd_v2_pct', 'bene_cc_ph_hyperlipidemia_v2_pct', 'bene_cc_ph_hypertension_v2_pct',
            'bene_cc_ph_ischemicheart_v2_pct', 'bene_cc_ph_osteoporosis_v2_pct', 'bene_cc_ph_parkinson_v2_pct', 'bene_cc_ph_stroke_tia_v2_pct',
            'bene_dual_cnt', 'bene_feml_cnt', 'bene_race_api_cnt', 'bene_race_black_cnt', 'bene_race_hspnc_cnt', 'bene_race_natind_cnt',
            'bene_race_othr_cnt', 'bene_race_wht_cnt', 'tot_benes', 'tot_cvrd_days', 'tot_days', 'tot_dschrgs',
        ],
    },
    'fct_mmd_prevalence': {
        'number': ['value_number'],
    },
    'fct_mua_component': {
        'date': ['designation_date', 'update_date', 'withdrawal_date'],
        'text': ['designation_type', 'mua_id', 'mua_status'],
        'number': ['imu_score'],
    },
    'fct_occupational_mix_survey': {
        'number': ['lpnsthr', 'naorathr', 'nursehr', 'rnhr', 'rnsal'],
    },
    'fct_onc_ehr_product': {
        'text': ['cehrt_id', 'chpl_id', 'developer_name', 'facility_id', 'product_database_id'],
        'date': ['end_date', 'start_date'],
        'boolean': ['meets_criteria_for_promoting_interoperability_of_ehrs'],
        'number': ['program_year'],
    },
    'fct_ownership_change': {
        'text': ['ccn_buyer', 'ccn_seller', 'chow_type_code', 'event_key'],
        'date': ['effective_date'],
    },
    'fct_places_county_value': {
        'number': ['data_value'],
    },
    'fct_ruca_code': {
        'text': [
            'county_fips', 'geography_id', 'geography_type', 'primary_published', 'primary_ruca', 'secondary_published', 'secondary_ruca',
            'vintage', 'zip_type',
        ],
    },
    'fct_rucc_county': {
        'text': ['county_fips', 'description', 'rucc_code', 'rucc_published', 'vintage'],
        'number': ['population'],
    },
    'fct_sahie_county_value': {
        'text': ['agecat', 'county_fips', 'iprcat', 'racecat', 'sexcat'],
        'number': ['estimate_year', 'nipr', 'nui', 'pctui', 'pctui_moe'],
        'boolean': ['is_all_groups'],
    },
    'fct_saipe_county_estimate': {
        'text': ['county_fips'],
        'number': [
            'estimate_year', 'median_household_income', 'median_household_income_lb90', 'median_household_income_ub90', 'poverty_all_count',
            'poverty_all_pct', 'poverty_all_pct_lb90', 'poverty_all_pct_ub90',
        ],
    },
    'fct_svi_county_value': {
        'number': ['value_number'],
    },
    'fct_timely_effective_care_window': {
        'number': ['score_number'],
    },
    'fct_unplanned_visits_window': {
        'number': ['score_number'],
    },
    'fct_vbp_program_year': {
        'number': ['total_performance_score_number', 'unweighted_normalized_safety_domain_score_number', 'weighted_safety_domain_score_number'],
    },
    'fct_wonder_county_deaths': {
        'number': ['crude_rate', 'crude_rate_lower_95', 'crude_rate_standard_error', 'crude_rate_upper_95', 'deaths', 'population'],
        'text': ['crude_rate_token', 'deaths_token'],
    },
} %}

with

seed_rows as (
    {%- for bridge_seed in bridge_seeds %}
    select
        fact,
        measure_key,
        value_column,
        vintage_start,
        vintage_end
    from {{ ref(bridge_seed) }}
    where measure_key is not null and measure_key <> ''
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor %}
),

seeded as (
    select distinct
        fact,
        measure_key,
        value_column,
        vintage_start,
        vintage_end
    from seed_rows
),

value_columns as (
    select distinct
        fact,
        value_column
    from seed_rows
    where value_column is not null
),

published as (
    {%- set ns = namespace(first=true) %}
    {%- for long_fact in long_facts %}
    {%- if not ns.first %}
    union
    {%- endif %}
    {%- set ns.first = false %}
    select
        '{{ long_fact.fact }}' as fact,
        cast({{ long_fact.key }} as varchar) as measure_key
    from {{ ref(long_fact.model) }}
    where {{ long_fact.key }} is not null
    {%- endfor %}
),

paired as (
    select
        published.fact,
        published.measure_key,
        value_columns.value_column,
        cast(null as varchar) as vintage_start,
        cast(null as varchar) as vintage_end
    from published
    inner join value_columns
        on published.fact = value_columns.fact
    where not exists (
        select 1
        from seeded
        where
            seeded.fact = published.fact
            and seeded.measure_key = published.measure_key
            and seeded.value_column = value_columns.value_column
    )
),

identities as (
    select * from seeded
    union all
    select * from paired
),

names_published as (
    {%- set ns = namespace(first=true) %}
    {%- for long_fact in long_facts if long_fact.named %}
    {%- if not ns.first %}
    union
    {%- endif %}
    {%- set ns.first = false %}
    select
        '{{ long_fact.fact }}' as fact,
        cast({{ long_fact.key }} as varchar) as measure_key,
        measure_name,
        release_date,
        window_end
    from {{ ref(long_fact.model) }}
    where {{ long_fact.key }} is not null and measure_name is not null
    {%- endfor %}
),

-- A measure published under more than one name keeps the name of its latest release (then the latest window, then the first name in order);
-- every published name stays on the fact rows.
names as (
    select distinct on (fact, measure_key)
        fact,
        measure_key,
        measure_name
    from names_published
    order by fact asc, measure_key asc, release_date desc nulls last, window_end desc nulls last, measure_name asc
),

units as (
    select distinct
        condition_label as measure_key,
        value_unit
    from {{ ref('int_mmd_prevalence') }}
    where condition_label is not null and value_unit is not null
),

value_kinds as (
    {%- set ns = namespace(first=true) %}
    {%- for kind_fact, by_kind in value_kinds.items() %}
    {%- for value_kind, kind_columns in by_kind.items() %}
    {%- for kind_column in kind_columns %}
    {%- if not ns.first %}
    union all
    {%- endif %}
    {%- set ns.first = false %}
    select
        '{{ kind_fact }}' as fact,
        '{{ kind_column }}' as value_column,
        '{{ value_kind }}' as value_kind
    {%- endfor %}
    {%- endfor %}
    {%- endfor %}
),

identity_rules as (
    select
        fact,
        code as measure_key,
        identity_rule
    from {{ ref('identity_splits') }}
    where dimension = 'dim_measure'
),

-- One edition per published name of a split measure; the name's first and last release date bound it.
editions as (
    select
        names_published.fact,
        names_published.measure_key,
        names_published.measure_name as edition_label,
        cast(min(names_published.release_date) as varchar) as edition_start,
        cast(max(names_published.release_date) as varchar) as edition_end
    from names_published
    inner join identity_rules
        on names_published.fact = identity_rules.fact and names_published.measure_key = identity_rules.measure_key
    where identity_rules.identity_rule = 'split' and names_published.release_date is not null
    group by names_published.fact, names_published.measure_key, names_published.measure_name
),

base as (
    {{ gold_measure_rows('identities', 'names', 'units', 'value_kinds') }}
),

ordinary as (
    select
        base.measure_sk,
        base.measure_identity,
        base.fact,
        base.measure_key,
        base.value_column,
        base.measure_label,
        base.unit,
        base.value_kind,
        base.vintage_start,
        base.vintage_end,
        cast(null as varchar) as edition_start,
        cast(null as varchar) as edition_end,
        identity_rules.identity_rule
    from base
    left join identity_rules
        on base.fact = identity_rules.fact and base.measure_key = identity_rules.measure_key
    where identity_rules.identity_rule is distinct from 'split'
),

split_rows as (
    select
        md5(concat_ws('|', base.fact, base.measure_key, base.value_column, editions.edition_start)) as measure_sk,
        concat_ws('|', base.fact, base.measure_key, base.value_column, editions.edition_start) as measure_identity,
        base.fact,
        base.measure_key,
        base.value_column,
        editions.edition_label as measure_label,
        base.unit,
        base.value_kind,
        base.vintage_start,
        base.vintage_end,
        editions.edition_start,
        editions.edition_end,
        'split' as identity_rule
    from base
    inner join editions
        on base.fact = editions.fact and base.measure_key = editions.measure_key
),

final as (
    select * from ordinary
    union all
    select * from split_rows
    union all
    select
        md5('<unknown>') as measure_sk,
        '<unknown>' as measure_identity,
        cast(null as varchar) as fact,
        cast(null as varchar) as measure_key,
        cast(null as varchar) as value_column,
        cast(null as varchar) as measure_label,
        cast(null as varchar) as unit,
        cast(null as varchar) as value_kind,
        cast(null as varchar) as vintage_start,
        cast(null as varchar) as vintage_end,
        cast(null as varchar) as edition_start,
        cast(null as varchar) as edition_end,
        cast(null as varchar) as identity_rule
)

select * from final
