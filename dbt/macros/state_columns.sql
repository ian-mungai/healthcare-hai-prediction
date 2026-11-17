{#- Typing rules and column lists of the state sources (group E): California HCAI utilization and financial disclosure, and the Illinois Hospital Report Card. -#}

{% macro hcai_util_numeric_columns() %}
{#- The HCAI utilization columns typed as numbers, in bronze order: licensed beds, licensed bed days, discharges, intra-transfers, census days, average length of stay, patient counts, emergency visits and diversion hours, operating rooms, births and cardiac procedures. Service flags, equipment and project descriptions stay in bronze. -#}
{{ return([
    'med_surg_lic_beds',
    'perinatal_lic_beds',
    'pediatric_lic_beds',
    'ic_lic_beds',
    'coronary_care_lic_beds',
    'acute_respiratory_care_lic_beds',
    'burn_lic_beds',
    'ic_newborn_lic_beds',
    'rehab_ctr_lic_beds',
    'gac_subtot_lic_beds',
    'chem_depend_recovery_lic_beds',
    'acute_psychiatric_lic_beds',
    'sn_lic_beds',
    'intermediate_care_lic_beds',
    'intermediate_care_dev_dis_lic_beds',
    'tot_lic_beds',
    'gac_cdrs_lic_beds',
    'acute_psych_lic_beds',
    'newborn_nursery_bassinets',
    'gen_acute_care_sn_swing_beds',
    'acute_psychiatric_pats_locked_on_1231',
    'acute_psychiatric_pats_open_on_1231',
    'acute_psychiatric_pats_tot_by_unit_on_1231',
    'acute_psychiatric_pats_0_to_17_on_1231',
    'acute_psychiatric_pats_18_to_64_on_1231',
    'acute_psychiatric_pats_65_and_up_on_1231',
    'acute_psychiatric_pats_tot_by_age_on_1231',
    'acute_psychiatric_pats_med_trad_on_1231',
    'acute_psychiatric_pats_med_managed_care_on_1231',
    'acute_psychiatric_pats_med_cal_trad_on_1231',
    'acute_psychiatric_pats_med_cal_managed_care_on_1231',
    'acute_psychiatric_pats_tot_by_payor',
    'med_surg_lic_bed_days',
    'perinatal_lic_bed_days',
    'pediatric_lic_bed_days',
    'ic_lic_bed_days',
    'coronary_care_lic_bed_days',
    'acute_respiratory_care_lic_bed_days',
    'burn_lic_bed_days',
    'ic_newborn_lic_bed_days',
    'rehab_ctr_lic_bed_days',
    'gac_subtot_lic_bed_days',
    'chem_depend_recovery_lic_bed_days',
    'acute_psychiatric_lic_bed_days',
    'sn_lic_bed_days',
    'intermediate_care_lic_bed_days',
    'intermediate_care_dev_dis_lic_bed_days',
    'tot_lic_bed_days',
    'med_surg_discharges',
    'perinatal_discharges',
    'pediatric_discharges',
    'ic_discharges',
    'coronary_care_discharges',
    'acute_respiratory_care_discharges',
    'burn_discharges',
    'ic_newborn_discharges',
    'rehab_ctr_discharges',
    'gac_subtot_discharges',
    'chem_depend_recovery_discharges',
    'acute_psychiatric_discharges',
    'sn_discharges',
    'intermediate_care_discharges',
    'intermediate_care_dev_dis_discharges',
    'tot_discharges',
    'gac_cdrs_discharges',
    'acute_psych_discharges',
    'newborn_nursery_infants',
    'ic_intra_transfers',
    'coronary_care_intra_transfers',
    'acute_respiratory_care_intra_transfers',
    'burn_intra_transfers',
    'ic_newborn_intra_transfers',
    'sn_intra_transfers',
    'med_surg_cen_days',
    'perinatal_cen_days',
    'pediatric_cen_days',
    'ic_cen_days',
    'coronary_care_cen_days',
    'acute_respiratory_care_cen_days',
    'burn_cen_days',
    'ic_newborn_cen_days',
    'rehab_ctr_cen_days',
    'gac_subtot_cen_days',
    'chem_depend_recov_cen_days',
    'acute_psychiatric_cen_days',
    'sn_cen_days',
    'intermediate_care_cen_days',
    'intermediate_care_dev_dis_cen_days',
    'tot_cen_days',
    'gac_cdrs_cen_days',
    'acute_psych_cen_days',
    'newborn_nursery_cen_days',
    'med_surg_alos_cy',
    'perinatal_alos_cy',
    'pediatric_alos_cy',
    'ic_alos_cy',
    'coronary_care_alos_cy',
    'acute_respiratory_care_alos_cy',
    'burn_alos_cy',
    'ic_newborn_alos_cy',
    'rehab_ctr_alos_cy',
    'gac_subtot_alos_cy',
    'chem_depend_recov_alos_cy',
    'acute_psychiatric_alos_cy',
    'sn_alos_cy',
    'intermediate_care_alos_cy',
    'intermediate_care_dev_dis_alos_cy',
    'tot_alos_cy',
    'gac_cdrs_alos_cy',
    'acute_psych_cdrs_alos_cy',
    'med_surg_alos_py',
    'perinatal_alos_py',
    'pediatric_alos_py',
    'ic_alos_py',
    'coronary_care_alos_py',
    'acute_respiratory_care_alos_py',
    'burn_alos_py',
    'ic_newborn_alos_py',
    'rehab_ctr_alos_py',
    'gac_subtot_alos_py',
    'chem_depend_recov_alos_py',
    'acute_psychiatric_alos_py',
    'sn_alos_py',
    'intermediate_care_alos_py',
    'intermediate_care_dev_dis_alos_py',
    'tot_alos_py',
    'gac_cdrs_alos_py',
    'acute_psych_cdrs_alos_py',
    'ems_visits_non_urgent_tot',
    'ems_visits_urgent_tot',
    'ems_visits_moderate_tot',
    'ems_visits_severe_tot',
    'ems_visits_critical_tot',
    'emer_dept_visits_not_result_admissions_tot',
    'emer_med_treat_stations_on_1231',
    'non_emer_visits_in_emer_dept',
    'emer_registrations_pats_leave_wo_being_seen',
    'emer_dept_ambulance_diversion_hours',
    'emer_dept_hr_diversion_jan',
    'emer_dept_hr_diversion_feb',
    'emer_dept_hr_diversion_mar',
    'emer_dept_hr_diversion_apr',
    'emer_dept_hr_diversion_may',
    'emer_dept_hr_diversion_jun',
    'emer_dept_hr_diversion_jul',
    'emer_dept_hr_diversion_aug',
    'emer_dept_hr_diversion_sep',
    'emer_dept_hr_diversion_oct',
    'emer_dept_hr_diversion_nov',
    'emer_dept_hr_diversion_dec',
    'emer_dept_hr_diversion_tot',
    'ems_visits_non_urgent_admitted',
    'ems_visits_urgent_admitted',
    'ems_visits_moderate_admitted',
    'ems_visits_severe_admitted',
    'ems_visits_critical_admitted',
    'admitted_from_emer_dept_tot',
    'er_traffic_tot',
    'inpatient_surg_oper',
    'outpatient_surg_oper',
    'inpat_oper_rm',
    'outpat_oper_rm',
    'inpat_outpat_oper_rm',
    'oper_rm_tot',
    'live_births_tot',
    'live_births_lt_2500gm',
    'live_births_lt_1500gm',
    'live_births_in_alternative_setting',
    'live_births_c_section',
    'cardiovascular_oper_rm',
    'cardiovascular_surg_oper_pediatric_bypass_used',
    'cardiovascular_surg_oper_adult_bypass_used',
    'cardiovascular_surg_oper_bypass_used_tot',
    'coronary_artery_bypass_graft_surg',
    'cardiac_catheterization_lab_rm',
    'card_cath_ped_ip_diag_vst',
    'card_cath_ped_op_diag_vst',
    'cardiac_catheterization_adult_inpat_diagnostic_visits',
    'cardiac_catheterization_adult_outpat_diagnostic_visits',
    'cardiac_catheterization_diagnostic_visits_tot',
    'diagnostic_cardiac_cath_proc',
    'myocardial_biopsy',
    'perm_pacemaker_implant',
    'oth_perm_pacemaker_proc',
    'implanable_cardio_defib_implantation',
    'oth_procedures',
    'pci_with_stent',
    'pci_wo_stent',
    'atherectomy',
    'thrombolytic_agents',
    'ptbv',
    'diagnostic_electrophysiology_ep',
    'catheter_ablation',
    'peripheral_vascular_angiography',
    'peripheral_vascular_interventional',
    'carotid_stenting',
    'intra_aortic_balloon_pump_insertion',
    'catheter_based_ventricular_assist_device_insertion',
    'all_other_catheterization_proc',
    'catheterization_proc_tot',
    'inpatient_surg_oper_rm_mins',
    'outpatient_surg_oper_rm_mins',
    'cardiovascular_surg_oper_pediatric_bypass_not_used',
    'cardiovascular_surg_oper_adult_bypass_not_used',
    'cardiovascular_surg_oper_bypass_not_used_tot',
    'card_cath_ped_ip_ther_vst',
    'card_cath_ped_op_ther_vst',
    'cardiac_catheterization_adult_inpat_therapeutic_visits',
    'cardiac_catheterization_adult_outpat_therapeutic_visits',
    'cardiac_catheterization_therapeutic_visits_tot',
    'inpatient_avg_per_surgery',
    'outpatient_avg_per_surgery',
]) }}
{% endmacro %}

{% macro hcai_util_withheld_columns() %}
{#- The HCAI utilization columns silver never carries: administrator and report preparer names, the facility phone and street address, and the parent corporation's name and address. Bronze redacts them; silver leaves them out as well, and assert_state_models_hold_no_person_columns names them. -#}
{{ return([
    'fac_str_addr',
    'fac_phone',
    'fac_admin_name',
    'fac_par_corp_name',
    'fac_par_corp_bus_addr',
    'fac_par_corp_city',
    'fac_par_corp_state',
    'fac_par_corp_zip',
    'rept_prep_name',
    'rev_rept_prep_name',
]) }}
{% endmacro %}

{% macro hcai_util_coordinate_columns() %}
{#- The facility's longitude and latitude, typed as numbers beside the counts. -#}
{{ return(['longitude', 'latitude']) }}
{% endmacro %}

{% macro state_missing_tokens() %}
{#- Spreadsheet error text and not-available marks that the HCAI files publish in numeric columns. A value in this list types as null and its published text is kept. -#}
{{ return(['#DIV/0!', '#N/A', '#NAME?', '#NULL!', '#NUM!', '#REF!', '#VALUE!', 'N/A', 'NA', '-', '--']) }}
{% endmacro %}

{% macro state_number(column) %}
{#- A trimmed value that is a plain number once thousands separators are removed, as a double; blank, a missing token and anything else give null. -#}
{%- set value = "nullif(trim(replace(" ~ column ~ ", ',', '')), '')" -%}
case when regexp_full_match({{ value }}, '-?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][-+]?[0-9]+)?') then {{ value }}::double end
{%- endmacro %}

{% macro state_date_shape(column, serial=false) %}
{#- True when the trimmed text has the shape of a date: M/D/YYYY, YYYY-MM-DD (either with a 00:00:00 time) and, only where the source's workbook cells are
    numbers, an Excel serial number of five digits or an eight-digit YYYYMMDD. Text of any other shape is never carried into silver. -#}
regexp_full_match(
    trim({{ column }}),
    '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}( 00:00:00)?|[0-9]{4}-[0-9]{2}-[0-9]{2}( 00:00:00)?{% if serial %}|[0-9]{5}(\.[0-9]+)?|20[0-9]{6}{% endif %}'
)
{%- endmacro %}

{% macro state_date(column, serial=false) %}
{#- A date written M/D/YYYY, MM/DD/YYYY or YYYY-MM-DD, optionally followed by a time of 00:00:00. With serial, a five-digit Excel serial number from 2000-01-01
    (36526) to 2099-12-31 (73414) counts days from 1899-12-30, and an eight-digit YYYYMMDD of the 2000s is a date; the published text stays beside the date. Anything else is null. -#}
{%- set value = "nullif(trim(" ~ column ~ "), '')" -%}
coalesce(
    try_strptime(regexp_replace({{ value }}, ' 00:00:00$', ''), '%m/%d/%Y'),
    try_strptime(regexp_replace({{ value }}, ' 00:00:00$', ''), '%Y-%m-%d')
    {%- if serial %},
    case
        when regexp_full_match({{ value }}, '[0-9]{5}(\.[0-9]+)?') and {{ value }}::double between 36526 and 73414
            then date '1899-12-30' + floor({{ value }}::double)::integer
    end::timestamp,
    case when regexp_full_match({{ value }}, '20[0-9]{6}') then try_strptime({{ value }}, '%Y%m%d') end
    {%- endif %}
)::date
{%- endmacro %}

{% macro state_county_key(column) %}
{#- A county name compared apart from case, spaces, dots, apostrophes and a trailing 'county': DeKalb, De Kalb and DEKALB COUNTY give one key. -#}
regexp_replace(regexp_replace(upper(trim({{ column }})), ' COUNTY$', ''), '[^A-Z]', '', 'g')
{%- endmacro %}

{% macro state_county_keys(state) %}
{#- The counties of one state that a name identifies exactly once: a name with two county codes is left out, so a published name never picks one at random. -#}
select
    {{ state_county_key('county_name') }} as county_key,
    min(county_fips) as county_fips
from {{ ref('state_county_names') }}
where state = '{{ state }}'
group by {{ state_county_key('county_name') }}
having count(distinct county_fips) = 1
{%- endmacro %}

{% macro state_missing_token_list() %}
{#- The missing tokens as a SQL list. -#}
({% for item in state_missing_tokens() %}'{{ item }}'{% if not loop.last %}, {% endif %}{% endfor %})
{%- endmacro %}

{% macro state_numeric_safe(column) %}
{#- True for a value that is blank, a reviewed missing token or a plain number once thousands separators are removed. Any other text is never carried into silver. -#}
(coalesce(trim({{ column }}), '') = '' or trim({{ column }}) in {{ state_missing_token_list() }} or ({{ state_number(column) }}) is not null)
{%- endmacro %}

{% macro state_uncast_count(column) %}
{#- The rows whose non-blank published value neither types as a number nor is a reviewed missing token. -#}
count(*) filter (where trim({{ column }}) <> '' and ({{ state_number(column) }}) is null and trim({{ column }}) not in {{ state_missing_token_list() }})
{%- endmacro %}

{% macro state_nulled_entry(name, column) %}
{#- One entry of nulled_values_published: the column name and its published text when the value did not type, else null. -#}
{
    'field': '{{ name }}',
    'published': case
        when ({{ state_number(column) }}) is null then nullif(trim({{ column }}), '')
    end
}
{%- endmacro %}

{% macro hcai_finance_value_form(column) %}
{#- The form of a financial-disclosure value, from the shapes that cannot spell a person: blank, the redaction mark, a missing token, a number (up to 8 plain
    digits, or digits grouped by commas; 9 or more plain digits could be a phone number or an identifier), a number in parentheses, an M/D/YYYY date or one
    letter or yes or no. Any other text is other and never carried into silver; line-specific rules wait for the HCAI dictionary review. -#}
{%- set value = "trim(" ~ column ~ ")" -%}
case
    when coalesce({{ value }}, '') = '' then 'blank'
    when {{ value }} = '[REDACTED]' then 'redacted'
    when {{ value }} in {{ state_missing_token_list() }} then 'missing_token'
    when regexp_full_match({{ value }}, '[-+]?[0-9]{1,8}(\.[0-9]+)?|[-+]?\.[0-9]+|[-+]?[0-9]{1,3}(,[0-9]{3})+(\.[0-9]+)?') then 'number'
    when regexp_full_match({{ value }}, '\([0-9]{1,3}(,[0-9]{3})*(\.[0-9]+)?\)|\([0-9]{1,8}(\.[0-9]+)?\)') then 'parenthesised_number'
    when regexp_full_match({{ value }}, '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}') then 'date'
    when regexp_full_match(upper({{ value }}), '[A-Z]|YES|NO') then 'flag'
    else 'other'
end
{%- endmacro %}

{% macro hcai_finance_staging_projection() %}
{#- The financial-disclosure columns staging carries: the identifiers only when they have the shape of an OSHPD number, a date and a publisher code, and the value only
    in a reviewed form (hcai_finance_value_form), with its form. The raw value, which can name a person, never leaves bronze. -#}
bronze._object_key,
    bronze._member_sha256,
    bronze._row_number,
    case when regexp_full_match(trim(bronze.oshpd_id), '[0-9]{9}') then trim(bronze.oshpd_id) end as oshpd_id,
    case when {{ state_date_shape('bronze.rpe') }} then trim(bronze.rpe) end as rpe,
    case when regexp_full_match(trim(bronze.pagecolumnline), '[0-9]+(\.[0-9]+){2,3}') then trim(bronze.pagecolumnline) end as pagecolumnline,
    {{ hcai_finance_value_form('bronze.values') }} as value_form,
    case when {{ hcai_finance_value_form('bronze.values') }} not in ('other', 'blank') then trim(bronze.values) end as value_text
{%- endmacro %}

{% macro hcai_util_staging_projection() %}
{#- The utilization columns staging carries: the facility number only in the shape of one, the period dates only in a date or serial shape, the descriptors and
    the typed numbers (hcai_util_numeric_columns, hcai_util_coordinate_columns) only when blank, a missing token or a number, and the names of the number columns
    that failed in unsafe_value_fields, joined by a vertical bar. The administrator, report preparer, phone, street address and parent corporation columns are never selected. -#}
bronze._object_key,
    bronze._member_sha256,
    bronze._row_number,
    case when regexp_full_match(trim(bronze.fac_no), '[0-9]{9}') then trim(bronze.fac_no) end as fac_no,
    nullif(trim(bronze.fac_name), '') as fac_name,
    nullif(trim(bronze.fac_city), '') as fac_city,
    nullif(trim(bronze.fac_zip), '') as fac_zip,
    nullif(trim(bronze.fac_operated_this_yr), '') as fac_operated_this_yr,
    case when {{ state_date_shape('bronze.fac_op_per_begin_dt', true) }} then trim(bronze.fac_op_per_begin_dt) end as fac_op_per_begin_dt,
    case when {{ state_date_shape('bronze.fac_op_per_end_dt', true) }} then trim(bronze.fac_op_per_end_dt) end as fac_op_per_end_dt,
    nullif(trim(bronze.license_no), '') as license_no,
    nullif(trim(bronze.license_status), '') as license_status,
    nullif(trim(bronze.facility_level), '') as facility_level,
    nullif(trim(bronze.trauma_ctr), '') as trauma_ctr,
    nullif(trim(bronze.teach_hosp), '') as teach_hosp,
    nullif(trim(bronze.teach_rural), '') as teach_rural,
    nullif(trim(bronze.census_key), '') as census_key,
    nullif(trim(bronze.cens_tract), '') as cens_tract,
    case when regexp_full_match(trim(bronze.county), '[A-Za-z .''-]{1,40}') then trim(bronze.county) end as county,
    nullif(trim(bronze.lic_cat), '') as lic_cat,
    nullif(trim(bronze.licee_toc), '') as licee_toc,
    nullif(trim(bronze.prin_service_type), '') as prin_service_type,
    nullif(trim(bronze.emsa_trauma_designation), '') as emsa_trauma_designation,
    nullif(trim(bronze.emsa_trauma_designation_pediatric), '') as emsa_trauma_designation_pediatric,
    nullif(trim(bronze.lic_ed_lev_begin), '') as lic_ed_lev_begin,
    nullif(trim(bronze.lic_ed_lev_end), '') as lic_ed_lev_end,
    {%- for column in hcai_util_coordinate_columns() + hcai_util_numeric_columns() %}
    case when bronze.{{ column }}_is_safe then bronze.{{ column }} end as {{ column }},
    {%- endfor %}
    nullif(concat_ws('|',
        {%- for column in hcai_util_coordinate_columns() + hcai_util_numeric_columns() %}
        case when not bronze.{{ column }}_is_safe then '{{ column }}' end{% if not loop.last %},{% endif %}
        {%- endfor %}
    ), '') as unsafe_value_fields
{%- endmacro %}

{% macro illinois_staging_projection() %}
{#- The report card columns staging carries: the numbers only in the shape of one, the dates only in a date shape, the county only as letters, and the value, numerator
    and denominator only when blank, a missing token or a number, with the names of those that failed in unsafe_value_fields, joined by a vertical bar. The measure description, rationale
    and evidence texts are never selected. -#}
bronze._object_key,
    bronze._member_sha256,
    bronze._row_number,
    case when regexp_full_match(trim(bronze.entity_id), '[0-9]{1,12}') then trim(bronze.entity_id) end as entity_id,
    nullif(trim(bronze.name), '') as name,
    nullif(trim(bronze.city), '') as city,
    case when regexp_full_match(trim(bronze.county_name), '[A-Za-z .''-]{1,40}') then trim(bronze.county_name) end as county_name,
    case when regexp_full_match(trim(bronze.aha_service_id), '[0-9A-Za-z]{1,12}') then trim(bronze.aha_service_id) end as aha_service_id,
    case when regexp_full_match(trim(bronze.measure_id), '[0-9]{1,9}') then trim(bronze.measure_id) end as measure_id,
    nullif(trim(bronze.name_friendly), '') as name_friendly,
    nullif(trim(bronze.measure_type), '') as measure_type,
    nullif(trim(bronze.default_unit), '') as default_unit,
    case when {{ state_date_shape('bronze.date_start') }} then trim(bronze.date_start) end as date_start,
    case when {{ state_date_shape('bronze.date_end') }} then trim(bronze.date_end) end as date_end,
    case when {{ state_numeric_safe('bronze.value') }} then bronze.value end as value,
    case when {{ state_numeric_safe('bronze.numerator') }} then bronze.numerator end as numerator,
    case when {{ state_numeric_safe('bronze.denominator') }} then bronze.denominator end as denominator,
    nullif(concat_ws('|',
        case when not {{ state_numeric_safe('bronze.value') }} then 'value' end,
        case when not {{ state_numeric_safe('bronze.numerator') }} then 'numerator' end,
        case when not {{ state_numeric_safe('bronze.denominator') }} then 'denominator' end
    ), '') as unsafe_value_fields
{%- endmacro %}

{% macro hcai_finance_hold_reason(alias) %}
{#- Why a financial-disclosure row cannot be kept, or null: the facility, period or code is not in its published shape, or the value is not in a reviewed form. -#}
case
    when {{ alias }}.oshpd_id is null then 'facility_not_a_facility_number'
    when {{ state_date(alias ~ '.rpe') }} is null then 'period_not_a_date'
    when {{ alias }}.pagecolumnline is null then 'line_not_a_publisher_code'
    when {{ alias }}.value_form = 'other' then 'value_not_a_reviewed_form'
end
{%- endmacro %}

{% macro hcai_util_hold_reason(alias, county) %}
{#- Why a utilization row cannot be kept, or null: no facility number, a period that is no date or runs backwards, a number that does not type or a county name that
    does not identify exactly one county of California. The description row fails the first test. -#}
case
    when {{ alias }}.fac_no is null then 'facility_not_a_facility_number'
    when {{ state_date(alias ~ '.fac_op_per_begin_dt', true) }} is null or {{ state_date(alias ~ '.fac_op_per_end_dt', true) }} is null then 'period_not_a_date'
    when {{ state_date(alias ~ '.fac_op_per_begin_dt', true) }} > {{ state_date(alias ~ '.fac_op_per_end_dt', true) }} then 'period_reversed'
    when {{ alias }}.unsafe_value_fields is not null then 'value_not_a_number'
    when {{ county }}.county_fips is null then 'county_not_matched'
end
{%- endmacro %}

{% macro illinois_hold_reason(alias, county) %}
{#- Why a report-card row cannot be kept, or null: the entity or the measure is no number, a period is no date or runs backwards, a number does not type or the county
    name does not identify exactly one county of Illinois. -#}
case
    when {{ alias }}.entity_id is null then 'entity_not_a_number'
    when {{ alias }}.measure_id is null then 'measure_not_a_number'
    when {{ state_date(alias ~ '.date_start') }} is null or {{ state_date(alias ~ '.date_end') }} is null then 'period_not_a_date'
    when {{ state_date(alias ~ '.date_start') }} > {{ state_date(alias ~ '.date_end') }} then 'period_reversed'
    when {{ alias }}.unsafe_value_fields is not null then 'value_not_a_number'
    when {{ county }}.county_fips is null then 'county_not_matched'
end
{%- endmacro %}

{% macro hcai_util_sources() %}
{#- The page files (responded) and the non-response files (nonresponse) of the utilization staging views. -#}
select
    'hcai_util_pages_csv' as bronze_table,
    'responded' as response_status,
    *
from {{ ref('stg_hcai_util_pages_csv') }}
union all by name
select
    'hcai_util_nonresponse_csv' as bronze_table,
    'nonresponse' as response_status,
    *
from {{ ref('stg_hcai_util_nonresponse_csv') }}
{%- endmacro %}

{% macro hcai_util_safe_flags() %}
{#- One flag per typed number column of the utilization files: true when the published value is blank, a reviewed missing token or a plain number. -#}
{%- for column in hcai_util_coordinate_columns() + hcai_util_numeric_columns() %}
{{ state_numeric_safe(column) }} as {{ column }}_is_safe{% if not loop.last %},{% endif %}
{%- endfor %}
{%- endmacro %}
