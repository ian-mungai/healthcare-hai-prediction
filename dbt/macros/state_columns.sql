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

{% macro state_missing_tokens() %}
{#- Spreadsheet error text and not-available marks that the HCAI files publish in numeric columns. A value in this list types as null and its published text is kept. -#}
{{ return(['#DIV/0!', '#N/A', '#NAME?', '#NULL!', '#NUM!', '#REF!', '#VALUE!', 'N/A', 'NA', '-', '--']) }}
{% endmacro %}

{% macro state_number(column) %}
{#- A trimmed value that is a plain number once thousands separators are removed, as a double; blank, a missing token and anything else give null. -#}
{%- set value = "nullif(trim(replace(" ~ column ~ ", ',', '')), '')" -%}
case when regexp_full_match({{ value }}, '-?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][-+]?[0-9]+)?') then {{ value }}::double end
{%- endmacro %}

{% macro state_date(column) %}
{#- A date written M/D/YYYY, MM/DD/YYYY or YYYY-MM-DD, optionally followed by a time of 00:00:00; anything else is null. -#}
{%- set value = "nullif(trim(" ~ column ~ "), '')" -%}
coalesce(
    try_strptime(regexp_replace({{ value }}, ' 00:00:00$', ''), '%m/%d/%Y'),
    try_strptime(regexp_replace({{ value }}, ' 00:00:00$', ''), '%Y-%m-%d')
)::date
{%- endmacro %}

{% macro state_county_key(column) %}
{#- A county name compared apart from case, spaces, dots, apostrophes and a trailing 'county': DeKalb, De Kalb and DEKALB COUNTY give one key. -#}
regexp_replace(regexp_replace(upper(trim({{ column }})), ' COUNTY$', ''), '[^A-Z]', '', 'g')
{%- endmacro %}

{% macro hcai_finance_hold_reason(alias) %}
{#- Why a financial-disclosure row cannot be keyed, or null: the facility is no 9-digit number, the period end is no date, or the line is no page.column.line. -#}
case
    when not coalesce(regexp_full_match(trim({{ alias }}.oshpd_id), '[0-9]{9}'), false) then 'facility_not_a_facility_number'
    when {{ state_date(alias ~ '.rpe') }} is null then 'period_not_a_date'
    when not coalesce(regexp_full_match(trim({{ alias }}.pagecolumnline), '[0-9]+\.[0-9]+\.[0-9]+'), false) then 'line_not_page_column_line'
end
{%- endmacro %}

{% macro hcai_util_hold_reason(alias) %}
{#- Why a utilization row cannot be keyed, or null: the facility is no 9-digit number (the description row is one), or a period date is no date. -#}
case
    when not coalesce(regexp_full_match(trim({{ alias }}.fac_no), '[0-9]{9}'), false) then 'facility_not_a_facility_number'
    when {{ state_date(alias ~ '.fac_op_per_begin_dt') }} is null or {{ state_date(alias ~ '.fac_op_per_end_dt') }} is null then 'period_not_a_date'
end
{%- endmacro %}

{% macro illinois_hold_reason(alias) %}
{#- Why a report-card row cannot be keyed, or null: the entity or the measure is no number, or a period date is no date. -#}
case
    when not coalesce(regexp_full_match(trim({{ alias }}.entity_id), '[0-9]+'), false) then 'entity_not_a_number'
    when not coalesce(regexp_full_match(trim({{ alias }}.measure_id), '[0-9]+'), false) then 'measure_not_a_number'
    when {{ state_date(alias ~ '.date_start') }} is null or {{ state_date(alias ~ '.date_end') }} is null then 'period_not_a_date'
end
{%- endmacro %}

{% macro state_missing_token_list() %}
{#- The missing tokens as a SQL list. -#}
({% for item in state_missing_tokens() %}'{{ item }}'{% if not loop.last %}, {% endif %}{% endfor %})
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
