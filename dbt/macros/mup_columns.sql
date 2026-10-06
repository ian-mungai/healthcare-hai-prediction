{% macro mup_provider_numeric_columns() %}
{#- The numeric columns of the Medicare inpatient provider summary: every published column except the provider identity
    and place columns (failure mode 359). -#}
{{ return([
    'tot_benes',
    'tot_submtd_cvrd_chrg',
    'tot_pymt_amt',
    'tot_mdcr_pymt_amt',
    'tot_dschrgs',
    'tot_cvrd_days',
    'tot_days',
    'bene_avg_age',
    'bene_age_lt_65_cnt',
    'bene_age_65_74_cnt',
    'bene_age_75_84_cnt',
    'bene_age_gt_84_cnt',
    'bene_feml_cnt',
    'bene_male_cnt',
    'bene_race_wht_cnt',
    'bene_race_black_cnt',
    'bene_race_api_cnt',
    'bene_race_hspnc_cnt',
    'bene_race_natind_cnt',
    'bene_race_othr_cnt',
    'bene_dual_cnt',
    'bene_ndual_cnt',
    'bene_cc_bh_adhd_othcd_v1_pct',
    'bene_cc_bh_alcohol_drug_v1_pct',
    'bene_cc_bh_tobacco_v1_pct',
    'bene_cc_bh_alz_nonalzdem_v2_pct',
    'bene_cc_bh_anxiety_v1_pct',
    'bene_cc_bh_bipolar_v1_pct',
    'bene_cc_bh_mood_v2_pct',
    'bene_cc_bh_depress_v1_pct',
    'bene_cc_bh_pd_v1_pct',
    'bene_cc_bh_ptsd_v1_pct',
    'bene_cc_bh_schizo_othpsy_v1_pct',
    'bene_cc_ph_asthma_v2_pct',
    'bene_cc_ph_afib_v2_pct',
    'bene_cc_ph_cancer6_v2_pct',
    'bene_cc_ph_ckd_v2_pct',
    'bene_cc_ph_copd_v2_pct',
    'bene_cc_ph_diabetes_v2_pct',
    'bene_cc_ph_hf_nonihd_v2_pct',
    'bene_cc_ph_hyperlipidemia_v2_pct',
    'bene_cc_ph_hypertension_v2_pct',
    'bene_cc_ph_ischemicheart_v2_pct',
    'bene_cc_ph_osteoporosis_v2_pct',
    'bene_cc_ph_parkinson_v2_pct',
    'bene_cc_ph_arthritis_v2_pct',
    'bene_cc_ph_stroke_tia_v2_pct',
    'bene_avg_risk_scre'
]) }}
{% endmacro %}

{% macro strict_number(column) %}
{#- A trimmed value that is a plain number, as a double; blank and anything else give null. -#}
{%- set value = "nullif(trim(" ~ column ~ "), '')" -%}
case when regexp_full_match({{ value }}, '-?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][-+]?[0-9]+)?') then {{ value }}::double end
{%- endmacro %}
