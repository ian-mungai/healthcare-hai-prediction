{% macro pos_typed_columns() %}
{#- The POS columns the hospital snapshots type: bronze column, model column, kind and code width [284] [287] [288] [292]. -#}
{{ return([
    ['prvdr_ctgry_sbtyp_cd', 'provider_subtype_code', 'code', 2],
    ['gnrl_cntl_type_cd', 'control_type_code', 'code', 2],
    ['pgm_trmntn_cd', 'termination_code', 'code', 2],
    ['fips_state_cd', 'fips_state_code', 'code', 2],
    ['fips_cnty_cd', 'fips_county_code', 'code', 3],
    ['ssa_state_cd', 'ssa_state_code', 'code', 2],
    ['ssa_cnty_cd', 'ssa_county_part', 'code', 3],
    ['cbsa_cd', 'cbsa_code', 'code', 5],
    ['mdcl_schl_afltn_cd', 'medical_school_affiliation_code', 'code', 1],
    ['dctd_er_srvc_cd', 'emergency_service_code', 'code', 1],
    ['icu_srvc_cd', 'icu_service_code', 'code', 1],
    ['srgcl_icu_srvc_cd', 'surgical_icu_service_code', 'code', 1],
    ['neontl_icu_srvc_cd', 'neonatal_icu_service_code', 'code', 1],
    ['ped_icu_srvc_cd', 'pediatric_icu_service_code', 'code', 1],
    ['burn_care_unit_srvc_cd', 'burn_care_unit_service_code', 'code', 1],
    ['acute_rnl_dlys_srvc_cd', 'acute_renal_dialysis_service_code', 'code', 1],
    ['ip_srgcl_srvc_cd', 'inpatient_surgical_service_code', 'code', 1],
    ['open_hrt_srgry_srvc_cd', 'cardiac_thoracic_surgery_service_code', 'code', 1],
    ['bed_cnt', 'bed_count', 'integer', none],
    ['crtfd_bed_cnt', 'certified_bed_count', 'integer', none],
    ['psych_unit_bed_cnt', 'psych_unit_bed_count', 'integer', none],
    ['rehab_unit_bed_cnt', 'rehab_unit_bed_count', 'integer', none],
    ['oprtg_room_cnt', 'operating_room_count', 'integer', none],
    ['endscpy_prcdr_rooms_cnt', 'endoscopy_room_count', 'integer', none],
    ['crdc_cthrtztn_prcdr_rooms_cnt', 'cardiac_catheterization_room_count', 'integer', none],
    ['tot_ofsite_emer_dept_cnt', 'offsite_emergency_department_count', 'integer', none],
    ['rn_cnt', 'rn_count', 'decimal', none],
    ['lpn_lvn_cnt', 'lpn_lvn_count', 'decimal', none],
    ['nrs_prctnr_cnt', 'nurse_practitioner_count', 'decimal', none],
    ['crna_cnt', 'crna_count', 'decimal', none],
    ['rsdnt_pgm_alpthc_sw', 'has_residency_allopathic', 'switch', none],
    ['rsdnt_pgm_dntl_sw', 'has_residency_dental', 'switch', none],
    ['rsdnt_pgm_ostpthc_sw', 'has_residency_osteopathic', 'switch', none],
    ['rsdnt_pgm_othr_sw', 'has_residency_other', 'switch', none],
    ['rsdnt_pgm_pdtrc_sw', 'has_residency_podiatric', 'switch', none],
    ['orgnl_prtcptn_dt', 'original_participation_date', 'date', none],
    ['crtfctn_dt', 'certification_date', 'date', none],
    ['trmntn_exprtn_dt', 'termination_date', 'date', none],
]) }}
{% endmacro %}

{% macro pos_typed_value(column, kind, width) %}
{#- One POS value typed by kind; a blank or a value that does not fit its kind is null, never rounded or guessed. -#}
{%- set value = "nullif(trim(" ~ column ~ "), '')" -%}
{%- if kind == 'code' -%}
    case when regexp_full_match({{ value }}, '[0-9]{1,{{ width }}}') then lpad({{ value }}, {{ width }}, '0') end
{%- elif kind == 'integer' -%}
    case when regexp_full_match({{ value }}, '[0-9]+') then {{ value }}::integer end
{%- elif kind == 'decimal' -%}
    case when regexp_full_match({{ value }}, '[0-9]+(\.[0-9]+)?') then {{ value }}::double end
{%- elif kind == 'switch' -%}
    case upper({{ value }}) when 'Y' then true when 'TRUE' then true when 'N' then false when 'FALSE' then false end
{%- elif kind == 'date' -%}
    case when regexp_full_match({{ value }}, '[0-9]{8}') then try_strptime({{ value }}, '%Y%m%d')::date end
{%- endif -%}
{% endmacro %}

{% macro us_states_and_dc() %}
{#- The 50 states and DC, the project's geographic universe. -#}
('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD',
    'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY')
{% endmacro %}
