{% macro hhs_numeric_columns() %}
{#- The HHS weekly numeric columns in bronze order: the 7-day averages, sums and coverage counts and the vaccination
    counts. A count of 1 to 3 is published as -999999 (failure mode 375). -#}
{{ return([
    'total_beds_7_day_avg',
    'all_adult_hospital_beds_7_day_avg',
    'all_adult_hospital_inpatient_beds_7_day_avg',
    'inpatient_beds_used_7_day_avg',
    'all_adult_hospital_inpatient_bed_occupied_7_day_avg',
    'inpatient_beds_used_covid_7_day_avg',
    'total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_avg',
    'total_adult_patients_hospitalized_confirmed_covid_7_day_avg',
    'total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_7_day_avg',
    'total_pediatric_patients_hospitalized_confirmed_covid_7_day_avg',
    'inpatient_beds_7_day_avg',
    'total_icu_beds_7_day_avg',
    'total_staffed_adult_icu_beds_7_day_avg',
    'icu_beds_used_7_day_avg',
    'staffed_adult_icu_bed_occupancy_7_day_avg',
    'staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_avg',
    'staffed_icu_adult_patients_confirmed_covid_7_day_avg',
    'total_patients_hospitalized_confirmed_influenza_7_day_avg',
    'icu_patients_confirmed_influenza_7_day_avg',
    'total_patients_hospitalized_confirmed_influenza_and_covid_7_day_avg',
    'total_beds_7_day_sum',
    'all_adult_hospital_beds_7_day_sum',
    'all_adult_hospital_inpatient_beds_7_day_sum',
    'inpatient_beds_used_7_day_sum',
    'all_adult_hospital_inpatient_bed_occupied_7_day_sum',
    'inpatient_beds_used_covid_7_day_sum',
    'total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_sum',
    'total_adult_patients_hospitalized_confirmed_covid_7_day_sum',
    'total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_7_day_sum',
    'total_pediatric_patients_hospitalized_confirmed_covid_7_day_sum',
    'inpatient_beds_7_day_sum',
    'total_icu_beds_7_day_sum',
    'total_staffed_adult_icu_beds_7_day_sum',
    'icu_beds_used_7_day_sum',
    'staffed_adult_icu_bed_occupancy_7_day_sum',
    'staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_sum',
    'staffed_icu_adult_patients_confirmed_covid_7_day_sum',
    'total_patients_hospitalized_confirmed_influenza_7_day_sum',
    'icu_patients_confirmed_influenza_7_day_sum',
    'total_patients_hospitalized_confirmed_influenza_and_covid_7_day_sum',
    'total_beds_7_day_coverage',
    'all_adult_hospital_beds_7_day_coverage',
    'all_adult_hospital_inpatient_beds_7_day_coverage',
    'inpatient_beds_used_7_day_coverage',
    'all_adult_hospital_inpatient_bed_occupied_7_day_coverage',
    'inpatient_beds_used_covid_7_day_coverage',
    'total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_coverage',
    'total_adult_patients_hospitalized_confirmed_covid_7_day_coverage',
    'total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_7_day_coverage',
    'total_pediatric_patients_hospitalized_confirmed_covid_7_day_coverage',
    'inpatient_beds_7_day_coverage',
    'total_icu_beds_7_day_coverage',
    'total_staffed_adult_icu_beds_7_day_coverage',
    'icu_beds_used_7_day_coverage',
    'staffed_adult_icu_bed_occupancy_7_day_coverage',
    'staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_coverage',
    'staffed_icu_adult_patients_confirmed_covid_7_day_coverage',
    'total_patients_hospitalized_confirmed_influenza_7_day_coverage',
    'icu_patients_confirmed_influenza_7_day_coverage',
    'total_patients_hospitalized_confirmed_influenza_and_covid_7_day_coverage',
    'previous_day_admission_adult_covid_confirmed_7_day_sum',
    'previous_day_admission_adult_covid_confirmed_18_19_7_day_sum',
    'previous_day_admission_adult_covid_confirmed_20_29_7_day_sum',
    'previous_day_admission_adult_covid_confirmed_30_39_7_day_sum',
    'previous_day_admission_adult_covid_confirmed_40_49_7_day_sum',
    'previous_day_admission_adult_covid_confirmed_50_59_7_day_sum',
    'previous_day_admission_adult_covid_confirmed_60_69_7_day_sum',
    'previous_day_admission_adult_covid_confirmed_70_79_7_day_sum',
    'previous_day_admission_adult_covid_confirmed_80_7_day_sum',
    'previous_day_admission_adult_covid_confirmed_unknown_7_day_sum',
    'previous_day_admission_pediatric_covid_confirmed_7_day_sum',
    'previous_day_covid_ed_visits_7_day_sum',
    'previous_day_admission_adult_covid_suspected_7_day_sum',
    'previous_day_admission_adult_covid_suspected_18_19_7_day_sum',
    'previous_day_admission_adult_covid_suspected_20_29_7_day_sum',
    'previous_day_admission_adult_covid_suspected_30_39_7_day_sum',
    'previous_day_admission_adult_covid_suspected_40_49_7_day_sum',
    'previous_day_admission_adult_covid_suspected_50_59_7_day_sum',
    'previous_day_admission_adult_covid_suspected_60_69_7_day_sum',
    'previous_day_admission_adult_covid_suspected_70_79_7_day_sum',
    'previous_day_admission_adult_covid_suspected_80_7_day_sum',
    'previous_day_admission_adult_covid_suspected_unknown_7_day_sum',
    'previous_day_admission_pediatric_covid_suspected_7_day_sum',
    'previous_day_total_ed_visits_7_day_sum',
    'previous_day_admission_influenza_confirmed_7_day_sum',
    'previous_day_admission_adult_covid_confirmed_7_day_coverage',
    'previous_day_admission_pediatric_covid_confirmed_7_day_coverage',
    'previous_day_admission_adult_covid_suspected_7_day_coverage',
    'previous_day_admission_pediatric_covid_suspected_7_day_coverage',
    'previous_week_personnel_covid_vaccinated_doses_administered_7_day',
    'total_personnel_covid_vaccinated_doses_none_7_day',
    'total_personnel_covid_vaccinated_doses_one_7_day',
    'total_personnel_covid_vaccinated_doses_all_7_day',
    'previous_week_patients_covid_vaccinated_doses_one_7_day',
    'previous_week_patients_covid_vaccinated_doses_all_7_day',
    'all_pediatric_inpatient_bed_occupied_7_day_avg',
    'all_pediatric_inpatient_bed_occupied_7_day_coverage',
    'all_pediatric_inpatient_bed_occupied_7_day_sum',
    'all_pediatric_inpatient_beds_7_day_avg',
    'all_pediatric_inpatient_beds_7_day_coverage',
    'all_pediatric_inpatient_beds_7_day_sum',
    'previous_day_admission_pediatric_covid_confirmed_0_4_7_day_sum',
    'previous_day_admission_pediatric_covid_confirmed_12_17_7_day_sum',
    'previous_day_admission_pediatric_covid_confirmed_5_11_7_day_sum',
    'previous_day_admission_pediatric_covid_confirmed_unknown_7_day_sum',
    'staffed_icu_pediatric_patients_confirmed_covid_7_day_avg',
    'staffed_icu_pediatric_patients_confirmed_covid_7_day_coverage',
    'staffed_icu_pediatric_patients_confirmed_covid_7_day_sum',
    'staffed_pediatric_icu_bed_occupancy_7_day_avg',
    'staffed_pediatric_icu_bed_occupancy_7_day_coverage',
    'staffed_pediatric_icu_bed_occupancy_7_day_sum',
    'total_staffed_pediatric_icu_beds_7_day_avg',
    'total_staffed_pediatric_icu_beds_7_day_coverage',
    'total_staffed_pediatric_icu_beds_7_day_sum'
]) }}
{% endmacro %}

{% macro hhs_number(column) %}
{#- A plain number of 0 or more; -999999 (suppressed), any other negative value and anything that is not a plain number
    are null [375]. -#}
case when ({{ strict_number(column) }}) >= 0 then {{ strict_number(column) }} end
{%- endmacro %}

{% macro slash_date(column) %}
{#- A date written YYYY/MM/DD; anything else is null [376]. -#}
{%- set value = "nullif(trim(" ~ column ~ "), '')" -%}
case when regexp_full_match({{ value }}, '[0-9]{4}/[0-9]{2}/[0-9]{2}') then try_strptime({{ value }}, '%Y/%m/%d')::date end
{%- endmacro %}

{% macro true_false(column) %}
{#- true and false in any case; anything else is null. -#}
case lower(trim({{ column }})) when 'true' then true when 'false' then false end
{%- endmacro %}

{% macro year_number(column) %}
{#- A 4-digit year; anything else is null [381]. -#}
case when regexp_full_match(trim({{ column }}), '[0-9]{4}') then trim({{ column }})::integer end
{%- endmacro %}

{% macro month_number(column) %}
{#- A month from 1 to 12; anything else is null [381]. -#}
case when regexp_full_match(trim({{ column }}), '0?[1-9]|1[0-2]') then trim({{ column }})::integer end
{%- endmacro %}
