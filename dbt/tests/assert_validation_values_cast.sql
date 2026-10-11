-- Fails when a group D window or program-year row differs from the published staging row it comes from. The expected rows
-- are rebuilt here from the staging views, the file dates and the reviewed seeds: the latest dated file per key with one
-- row. The expectations read no column of the model they check and no macro the models call (not strict_number and not
-- validation_date). One finding per row and field:
--   row missing: a source row that should be kept has no model row (an alias row beside its exact ID is not expected);
--   row not in source: a model row has no source row of that file;
--   a text column: the model differs from the published text, or the published text is neither a number nor a published
--     mark (Not Available, Not Applicable, N/A, Too Few to Report) nor a reviewed value;
--   a number column: the model differs from the number the published text gives under the frozen decimal and exponent
--     grammar of mup_columns.sql (written again here), finite values only;
--   a HAC date: the model differs from the date written out of the digits of the published MM/DD/YYYY text, its text
--     companion differs from the published text or a non-blank text that is not a published mark gives a null date
--     (FY 2023 publishes the PSI-90 period as N/A);
--   measure_id: the model differs from the reviewed ID of measure_id_aliases or from its published ID.
-- [510] [519] [629] [630] [786] [787] [789]
{%- set grammar = "'-?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][-+]?[0-9]+)?'" %}
{%- set finite_number = "regexp_full_match(nullif(trim(@@), ''), " ~ grammar ~ ") and isfinite(try_cast(nullif(trim(@@), '') as double))" %}
{%- set expected_number = "case when " ~ finite_number ~ " then try_cast(nullif(trim(@@), '') as double) end" %}
{%- set pattern = "'^([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})$'" %}
{%- set date_part = "lpad(regexp_extract(trim(@@), " ~ pattern ~ ", %n), 2, '0')" %}
{%- set date_head = "try_cast(regexp_extract(trim(@@), " ~ pattern ~ ", 3) || '-' || " %}
{%- set expected_date = date_head ~ date_part | replace('%n', '1') ~ " || '-' || " ~ date_part | replace('%n', '2') ~ " as date)" %}
{%- set window_texts = ['score', 'denominator', 'lower_estimate', 'higher_estimate'] %}
{%- set models = [
    {
        'name': 'int_cc_unplanned_visits_windows',
        'table': 'cms_cc_unplanned_hospital_visits_hospital',
        'entity': 'coalesce(stg.facility_id, stg.provider_id)',
        'measure': 'stg.measure_id',
        'start': 'coalesce(stg.start_date, stg.measure_start_date)',
        'end': 'coalesce(stg.end_date, stg.measure_end_date)',
        'texts': window_texts,
        'numbers': [['score_number', 'score']]
    },
    {
        'name': 'int_cc_complications_deaths_windows',
        'table': 'cms_cc_complications_and_deaths_hospital',
        'entity': 'coalesce(stg.facility_id, stg.provider_id)',
        'measure': 'stg.measure_id',
        'start': 'coalesce(stg.start_date, stg.measure_start_date)',
        'end': 'coalesce(stg.end_date, stg.measure_end_date)',
        'texts': window_texts,
        'numbers': [['score_number', 'score']]
    },
    {
        'name': 'int_cc_hrrp_windows',
        'table': 'cms_cc_hospital_readmissions_reduction_program_hospital',
        'entity': 'stg.facility_id',
        'measure': 'stg.measure_name',
        'start': 'stg.start_date',
        'end': 'stg.end_date',
        'texts': [
            'excess_readmission_ratio',
            'predicted_readmission_rate',
            'expected_readmission_rate',
            'number_of_discharges',
            'number_of_readmissions'
        ],
        'numbers': [['excess_readmission_ratio_number', 'excess_readmission_ratio']]
    }
] %}
{%- set programs = [
    {
        'name': 'int_hac_program_years',
        'table': 'cms_cc_hac_reduction_program_hospital',
        'entity': 'stg.facility_id',
        'year': 'try_cast(trim(stg.fiscal_year) as integer)',
        'texts': [
            ['total_hac_score', 'stg.total_hac_score'],
            ['psi_90_value', 'coalesce(stg.psi_90_composite_value, stg.psi_90_composite)'],
            ['psi_90_w_z_score', 'stg.psi_90_w_z_score'],
            ['clabsi_w_z_score', 'stg.clabsi_w_z_score'],
            ['cauti_w_z_score', 'stg.cauti_w_z_score'],
            ['ssi_w_z_score', 'stg.ssi_w_z_score'],
            ['cdi_w_z_score', 'stg.cdi_w_z_score'],
            ['mrsa_w_z_score', 'stg.mrsa_w_z_score']
        ],
        'numbers': [['total_hac_score_number', 'total_hac_score']],
        'dates': [
            ['psi_90_start_date', 'stg.psi_90_start_date'],
            ['psi_90_end_date', 'stg.psi_90_end_date'],
            ['hai_measures_start_date', 'stg.hai_measures_start_date'],
            ['hai_measures_end_date', 'stg.hai_measures_end_date']
        ]
    },
    {
        'name': 'int_vbp_program_years',
        'table': 'cms_cc_hvbp_tps',
        'entity': 'coalesce(stg.facility_id, stg.provider_number)',
        'year': 'vbp',
        'texts': [
            ['unweighted_normalized_clinical_care_domain_score', 'stg.unweighted_normalized_clinical_care_domain_score'],
            ['weighted_normalized_clinical_care_domain_score', 'stg.weighted_normalized_clinical_care_domain_score'],
            ['unweighted_normalized_clinical_outcomes_domain_score', 'stg.unweighted_normalized_clinical_outcomes_domain_score'],
            ['weighted_normalized_clinical_outcomes_domain_score', 'stg.weighted_normalized_clinical_outcomes_domain_score'],
            ['unweighted_person_and_community_engagement_domain_score', 'stg.unweighted_person_and_community_engagement_domain_score'],
            ['weighted_person_and_community_engagement_domain_score', 'stg.weighted_person_and_community_engagement_domain_score'],
            ['unweighted_normalized_safety_domain_score', 'stg.unweighted_normalized_safety_domain_score'],
            ['weighted_safety_domain_score', 'stg.weighted_safety_domain_score'],
            ['unweighted_normalized_efficiency_and_cost_reduction_domain_score', 'stg.unweighted_normalized_efficiency_and_cost_reduction_domain_score'],
            ['weighted_efficiency_and_cost_reduction_domain_score', 'stg.weighted_efficiency_and_cost_reduction_domain_score'],
            ['total_performance_score', 'stg.total_performance_score']
        ],
        'numbers': [
            ['unweighted_normalized_clinical_care_domain_score_number', 'unweighted_normalized_clinical_care_domain_score'],
            ['weighted_normalized_clinical_care_domain_score_number', 'weighted_normalized_clinical_care_domain_score'],
            ['unweighted_normalized_clinical_outcomes_domain_score_number', 'unweighted_normalized_clinical_outcomes_domain_score'],
            ['weighted_normalized_clinical_outcomes_domain_score_number', 'weighted_normalized_clinical_outcomes_domain_score'],
            ['unweighted_person_and_community_engagement_domain_score_number', 'unweighted_person_and_community_engagement_domain_score'],
            ['weighted_person_and_community_engagement_domain_score_number', 'weighted_person_and_community_engagement_domain_score'],
            ['unweighted_normalized_safety_domain_score_number', 'unweighted_normalized_safety_domain_score'],
            ['weighted_safety_domain_score_number', 'weighted_safety_domain_score'],
            ['unweighted_normalized_efficiency_and_cost_reduction_domain_score_number', 'unweighted_normalized_efficiency_and_cost_reduction_domain_score'],
            ['weighted_efficiency_and_cost_reduction_domain_score_number', 'weighted_efficiency_and_cost_reduction_domain_score'],
            ['total_performance_score_number', 'total_performance_score']
        ],
        'dates': []
    }
] %}
{%- set prep = {} %}
{%- for m in models + programs %}
{%- set is_window = m.measure is defined %}
{%- set source_keys = ['entity_id', 'measure_id', 'window_start', 'window_end'] if is_window else ['ccn', 'fiscal_year'] %}
{%- set model_keys = ['entity_id', 'published_measure_id', 'window_start', 'window_end'] if is_window else ['ccn', 'fiscal_year'] %}
{%- set expected_key = [] %}
{%- set built_key = [] %}
{%- set not_null_keys = [] %}
{%- set match = ['built.member_sha256 = expected.member_sha256'] %}
{%- for key in source_keys %}
{%- do expected_key.append('expected.' ~ key) %}
{%- do built_key.append('built.' ~ model_keys[loop.index0]) %}
{%- do not_null_keys.append('published.' ~ key ~ ' is not null') %}
{%- do match.append('built.' ~ model_keys[loop.index0] ~ ' = expected.' ~ key) %}
{%- endfor %}
{%- do prep.update({m.name: {
    'is_window': is_window,
    'is_vbp': not is_window and m.year == 'vbp',
    'source_keys': source_keys,
    'partition': source_keys | join(', '),
    'texts': m.texts if is_window else m.texts | map(attribute=0) | list,
    'expected_key': expected_key | join(', '),
    'built_key': built_key | join(', '),
    'not_null': not_null_keys | join('\n        and '),
    'join_on': match | join('\n            and '),
    'seed_join': ('\n    left join ' ~ ref('vbp_file_fiscal_years') ~ ' as reviewed\n        on stg._member_path = reviewed.file_name') if not is_window and m.year == 'vbp' else '',
    'alias_filter': ("\n        and not exists (\n            select 1\n            from " ~ ref('measure_id_aliases') ~ " as aliases\n            inner join " ~ m.name ~ "_expected as exact_row\n                on\n                    aliases.measure_id = exact_row.measure_id\n                    and expected.entity_id = exact_row.entity_id\n                    and expected.window_start = exact_row.window_start\n                    and expected.window_end = exact_row.window_end\n            where\n                aliases.source_model = '" ~ m.name ~ "'\n                and aliases.published_measure_id = expected.measure_id\n        )") if is_window else '',
    'where_on': match | join('\n                and ')
}}) %}
{%- endfor %}
with
{% for m in models + programs %}
{%- set p = prep[m.name] %}
{{ m.name }}_source as (
    select
        case
            when regexp_full_match(trim({{ m.entity }}), '[0-9]{5}') then lpad(trim({{ m.entity }}), 6, '0')
            when regexp_full_match(trim({{ m.entity }}), '[0-9A-Z]{6}') then trim({{ m.entity }})
        end as {{ p.source_keys[0] }},
        {%- if p.is_window %}
        nullif(trim({{ m.measure }}), '') as measure_id,
        try_strptime({{ m.start }}, '%m/%d/%Y')::date as window_start,
        try_strptime({{ m.end }}, '%m/%d/%Y')::date as window_end,
        {%- for text in m.texts %}
        stg.{{ text }},
        {%- endfor %}
        {%- else %}
        {{ 'coalesce(try_cast(trim(stg.fiscal_year) as integer), reviewed.fiscal_year)' if p.is_vbp else m.year }} as fiscal_year,
        {%- for alias, expression in m.texts + m.dates %}
        {{ expression }} as {{ alias }},
        {%- endfor %}
        {%- endif %}
        stg._member_sha256 as member_sha256,
        stg.is_label_held
    from {{ ref('stg_' ~ m.table) }} as stg{{ p.seed_join }}
),

{{ m.name }}_dated as (
    select
        published.*,
        files.latest_publication_date as release_date
    from {{ m.name }}_source as published
    inner join {{ ref('stg_bronze__files') }} as files
        on
            files.bronze_table = '{{ m.table }}'
            and published.member_sha256 = files.member_sha256
    where
        not published.is_label_held
        and files.latest_publication_date is not null
        and {{ p.not_null }}
),

{{ m.name }}_newest as (
    select
        *,
        max(release_date) over (partition by {{ p.partition }}) as newest_release
    from {{ m.name }}_dated
),

{{ m.name }}_counted as (
    select
        *,
        count(*) over (partition by {{ p.partition }}) as newest_rows
    from {{ m.name }}_newest
    where release_date = newest_release
),

{{ m.name }}_expected as (
    select *
    from {{ m.name }}_counted
    where newest_rows = 1
),
{% endfor %}
findings as (
    {%- for m in models + programs %}
    {%- set p = prep[m.name] %}
    {{ 'union all' if not loop.first }}
    select
        '{{ m.name }}' as checked_model,
        concat_ws(':', {{ p.expected_key }}) as window_key,
        'row missing' as field
    from {{ m.name }}_expected as expected
    where
        not exists (
            select 1
            from {{ ref(m.name) }} as built
            where
                {{ p.where_on }}
        )
        {{ p.alias_filter }}
    union all
    select
        '{{ m.name }}' as checked_model,
        concat_ws(':', {{ p.built_key }}) as window_key,
        'row not in source' as field
    from {{ ref(m.name) }} as built
    where
        not exists (
            select 1
            from {{ m.name }}_expected as expected
            where
                {{ p.where_on }}
        )
    {%- if p.is_window %}
    union all
    select
        '{{ m.name }}' as checked_model,
        concat_ws(':', {{ p.built_key }}) as window_key,
        'measure_id' as field
    from {{ ref(m.name) }} as built
    left join {{ ref('measure_id_aliases') }} as aliases
        on
            aliases.source_model = '{{ m.name }}'
            and built.published_measure_id = aliases.published_measure_id
    where built.measure_id is distinct from coalesce(aliases.measure_id, built.published_measure_id)
    {%- endif %}
    {%- for text in p.texts %}
    union all
    select
        '{{ m.name }}' as checked_model,
        concat_ws(':', {{ p.expected_key }}) as window_key,
        '{{ text }}' as field
    from {{ ref(m.name) }} as built
    inner join {{ m.name }}_expected as expected
        on
            {{ p.join_on }}
    where
        built.{{ text }} is distinct from expected.{{ text }}
        or (
            nullif(trim(expected.{{ text }}), '') is not null
            and trim(expected.{{ text }}) not in {{ validation_tokens() }}
            {%- if not p.is_window %}
            and trim(expected.{{ text }}) not in {{ validation_reviewed_values() }}
            {%- endif %}
            and ({{ finite_number | replace('@@', 'expected.' ~ text) }}) is not true
        )
    {%- endfor %}
    {%- for number_column, text_column in m.numbers %}
    union all
    select
        '{{ m.name }}' as checked_model,
        concat_ws(':', {{ p.expected_key }}) as window_key,
        '{{ number_column }}' as field
    from {{ ref(m.name) }} as built
    inner join {{ m.name }}_expected as expected
        on
            {{ p.join_on }}
    where built.{{ number_column }} is distinct from {{ expected_number | replace('@@', 'expected.' ~ text_column) }}
    {%- endfor %}
    {%- if not p.is_window %}
    {%- for date_column, expression in m.dates %}
    union all
    select
        '{{ m.name }}' as checked_model,
        concat_ws(':', {{ p.expected_key }}) as window_key,
        '{{ date_column }}' as field
    from {{ ref(m.name) }} as built
    inner join {{ m.name }}_expected as expected
        on
            {{ p.join_on }}
    where
        built.{{ date_column }}_text is distinct from expected.{{ date_column }}
        or built.{{ date_column }} is distinct from {{ expected_date | replace('@@', 'expected.' ~ date_column) }}
        or (
            nullif(trim(expected.{{ date_column }}), '') is not null
            and trim(expected.{{ date_column }}) not in {{ validation_tokens() }}
            and {{ expected_date | replace('@@', 'expected.' ~ date_column) }} is null
        )
    {%- endfor %}
    {%- endif %}
    {%- endfor %}
)

select * from findings
