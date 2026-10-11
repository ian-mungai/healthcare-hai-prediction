{% macro owner_flag_columns() %}
{#- The owner type flags, as published (Y, N or blank). The last four start with the April 2025 layout. -#}
{{ return([
    'created_for_acquisition_owner',
    'corporation_owner',
    'llc_owner',
    'medical_provider_supplier_owner',
    'management_services_company_owner',
    'medical_staffing_company_owner',
    'holding_company_owner',
    'investment_firm_owner',
    'financial_institution_owner',
    'consulting_firm_owner',
    'for_profit_owner',
    'non_profit_owner',
    'other_type_owner',
    'private_equity_company_owner',
    'reit_owner',
    'chain_home_office_owner',
    'owned_by_another_org_or_ind_owner'
]) }}
{% endmacro %}

{% macro enrollment_flag_columns() %}
{#- The enrollment flags, as published (Y, N or blank). -#}
{{ return([
    'multiple_npi_flag',
    'subgroup_general',
    'subgroup_acute_care',
    'subgroup_alcohol_drug',
    'subgroup_childrens',
    'subgroup_long_term',
    'subgroup_psychiatric',
    'subgroup_rehabilitation',
    'subgroup_short_term',
    'subgroup_swing_bed_approved',
    'subgroup_psychiatric_unit',
    'subgroup_rehabilitation_unit',
    'subgroup_specialty_hospital',
    'subgroup_other',
    'reh_conversion_flag'
]) }}
{% endmacro %}

{% macro yes_no(column) %}
{#- Y is true and N is false; blank, null and anything else are null [366]. -#}
case upper(trim({{ column }})) when 'Y' then true when 'N' then false end
{%- endmacro %}

{% macro month_day_year(column) %}
{#- A date written M/D/YYYY, with one or two digits for month and day; anything else is null [369]. -#}
{%- set value = "nullif(trim(" ~ column ~ "), '')" -%}
case when regexp_full_match({{ value }}, '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}') then try_strptime({{ value }}, '%m/%d/%Y')::date end
{%- endmacro %}

{% macro published_ccn(column) %}
{#- A CCN of 6 characters as published, or a 5-digit CCN that lost its leading zero padded to 6; anything else is null [370]. -#}
{%- set value = "upper(nullif(trim(" ~ column ~ "), ''))" -%}
case
    when regexp_full_match({{ value }}, '[0-9]{5}') then lpad({{ value }}, 6, '0')
    when regexp_full_match({{ value }}, '[0-9A-Z]{6}') then {{ value }}
end
{%- endmacro %}

{% macro associate_id(column) %}
{#- A 10-digit PECOS associate ID; some releases drop its leading zeros, so 1 to 9 digits are padded to 10. Anything
    else stays as published [714]. -#}
{%- set value = "nullif(trim(" ~ column ~ "), '')" -%}
case
    when regexp_full_match({{ value }}, '[0-9]{1,9}') then lpad({{ value }}, 10, '0')
    else {{ value }}
end
{%- endmacro %}

{% macro associate_id_padded(column) %}
{#- True when associate_id() padded the published value [714]. -#}
coalesce(regexp_full_match(nullif(trim({{ column }}), ''), '[0-9]{1,9}'), false)
{%- endmacro %}

{% macro ccn_parent_candidates(column) %}
{#- The parent CCNs a suffixed or unit identifier can stand for, each with its route [619]; empty for any other shape. -#}
{%- set value = "upper(nullif(trim(" ~ column ~ "), ''))" -%}
case
    when regexp_full_match({{ value }}, '[0-9]{7}')
        then [
            {'ccn': left(lpad({{ value }}, 8, '0'), 6), 'route': 'leading_zero'},
            {'ccn': left({{ value }}, 6), 'route': 'location_suffix'}
        ]
    when regexp_full_match({{ value }}, '[0-9A-Z]{2}[0-9]{4}([0-9]{2,3}|[A-Z])')
        then [{'ccn': left({{ value }}, 6), 'route': 'location_suffix'}]
    when regexp_full_match({{ value }}, '[0-9]{2}[STU][0-9]{3}([0-9]{2,3}|[A-Z])?')
        then [{'ccn': left({{ value }}, 2) || '0' || substr({{ value }}, 4, 3), 'route': 'unit_parent'}]
    else []::struct(ccn varchar, route varchar)[]
end
{%- endmacro %}


{% macro change_of_ownership_release_rows() %}
{#- Every change-of-ownership row of every stored release, typed, with its release period, CCN routes, event_key and
    chow_row_key [365] [369] to [372] [619] to [622] [714]. int_change_of_ownership_rows keeps one row per event from it
    and int_change_of_ownership_holds compares the releases (decision 4; failure modes 798 and 799). -#}
with

periods as (
    select
        member_sha256,
        release_id,
        release_label,
        period_start,
        period_end
    from {{ ref('ownership_release_periods') }}
    where bronze_table = 'cms_change_of_ownership'
),

changes as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        nullif(trim(enrollment_id_buyer), '') as enrollment_id_buyer,
        upper(nullif(trim(enrollment_state_buyer), '')) as enrollment_state_buyer,
        nullif(trim(provider_type_code_buyer), '') as provider_type_code_buyer,
        nullif(trim(provider_type_text_buyer), '') as provider_type_text_buyer,
        nullif(trim(npi_buyer), '') as npi_buyer,
        {{ yes_no('multiple_npi_flag_buyer') }} as multiple_npi_flag_buyer,
        {{ published_ccn('ccn_buyer') }} as ccn_buyer,
        upper(nullif(trim(ccn_buyer), '')) as ccn_buyer_published,
        {{ associate_id('associate_id_buyer') }} as associate_id_buyer,
        nullif(trim(organization_name_buyer), '') as organization_name_buyer,
        nullif(trim(doing_business_as_name_buyer), '') as doing_business_as_name_buyer,
        upper(nullif(trim(chow_type_code), '')) as chow_type_code,
        nullif(trim(chow_type_text), '') as chow_type_text,
        {{ month_day_year('effective_date') }} as effective_date,
        nullif(trim(enrollment_id_seller), '') as enrollment_id_seller,
        upper(nullif(trim(enrollment_state_seller), '')) as enrollment_state_seller,
        nullif(trim(provider_type_code_seller), '') as provider_type_code_seller,
        nullif(trim(provider_type_text_seller), '') as provider_type_text_seller,
        nullif(trim(npi_seller), '') as npi_seller,
        {{ yes_no('multiple_npi_flag_seller') }} as multiple_npi_flag_seller,
        {{ published_ccn('ccn_seller') }} as ccn_seller,
        upper(nullif(trim(ccn_seller), '')) as ccn_seller_published,
        {{ associate_id('associate_id_seller') }} as associate_id_seller,
        {{ associate_id_padded('associate_id_buyer') }} or {{ associate_id_padded('associate_id_seller') }} as associate_ids_padded,
        nullif(trim(organization_name_seller), '') as organization_name_seller,
        nullif(trim(doing_business_as_name_seller), '') as doing_business_as_name_seller
    from {{ ref('stg_cms_change_of_ownership') }}
),

pos_states as (
    select distinct
        ccn,
        state_code
    from {{ ref('int_pos_hospital_snapshots') }}
    where ccn is not null
),

candidates as (
    select
        member_sha256,
        source_row_number,
        'buyer' as party,
        enrollment_state_buyer as state,
        unnest({{ ccn_parent_candidates('ccn_buyer_published') }}) as candidate
    from changes
    where ccn_buyer is null
    union all
    select
        member_sha256,
        source_row_number,
        'seller' as party,
        enrollment_state_seller as state,
        unnest({{ ccn_parent_candidates('ccn_seller_published') }}) as candidate
    from changes
    where ccn_seller is null
),

candidate_ccns as (
    select
        member_sha256,
        source_row_number,
        party,
        state,
        struct_extract(candidate, 'ccn') as ccn,
        struct_extract(candidate, 'route') as route
    from candidates
),

parents as (
    -- Exactly one parent that POS lists in the party's enrollment state [619].
    select
        candidate_ccns.member_sha256,
        candidate_ccns.source_row_number,
        candidate_ccns.party,
        min(candidate_ccns.ccn) as ccn,
        min(candidate_ccns.route) as route
    from candidate_ccns
    inner join pos_states
        on
            candidate_ccns.ccn = pos_states.ccn
            and candidate_ccns.state = pos_states.state_code
    group by
        candidate_ccns.member_sha256,
        candidate_ccns.source_row_number,
        candidate_ccns.party
    having count(distinct candidate_ccns.ccn) = 1
)

select
    changes.* exclude (ccn_buyer, ccn_seller),
    periods.release_id,
    periods.release_label,
    periods.period_start,
    periods.period_end,
    coalesce(changes.ccn_buyer, buyers.ccn) as ccn_buyer,
    case
        when changes.ccn_buyer is not null then 'published'
        else buyers.route
    end as ccn_buyer_source,
    coalesce(changes.ccn_seller, sellers.ccn) as ccn_seller,
    case
        when changes.ccn_seller is not null then 'published'
        else sellers.route
    end as ccn_seller_source,
    concat_ws(
        '|',
        coalesce(changes.enrollment_id_buyer, ''),
        coalesce(changes.enrollment_id_seller, ''),
        coalesce(changes.effective_date::varchar, ''),
        coalesce(changes.chow_type_code, '')
    ) as event_key,
    changes.member_sha256 || ':' || changes.source_row_number as chow_row_key
from changes
left join periods on changes.member_sha256 = periods.member_sha256
left join parents as buyers
    on
        changes.member_sha256 = buyers.member_sha256
        and changes.source_row_number = buyers.source_row_number
        and buyers.party = 'buyer'
left join parents as sellers
    on
        changes.member_sha256 = sellers.member_sha256
        and changes.source_row_number = sellers.source_row_number
        and sellers.party = 'seller'
{% endmacro %}
