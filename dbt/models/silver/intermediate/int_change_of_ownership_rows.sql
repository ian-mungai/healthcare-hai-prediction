-- One row per change of ownership and stored change-of-ownership file (release), with the release label and catalog
-- period from the ownership_release_periods seed. Each release lists events back to 2016, so the same event appears in
-- several files under one event_key (buyer enrollment, seller enrollment, effective date, type); the alignment step chooses
-- the release. CCNs follow the enrollment rule, including parent CCNs for suffixed identifiers in the party's enrollment
-- state; the published values are kept and ccn_buyer_source and ccn_seller_source name the route (failure modes 365,
-- 369 to 372, 619 to 622 and 714; associate IDs that lost leading zeros are padded to 10 digits).
{{ config(materialized='table') }}

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
