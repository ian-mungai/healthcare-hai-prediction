-- One row per IPPS and occupational-mix observation with its labels from the generated map: role, rule and data
-- fiscal years, identity year and rule stage, read from its names and stored container (failure modes 178 to 182).
{{ config(materialized='table', pre_hook='{{ assert_original_label_keys() }}') }}

with

copies as (
    select
        observation_id,
        copy_key,
        bronze_table,
        object_key,
        member_sha256,
        file_name
    from {{ ref('stg_bronze__release_observations') }}
    where source_family in ('ipps', 'occupational_mix')
),

labels as (
    select distinct
        bronze_table,
        object_key,
        member_sha256,
        label_source,
        family,
        role,
        rule_fiscal_year,
        data_fiscal_year,
        identity_year,
        rule_stage
    from {{ ref('ipps_occmix_copy_labels') }}
),

final as (
    select
        copies.observation_id,
        copies.observation_id as copy_key,
        copies.bronze_table,
        copies.object_key,
        copies.member_sha256,
        copies.file_name,
        labels.label_source,
        labels.family,
        labels.role,
        labels.rule_fiscal_year,
        labels.data_fiscal_year,
        labels.identity_year,
        labels.rule_stage,
        labels.member_sha256 = copies.member_sha256 as is_label_checksum_match
    from copies
    left join labels
        on
            copies.bronze_table = labels.bronze_table
            and copies.object_key = labels.object_key
)

select * from final
