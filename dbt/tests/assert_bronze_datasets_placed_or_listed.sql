-- Every dataset ID that bronze holds is placed by the registry or listed as unplaced in the source_labels seed; an ID in neither list fails the build
-- (failure mode 945). Only the end-to-end fixture target exempts a dataset: its synthetic bronze gives every stored copy the ID fixture_dataset, which the
-- registry does not list. The exemption follows the target, not a variable: the launcher's fixture_dbt template always passes --target fixture and the real
-- build passes --target lakehouse, so a real build exempts nothing and a variable that names an exempt dataset on any other target is a compile error.
{%- set synthetic = var('synthetic_dataset_ids', ['fixture_dataset'] if target.name == 'fixture' else []) %}
{%- set refusal = 'synthetic_dataset_ids may exempt datasets only on the fixture target, not on ' ~ target.name %}
{%- if synthetic and target.name != 'fixture' %}{{ exceptions.raise_compiler_error(refusal) }}{% endif %}
select distinct observations.dataset_id
from {{ ref('stg_bronze__release_observations') }} as observations
left join {{ ref('source_labels') }} as source_labels
    on observations.dataset_id = source_labels.dataset_id
where
    observations.dataset_id is not null
    and source_labels.dataset_id is null {%- if synthetic %}
    and observations.dataset_id not in ({% for item in synthetic %}'{{ item }}'{% if not loop.last %}, {% endif %}{% endfor %}){% endif %}
