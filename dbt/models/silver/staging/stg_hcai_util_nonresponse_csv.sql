-- hcai_util_nonresponse_csv with one copy of each stored file, through a projection of safe columns only; the label hold stays. Rejected text and the
-- person, phone and address columns never leave bronze.
{{ canonical_projection('hcai_util_nonresponse_csv', hcai_util_staging_projection(), hcai_util_safe_flags()) }}
