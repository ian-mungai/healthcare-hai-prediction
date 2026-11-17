-- illinois_report_card_csv with one copy of each stored file, through a projection of safe columns only; the label hold stays. Rejected text and the
-- person, phone and address columns never leave bronze.
{{ canonical_projection('illinois_report_card_csv', illinois_staging_projection()) }}
