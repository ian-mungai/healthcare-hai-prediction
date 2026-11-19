"""One list of bundle documents and the rule each one breaks, for every reader of the model bundle files.

The model API loader (``scripts/serving/mlflow_loader.py``), the promote action (``scripts/training/promote.py``) and the promotion check
(``scripts/training/promotion_check.py``) all apply the data rules of ``scripts/model_code.py``. Each consumer's cases run this list, so the readers cannot
drift apart again: a document that one refuses and another accepts fails a case. An entry names the file it replaces, a ``build`` that returns the document
(a JSON value, or raw bytes) from a deep copy of the consumer's own good document, the shared rule it breaks (an empty text when every reader accepts it)
and the reason the promote action reports.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any, NamedTuple

FEATURE_LIST, PROVENANCE, SELF_CHECK = "feature_list", "provenance", "self_check"
Build = Callable[[Any], Any]
MIB = 1 << 20
# What the promote action reports for each rule of the shared module, by file (a rule not named for a file is reported as its own name).
PROMOTE_REASONS = {
    (FEATURE_LIST, "unreadable"): "self_check_feature_list",
    (FEATURE_LIST, "too_large"): "self_check_feature_list",
    (FEATURE_LIST, "not_json"): "self_check_feature_list",
    (FEATURE_LIST, "feature_list"): "self_check_feature_list",
    (PROVENANCE, "unreadable"): "model_provenance_unreadable",
    (PROVENANCE, "too_large"): "model_provenance_unreadable",
    (PROVENANCE, "not_json"): "model_provenance_unreadable",
    (PROVENANCE, "provenance"): "model_provenance_unreadable",
    (SELF_CHECK, "unreadable"): "self_check_missing",
    (SELF_CHECK, "too_large"): "self_check_too_large",
    (SELF_CHECK, "not_json"): "self_check_not_json",
    (SELF_CHECK, "self_check_not_object"): "self_check_not_json",
}


class Entry(NamedTuple):
    """One document of the list. ``others`` replaces further files of the same bundle (the self-check's references equal the feature list's, so a case about
    them changes both). ``file_only`` marks a document whose verdict is the rule of its own file alone: a feature list of a result that is not registrable
    (empty lists, null slots) is a good feature list that has no self-check and no model, so only the feature list rule judges it."""

    name: str
    file: str
    build: Build
    rule: str
    others: tuple[tuple[str, Build], ...] = ()
    file_only: bool = False


def reason_of(entry: Entry) -> str:
    """The reason the promote action reports for the entry, or an empty text for a document every reader accepts."""
    return PROMOTE_REASONS.get((entry.file, entry.rule), entry.rule) if entry.rule else ""


def encoded(document: Any) -> bytes:
    """A JSON value as text with Python's own leniency (NaN and Infinity written as tokens), the way a careless writer would save it."""
    return json.dumps(document).encode()


def edited(change: Callable[[Any], object]) -> Build:
    def build(document: Any) -> Any:
        change(document)
        return document

    return build


def put(key: str, value: Any) -> Build:
    return edited(lambda document: document.__setitem__(key, value))


def drop(*keys: str) -> Build:
    return edited(lambda document: [document.pop(key, None) for key in keys])


def replace_token(change: Callable[[Any], None], placeholder: str, token: str) -> Build:
    """Set a placeholder text in the document with ``change``, then write the document with the token (``NaN``, ``Infinity``, ``1e999``) in its place."""

    def build(document: Any) -> bytes:
        change(document)
        return encoded(document).replace(f'"{placeholder}"'.encode(), token.encode())

    return build


def first_key(key: str, value: Any) -> Build:
    """The document with ``key`` written twice: first with ``value``, then (last, so a lenient parser keeps it) with the good document's own value."""

    def build(document: Any) -> bytes:
        return ("{" + json.dumps(key) + ": " + json.dumps(value) + ", " + json.dumps(document)[1:]).encode()

    return build


def as_text(encoding: str) -> Build:
    return lambda document: json.dumps(document).encode(encoding)


def cell(value: Any) -> Build:
    return edited(lambda document: document["rows"][0].__setitem__(0, value))


def prediction(value: Any) -> Build:
    return edited(lambda document: document["predictions"].__setitem__(0, value))


def many(document: Any, count: int) -> None:
    document.update(references=[f"r:{number}" for number in range(count)], columns=[f"c{number}" for number in range(count)])


def short_row(document: Any) -> None:
    document["rows"][0] = document["rows"][0][:-1]


def long_row(document: Any) -> None:
    document["rows"][0] = [*document["rows"][0], 1]


def nine_rows(document: Any) -> None:
    document["rows"] = [document["rows"][0]] * 9
    document["predictions"] = [0.5] * 9


def no_rows(document: Any) -> None:
    document["rows"], document["predictions"] = [], []


def one_prediction_short(document: Any) -> None:
    document["predictions"] = document["predictions"][:-1]


def references_differ(document: Any) -> None:
    document["references"] = [*document["references"], "extra"]


def kept_one(document: Any) -> None:
    document.pop("kept_columns", None)
    document["kept_references"] = ["a"]


def kept_unequal(document: Any) -> None:
    document["kept_references"], document["kept_columns"] = ["a", "b"], ["a"]


def columns_one_short(document: Any) -> None:
    document["columns"] = document["columns"][:-1]


def empty_first_reference(document: Any) -> None:
    document["references"][0] = ""


def no_references(document: Any) -> None:
    document.update(references=[], columns=[])
    for key in ("kept_references", "kept_columns"):
        if key in document:
            document[key] = []


def no_references_in_a_self_check(document: Any) -> None:
    document["references"] = []
    document["rows"] = [[] for _ in document["rows"]]


def not_registrable(document: Any) -> None:
    """The feature list the writer saves for a result with no registry candidate: null slots and empty lists, and no self-check or model beside it."""
    document.clear()
    document.update(slot=None, references=[], columns=[], fitted_slot=None, kept_references=[], kept_columns=[])


def deep(depth: int) -> Any:
    value: Any = 1
    for _ in range(depth):
        value = [value]
    return value


def too_large() -> Build:
    return lambda document: b"{" + b" " * MIB + b"}"


def entries() -> list[Entry]:
    """Every document of the list. The three probes of the Nov 19 2026 review of slice S6 are ``duplicate_schema_key``, ``utf16_self_check`` and
    ``valid_provenance_with_an_unused_nan_field``."""
    s, f, p = SELF_CHECK, FEATURE_LIST, PROVENANCE
    long = "x" * 257
    items = [
        # How a file is read: one UTF-8 document, no repeated key, no NaN or Infinity, at most 1 MiB.
        Entry("duplicate_schema_key", s, first_key("schema", 2), "not_json"),
        Entry("utf16_self_check", s, as_text("utf-16"), "not_json"),
        Entry("utf16_without_a_mark_self_check", s, as_text("utf-16-le"), "not_json"),
        Entry("utf32_self_check", s, as_text("utf-32"), "not_json"),
        Entry("utf8_byte_order_mark_self_check", s, as_text("utf-8-sig"), "not_json"),
        Entry("self_check_that_is_not_json", s, lambda document: b"{", "not_json"),
        Entry("self_check_over_one_mebibyte", s, too_large(), "too_large"),
        Entry("nan_cell", s, replace_token(lambda d: d["rows"][0].__setitem__(0, "@@"), "@@", "NaN"), "not_json"),
        Entry("infinite_prediction", s, replace_token(lambda d: d["predictions"].__setitem__(0, "@@"), "@@", "Infinity"), "not_json"),
        Entry("negative_infinite_prediction", s, replace_token(lambda d: d["predictions"].__setitem__(0, "@@"), "@@", "-Infinity"), "not_json"),
        Entry("duplicate_slot_key_feature_list", f, first_key("slot", "other"), "not_json"),
        Entry("utf16_feature_list", f, as_text("utf-16"), "not_json"),
        Entry("feature_list_over_one_mebibyte", f, too_large(), "too_large"),
        Entry("duplicate_key_provenance", p, first_key("run_id", "other"), "not_json"),
        Entry("utf16_provenance", p, as_text("utf-16"), "not_json"),
        Entry("utf32_provenance", p, as_text("utf-32"), "not_json"),
        Entry(
            "valid_provenance_with_an_unused_nan_field", p, replace_token(lambda d: d.__setitem__("timings", {"fit_seconds": "@@"}), "@@", "NaN"), "not_json"
        ),
        Entry("provenance_over_one_mebibyte", p, too_large(), "too_large"),
        # The self-check: keys, schema, slot, references, rows, cells and predictions.
        Entry("self_check_that_is_not_an_object", s, lambda document: [1], "self_check_not_object"),
        Entry("self_check_with_an_extra_key", s, put("note", "x"), "self_check_keys"),
        Entry("self_check_with_no_slot", s, drop("slot"), "self_check_keys"),
        Entry("self_check_of_another_schema", s, put("schema", 2), "self_check_keys"),
        Entry("self_check_with_a_boolean_schema", s, put("schema", True), "self_check_keys"),
        Entry("self_check_slot_that_is_not_text", s, put("slot", 3), "self_check_keys"),
        Entry("self_check_slot_of_257_characters", s, put("slot", long), "self_check_keys"),
        Entry("self_check_slot_of_256_characters", s, put("slot", "x" * 256), ""),
        Entry("self_check_with_an_empty_slot", s, put("slot", ""), "self_check_keys"),
        Entry("self_check_with_an_empty_reference_name", s, edited(empty_first_reference), "self_check_references", ((f, edited(empty_first_reference)),)),
        Entry("self_check_with_no_references", s, edited(no_references_in_a_self_check), "self_check_references", ((f, edited(no_references)),)),
        Entry("self_check_references_that_differ", s, edited(references_differ), "self_check_references"),
        Entry("self_check_with_no_rows", s, edited(no_rows), "self_check_row_count"),
        Entry("self_check_with_nine_rows", s, edited(nine_rows), "self_check_row_count"),
        Entry("self_check_rows_that_are_not_a_list", s, put("rows", "rows"), "self_check_row_count"),
        Entry("self_check_row_one_value_short", s, edited(short_row), "self_check_row_width"),
        Entry("self_check_row_one_value_long", s, edited(long_row), "self_check_row_width"),
        Entry("self_check_row_that_is_not_a_list", s, edited(lambda d: d["rows"].__setitem__(0, "private")), "self_check_row_width"),
        Entry("self_check_cell_that_is_a_boolean", s, cell(True), "self_check_cells"),
        Entry("self_check_cell_that_is_a_list", s, cell([1]), "self_check_cells"),
        Entry("self_check_cell_of_257_characters", s, cell(long), "self_check_cells"),
        Entry("self_check_cell_that_is_an_integer_beyond_the_float_range", s, cell(10**400), "self_check_cells"),
        Entry("self_check_cell_that_overflows", s, replace_token(lambda d: d["rows"][0].__setitem__(0, "@@"), "@@", "1e999"), "self_check_cells"),
        Entry("self_check_with_one_prediction_too_few", s, edited(one_prediction_short), "self_check_predictions"),
        Entry("self_check_prediction_that_is_text", s, prediction("0.9"), "self_check_predictions"),
        Entry("self_check_prediction_that_is_a_boolean", s, prediction(True), "self_check_predictions"),
        Entry("self_check_prediction_that_is_an_integer_beyond_the_float_range", s, prediction(10**400), "self_check_predictions"),
        Entry(
            "self_check_prediction_that_overflows", s, replace_token(lambda d: d["predictions"].__setitem__(0, "@@"), "@@", "-1e999"), "self_check_predictions"
        ),
        Entry("self_check_predictions_that_are_not_a_list", s, put("predictions", 0.9), "self_check_predictions"),
        # The feature list: its six keys, its two required lists and their bounds.
        Entry("feature_list_that_is_not_an_object", f, lambda document: ["a"], "feature_list"),
        Entry("feature_list_with_an_extra_key", f, put("note", "x"), "feature_list"),
        Entry("feature_list_with_no_references", f, drop("references"), "feature_list"),
        Entry("feature_list_with_no_columns", f, drop("columns"), "feature_list"),
        Entry("feature_list_with_columns_that_are_not_a_list", f, put("columns", {"a": 1}), "feature_list"),
        Entry("feature_list_with_one_column_too_few", f, edited(columns_one_short), "feature_list"),
        Entry("feature_list_with_empty_columns", f, put("columns", []), "feature_list"),
        Entry("feature_list_with_a_column_that_is_not_text", f, edited(lambda d: d["columns"].__setitem__(0, 5)), "feature_list"),
        Entry("feature_list_with_a_reference_that_is_not_text", f, edited(lambda d: d["references"].__setitem__(0, 5)), "feature_list"),
        Entry("feature_list_with_2001_references", f, edited(lambda d: many(d, 2001)), "feature_list"),
        Entry("feature_list_with_a_reference_of_257_characters", f, edited(lambda d: d["references"].__setitem__(0, long)), "feature_list"),
        Entry("feature_list_with_a_column_of_257_characters", f, edited(lambda d: d["columns"].__setitem__(0, long)), "feature_list"),
        Entry("feature_list_slot_of_257_characters", f, put("slot", long), "feature_list"),
        Entry("feature_list_fitted_slot_of_257_characters", f, put("fitted_slot", long), "feature_list"),
        Entry("feature_list_slot_that_is_not_text", f, put("slot", 3), "feature_list"),
        Entry("feature_list_with_one_kept_list_only", f, edited(kept_one), "feature_list"),
        Entry("feature_list_with_kept_lists_of_unequal_length", f, edited(kept_unequal), "feature_list"),
        Entry("feature_list_of_a_result_that_is_not_registrable", f, edited(not_registrable), "", (), True),
        Entry("feature_list_of_the_two_required_keys_only", f, edited(lambda d: [d.pop(key) for key in list(d) if key not in ("references", "columns")]), ""),
        # The provenance: its nine keys, the kind of each and the three budgets of the nested values.
        Entry("provenance_that_is_not_an_object", p, lambda document: ["a"], "provenance"),
        Entry("provenance_with_an_unknown_key", p, put("note", "x"), "provenance"),
        Entry("provenance_with_a_value_nested_too_deep", p, put("timings", {"a": deep(5)}), "provenance"),
        Entry("provenance_with_a_text_of_257_characters", p, put("timings", {"a": long}), "provenance"),
        Entry("provenance_with_a_key_of_257_characters", p, put("timings", {long: 1}), "provenance"),
        Entry("provenance_with_an_array_of_65_entries", p, put("timings", {"a": [1] * 65}), "provenance"),
        Entry("provenance_with_more_than_1024_values", p, put("inputs", {f"k{n}": {f"v{m}": 1 for m in range(60)} for n in range(18)}), "provenance"),
        Entry("provenance_with_a_run_id_that_is_a_number", p, put("run_id", 5), "provenance"),
        Entry("provenance_with_a_dirty_flag_that_is_text", p, put("git_dirty", "yes"), "provenance"),
        Entry("provenance_with_a_try_that_is_a_boolean", p, put("try", True), "provenance"),
        Entry("provenance_with_timings_that_are_a_list", p, put("timings", [1]), "provenance"),
        Entry("provenance_with_versions_that_are_a_list", p, put("resolved_versions", ["1.0"]), "provenance"),
        Entry("provenance_with_a_version_that_is_a_number", p, put("resolved_versions", {"numpy": 2}), "provenance"),
        Entry("provenance_with_a_value_that_is_an_integer_beyond_the_float_range", p, put("timings", {"a": 10**400}), "provenance"),
        Entry("provenance_with_nulls_and_a_flag", p, edited(lambda d: d.update(git_commit=None, git_dirty=None, run_id=None, **{"try": 2})), ""),
    ]
    return items


def build(entry: Entry, good: Any) -> Any:
    """The document of the entry made from a deep copy of the consumer's own good document of that file (a JSON value or raw bytes)."""
    return entry.build(json.loads(json.dumps(good)))


def documents(entry: Entry, good: Mapping[str, Any]) -> dict[str, Any]:
    """Every file the entry replaces, by file name (``feature_list``, ``provenance`` or ``self_check``), each made from the consumer's own good document."""
    made = {entry.file: build(entry, good[entry.file])}
    for file, builder in entry.others:
        made[file] = builder(json.loads(json.dumps(good[file])))
    return made
