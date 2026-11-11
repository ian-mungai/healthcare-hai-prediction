"""The SNS topics that the Terraform sources under ``infra/`` create, read with a block parser that refuses every form it cannot read.

The project user's subscribe grant names each alert topic by its exact ARN. A pattern cannot show that a topic belongs to the project, so the list of
topics must come from the Terraform sources, and a topic must never be missed. The reader therefore supports one form only and fails closed on the rest:

- ``resource "aws_sns_topic" "<label>"`` in a ``.tf`` or ``.tf.json`` file, with no ``count``, ``for_each`` or ``dynamic`` block, whose ``name`` is
  ``format(local.<name>, "<suffix>")`` and whose ``local.<name>`` in the same folder is exactly ``"${var.project_name}_%s_${var.environment}"``;
- a ``module`` call only from a local source folder that creates no topic, so a module cannot build its own topic names;
- no CloudFormation stack resource and no ``AWS::SNS::Topic`` or ``create-topic`` text anywhere.

``inventory`` returns the suffixes of the supported topics and one problem for every file, block or text it refuses. The HCL reader finds the blocks and
top-level attributes only: comments, quoted strings with their interpolations and heredocs are masked first, so a brace or a keyword inside them is never read.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

NAME_TEMPLATE = "${var.project_name}_%s_${var.environment}"
TOPIC_TYPE = "aws_sns_topic"
UNSUPPORTED_TYPES = frozenset(
    {"aws_cloudformation_stack", "aws_cloudformation_stack_set", "aws_cloudformation_stack_set_instance", "aws_cloudformation_stack_instances"}
)
TOPIC_TEXT = re.compile(r"AWS::SNS::Topic|create-topic", re.IGNORECASE)
TOPIC_NAME = re.compile(r"format\(\s*local\.([A-Za-z_]\w*)\s*,\s*\"([a-z][a-z0-9_]*)\"\s*\)")
HEADER = re.compile(r"\s*([A-Za-z_][\w-]*)((?:\s+(?:\"[^\"\n]*\"|[A-Za-z_][\w-]*))*)\s*\{")
ATTRIBUTE = re.compile(r"\s*([A-Za-z_][\w-]*)\s*=(?!=)")
HEREDOC = re.compile(r"<<-?([A-Za-z_]\w*)[ \t]*\n")
ONE_INTERPOLATION = re.compile(r"^\$\{(.*)\}$", re.DOTALL)
SKIPPED_PARTS = frozenset({".terraform", "tests"})
DYNAMIC_KEYS = frozenset({"count", "for_each", "dynamic"})


class UnsupportedForm(ValueError):
    """A Terraform source that this reader cannot read completely."""


@dataclass
class Block:
    kind: str
    labels: tuple[str, ...]
    attributes: dict[str, str] = field(default_factory=dict)
    children: list[Block] = field(default_factory=list)


def masked(text: str) -> str:
    """The text with comments, string contents and heredoc bodies replaced by blanks and underscores, so that offsets still match the original."""
    out, length = list(text), len(text)

    def blank(start: int, end: int, fill: str) -> None:
        for position in range(start, end):
            if out[position] != "\n":
                out[position] = fill

    def interpolation(position: int) -> int:
        depth = 1
        while position < length and depth:
            character = text[position]
            if character == '"':
                position = string(position)
                continue
            depth += (character == "{") - (character == "}")
            position += 1
        return position

    def string(start: int) -> int:
        position = start + 1
        while position < length and text[position] != '"':
            if text[position] == "\\":
                position += 2
            elif text.startswith(("$${", "%%{"), position):
                position += 3
            elif text[position] in "$%" and text.startswith("{", position + 1):
                position = interpolation(position + 2)
            else:
                position += 1
        blank(start + 1, min(position, length), "_")
        return position + 1

    position = 0
    while position < length:
        character = text[position]
        if character == "#" or text.startswith("//", position):
            end = text.find("\n", position)
            end = length if end < 0 else end
            blank(position, end, " ")
            position = end
        elif text.startswith("/*", position):
            end = text.find("*/", position + 2)
            end = length if end < 0 else end + 2
            blank(position, end, " ")
            position = end
        elif character == '"':
            position = string(position)
        elif text.startswith("<<", position) and (heredoc := HEREDOC.match(text, position)):
            closing = re.compile(rf"^[ \t]*{re.escape(heredoc.group(1))}[ \t]*$", re.MULTILINE).search(text, heredoc.end())
            end = closing.start() if closing else length
            blank(heredoc.end(), end, "_")
            position = end
        else:
            position += 1
    return "".join(out)


def closing_brace(cover: str, opening: int) -> int:
    """The index of the brace that closes the one at ``opening``, in masked text."""
    depth = 0
    for position in range(opening, len(cover)):
        depth += (cover[position] == "{") - (cover[position] == "}")
        if depth == 0:
            return position
    raise UnsupportedForm("a block is never closed")


def attribute_end(cover: str, start: int, end: int) -> int:
    """The end of the attribute value that starts at ``start``: the first line end outside every bracket."""
    depth = 0
    for position in range(start, end):
        character = cover[position]
        depth += (character in "([{") - (character in ")]}")
        if character == "\n" and depth <= 0:
            return position
    return end


def parse_span(text: str, cover: str, start: int, end: int) -> tuple[dict[str, str], list[Block]]:
    attributes: dict[str, str] = {}
    children: list[Block] = []
    position = start
    while position < end:
        if not cover[position:end].strip():
            break
        if header := HEADER.match(cover, position):
            opening = header.end() - 1
            close = closing_brace(cover, opening)
            labels = tuple(item.strip('"') for item in re.findall(r"\"[^\"\n]*\"|[A-Za-z_][\w-]*", text[header.start(2) : header.end(2)]))
            inner_attributes, inner_children = parse_span(text, cover, opening + 1, close)
            children.append(Block(header.group(1), labels, inner_attributes, inner_children))
            position = close + 1
        elif attribute := ATTRIBUTE.match(cover, position):
            heredoc = HEREDOC.match(text, attribute.end() + len(text[attribute.end() :]) - len(text[attribute.end() :].lstrip()))
            if heredoc:
                closing = re.compile(rf"^[ \t]*{re.escape(heredoc.group(1))}[ \t]*$", re.MULTILINE).search(text, heredoc.end())
                stop = closing.end() if closing else end
            else:
                stop = attribute_end(cover, attribute.end(), end)
            attributes[attribute.group(1)] = text[attribute.end() : stop].strip()
            position = stop
        else:
            raise UnsupportedForm(f"text that is neither a block nor an attribute at offset {position}")
    return attributes, children


def parse_hcl(text: str) -> list[Block]:
    cover = masked(text)
    return parse_span(text, cover, 0, len(text))[1]


def json_expression(value: object) -> str:
    """The expression text of a JSON value: a string that is one interpolation is that expression, any other value stays JSON text."""
    if isinstance(value, str):
        single = ONE_INTERPOLATION.match(value)
        return single.group(1) if single and "${" not in single.group(1) else json.dumps(value)
    return json.dumps(value)


def parse_json(text: str) -> list[Block]:
    """The blocks of a Terraform JSON file: resources, modules and locals. Any other top-level shape is refused."""
    document = json.loads(text)
    if not isinstance(document, dict):
        raise UnsupportedForm("a Terraform JSON file that is not an object")
    blocks: list[Block] = []

    def bodies(value: object) -> list[dict[str, object]]:
        items = value if isinstance(value, list) else [value]
        if not all(isinstance(item, dict) for item in items):
            raise UnsupportedForm("a Terraform JSON block that is not an object")
        return [item for item in items if isinstance(item, dict)]

    for kind, content in document.items():
        if not isinstance(content, dict):
            if kind in {"resource", "module", "locals"}:
                raise UnsupportedForm(f"a Terraform JSON {kind} section that is not an object")
            continue
        if kind == "locals":
            blocks.append(Block("locals", (), {name: json_expression(value) for name, value in content.items()}))
        elif kind == "module":
            for label, body in content.items():
                for item in bodies(body):
                    blocks.append(Block("module", (label,), {key: json_expression(value) for key, value in item.items()}))
        elif kind == "resource":
            for resource_type, by_label in content.items():
                if not isinstance(by_label, dict):
                    raise UnsupportedForm("a Terraform JSON resource type that is not an object")
                for label, body in by_label.items():
                    for item in bodies(body):
                        blocks.append(Block("resource", (resource_type, label), {key: json_expression(value) for key, value in item.items()}))
    return blocks


def quoted(expression: str) -> str | None:
    return expression[1:-1] if len(expression) >= 2 and expression[0] == expression[-1] == '"' else None


@dataclass
class Folder:
    topics: list[tuple[str, Block]] = field(default_factory=list)
    modules: list[tuple[str, str | None]] = field(default_factory=list)
    locals: dict[str, str] = field(default_factory=dict)
    forms: list[str] = field(default_factory=list)


def read_folder(folder: Path, root: Path, problems: list[str]) -> Folder:
    result = Folder()
    for path in sorted([*folder.glob("*.tf"), *folder.glob("*.tf.json")]):
        name = path.relative_to(root).as_posix()
        text = path.read_text(encoding="utf-8")
        try:
            blocks = parse_json(text) if path.name.endswith(".json") else parse_hcl(text)
        except (UnsupportedForm, json.JSONDecodeError) as error:
            problems.append(f"{name}: the file cannot be read completely, so the topics in it are unknown: {error}")
            continue
        if TOPIC_TEXT.search(text):
            result.forms.append(f"{name}: a text that names an SNS topic type or a create-topic call")
        for block in blocks:
            if block.kind == "locals":
                result.locals.update({key: value for key, value in block.attributes.items()})
            elif block.kind == "module":
                result.modules.append((f"{name}: module {block.labels[0] if block.labels else '?'}", quoted(block.attributes.get("source", ""))))
            elif block.kind == "resource" and len(block.labels) == 2:
                if block.labels[0] == TOPIC_TYPE:
                    result.topics.append((name, block))
                elif block.labels[0] in UNSUPPORTED_TYPES:
                    result.forms.append(f"{name}: {block.labels[0]}.{block.labels[1]} can create a topic that this inventory cannot read")
    return result


def inventory(infra: Path) -> tuple[set[str], list[str]]:
    """The suffixes of the project topics under ``infra`` (the ``%s`` of the project name template) and the problems: every form that is not supported."""
    problems: list[str] = []
    folders = sorted(
        {path.parent for pattern in ("*.tf", "*.tf.json") for path in infra.rglob(pattern) if not SKIPPED_PARTS & set(path.relative_to(infra).parts)}
    )
    read = {folder: read_folder(folder, infra, problems) for folder in folders}
    targets: dict[Path, str] = {}
    for folder, content in read.items():
        for label, source in content.modules:
            if source is None or not source.startswith(("./", "../")):
                problems.append(f"{label}: the source is not a local folder, so the topics it creates are unknown")
                continue
            targets[(folder / source).resolve()] = label
    suffixes: set[str] = set()
    for folder, content in read.items():
        owner = next((label for target, label in targets.items() if folder.resolve() == target or target in folder.resolve().parents), None)
        for form in content.forms:
            problems.append(f"{form}: not supported by the inventory")
        for name, block in content.topics:
            where = f"{name}: aws_sns_topic.{block.labels[1]}"
            if owner:
                problems.append(f"{where} is created inside a module ({owner}): a module cannot build its own topic names")
            elif DYNAMIC_KEYS & set(block.attributes) or any(child.kind in DYNAMIC_KEYS for child in block.children):
                problems.append(f"{where} uses count, for_each or a dynamic block, which this inventory does not follow")
            elif not (found := TOPIC_NAME.fullmatch(block.attributes.get("name", ""))):
                problems.append(f'{where}: the name is not format(local.<name>, "<suffix>"), so the topic cannot be checked against the policy')
            elif quoted(content.locals.get(found.group(1), "")) != NAME_TEMPLATE:
                problems.append(f'{where}: local.{found.group(1)} in this folder is not "{NAME_TEMPLATE}"')
            else:
                suffixes.add(found.group(2))
    return suffixes, problems
