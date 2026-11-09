"""Check the relative links of the tracked Markdown files on markdown-it tokens.

``check_links`` is the engine of ``python -m scripts.quality.repo_checks markdown-links``, run from the repository root with the Git index of that
repository. A relative link, image or HTML ``href`` or ``src`` must reach a tracked file or a folder holding tracked files, and its ``#fragment`` must
match a heading or an HTML anchor of the file it reaches (the README of a folder link). External addresses, autolinks and ``mailto:`` links are out of scope.

Parsing follows GitHub: markdown-it-py tokens give every destination (parentheses, escapes, angle brackets, titles, line breaks, reference definitions)
and leave out code blocks, code spans, front matter and HTML comments. Heading anchors follow github-slugger on the rendered heading text; an HTML ``id``
on any element or a ``name`` on ``<a>`` is an anchor, compared with its case kept. Tracked symlinks resolve inside the tracked tree only.
E2E: ``.venv/bin/python -m scripts.quality.run_checks_e2e --hook markdown-links``.
"""

from __future__ import annotations

import os
import posixpath
import re
import unicodedata
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from markdown_it import MarkdownIt
from markdown_it.token import Token

MARKDOWN_SUFFIXES = (".md", ".markdown")
README_NAMES = ("readme.md", "readme.markdown")
URL_SCHEME = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*:")
MAX_SYMLINK_HOPS = 40
SYMLINK_MODE = "120000"
GITLINK_MODE = "160000"
PARSER = MarkdownIt("commonmark", {"store_labels": True}).enable("table").enable("strikethrough")


@dataclass
class Page:
    """What one Markdown file offers and asks for: link destinations with their lines, heading anchors and HTML anchors."""

    destinations: list[tuple[int, str]] = field(default_factory=list)
    headings: set[str] = field(default_factory=set)
    ids: set[str] = field(default_factory=set)

    def has_anchor(self, fragment: str) -> bool:
        """A heading matches in lower case, an HTML id or name exactly; ``#top`` always reaches the page top."""
        return fragment.lower() == "top" or fragment in self.ids or fragment.lower() in self.headings


@dataclass
class Tree:
    """The tracked files that exist on disk, the tracked symlinks with their link text and the folders that hold tracked files."""

    files: set[str] = field(default_factory=set)
    symlinks: dict[str, str] = field(default_factory=dict)
    folders: set[str] = field(default_factory=lambda: {"."})


def is_markdown(path: str) -> bool:
    return path.lower().endswith(MARKDOWN_SUFFIXES)


def read_tree(index: str) -> Tree:
    """The tree of ``git ls-files -s -z`` output: only a regular file or a symlink that is also on disk counts, so deleted files count as missing."""
    tree = Tree()
    for entry in index.split("\0"):
        info, _, path = entry.partition("\t")
        mode = info.split(" ")[0]
        if not path or mode == GITLINK_MODE:
            continue
        if mode == SYMLINK_MODE:
            if os.path.islink(path):
                tree.symlinks[path] = os.readlink(path)
        elif os.path.isfile(path) and not os.path.islink(path):
            tree.files.add(path)
            tree.folders.update(str(parent) for parent in Path(path).parents)
    return tree


def resolve(tree: Tree, target: str) -> tuple[str | None, bool]:
    """Follow the tracked symlinks along a normalized path: the real path (None when it leaves the tree or loops) and whether a symlink was followed."""
    parts = target.split("/")
    resolved = ""
    hops = 0
    index = 0
    while index < len(parts):
        part = parts[index]
        index += 1
        if part in ("", "."):
            continue
        candidate = posixpath.join(resolved, part)
        if candidate not in tree.symlinks:
            resolved = candidate
            continue
        hops += 1
        text = tree.symlinks[candidate]
        link = posixpath.normpath(posixpath.join(resolved, text))
        if hops > MAX_SYMLINK_HOPS or text.startswith("/") or link == ".." or link.startswith("../"):
            return None, True
        parts = [*link.split("/"), *parts[index:]]
        resolved = ""
        index = 0
    return resolved or ".", hops > 0


def strip_front_matter(text: str) -> str:
    """Blank the front matter lines, so the lines that follow keep their numbers."""
    lines = text.split("\n")
    if lines[0].strip() == "---":
        for end in range(1, len(lines)):
            if lines[end].strip() == "---":
                return "\n" * (end + 1) + "\n".join(lines[end + 1 :])
    return text


def slug_text(text: str) -> str:
    """github-slugger on the heading's text: lower case, keep letters, marks, numbers, connector punctuation, hyphens and spaces, spaces to hyphens."""
    kept = [char for char in text.lower() if char in " -" or unicodedata.category(char)[0] in "LMN" or unicodedata.category(char) == "Pc"]
    return "".join(kept).replace(" ", "-")


class Slugger:
    """Heading anchors in document order; a repeat gets ``-1``, ``-2`` and so on, skipping names an earlier heading already took."""

    def __init__(self) -> None:
        self.counts: dict[str, int] = {}

    def slug(self, text: str) -> str:
        base = slug_text(text)
        slug = base
        while slug in self.counts:
            self.counts[base] += 1
            slug = f"{base}-{self.counts[base]}"
        self.counts[slug] = 0
        return slug


def heading_text(inline: Token) -> str:
    """The text GitHub renders for a heading: text and code, no image, no HTML tag."""
    parts = []
    for child in inline.children or []:
        if child.type in ("text", "text_special", "code_inline"):
            parts.append(child.content)
        elif child.type in ("softbreak", "hardbreak"):
            parts.append("\n")
    return "".join(parts)


class HtmlReader(HTMLParser):
    """Collect ``href`` and ``src`` values with their lines and the ``id`` and ``name`` anchors of an HTML fragment."""

    def __init__(self, first_line: int, page: Page) -> None:
        super().__init__(convert_charrefs=True)
        self.first_line = first_line
        self.page = page

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        line = self.first_line + self.getpos()[0] - 1
        for name, value in attrs:
            if value is None:
                continue
            if name in ("href", "src"):
                self.page.destinations.append((line, value))
            elif name == "id" or (name == "name" and tag == "a"):
                self.page.ids.add(value)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)


def read_html(content: str, first_line: int, page: Page) -> None:
    reader = HtmlReader(first_line, page)
    reader.feed(content)
    reader.close()


def read_inline(inline: Token, page: Page) -> None:
    """Links, images and inline HTML of one inline token. A reference-style use is skipped: its definition is checked where it stands."""
    line = (inline.map[0] if inline.map else 0) + 1
    for child in inline.children or []:
        if child.type in ("softbreak", "hardbreak"):
            line += 1
        elif child.type in ("link_open", "image") and "label" not in child.meta:
            page.destinations.append((line, str(child.attrs["href" if child.type == "link_open" else "src"])))
        elif child.type == "html_inline":
            read_html(child.content, line, page)
            line += child.content.count("\n")


def read_page(text: str) -> Page:
    """Parse one Markdown file into its destinations, heading anchors and HTML anchors."""
    page = Page()
    env: dict[str, Any] = {}
    tokens = PARSER.parse(strip_front_matter(text), env)
    slugger = Slugger()
    for position, token in enumerate(tokens):
        if token.type == "heading_open":
            page.headings.add(slugger.slug(heading_text(tokens[position + 1])))
        elif token.type == "inline":
            read_inline(token, page)
        elif token.type == "html_block":
            read_html(token.content, (token.map[0] if token.map else 0) + 1, page)
    for label, definition in env.get("references", {}).items():
        if not label.startswith("^"):  # A footnote definition is no link.
            page.destinations.append((definition["map"][0] + 1, definition["href"]))
    return page


class Pages:
    """Parsed pages by path, read from disk once."""

    def __init__(self) -> None:
        self.cache: dict[str, Page] = {}

    def get(self, path: str) -> Page:
        if path not in self.cache:
            self.cache[path] = read_page(Path(path).read_text(errors="replace"))
        return self.cache[path]


def folder_readme(tree: Tree, folder: str) -> str | None:
    """The README that GitHub shows under a folder: ``README.md`` before ``README.markdown``, any letter case."""
    prefix = "" if folder == "." else folder + "/"
    names = {path: path.lower() for path in tree.files if path.startswith(prefix) and "/" not in path[len(prefix) :]}
    for wanted in README_NAMES:
        match = sorted(path for path, lowered in names.items() if lowered == prefix.lower() + wanted)
        if match:
            return match[0]
    return None


def check_destination(source: str, destination: str, tree: Tree, pages: Pages) -> str | None:
    """The problem with one relative destination of ``source``, or None when it is external, empty or fine."""
    if URL_SCHEME.match(destination) or destination.startswith("//"):
        return None
    written, _, fragment = destination.partition("#")
    written, fragment = unquote(written.partition("?")[0]), unquote(fragment)
    if not written and not fragment:
        return None
    if written:
        base = "" if written.startswith("/") else posixpath.dirname(source)
        target = posixpath.normpath(posixpath.join(base, written.lstrip("/")))
    else:
        target = source
    real, through_symlink = (None, False) if target == ".." or target.startswith("../") else resolve(tree, target)
    if real is None or not (real in tree.files or real in tree.folders):
        if through_symlink:
            return f"link target '{written}' is a symlink that does not resolve to a tracked file"
        return f"link target '{written}' is not a tracked file"
    if not fragment:
        return None
    if real in tree.folders:
        readme = folder_readme(tree, real)
        if readme is None:
            return f"link anchor '#{fragment}' points into folder '{real}', which has no tracked README"
        real = readme
    if is_markdown(real) and not pages.get(real).has_anchor(fragment):
        return f"link anchor '#{fragment}' matches no heading in {real}"
    return None


def check_links(index: str, skip: tuple[str, ...] = ()) -> list[tuple[str, str]]:
    """Every broken relative link of the tracked Markdown files as (``file:line``, problem); files under a ``skip`` prefix are not read as sources."""
    tree = read_tree(index)
    pages = Pages()
    problems = []
    for source in sorted(path for path in tree.files if is_markdown(path) and not path.startswith(skip)):
        for line, destination in sorted(pages.get(source).destinations):
            problem = check_destination(source, destination, tree, pages)
            if problem:
                problems.append((f"{source}:{line}", problem))
    return problems
