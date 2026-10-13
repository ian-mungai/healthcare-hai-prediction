---
title: Architecture Rendering
description: How the architecture diagram is validated with Archify and rendered to PNG with headless Chrome.
last_updated: 2026-10-13
---

# Architecture Rendering

The diagram source is `docs/architecture/architecture.json`, drawn with
[Archify](https://github.com/tt-a1i/archify) v3.0.1. Each component cites the
lines of code or configuration it was checked against. Its `finalize` command validates the JSON, writes
`docs/architecture/architecture.html` and checks it in Chrome; the PNG is
rendered from the HTML with headless Chrome. Planned components are listed in
the "Planned (not built)" card. With Archify unpacked at `<archify>`:

```sh
ARCHIFY_UPDATE_CHECK_DISABLED=1 node <archify>/bin/archify.mjs finalize architecture \
  docs/architecture/architecture.json docs/architecture/architecture.html --repo-root . --quality showcase
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --headless=new --hide-scrollbars \
  --window-size=1600,1200 --virtual-time-budget=5000 \
  --screenshot=docs/architecture/architecture.png "file://$PWD/docs/architecture/architecture.html"
```

`finalize` also writes receipt files beside the HTML; keep them out of Git.
