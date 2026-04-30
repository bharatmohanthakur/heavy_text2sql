"""Generate the Ed-Fi Text-to-SQL client-facing architecture deck.

Run with:
    uvx --from python-pptx python docs/build_architecture_deck.py

Produces docs/architecture_deck.pptx.

Audience: client / business stakeholder. Frame the work in outcomes,
not file paths. Status is grounded in the actual repo audit (Apr 30
2026): 13 core components built and tested; agent loop and frontend
shipped; remaining 11 working days target Leiden sub-clustering,
observability, multi-provider matrix tests, and a recorded demo for
the v0.9 release on Sat May 16.
"""
from __future__ import annotations

from pathlib import Path

from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches, Pt

# ── Palette ────────────────────────────────────────────────────────────────
BG       = RGBColor(0x0F, 0x14, 0x1A)
PANEL    = RGBColor(0x1A, 0x21, 0x2A)
BORDER   = RGBColor(0x2A, 0x33, 0x3F)
TEXT     = RGBColor(0xE6, 0xEA, 0xF0)
MUTED    = RGBColor(0x8A, 0x95, 0xA5)
ACCENT   = RGBColor(0x6E, 0xC1, 0xFF)
GREEN    = RGBColor(0x4A, 0xD0, 0x9C)
AMBER    = RGBColor(0xF2, 0xB8, 0x57)
RED      = RGBColor(0xE5, 0x6B, 0x6B)


# ── Helpers ────────────────────────────────────────────────────────────────


def _set_text(tf, text, *, size=18, bold=False, color=TEXT, align=PP_ALIGN.LEFT):
    tf.clear()
    p = tf.paragraphs[0]
    p.alignment = align
    r = p.add_run()
    r.text = text
    r.font.size = Pt(size)
    r.font.bold = bold
    r.font.color.rgb = color
    r.font.name = "Helvetica Neue"


def _add_text(slide, text, *, left, top, width, height,
              size=18, bold=False, color=TEXT, align=PP_ALIGN.LEFT):
    box = slide.shapes.add_textbox(Inches(left), Inches(top), Inches(width), Inches(height))
    tf = box.text_frame
    tf.margin_left = tf.margin_right = Inches(0.05)
    tf.margin_top = tf.margin_bottom = Inches(0.05)
    tf.word_wrap = True
    _set_text(tf, text, size=size, bold=bold, color=color, align=align)
    return box


def _bullets(slide, items, *, left, top, width, height, size=14, color=TEXT):
    box = slide.shapes.add_textbox(Inches(left), Inches(top), Inches(width), Inches(height))
    tf = box.text_frame
    tf.word_wrap = True
    tf.margin_left = tf.margin_right = Inches(0.1)
    for i, item in enumerate(items):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = PP_ALIGN.LEFT
        p.space_after = Pt(4)
        r = p.add_run()
        r.text = "• " + item if not item.startswith(("• ", "  ")) else item
        r.font.size = Pt(size)
        r.font.color.rgb = color
        r.font.name = "Helvetica Neue"


def _panel(slide, *, left, top, width, height, fill=PANEL, border=BORDER):
    s = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE,
                                Inches(left), Inches(top), Inches(width), Inches(height))
    s.fill.solid()
    s.fill.fore_color.rgb = fill
    s.line.color.rgb = border
    s.line.width = Pt(0.75)
    s.shadow.inherit = False
    return s


def _slide_bg(slide):
    fill = slide.background.fill
    fill.solid()
    fill.fore_color.rgb = BG


def _add_title(slide, title, subtitle=None):
    _add_text(slide, title, left=0.5, top=0.35, width=12.3, height=0.7,
              size=30, bold=True, color=ACCENT)
    if subtitle:
        _add_text(slide, subtitle, left=0.5, top=1.05, width=12.3, height=0.45,
                  size=14, color=MUTED)


def _footer(slide, left_text, right_text):
    _add_text(slide, left_text, left=0.5, top=7.05, width=8, height=0.3,
              size=10, color=MUTED)
    _add_text(slide, right_text, left=8.5, top=7.05, width=4.3, height=0.3,
              size=10, color=MUTED, align=PP_ALIGN.RIGHT)


def _notes(slide, text):
    slide.notes_slide.notes_text_frame.text = text


def _status_chip(slide, *, left, top, label, color, width=1.25):
    chip = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE,
                                    Inches(left), Inches(top), Inches(width), Inches(0.32))
    chip.fill.solid(); chip.fill.fore_color.rgb = color
    chip.line.fill.background()
    tb = slide.shapes.add_textbox(Inches(left), Inches(top), Inches(width), Inches(0.32))
    tf = tb.text_frame; tf.margin_left = tf.margin_right = Inches(0); tf.margin_top = Inches(0.04)
    _set_text(tf, label, size=10, bold=True, color=BG, align=PP_ALIGN.CENTER)


def _section_divider(slide, kicker, headline, subhead, color=ACCENT):
    _add_text(slide, kicker, left=0.8, top=2.5, width=11.7, height=0.5,
              size=18, color=MUTED)
    _add_text(slide, headline, left=0.8, top=3.05, width=11.7, height=1.0,
              size=42, bold=True, color=color)
    _add_text(slide, subhead, left=0.8, top=4.05, width=11.7, height=0.5,
              size=18, color=TEXT)


# ── Deck construction ─────────────────────────────────────────────────────


def build():
    prs = Presentation()
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)
    blank = prs.slide_layouts[6]

    # ── Slide 1: Title ────────────────────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_text(s, "Ed-Fi Text-to-SQL", left=0.8, top=2.1, width=11.7, height=1.0,
              size=46, bold=True, color=ACCENT)
    _add_text(s, "Ask questions of your school data in plain English",
              left=0.8, top=3.15, width=11.7, height=0.6,
              size=22, color=TEXT)
    _add_text(s, "Architecture review · platform built · 11 working days to v0.9 release",
              left=0.8, top=3.85, width=11.7, height=0.5,
              size=16, color=MUTED)
    _add_text(s, "Apr 30, 2026  ·  client review  ·  v0.9 release Sat May 16",
              left=0.8, top=6.6, width=11.7, height=0.4,
              size=13, color=MUTED)
    _notes(s, "Set the room: this is an architecture-and-status review. The platform's foundation "
              "is built — knowledge layer, NL→SQL pipeline, agent loop, API, frontend, and ops "
              "tooling are all functionally complete and tested. The next 11 working days "
              "(Apr 30 through Fri May 15, weekends + May 1 holiday excluded) close the loop on "
              "agentic polish, multi-provider hardening, observability, and a recorded demo. "
              "v0.9 release is tagged Sat May 16.")

    # ── Slide 2: The problem ──────────────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "The problem we're solving",
                  "An analyst asks a question. The platform answers with data.")

    _panel(s, left=0.5, top=1.6, width=12.3, height=2.1, fill=PANEL)
    _add_text(s, "Today, in any Ed-Fi-shaped database",
              left=0.7, top=1.7, width=11.9, height=0.4, size=14, bold=True, color=AMBER)
    _bullets(s, [
        "1,048 tables · ~10,000 columns · 1,663 foreign keys (Ed-Fi DS 6.1.0)",
        "An analyst with a question must (a) know which tables to join, (b) write the SQL, (c) execute it, (d) read it",
        "End-to-end this is hours of work for a senior engineer — and impossible for a non-engineer",
    ], left=0.7, top=2.1, width=11.9, height=1.6, size=13)

    _panel(s, left=0.5, top=3.9, width=12.3, height=2.9, fill=PANEL)
    _add_text(s, "What we're delivering by Sat May 16",
              left=0.7, top=4.0, width=11.9, height=0.4, size=14, bold=True, color=GREEN)
    _bullets(s, [
        "Type a question in English  →  receive correct SQL, the rows it returns, a chart, and a written summary",
        "Have a multi-turn conversation with the platform — it remembers what you asked before",
        "Works on any Ed-Fi database (Postgres / MSSQL / SQLite) without changing the database itself",
        "Cites the tables it used so an engineer can verify the answer in seconds",
        "Learns from approved queries — the more it's used, the better it gets",
    ], left=0.7, top=4.4, width=11.9, height=2.4, size=13)

    _footer(s, "Why this project · Section 1", "")
    _notes(s, "Anchor in the user pain. Most Ed-Fi work today goes through a small handful of "
              "people who can write SQL against the model. We're widening that bottleneck — not "
              "by training more SQL writers but by automating the translation from English. "
              "Mention the conversation bullet — that's the agent-loop work that closes in this "
              "release.")

    # ── Slide 3: Status snapshot ──────────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "Status: foundation built, polish ahead",
                  "13 core components green and tested · 4 workstreams remaining for v0.9 on May 16")

    rows = [
        ("Knowledge layer",
         "Done", GREEN,
         "Domain classification (829/829 tables), FK graph (1,663 edges), APSP, Steiner solver, table catalog, hybrid retrieval, 4-tier entity resolver"),
        ("NL → SQL pipeline",
         "Done", GREEN,
         "Routed orchestrator: classify → retrieve → resolve → generate → validate → repair → execute → chart + summary"),
        ("Agentic layer",
         "Done", GREEN,
         "8-tool agent loop, multi-turn conversations with cross-dialect history, server-sent streaming"),
        ("API + Web UI",
         "Done", GREEN,
         "18 endpoints (REST + WebSocket); 7 pages — Query, Chat, Tables, Domains, Gold, Settings, browser; provider-aware refresh"),
        ("Operations",
         "Done", GREEN,
         "Multi-dialect metadata DB, runtime Settings UI, eval harness with JSON+Markdown reports, ships zero-infra demo SQLite"),
        ("Auto sub-clustering (Leiden)",
         "In flight", AMBER,
         "Refines oversize Ed-Fi domains into 8–20-table clusters for sharper routing; static taxonomy works today"),
        ("Multi-provider matrix tests",
         "In flight", AMBER,
         "5 LLM providers + 4 embedding providers wired; cross-product regression matrix lands by May 16"),
        ("Agentic polish",
         "In flight", AMBER,
         "Tool reliability, streaming UX refinements, more agent tools (chart export, eval drilldown)"),
        ("Observability (OTel + Prometheus)",
         "Coming", RED,
         "Per-stage spans + counters; logging is in place today, traces and metrics are next"),
    ]
    _panel(s, left=0.5, top=1.6, width=12.3, height=5.4, fill=PANEL)
    y = 1.72
    for label, status, color, what in rows:
        _add_text(s, label, left=0.7, top=y, width=4.3, height=0.32,
                  size=12, bold=True, color=TEXT)
        _status_chip(s, left=5.1, top=y + 0.02, label=status, color=color, width=1.15)
        _add_text(s, what, left=6.45, top=y + 0.02, width=6.3, height=0.6,
                  size=10, color=MUTED)
        y += 0.58

    _footer(s, "Status snapshot", "v0.9 release · Sat May 16, 2026")
    _notes(s, "This is the headline. Five rows green = foundation done; three amber = active work "
              "the team finishes by May 16; one red = stretch item that may slip into v0.10. The "
              "shift from May 8 to May 16 buys time for the multi-provider matrix and the agentic "
              "polish — the agent loop core is working today, but we want demo-grade reliability.")

    # ── Slide 4: System map ───────────────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "The big picture",
                  "Five layers carry every question from English to answer")

    layers = [
        ("Question",       ["Analyst types: 'How many Hispanic students enrolled in Grade 9 last year?'"], MUTED),
        ("Knowledge",      ["Classification routes the question to the right Ed-Fi domains",
                            "Hybrid retrieval narrows 1,048 tables to a small relevant set",
                            "FK graph + Steiner returns the cheapest JOIN tree connecting them",
                            "Entity resolver maps real-world terms (Hispanic, Grade 9) to codes"], GREEN),
        ("Pipeline",       ["LLM writes SQL grounded in real columns; validator parses + plans + dry-runs",
                            "Repair loop (3 attempts) auto-fixes the common failure modes"], GREEN),
        ("Agentic",        ["Agent loop calls 8 tools across multi-turn conversations",
                            "Server-sent streaming gives token-level feedback in the browser"], GREEN),
        ("Answer",         ["SQL · rows · auto-picked chart · plain-English summary · cited tables"], GREEN),
    ]
    y = 1.4
    for label, lines, color in layers:
        h = 0.55 + 0.32 * len(lines)
        _panel(s, left=0.5, top=y, width=12.3, height=h, fill=PANEL)
        _add_text(s, label, left=0.7, top=y + 0.1, width=2.6, height=0.4,
                  size=13, bold=True, color=color)
        for j, ln in enumerate(lines):
            _add_text(s, "• " + ln, left=3.4, top=y + 0.1 + j * 0.30, width=9.2, height=0.32,
                      size=11, color=TEXT)
        y += h + 0.08

    _add_text(s, "Every layer is built and tested today; the work to May 16 is polish and recorded demo",
              left=0.5, top=7.05, width=12.3, height=0.3,
              size=11, color=MUTED, align=PP_ALIGN.CENTER)
    _notes(s, "Walk top-to-bottom. Each row corresponds to a tested module set in the repo. "
              "Spend the most time on Pipeline + Agentic — those are the parts the demo audience "
              "actually sees in motion.")

    # ── Slide 5: By the numbers ───────────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "By the numbers",
                  "What the platform actually contains today")

    cols = [
        ("Knowledge", GREEN, [
            ("Tables modeled",            "1,048"),
            ("Foreign keys parsed",        "1,663"),
            ("Domains (Ed-Fi taxonomy)",      "35"),
            ("Catalog entries",             "829"),
            ("APSP matrix",          "829 × 829"),
            ("Steiner p99 latency",         "< 1 ms"),
        ]),
        ("Pipeline", GREEN, [
            ("Pipeline stages end-to-end",     "9"),
            ("Repair attempts before fail",    "3"),
            ("Entity-resolver tiers",          "4"),
            ("Hybrid retrieval blend",  "0.6 / 0.4"),
            ("Gold-store tests passing",     "7/7"),
            ("Pipeline tests passing",       "4/4"),
        ]),
        ("Surface", GREEN, [
            ("API endpoints",                 "18"),
            ("WebSocket streaming routes",     "2"),
            ("Frontend pages",                 "7"),
            ("Agent tools wired",              "8"),
            ("LLM providers integrated",       "5"),
            ("Database dialects supported",    "3"),
        ]),
    ]
    x = 0.5
    for header, color, rows in cols:
        _panel(s, left=x, top=1.6, width=4.1, height=5.3, fill=PANEL)
        _add_text(s, header, left=x + 0.2, top=1.7, width=3.7, height=0.4,
                  size=15, bold=True, color=color)
        y = 2.2
        for k, v in rows:
            _add_text(s, k, left=x + 0.2, top=y, width=2.5, height=0.32,
                      size=11, color=MUTED)
            _add_text(s, v, left=x + 2.6, top=y, width=1.4, height=0.32,
                      size=12, bold=True, color=TEXT, align=PP_ALIGN.RIGHT)
            y += 0.55
        x += 4.25

    _footer(s, "Numbers are from the latest test run + Ed-Fi DS 6.1.0 build", "")
    _notes(s, "These are the real numbers from the repo audit. Use them to anchor any 'is this "
              "actually working?' question — every figure has a corresponding test or artifact.")

    # ── Slide 6: Section divider — Knowledge ──────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _section_divider(s,
        "Section 2 of 5",
        "The Knowledge Layer",
        "How the platform turns 1,048 tables into a navigable, queryable index",
        color=GREEN)
    _notes(s, "Foundation. Without this layer the LLM would be guessing across 1,048 tables.")

    # ── Slide 7: Domain classification + table catalog ────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "Cataloging every Ed-Fi table",
                  "Four-stage classifier · 829/829 tables placed · descriptions captured at build time")

    _panel(s, left=0.5, top=1.6, width=6.0, height=5.3, fill=PANEL)
    _add_text(s, "Four-stage classifier", left=0.7, top=1.7, width=5.6, height=0.4,
              size=15, bold=True, color=ACCENT)
    rows = [
        ("Stage 1 · Direct ApiModel domains",  "750 tables"),
        ("Stage 2 · Aggregate inheritance",      "75 tables"),
        ("Stage 3 · Descriptor FK voting",        "3 tables"),
        ("Stage 4 · LLM fallback",         "rare residual"),
        ("Override layer (operator)",     "human in the loop"),
        ("Output artifact",      "table_classification.json"),
    ]
    y = 2.2
    for k, v in rows:
        _add_text(s, k, left=0.7, top=y, width=3.5, height=0.32, size=12, color=MUTED)
        _add_text(s, v, left=4.2, top=y, width=2.0, height=0.32,
                  size=12, bold=True, color=TEXT, align=PP_ALIGN.RIGHT)
        y += 0.58

    _panel(s, left=6.85, top=1.6, width=6.0, height=5.3, fill=PANEL)
    _add_text(s, "Per-table catalog entry",
              left=7.05, top=1.7, width=5.6, height=0.4, size=15, bold=True, color=AMBER)
    _bullets(s, [
        "Authoritative description (Ed-Fi prose, no LLM hallucination)",
        "Domain tags + identifying columns + linked descriptors",
        "Connected tables (FK neighbors) for join intuition",
        "Sample column values for entity resolution",
        "Proven-query count — biases retrieval to tables we've answered before",
        "Reflects unknown tables on the live DB so non-Ed-Fi columns join cleanly",
    ], left=7.05, top=2.15, width=5.6, height=4.5, size=12)

    _footer(s, "Knowledge · Classification & Catalog", "test_component2 + test_component4 green")
    _notes(s, "Stage 1 handles 90% of tables deterministically. The LLM tier is the safety net "
              "for residual stragglers. Reflection of unknown DB tables (P7) handles the case "
              "where the operator's database has 36 extra tables that aren't in the Ed-Fi spec.")

    # ── Slide 8: FK graph + Steiner ───────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "Foreign-Key Graph + JOIN solver",
                  "1,663 edges · 829×829 APSP precomputed · KMB Steiner approximation")

    _panel(s, left=0.5, top=1.6, width=6.0, height=5.3, fill=PANEL)
    _add_text(s, "What the graph contains", left=0.7, top=1.7, width=5.6, height=0.4,
              size=15, bold=True, color=ACCENT)
    rows = [
        ("Nodes (tables)",                      "829"),
        ("Edges (FK relationships)",          "1,663"),
        ("APSP storage on disk",               "<10 MB"),
        ("Load time at startup",              "<100 ms"),
        ("Same-aggregate join weight",            "1.0"),
        ("Cross-aggregate join weight",           "2.0"),
        ("Cross-domain join weight",              "4.0"),
        ("Composite-FK bonus multiplier",         "0.9"),
    ]
    y = 2.15
    for k, v in rows:
        _add_text(s, k, left=0.7, top=y, width=3.7, height=0.32, size=12, color=MUTED)
        _add_text(s, v, left=4.4, top=y, width=1.8, height=0.32,
                  size=12, bold=True, color=TEXT, align=PP_ALIGN.RIGHT)
        y += 0.50

    _panel(s, left=6.85, top=1.6, width=6.0, height=5.3, fill=PANEL)
    _add_text(s, "Steiner solver — three paths",
              left=7.05, top=1.7, width=5.6, height=0.4, size=15, bold=True, color=AMBER)
    rows = [
        ("k = 2 tables",
         "Bidirectional Dijkstra; returns top-3 alternatives so the model picks the most readable",
         "<5 ms"),
        ("k = 3–8 tables",
         "KMB Steiner-tree approximation — provably within 2× of optimal",
         "<50 ms"),
        ("k > 8 (edge case)",
         "Greedy fallback; rare on real Ed-Fi questions",
         "<200 ms"),
    ]
    y = 2.15
    for label, what, perf in rows:
        _add_text(s, label, left=7.05, top=y, width=5.6, height=0.32,
                  size=12, bold=True, color=TEXT)
        _add_text(s, what, left=7.05, top=y + 0.30, width=5.6, height=0.7, size=11, color=MUTED)
        _add_text(s, perf, left=11.0, top=y, width=1.7, height=0.32,
                  size=11, bold=True, color=GREEN, align=PP_ALIGN.RIGHT)
        y += 1.30

    _footer(s, "Knowledge · FK Graph & Steiner",
            "test_component3 + test_component3_benchmark green · p99 < 1 ms")
    _notes(s, "Two ideas to leave the room with: graph is small (10 MB) and fast (sub-millisecond "
              "responses), and weights bias joins to stay inside the natural Ed-Fi entity "
              "boundaries — which is what a senior analyst would do by hand.")

    # ── Slide 9: Embedding + retrieval + entity resolution ────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "Finding the right tables, resolving real-world terms",
                  "Hybrid retrieval shortlists tables · 4-tier resolver maps user words to codes")

    _panel(s, left=0.5, top=1.6, width=6.0, height=5.3, fill=PANEL)
    _add_text(s, "Hybrid retrieval (semantic + keyword)",
              left=0.7, top=1.7, width=5.6, height=0.4, size=15, bold=True, color=ACCENT)
    _bullets(s, [
        "Per-table semantic blob: description, key columns, neighbors, linked descriptors",
        "Embedded once at build time; queried millions of times after",
        "Domain pre-filter narrows the candidate pool before scoring",
        "Cosine similarity (0.6) + BM25 keyword score (0.4) — best of both",
        "Returns the small set (typically 3–8 tables) that best matches the question",
    ], left=0.7, top=2.15, width=5.6, height=4.5, size=12)

    _panel(s, left=6.85, top=1.6, width=6.0, height=5.3, fill=PANEL)
    _add_text(s, "Four-tier entity resolver",
              left=7.05, top=1.7, width=5.6, height=0.4, size=15, bold=True, color=AMBER)
    rows = [
        ("Tier 1 · Exact lookup",  "Hash hit on the value index", "<1 ms"),
        ("Tier 2 · Fuzzy match",    "Typos: 'Hispanc' → 'Hispanic'", "5 ms"),
        ("Tier 3 · Semantic ANN",  "Match meaning when spelling differs", "20 ms"),
        ("Tier 4 · LLM disambiguate","Resolve when one term means two things", "200 ms"),
    ]
    y = 2.2
    for tier, what, perf in rows:
        _add_text(s, tier, left=7.05, top=y, width=3.6, height=0.32, size=12, bold=True, color=TEXT)
        _add_text(s, what, left=7.05, top=y + 0.30, width=4.4, height=0.32, size=11, color=MUTED)
        _add_text(s, perf, left=11.4, top=y, width=1.3, height=0.32,
                  size=11, color=GREEN, align=PP_ALIGN.RIGHT)
        y += 0.85

    _footer(s, "Knowledge · Retrieval & Entity Resolution",
            "test_component5 + test_component6 green · Tier 1+2 handle ~95% of cases")
    _notes(s, "This is where 'plain English' becomes 'database-correct'. Without it, "
              "questions like 'last year', 'Grade 9', or 'free-and-reduced lunch' would never "
              "land on the right rows.")

    # ── Slide 10: Section divider — Pipeline ──────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _section_divider(s,
        "Section 3 of 5",
        "The NL → SQL Pipeline",
        "Nine stages, end-to-end · validation + 3-attempt repair · charts and descriptions",
        color=GREEN)
    _notes(s, "This is the orchestrator that knits the knowledge layer into an answer.")

    # ── Slide 11: Pipeline orchestrator ───────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "The nine pipeline stages",
                  "answer(nl) → SQL + rows + chart + summary · all wired and tested today")

    stages = [
        ("1. Classify",       "Route question → primary + secondary + tertiary domains"),
        ("2. Retrieve",       "Hybrid search inside routed domains → 3–8 candidate tables"),
        ("3. Resolve",        "Map user terms to descriptor codes (4-tier funnel)"),
        ("4. Recall gold",    "Top-3 proven NL/SQL pairs by AST + NL similarity"),
        ("5. Generate",       "LLM writes SQL grounded in real columns + JOINs"),
        ("6. Validate",       "sqlglot parse + EXPLAIN + LIMIT 0 dry-run"),
        ("7. Repair",         "3-attempt loop fixes typos / missing joins / wrong columns"),
        ("8. Execute",        "Run against the active target DB engine"),
        ("9. Visualize + describe", "Vega-Lite chart + plain-English summary in parallel"),
    ]
    _panel(s, left=0.5, top=1.6, width=12.3, height=5.4, fill=PANEL)
    y = 1.78
    for label, what in stages:
        _add_text(s, label, left=0.7, top=y, width=3.7, height=0.32,
                  size=13, bold=True, color=ACCENT)
        _add_text(s, what, left=4.5, top=y, width=8.2, height=0.32,
                  size=12, color=TEXT)
        y += 0.56

    _footer(s, "Pipeline · 9 stages · test_component8 green",
            "Postgres / MSSQL / SQLite all pass")
    _notes(s, "This slide is the heart of the demo. Walk through each stage with the audience "
              "imagining their own question — by the end they should feel like they've watched "
              "the platform think.")

    # ── Slide 12: Validation, repair, charts, descriptions ────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "Self-correcting and self-explaining",
                  "Bad SQL never reaches production · charts and summaries land in parallel")

    _panel(s, left=0.5, top=1.6, width=6.0, height=5.3, fill=PANEL)
    _add_text(s, "Validation + repair loop",
              left=0.7, top=1.7, width=5.6, height=0.4, size=15, bold=True, color=ACCENT)
    _bullets(s, [
        "Parse-check — catches syntax errors (sqlglot)",
        "Plan-check — catches semantic errors (EXPLAIN)",
        "Dry-execute — catches runtime errors (LIMIT 0)",
        "Up to three repair attempts with fresh context",
        "Live-fix verified end-to-end (test_repair_fixes_real_explain_error)",
        "Short-circuits on clean first SQL — no overhead in the common case",
    ], left=0.7, top=2.15, width=5.6, height=4.5, size=12)

    _panel(s, left=6.85, top=1.6, width=6.0, height=5.3, fill=PANEL)
    _add_text(s, "Charts + descriptions (parallel)",
              left=7.05, top=1.7, width=5.6, height=0.4, size=15, bold=True, color=AMBER)
    _bullets(s, [
        "Result-shape inference: row count, column types, aggregates, temporal axes",
        "Vega-Lite spec generated via LLM with strict JSON schema",
        "Plain-English summary generated in parallel via ThreadPoolExecutor",
        "Browser receives chart spec + summary the moment they're ready",
        "Single-row, multi-row, time-series, and aggregate cases all handled",
    ], left=7.05, top=2.15, width=5.6, height=4.5, size=12)

    _footer(s, "Pipeline · Validation & Output",
            "test_component9 + test_component10 green")
    _notes(s, "Repair is the magic that lets the platform be honest with the user. If the LLM "
              "writes a wrong column, EXPLAIN catches it; the system asks the LLM to fix it with "
              "the actual error message; we get the right answer most of the time. This is what "
              "makes the platform safe for non-engineers.")

    # ── Slide 13: Section divider — Agentic ───────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _section_divider(s,
        "Section 4 of 5",
        "The Agentic Layer",
        "8 tools · multi-turn conversations · server-sent streaming",
        color=GREEN)
    _notes(s, "The agent loop is what makes this feel like a partner, not a one-shot translator.")

    # ── Slide 14: Agent loop ──────────────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "Eight tools the agent can call",
                  "Each tool is the same primitive an engineer would reach for — exposed to the LLM")

    tools = [
        ("CLASSIFY_DOMAINS",     "Find the Ed-Fi domains relevant to a question"),
        ("SEARCH_TABLES",         "Hybrid search inside the catalog · top-K tables"),
        ("INSPECT_TABLE",         "Full schema + sample rows for one table"),
        ("RESOLVE_ENTITY",        "Map a user term ('Hispanic') to (table, column, code)"),
        ("FIND_JOIN_PATH",        "Steiner solver between two or more tables"),
        ("FIND_SIMILAR_QUERIES", "Top-3 proven NL/SQL pairs from the gold store"),
        ("RUN_SQL",               "Execute SQL against the active target DB · returns rows"),
        ("FINAL_ANSWER",          "Terminate the turn with a structured answer"),
    ]
    _panel(s, left=0.5, top=1.6, width=12.3, height=5.4, fill=PANEL)
    y = 1.8
    for tool, what in tools:
        _add_text(s, tool, left=0.7, top=y, width=4.0, height=0.32,
                  size=12, bold=True, color=ACCENT)
        _add_text(s, what, left=4.8, top=y, width=8.0, height=0.32,
                  size=12, color=TEXT)
        y += 0.62

    _footer(s, "Agentic · 8 tools wired · max 20 steps per turn",
            "test_agent_loop · streaming events verified")
    _notes(s, "The agent never invents tables or values — it must call SEARCH_TABLES and "
              "RESOLVE_ENTITY first. RUN_SQL is the only side-effecting tool, and it executes "
              "validated SQL only.")

    # ── Slide 15: Conversations + streaming + providers ───────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "Conversations, streaming, and provider portability",
                  "The agent works the same on Anthropic, Bedrock, Azure, OpenAI, OpenRouter")

    _panel(s, left=0.5, top=1.6, width=6.0, height=5.3, fill=PANEL)
    _add_text(s, "Multi-turn conversations",
              left=0.7, top=1.7, width=5.6, height=0.4, size=15, bold=True, color=ACCENT)
    _bullets(s, [
        "SQLAlchemy-backed conversation store — Postgres, MSSQL, SQLite",
        "Each conversation tagged with the dialect it was authored in",
        "Cross-dialect history visible (badge in the chat UI)",
        "Server-sent events stream tokens, tool calls, and final answers",
        "Frontend reducer renders updates without page reloads",
    ], left=0.7, top=2.15, width=5.6, height=4.5, size=12)

    _panel(s, left=6.85, top=1.6, width=6.0, height=5.3, fill=PANEL)
    _add_text(s, "LLM providers integrated",
              left=7.05, top=1.7, width=5.6, height=0.4, size=15, bold=True, color=AMBER)
    rows = [
        ("Anthropic Claude (direct)",  "tool_use · strict schemas"),
        ("AWS Bedrock",                "Converse API · same Claude family"),
        ("Azure OpenAI",                "tool_calls · deployments"),
        ("OpenAI",                       "tool_calls · function schemas"),
        ("OpenRouter",                   "runtime schema probe + cache"),
    ]
    y = 2.2
    for k, v in rows:
        _add_text(s, k, left=7.05, top=y, width=3.4, height=0.32, size=12, bold=True, color=TEXT)
        _add_text(s, v, left=10.4, top=y, width=2.4, height=0.32, size=11,
                  color=MUTED, align=PP_ALIGN.RIGHT)
        y += 0.55
    _add_text(s, "Cross-product regression matrix lands by May 16",
              left=7.05, top=5.4, width=5.6, height=0.32, size=11, color=AMBER)

    _footer(s, "Agentic · Conversations & Providers",
            "All 5 providers wired · matrix tests in progress")
    _notes(s, "Provider portability is what makes this safe to deploy in different agencies — "
              "Bedrock for AWS-native, Azure for Microsoft-native, Anthropic direct for "
              "everyone else. The agent code never branches on provider; the translator layer "
              "absorbs the differences.")

    # ── Slide 16: Section divider — Surface & Ops ─────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _section_divider(s,
        "Section 5 of 5",
        "Surface & Operations",
        "API, frontend, settings, multi-dialect metadata, eval harness — the parts an operator touches",
        color=GREEN)
    _notes(s, "Everything an operator or user actually interacts with day-to-day.")

    # ── Slide 17: API + Frontend ──────────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "API surface + browser experience",
                  "18 endpoints · WebSocket + SSE streaming · 7 browser pages")

    _panel(s, left=0.5, top=1.6, width=6.0, height=5.3, fill=PANEL)
    _add_text(s, "REST + streaming endpoints",
              left=0.7, top=1.7, width=5.6, height=0.4, size=15, bold=True, color=ACCENT)
    _bullets(s, [
        "/health · status, active provider, dialect, catalog readiness",
        "/query, /query/stream — synchronous + WebSocket pipeline",
        "/chat, /chat/stream — agent loop sync + SSE streaming",
        "/conversations — multi-turn lifecycle (CRUD + dialect badges)",
        "/tables, /tables/{fqn}, /domains — catalog browse",
        "/gold — CRUD + approve/reject + retrieval refresh",
        "/admin — config, jobs/rebuild SSE, test_metadata_db",
    ], left=0.7, top=2.15, width=5.6, height=4.5, size=11)

    _panel(s, left=6.85, top=1.6, width=6.0, height=5.3, fill=PANEL)
    _add_text(s, "Browser pages (Next.js)",
              left=7.05, top=1.7, width=5.6, height=0.4, size=15, bold=True, color=AMBER)
    _bullets(s, [
        "Query — single-shot NL → SQL with chart & summary",
        "Chat — multi-turn agent loop with conversation list",
        "Tables — schema browser, FQN detail, sample rows",
        "Domains — domain list with table counts",
        "Gold — gold-query CRUD, approve workflow, eval drilldown",
        "Settings — DB / LLM / embedding connector forms (editable)",
        "Onboarding — sitewide banner when artifacts not yet built",
    ], left=7.05, top=2.15, width=5.6, height=4.5, size=11)

    _footer(s, "Surface · API + Frontend",
            "test_component11 + test_component12 green · provider-aware refresh")
    _notes(s, "Provider-aware refresh (P1/P2) is the secret sauce: switch the active database "
              "in Settings, the Tables and Domains pages refresh automatically — no stale state.")

    # ── Slide 18: Operations — settings, multi-dialect, eval, demo DB ─────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "Operator-grade plumbing",
                  "Settings UI · multi-dialect metadata · eval harness · zero-infra demo target")

    panels = [
        ("Settings UI",
         ["DB / LLM / embedding connector forms with inline test buttons",
          "Secrets stored in gitignored runtime_secrets.json (not in body)",
          "Runtime overlay merged on top of default.yaml at boot",
          "Rebuild orchestrator: queue stages, watch live log via SSE"]),
        ("Multi-dialect metadata DB",
         ["SQLite (default zero-infra), Postgres, MSSQL all supported",
          "URL.create() escaping handles passwords with @ : / ?",
          "Forward-only ALTER TABLE migrations for legacy installs",
          "Password redacted from driver error responses"]),
        ("Eval harness",
         ["Gold-question suite with JSON + Markdown reports",
          "Schema-hit, join-hit, descriptor-leakage, BLEU, exact-match metrics",
          "Per-build comparison so regressions surface immediately",
          "CI gate hook in place; expanding to 100 questions for v0.9"]),
        ("Zero-infra demo target",
         ["sample_demo.sqlite shipped with the repo (Ed-Fi-shaped, ~70 rows)",
          "git clone + text2sql serve works without an operator-supplied DB",
          "Reflection picks up unknown tables on operator-supplied DBs",
          "Onboarding banner walks new users to first query"]),
    ]
    rows_per = 2
    cell_w, cell_h = 6.15, 2.6
    for i, (title, items) in enumerate(panels):
        col = i % rows_per
        row = i // rows_per
        x = 0.5 + col * (cell_w + 0.05)
        y = 1.6 + row * (cell_h + 0.1)
        _panel(s, left=x, top=y, width=cell_w, height=cell_h, fill=PANEL)
        _add_text(s, title, left=x + 0.2, top=y + 0.1, width=cell_w - 0.4, height=0.4,
                  size=14, bold=True, color=ACCENT)
        _bullets(s, items, left=x + 0.2, top=y + 0.5, width=cell_w - 0.4,
                 height=cell_h - 0.6, size=10)

    _footer(s, "Operations · Settings + Multi-dialect + Eval + Demo target",
            "All four committed and tested")
    _notes(s, "These are the pieces that make the platform actually usable on day one — and the "
              "reason a fresh git clone can demo without a populated database.")

    # ── Slide 19: What ships next (May 16 release) ────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "What ships in the v0.9 release",
                  "11 working days of focused polish · then tag and announce")

    items = [
        ("Auto sub-clustering (Leiden)",
         "AMBER",
         "Refines oversize Ed-Fi domains (Student, Assessment, Discipline) into 8–20-table sub-clusters. "
         "Today: static taxonomy works. Next: sharper retrieval inside the heaviest domains."),
        ("Multi-provider regression matrix",
         "AMBER",
         "All 5 LLM providers + 4 embedding providers wired. Matrix expands to test "
         "every consumer (chat, query, eval, agent) against every provider on every supported dialect."),
        ("Agentic polish",
         "AMBER",
         "Tool-call reliability under streaming, richer agent tools (chart export, eval drilldown), "
         "smoother conversation list UX, retry on transient provider errors."),
        ("Observability (OTel + Prometheus)",
         "RED",
         "Per-stage tracing spans + counters + Grafana dashboards. Logging is in place today; "
         "spans and metrics are next. May slip to v0.10 if matrix work runs long."),
        ("Recorded demo + release cut",
         "AMBER",
         "End-to-end recording on a clean Mac and Windows machine; v0.9 tag pushed Sat May 16; "
         "release notes + operator runbook + provider-swap guide finalized."),
    ]
    _panel(s, left=0.5, top=1.6, width=12.3, height=5.4, fill=PANEL)
    y = 1.75
    for label, level, what in items:
        color = AMBER if level == "AMBER" else RED
        _add_text(s, "●", left=0.7, top=y, width=0.3, height=0.3, size=18, bold=True, color=color)
        _add_text(s, label, left=1.05, top=y, width=11.7, height=0.3, size=13, bold=True, color=TEXT)
        _add_text(s, what, left=1.05, top=y + 0.32, width=11.7, height=0.7, size=11, color=MUTED)
        y += 1.05

    _footer(s, "Plan · Section 6", "")
    _notes(s, "The honest framing: most of the platform is already built. The May 16 date buys "
              "headroom to land the things that matter for a confident demo — provider matrix, "
              "agent reliability, and a recorded walkthrough. Observability is the stretch item; "
              "if multi-provider work runs long, OTel slips to v0.10 without affecting the demo.")

    # ── Slide 20: Day-by-day plan ─────────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "Day-by-day plan",
                  "Apr 30 → Fri May 15 · 11 working days · v0.9 release Sat May 16")

    days = [
        ("Thu Apr 30",
         "Multi-provider regression matrix — kickoff",
         ["Lock the matrix shape: 5 LLMs × 4 embeddings × 3 dialects × 4 consumers",
          "Stand up CI fixture set; run agent loop against Anthropic + Bedrock first"]),
        ("Mon May 4",
         "Multi-provider matrix — coverage",
         ["Add Azure OpenAI + OpenAI + OpenRouter coverage; capture per-provider regressions",
          "Patch agent translator layer for any newly surfaced gaps"]),
        ("Tue May 5",
         "Auto sub-clustering (Leiden) inside oversize domains",
         ["Build affinity matrix (cosine + graph proximity + name jaccard)",
          "Tune Leiden for 8–20 tables/cluster; LLM auto-naming for sub-clusters",
          "Hungarian assignment for cluster-ID stability across rebuilds"]),
        ("Wed May 6",
         "Agentic polish — tool reliability + UX",
         ["Tool-call retries on transient provider errors; structured failure surfaces",
          "Streaming UX refinements; chart-export and eval-drilldown agent tools",
          "Conversation list polish (rename, archive, filter)"]),
        ("Thu May 7",
         "Observability scaffolding",
         ["OpenTelemetry spans on every pipeline + agent stage",
          "Prometheus counters/histograms; Grafana dashboards in infra/grafana/",
          "Wire structured logging context (request id, provider, dialect)"]),
        ("Fri May 8",
         "Eval suite expansion + nightly gate",
         ["Expand gold-question suite from 50 to 100 questions",
          "Wire CI nightly run; gate on >2pp execution-accuracy regression",
          "Eval Dashboard polish: per-build deltas, regression alerts"]),
        ("Mon May 11",
         "Performance pass + caching",
         ["APSP load + retrieval cache hot paths; verify p95 latency budget per spec §17",
          "Per-user quota + rate limits; secret-leak scrubber on all error paths"]),
        ("Tue May 12",
         "Cross-platform smoke + provider-swap drill",
         ["Clean Windows 11 + macOS dry runs from git clone",
          "Provider-swap drill: Anthropic → Bedrock → Azure mid-session, no restart"]),
        ("Wed May 13",
         "Recorded demo — first cut",
         ["Script: connect SQLite demo → ask question → chart + summary → switch to Postgres",
          "Capture cluster manager drag-to-reassign + Settings UI editing",
          "Edit + caption pass"]),
        ("Thu May 14",
         "Recorded demo polish + final eval gate",
         ["Re-record any segments where the live system stuttered",
          "Final eval suite run against v0.9 candidate; capture metrics for release notes"]),
        ("Fri May 15",
         "Release-day prep",
         ["v0.9 release-candidate tag; smoke test on clean machines",
          "Operator runbook + provider-swap guide finalized",
          "Final read-through of release notes"]),
        ("Sat May 16",
         "v0.9 RELEASE",
         ["Tag v0.9 on main; push to GitHub Releases with recorded demo attached",
          "Announce internally; client-facing summary distributed"]),
    ]

    _panel(s, left=0.5, top=1.55, width=12.3, height=5.5, fill=PANEL)
    y = 1.65
    for day, theme, tasks in days:
        is_release = day == "Sat May 16"
        day_color = GREEN if is_release else ACCENT
        _add_text(s, day, left=0.7, top=y, width=1.7, height=0.26,
                  size=11, bold=True, color=day_color)
        _add_text(s, theme, left=2.5, top=y, width=10.3, height=0.26,
                  size=11, bold=True, color=TEXT)
        for t in tasks:
            y += 0.19
            _add_text(s, "•  " + t, left=2.5, top=y, width=10.3, height=0.22,
                      size=9, color=MUTED)
        y += 0.22

    _footer(s, "Plan · 11 working days + release",
            "May 1 holiday + weekends excluded")
    _notes(s, "Days 1-2 lead with the matrix because that work is the gate for everything else — "
              "provider regressions cascade. Days 3-4 are the agentic + sub-clustering polish. "
              "Day 5 is observability (the riskiest item — may compress). Days 6-9 are eval, "
              "perf, smoke, and the recorded demo. Days 10-11 are the release-day choreography.")

    # ── Slide 21: Risks ───────────────────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "Risks & how we'll handle them",
                  "Known unknowns flagged early; fallbacks scoped into the plan")

    risks = [
        ("Provider matrix surprises",
         "A new provider/consumer combination may need translator-layer fixes that take longer than a half-day.",
         "Two-day budget covers worst case; OpenRouter's runtime probe absorbs edge cases automatically.",
         AMBER),
        ("Leiden tuning quality",
         "Sub-cluster quality varies with affinity-matrix weights; may not converge cleanly on Day 3.",
         "Static taxonomy stays as the v0.9 default; Leiden ships behind a feature flag if needed.",
         AMBER),
        ("Observability slippage",
         "OTel + Prometheus spans across 9 pipeline stages + 8 agent tools is non-trivial.",
         "Stretch goal — slips to v0.10 cleanly. Logging is comprehensive today, so debug-ability holds.",
         RED),
        ("Live demo connectivity",
         "Live demos can stutter on conference Wi-Fi or provider rate limits.",
         "Pre-record the full flow Wed May 13 + polish Thu May 14; live Q&A only.",
         GREEN),
    ]
    _panel(s, left=0.5, top=1.6, width=12.3, height=5.4, fill=PANEL)
    y = 1.8
    for label, what, plan, color in risks:
        _add_text(s, "●", left=0.7, top=y, width=0.3, height=0.3, size=20, bold=True, color=color)
        _add_text(s, label, left=1.1, top=y, width=11.7, height=0.3, size=14, bold=True, color=TEXT)
        _add_text(s, "Risk: "  + what, left=1.1, top=y + 0.32, width=11.7, height=0.32, size=11, color=MUTED)
        _add_text(s, "Plan: "  + plan, left=1.1, top=y + 0.62, width=11.7, height=0.32, size=11, color=ACCENT)
        y += 1.27

    _footer(s, "Risk register", "")
    _notes(s, "We're not promising perfection by Saturday May 16. We are promising a tested "
              "platform with a recorded demo, the multi-provider matrix passing, and stretch "
              "items called out before they slip silently.")

    # ── Slide 22: What you'll see ─────────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_text(s, "What you'll see at the May 16 demo", left=0.8, top=1.4, width=11.7, height=0.6,
              size=24, bold=True, color=ACCENT)

    _panel(s, left=0.5, top=2.1, width=12.3, height=4.6, fill=PANEL)
    _bullets(s, [
        "Connect any Ed-Fi-shaped database from a browser — no code, no config files",
        "Ask plain-English questions and watch SQL + rows + chart + summary land in seconds",
        "Hold a multi-turn conversation — follow-up questions reuse earlier context",
        "Watch the agent stream tool calls live: classify → retrieve → resolve → run",
        "See the SQL color-coded against the catalog with cited tables",
        "Approve a query — and watch the platform get smarter for the next question like it",
        "Switch from one database (or LLM provider) to another mid-session and see results refresh automatically",
        "Open the Eval Dashboard to see how accuracy is trending across builds",
    ], left=0.7, top=2.25, width=11.9, height=4.4, size=13, color=TEXT)

    _add_text(s, "Demo machine: clean Windows install · no prior setup · v0.9 tagged Sat May 16",
              left=0.5, top=6.85, width=12.3, height=0.4, size=12, color=MUTED, align=PP_ALIGN.CENTER)

    _notes(s, "End on the demo. The 8 specific things on this slide are what you'll watch. "
              "Each bullet is gated on a workstream above. We sequence them so even if observability "
              "slips, the first 7 are still demo-ready.")

    # ── Save ──────────────────────────────────────────────────────────────
    out = Path(__file__).resolve().parent / "architecture_deck.pptx"
    prs.save(str(out))
    print(f"Wrote {out}  ({out.stat().st_size / 1024:.1f} KB · {len(prs.slides)} slides)")


if __name__ == "__main__":
    build()
