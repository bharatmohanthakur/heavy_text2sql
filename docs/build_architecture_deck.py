"""Generate the Ed-Fi Text-to-SQL client-facing architecture deck.

Run with:
    uvx --from python-pptx python docs/build_architecture_deck.py

Produces docs/architecture_deck.pptx.

Audience: client / business stakeholder. Frame the work in outcomes,
not file paths. The semantic schema layer and the foreign-key graph
are positioned as the foundation we've delivered; everything else is
the upcoming work tracked through May 8.
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


def _connector(slide, x1, y1, x2, y2, *, color=BORDER, weight=1.5, arrow=True):
    line = slide.shapes.add_connector(1, Inches(x1), Inches(y1), Inches(x2), Inches(y2))
    line.line.color.rgb = color
    line.line.width = Pt(weight)
    if arrow:
        from pptx.oxml.ns import qn
        ln = line.line._get_or_add_ln()
        tail = ln.makeelement(qn("a:tailEnd"), {"type": "triangle", "w": "med", "len": "med"})
        ln.append(tail)


def _slide_bg(slide):
    fill = slide.background.fill
    fill.solid()
    fill.fore_color.rgb = BG


def _add_title(slide, title, subtitle=None):
    _add_text(slide, title, left=0.5, top=0.35, width=12.3, height=0.7,
              size=32, bold=True, color=ACCENT)
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


def _status_chip(slide, *, left, top, label, color):
    chip = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE,
                                    Inches(left), Inches(top), Inches(1.2), Inches(0.32))
    chip.fill.solid(); chip.fill.fore_color.rgb = color
    chip.line.fill.background()
    tb = slide.shapes.add_textbox(Inches(left), Inches(top), Inches(1.2), Inches(0.32))
    tf = tb.text_frame; tf.margin_left = tf.margin_right = Inches(0); tf.margin_top = Inches(0.04)
    _set_text(tf, label, size=10, bold=True, color=BG, align=PP_ALIGN.CENTER)


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
    _add_text(s, "Progress update — foundation delivered, integration ahead",
              left=0.8, top=3.85, width=11.7, height=0.5,
              size=16, color=MUTED)
    _add_text(s, "Apr 30, 2026  ·  client review  ·  target demo Fri May 8",
              left=0.8, top=6.6, width=11.7, height=0.4,
              size=13, color=MUTED)
    _notes(s, "Set the room: this is a status update, not a final review. Two big technical "
              "pieces are done — the semantic schema layer and the foreign-key graph. Those are "
              "the foundation everything else sits on. Today I'll show what each is, what it "
              "unlocks, and the concrete plan to demo by Friday May 8.")

    # ── Slide 2: The problem ──────────────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "The problem we're solving",
                  "An analyst asks a question. The platform answers with data.")

    _panel(s, left=0.5, top=1.6, width=12.3, height=2.1, fill=PANEL)
    _add_text(s, "Today, in any Ed-Fi-shaped database",
              left=0.7, top=1.7, width=11.9, height=0.4, size=14, bold=True, color=AMBER)
    _bullets(s, [
        "1,048 tables · ~10,000 columns · hundreds of foreign keys",
        "An analyst with a question must (a) know which tables to join, (b) write the SQL, (c) execute it, (d) read it",
        "End-to-end this is hours of work for a senior engineer — and impossible for a non-engineer",
    ], left=0.7, top=2.1, width=11.9, height=1.6, size=13)

    _panel(s, left=0.5, top=3.9, width=12.3, height=2.9, fill=PANEL)
    _add_text(s, "What we're building",
              left=0.7, top=4.0, width=11.9, height=0.4, size=14, bold=True, color=GREEN)
    _bullets(s, [
        "Type a question in English  →  receive correct SQL, the rows it returns, a chart, and a written summary",
        "Works on any Ed-Fi database (Postgres / MSSQL / SQLite) without changing the database itself",
        "Cites the tables it used so an engineer can verify the answer in seconds",
        "Learns from approved queries — the more it's used, the better it gets",
    ], left=0.7, top=4.4, width=11.9, height=2.4, size=13)

    _footer(s, "Why this project · Section 1", "")
    _notes(s, "Anchor in the user pain. Most Ed-Fi work today goes through a small handful of "
              "people who can write SQL against the model. We're widening that bottleneck — not "
              "by training more SQL writers but by automating the translation from English.")

    # ── Slide 3: Status snapshot ──────────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "Status: foundation built, integration ahead",
                  "Two of seven workstreams complete; remaining five planned for the next 7 working days")

    rows = [
        ("Foreign-Key Graph",       "Done",  GREEN,
         "Maps how every Ed-Fi table connects to every other; computes the cheapest joins."),
        ("Semantic Schema Layer",   "Done",  GREEN,
         "Embeds each table's meaning so questions can find the right tables."),
        ("Question → SQL pipeline", "Next",  AMBER,
         "Wire graph + semantics into a routed, validated SQL generator with auto-repair."),
        ("Validation & Repair",     "Next",  AMBER,
         "Catch bad SQL before execution; auto-fix the common failure modes."),
        ("Charts & Descriptions",    "Next",  AMBER,
         "Pick the right visualization automatically; explain the answer in English."),
        ("Web UI · Settings · Eval","Next",  AMBER,
         "Browser experience, operator controls, accuracy dashboards."),
        ("Hardening & Demo",         "Next",  AMBER,
         "Multi-provider polish, observability, recorded demo on a clean machine."),
    ]
    _panel(s, left=0.5, top=1.6, width=12.3, height=5.2, fill=PANEL)
    y = 1.75
    for label, status, color, what in rows:
        _add_text(s, label, left=0.8, top=y, width=4.0, height=0.35,
                  size=14, bold=True, color=TEXT)
        _status_chip(s, left=4.95, top=y + 0.02, label=status, color=color)
        _add_text(s, what, left=6.4, top=y + 0.02, width=6.4, height=0.35,
                  size=12, color=MUTED)
        y += 0.72

    _footer(s, "Status snapshot", "Two of seven complete · 7 working days to demo")
    _notes(s, "This single slide is the headline. The two foundational pieces are done — the parts "
              "that take longest to get right because they involve schema modeling and embeddings. "
              "Everything else is integration: hooking those pieces together, polishing the UX, "
              "and verifying accuracy. That's why a 7-working-day plan is realistic.")

    # ── Slide 4: System map ───────────────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "The big picture",
                  "How a question becomes an answer · two layers carry it")

    layers = [
        ("Question",     ["Analyst types: 'How many Hispanic students enrolled in Grade 9 last year?'"], MUTED),
        ("Semantic Schema",
         ["Finds the small set of tables that matter — out of 1,048",
          "Resolves real-world terms (e.g. 'Hispanic') to the right code in the right table"], GREEN),
        ("Foreign-Key Graph",
         ["Computes the cheapest join path between those tables",
          "Hands the SQL generator ready-to-use JOIN clauses"], GREEN),
        ("SQL · Validate · Run",
         ["LLM writes SQL grounded in real columns",
          "Validator catches errors; repair loop fixes them; engine executes"], AMBER),
        ("Answer",
         ["Rows + auto-picked chart + plain-English summary, with the tables cited"], AMBER),
    ]
    y = 1.6
    for label, lines, color in layers:
        _panel(s, left=0.5, top=y, width=12.3, height=0.95, fill=PANEL)
        _add_text(s, label, left=0.7, top=y + 0.1, width=2.7, height=0.4,
                  size=14, bold=True, color=color)
        for j, ln in enumerate(lines):
            _add_text(s, "• " + ln, left=3.4, top=y + 0.1 + j * 0.32, width=9.2, height=0.4,
                      size=12, color=TEXT)
        y += 1.10

    _add_text(s, "Green = built today  ·  Amber = next 7 working days",
              left=0.5, top=7.05, width=12.3, height=0.3,
              size=11, color=MUTED, align=PP_ALIGN.CENTER)
    _notes(s, "Walk top-to-bottom. Pause on the Semantic Schema and Foreign-Key Graph rows — "
              "those are this deck's protagonists. The bottom rows are next-week work.")

    # ── Slide 5: Section divider — Semantic ───────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_text(s, "What's built · 1 of 2", left=0.8, top=2.5, width=11.7, height=0.5,
              size=18, color=MUTED)
    _add_text(s, "The Semantic Schema Layer", left=0.8, top=3.05, width=11.7, height=1.0,
              size=44, bold=True, color=GREEN)
    _add_text(s, "How the platform finds the right tables for an English question",
              left=0.8, top=4.05, width=11.7, height=0.5, size=18, color=TEXT)
    _notes(s, "If we just gave the LLM 1,048 tables and asked it to pick, accuracy would collapse "
              "and cost would explode. The semantic layer is the search index that fixes that.")

    # ── Slide 6: Per-table understanding ──────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "What we know about each table",
                  "Captured once at build time; queried millions of times after")

    _panel(s, left=0.5, top=1.6, width=7.4, height=5.4, fill=PANEL)
    blob = [
        "Table:  edfi.StudentSchoolAssociation",
        "Domains:  Student · Enrollment",
        "",
        "Description:",
        "Tracks a student's relationship with a school —",
        "primary or secondary enrollment, entry / exit",
        "dates, grade level, and entry type.",
        "",
        "Identifying columns:",
        "  StudentUSI  ·  SchoolId  ·  EntryDate",
        "",
        "Linked columns:",
        "  EntryGradeLevelDescriptorId → GradeLevelDescriptor",
        "  ExitWithdrawTypeDescriptorId → enums",
        "",
        "Connected tables:",
        "  edfi.Student · edfi.School · edfi.Section ...",
        "",
        "Proven queries that use it:  12",
    ]
    y = 1.78
    for ln in blob:
        bold = ln.endswith(":") or ln.startswith(("Table:", "Domains:"))
        c = ACCENT if bold else (TEXT if ln else MUTED)
        sz = 13 if bold else 12
        _add_text(s, ln, left=0.7, top=y, width=7.0, height=0.28, size=sz, bold=bold, color=c)
        y += 0.26

    _panel(s, left=8.15, top=1.6, width=4.8, height=5.4, fill=PANEL)
    _add_text(s, "Why this works", left=8.35, top=1.7, width=4.4, height=0.4,
              size=16, bold=True, color=AMBER)
    _bullets(s, [
        "Domains let us narrow 1,048 tables to ~30 in milliseconds",
        "Description carries authoritative Ed-Fi prose — no LLM hallucination",
        "Identifying columns surface what each row 'is'",
        "Linked columns chase descriptor codes (e.g. 'Hispanic' → RaceDescriptor)",
        "Connected tables remind the model what joins are natural",
        "Proven-query count biases the search toward tables the platform has answered before",
    ], left=8.35, top=2.15, width=4.4, height=4.5, size=11)

    _footer(s, "Built — Semantic Schema · 1 of 3", "")
    _notes(s, "Each of the 1,048 tables has this kind of profile, captured at build time. The "
              "profile is then turned into a numerical fingerprint (an embedding) that the "
              "platform can match against any incoming question.")

    # ── Slide 7: Search & route ───────────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "How a question finds its tables",
                  "Two fast searches narrow 1,048 tables to a handful")

    _panel(s, left=0.5, top=1.6, width=12.3, height=2.7, fill=PANEL)
    _add_text(s, "Step 1 — pick the right neighborhood",
              left=0.7, top=1.7, width=12, height=0.4, size=14, bold=True, color=ACCENT)
    _bullets(s, [
        "Compare the question's meaning against every domain (e.g. Student, Discipline, Assessment)",
        "Combine semantic meaning (60%) with keyword match (40%) — best of both worlds",
        "Return the top three neighborhoods so the model has options",
    ], left=0.7, top=2.15, width=12, height=2.0, size=12)

    _panel(s, left=0.5, top=4.5, width=12.3, height=2.4, fill=PANEL)
    _add_text(s, "Step 2 — pick tables inside that neighborhood",
              left=0.7, top=4.6, width=12, height=0.4, size=14, bold=True, color=ACCENT)
    _bullets(s, [
        "Inside the chosen neighborhoods, score every table profile against the question",
        "Pull the small set (typically 3–8 tables) that best matches",
        "Hand them to the graph layer to figure out how to join them",
    ], left=0.7, top=5.05, width=12, height=2.0, size=12)

    _footer(s, "Built — Semantic Schema · 2 of 3", "")
    _notes(s, "Two-stage search. Without it, accuracy on 1,048 tables drops below useful. With it, "
              "we routinely route to the correct 3–8 tables for an Ed-Fi question.")

    # ── Slide 8: Entity resolution ────────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "Resolving real-world terms",
                  "'Hispanic' is text on a screen — the database speaks codes")

    _panel(s, left=0.5, top=1.6, width=12.3, height=2.0, fill=PANEL)
    _add_text(s, "Worked example",
              left=0.7, top=1.7, width=12, height=0.4, size=14, bold=True, color=AMBER)
    _bullets(s, [
        "Analyst types: 'How many Hispanic students enrolled last year?'",
        "Database has no field called 'Hispanic' — it stores a foreign key to RaceDescriptor",
        "We need to: recognize 'Hispanic' is a value, find which descriptor table it lives in, attach the right join",
    ], left=0.7, top=2.15, width=12, height=1.5, size=12)

    _panel(s, left=0.5, top=3.8, width=12.3, height=3.1, fill=PANEL)
    _add_text(s, "Four-tier resolver — fast first, smart last",
              left=0.7, top=3.9, width=12, height=0.4, size=14, bold=True, color=ACCENT)
    rows = [
        ("Tier 1 · Bloom filter",  "Is this term even in the database?", "<1 ms"),
        ("Tier 2 · Fuzzy match",    "Handle typos: 'Hispanc' → 'Hispanic'", "5 ms"),
        ("Tier 3 · Semantic lookup","Match meaning when spelling differs", "20 ms"),
        ("Tier 4 · LLM disambiguate","When the value could mean two things, ask the model", "200 ms"),
    ]
    y = 4.4
    for tier, what, perf in rows:
        _add_text(s, tier, left=0.7, top=y, width=4.0, height=0.32, size=12, bold=True, color=TEXT)
        _add_text(s, what, left=4.8, top=y, width=6.0, height=0.32, size=12, color=MUTED)
        _add_text(s, perf, left=11.0, top=y, width=1.8, height=0.32, size=12, color=GREEN, align=PP_ALIGN.RIGHT)
        y += 0.55

    _footer(s, "Built — Semantic Schema · 3 of 3", "Tier 1+2 handle ~95% of cases")
    _notes(s, "This is the layer that turns 'plain English' into 'database-correct'. Without it, "
              "questions like 'last year', 'Grade 9', or 'free-and-reduced lunch' would never "
              "land on the right rows.")

    # ── Slide 9: Section divider — Graph ──────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_text(s, "What's built · 2 of 2", left=0.8, top=2.5, width=11.7, height=0.5,
              size=18, color=MUTED)
    _add_text(s, "The Foreign-Key Graph", left=0.8, top=3.05, width=11.7, height=1.0,
              size=44, bold=True, color=GREEN)
    _add_text(s, "How the platform figures out which JOINs make sense",
              left=0.8, top=4.05, width=11.7, height=0.5, size=18, color=TEXT)
    _notes(s, "If semantic gives us 'these 5 tables', graph tells us 'this is the cheapest, "
              "most natural way to connect them'. Without it, even an LLM that knows Ed-Fi "
              "well would burn time guessing join paths.")

    # ── Slide 10: What the graph contains ─────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "What the graph contains",
                  "Every Ed-Fi table and every relationship between them, weighted by closeness")

    _panel(s, left=0.5, top=1.6, width=6.0, height=5.2, fill=PANEL)
    _add_text(s, "By the numbers", left=0.7, top=1.7, width=5.6, height=0.4,
              size=16, bold=True, color=ACCENT)
    rows = [
        ("Tables",               "1,048"),
        ("Relationships (FKs)",   "~1,900"),
        ("Domains",                "35"),
        ("Storage on disk",        "<10 MB"),
        ("Load time at startup",  "<100 ms"),
        ("Find best join (k=2)",   "<5 ms"),
        ("Find best join tree",    "<50 ms typical"),
    ]
    y = 2.15
    for k, v in rows:
        _add_text(s, k, left=0.7, top=y, width=3.4, height=0.34, size=13, color=MUTED)
        _add_text(s, v, left=4.1, top=y, width=2.0, height=0.34, size=13, bold=True, color=TEXT)
        y += 0.55

    _panel(s, left=6.85, top=1.6, width=6.0, height=5.2, fill=PANEL)
    _add_text(s, "How relationships are weighted",
              left=7.05, top=1.7, width=5.6, height=0.4, size=16, bold=True, color=AMBER)
    _bullets(s, [
        "Inside the same Ed-Fi entity (e.g. Student): cheapest",
        "Between related entities: more expensive",
        "Between unrelated domains: most expensive",
        "Multi-column keys earn a small bonus",
        "Custom / non-Ed-Fi tables: handled with caution",
    ], left=7.05, top=2.15, width=5.6, height=4.5, size=13)
    _add_text(s, "Net effect: joins follow the natural shape of Ed-Fi.",
              left=7.05, top=6.2, width=5.6, height=0.4, size=12, color=GREEN)

    _footer(s, "Built — Foreign-Key Graph · 1 of 2", "")
    _notes(s, "Two ideas to leave the room with: the graph is small (10 MB) and fast (millisecond "
              "responses), and the weighting biases it to keep joins inside the natural Ed-Fi "
              "boundaries — which is also what a senior analyst would do by hand.")

    # ── Slide 11: Steiner ─────────────────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "Finding the best join path",
                  "A classical computer-science problem, solved three ways")

    _panel(s, left=0.5, top=1.6, width=12.3, height=1.9, fill=PANEL)
    _add_text(s, "The challenge",
              left=0.7, top=1.7, width=12, height=0.4, size=14, bold=True, color=AMBER)
    _bullets(s, [
        "Given the small set of tables semantic has chosen, find the cheapest JOIN tree connecting them",
        "Optimal solutions are mathematically expensive; we use proven approximations",
    ], left=0.7, top=2.15, width=12, height=1.5, size=12)

    _panel(s, left=0.5, top=3.7, width=12.3, height=3.2, fill=PANEL)
    rows = [
        ("Connecting 2 tables",
         "Bidirectional shortest-path; returns three alternatives so the model picks the most readable",
         "<5 ms",  GREEN),
        ("Connecting 3–8 tables",
         "Steiner-tree approximation (KMB) — provably within 2× of optimal",
         "<50 ms", GREEN),
        ("Edge cases (>8 tables)",
         "Greedy fallback; rare on real Ed-Fi questions",
         "<200 ms", AMBER),
    ]
    y = 3.8
    for label, what, perf, color in rows:
        _add_text(s, label, left=0.7, top=y, width=3.6, height=0.34, size=13, bold=True, color=TEXT)
        _add_text(s, what, left=4.4, top=y, width=6.7, height=0.7, size=12, color=MUTED)
        _add_text(s, perf, left=11.2, top=y + 0.3, width=1.7, height=0.34, size=12, color=color, align=PP_ALIGN.RIGHT)
        y += 1.0

    _footer(s, "Built — Foreign-Key Graph · 2 of 2", "")
    _notes(s, "Don't dwell on KMB by name. The takeaway is: this is solved, fast, and we have "
              "fallbacks so the platform never freezes on an unusual question.")

    # ── Slide 12: Section divider — What's next ───────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_text(s, "What ships next", left=0.8, top=2.4, width=11.7, height=0.6,
              size=18, color=MUTED)
    _add_text(s, "Five workstreams to demo", left=0.8, top=2.95, width=11.7, height=1.0,
              size=42, bold=True, color=AMBER)
    _add_text(s, "Question pipeline · Validation · Charts · UI · Hardening",
              left=0.8, top=3.95, width=11.7, height=0.5, size=18, color=TEXT)
    _add_text(s, "7 working days · Apr 30 → Fri May 8 · weekend-free",
              left=0.8, top=4.55, width=11.7, height=0.4, size=14, color=MUTED)

    # ── Slide 13: Workstream catalog ──────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "Five workstreams in detail", "What each one means for the demo")

    items = [
        ("1.  Question → SQL pipeline",
         "The end-to-end orchestrator that takes an English question, calls the semantic + graph "
         "layers, and emits SQL grounded in real columns."),
        ("2.  Validation & repair",
         "Catch broken SQL before it runs (parse, plan check, dry-execute). Auto-fix the common "
         "failure modes — typos, missing joins, wrong column names — up to three retries."),
        ("3.  Charts & descriptions",
         "Pick the right chart automatically based on the result shape. Generate a one-paragraph "
         "summary in plain English so the answer reads itself."),
        ("4.  Web UI · Settings · Eval",
         "Browser experience for analysts (Query, Chat, Tables); operator controls for connecting "
         "databases and LLMs; an Eval Dashboard tracking accuracy across builds."),
        ("5.  Hardening & demo",
         "Multi-provider readiness, observability, recorded demo on a clean Windows machine; "
         "v0.8 release tag."),
    ]
    _panel(s, left=0.5, top=1.6, width=12.3, height=5.3, fill=PANEL)
    y = 1.8
    for label, what in items:
        _add_text(s, label, left=0.7, top=y, width=12, height=0.35, size=14, bold=True, color=ACCENT)
        _add_text(s, what, left=0.9, top=y + 0.32, width=11.8, height=0.7, size=12, color=MUTED)
        y += 1.05

    _footer(s, "Plan · Section 4", "")
    _notes(s, "Each workstream produces a tangible thing the client can see at the demo. The order "
              "is dependency-driven: pipeline depends on graph + semantic; UI depends on pipeline; "
              "hardening goes last.")

    # ── Slide 14: Day-by-day plan ─────────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "Day-by-day plan",
                  "Apr 30 → Fri May 8 · 7 working days · weekends excluded")

    days = [
        ("Thu Apr 30",
         "Pipeline scaffold + auto-grouping spike",
         ["Wire question → semantic search → graph join into a single orchestrator",
          "Spike Leiden auto-grouping inside oversize domains for sharper routing",
          "Verify the platform handles real-world databases that have non-Ed-Fi tables"]),
        ("Fri May 1",
         "Pipeline land + retrieval polish",
         ["Productionize the orchestrator with structured logs at every stage",
          "Add proven-query retrieval (top-3 examples passed to the SQL generator)",
          "First end-to-end NL → SQL → rows on the demo SQLite database"]),
        ("Mon May 4",
         "Validation & repair loop",
         ["Parse-check, plan-check, and dry-execute every generated query",
          "Three-attempt repair loop for typical failure modes",
          "Expand the gold-query test suite to 50 questions; run nightly"]),
        ("Tue May 5",
         "Charts, descriptions, and Settings UI",
         ["Auto-pick chart type from result shape (Vega-Lite spec generation)",
          "Plain-English answer summary running in parallel with chart render",
          "Settings page editable end-to-end — pick database, LLM, embedding via UI"]),
        ("Wed May 6",
         "Eval dashboard + cluster manager",
         ["Eval Dashboard page: per-build metrics, regression alerts",
          "Cluster Manager page: drag tables between groups, trigger rebuild",
          "Operator runbook + provider-swap guide"]),
        ("Thu May 7",
         "Hardening + multi-provider polish",
         ["Multi-provider matrix tested across all combinations",
          "Performance pass — caching, observability dashboards",
          "Two security fixes flagged in earlier review"]),
        ("Fri May 8",
         "Demo prep + release",
         ["End-to-end run on a clean Mac and Windows machine",
          "Recorded demo: connect database → ask question → see chart + summary",
          "v0.8 release tag pushed; final docs"]),
    ]

    _panel(s, left=0.5, top=1.6, width=12.3, height=5.5, fill=PANEL)
    y = 1.75
    for day, theme, tasks in days:
        _add_text(s, day, left=0.7, top=y, width=1.7, height=0.3,
                  size=12, bold=True, color=ACCENT)
        _add_text(s, theme, left=2.5, top=y, width=10.3, height=0.3,
                  size=12, bold=True, color=TEXT)
        for t in tasks:
            y += 0.21
            _add_text(s, "•  " + t, left=2.5, top=y, width=10.3, height=0.25,
                      size=10, color=MUTED)
        y += 0.30

    _footer(s, "Plan · Section 4", "Buffer day not built in — risks below")
    _notes(s, "Front-loaded the pipeline because it unblocks UI and eval. Auto-grouping is a spike — "
              "if it doesn't tune well, we ship per-domain routing for v0.8 and revisit.")

    # ── Slide 15: Risks ───────────────────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_title(s, "Risks & how we'll handle them",
                  "Known unknowns flagged early; mitigations in the plan above")

    risks = [
        ("Auto-grouping accuracy",
         "Tuning the algorithm well takes iteration; we may not have it perfect by Friday.",
         "Fallback to per-domain routing for v0.8 — already proven; auto-grouping ships in v0.9.",
         AMBER),
        ("Two flagged security fixes",
         "Earlier code review flagged a credential-exposure path and a legacy-DB migration gap.",
         "Both have ~half-day fixes scoped for Tuesday May 5.",
         RED),
        ("Concurrent users on SQLite metadata",
         "SQLite serializes writes; multi-user demos may hit lock contention.",
         "Documented as single-user demo; multi-user uses Postgres metadata (already supported).",
         AMBER),
        ("Live demo connectivity",
         "Recorded demos are safer than live demos for client meetings.",
         "Pre-record the full flow Friday morning; show recording with live Q&A.",
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
    _notes(s, "We're not promising perfection by Friday. We are promising a working end-to-end "
              "demo on a clean machine, with the foundation already proven. The flagged security "
              "items are scoped fixes, not unknowns.")

    # ── Slide 16: What you'll see ─────────────────────────────────────────
    s = prs.slides.add_slide(blank); _slide_bg(s)
    _add_text(s, "What you'll see at the demo", left=0.8, top=1.4, width=11.7, height=0.6,
              size=24, bold=True, color=ACCENT)

    _panel(s, left=0.5, top=2.1, width=12.3, height=4.6, fill=PANEL)
    _bullets(s, [
        "Connect any Ed-Fi-shaped database from a browser — no code, no config files",
        "Ask plain-English questions and watch the answer arrive in seconds",
        "See the SQL the platform generated, color-coded against the catalog",
        "Look at the auto-picked chart and read the written summary",
        "Approve a query — and watch the platform get smarter for the next question like it",
        "Switch from one database to another mid-session and see results refresh automatically",
        "Open the Eval Dashboard to see how accuracy is trending across builds",
    ], left=0.7, top=2.25, width=11.9, height=4.4, size=14, color=TEXT)

    _add_text(s, "Demo machine: clean Windows install · no prior setup", left=0.5, top=6.85,
              width=12.3, height=0.4, size=12, color=MUTED, align=PP_ALIGN.CENTER)

    _notes(s, "End on the demo. The 7 specific things on this slide are what you'll watch. "
              "Each bullet is gated on a workstream above. We sequence them so even if Friday "
              "ends with the last item half-baked, the first six are still demo-ready.")

    # ── Save ──────────────────────────────────────────────────────────────
    out = Path(__file__).resolve().parent / "architecture_deck.pptx"
    prs.save(str(out))
    print(f"Wrote {out}  ({out.stat().st_size / 1024:.1f} KB · {len(prs.slides)} slides)")


if __name__ == "__main__":
    build()
