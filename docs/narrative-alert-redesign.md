# Narrative alert system — redesign

**Status: alert conditions CLOSED (2026-07-17); reframed as a philosophy (2026-07-20).**
No implementation yet.

The whole design falls out of **two core decisions**. Everything below — the axes, the
cohorts, the weights, the labels, the geometry — is a *consequence* of one or the other,
not an independent choice. If you only read one section, read the next one.

---

## The two core decisions

**C1 — We only rank what we measured.**
A badge is a claim about a narrative. We make the claim only where we have the observation
to back it. A narrative we did not look at is *unmeasured*, not *quiet*; it must be excluded
from the ranking, never ranked as the least-active one. The two failure modes of the
current system — the 93%-zero acceleration axis and the volatile, scraper-driven composite
pool — are both this rule being broken, in opposite directions.

**C2 — We measure exactly two things: how far a narrative has spread, and how fast that
spread is changing.**
Spread is a **state** (a level): how big is this narrative right now. Change-in-spread is a
**rate**: how fast is it moving. These are the two axes — **composite** carries the state,
**acceleration** carries the rate — and they must stay clean: every state signal on
composite, every change signal on acceleration, nothing straddling. The current system
mixes them (a growth term rides on the composite/level axis; the rate axis is built so it
cancels its own growth signal), and that mixing is the source of the "movers are noise"
complaint.

Everything else is a derivation:

| Decision | Follows from | What it is |
|---|---|---|
| **D0** — rank only what we measured | **is C1** | the ranking rule, plus its mechanism |
| **D1** — four labels and their regions | C2 | the two axes → four corners of meaning, as rectangles on the percentile plane |
| **D2** — percentiles, not absolutes | C1 | ranks are self-calibrating against a drifting scraper |
| **D3** — the two axes rank over different pools | C1 + C2 | state needs one measurement ever; a rate needs one today |
| **D4** — acceleration is growth *per day* | C2 | a rate is per unit time or it is not a rate |
| **D5** — `viral` is a conjunction | C2 | big *and* still climbing = both axes at once |
| **D6** — all change moves to the acceleration axis | C2 | evict velocity from composite; reweight so the rate axis can carry growth |

**Still open** — none of them alert conditions, all downstream of the two cores:
- **O5.3 / O5.6** — denominator scope and aggregation. One decision, gated on O6.
- **O6** — the scraper's revisit strategy. *Not answerable from this database; the critical
  path; belongs to whoever owns the scraper.*
- **O7** — composite has no staleness bound.
- **O8** — the `max(0, …)` floor on acceleration.
- **O4** — the enum migration and its consumers. Implementation coordination.

**Branch:** `redesign/narrative-alert-system`, based on `fix/indicators-calc-date-stamp`
(the classifier on `main` reads the previous run's indicators, so any evaluation done there
measures the bug rather than the design).

---

## The two axes — C2 made concrete

C2 says spread is a state and change-in-spread is a rate. That single distinction fixes
which signal lives where, how each is pooled, and why the axes cannot be size-adjusted or
blended.

### Composite — the spread-state axis (a level)

Per narrative, from its last known cumulative totals (carry-forward — a level is still known
from the last time we looked):

```
engagement_score = (likes×1 + comments×5) / views      # quality of spread — a state
reach_score      = min(views / avg_views_all, 10) / 10 # size of spread — a state
composite        = engagement_pct×0.625 + reach_pct×0.375
```

Each component is `PERCENT_RANK`-ed across the cohort, blended, and the blend ranked again
to give `composite_pct`.

Both signals are **state**: they are still meaningful for a narrative we did not re-scrape
today, because "how engaging" and "how big" do not become unknown just because we did not
look. That is exactly why composite can rank over the whole corpus (D3) and why a *change*
measure may not live here (D6, half one).

### Acceleration — the change-in-spread axis (a rate)

Carry-forward totals as-of `calc_date` vs as-of `calc_date − 1`, normalised per elapsed day
(D4):

```
change_views       = per-day growth in views
change_video_count = videos linked today − videos linked as of yesterday
change_engagement  = per-day change in engagement_score
acceleration       = 0.10×change_engagement + 0.35×change_video_count + 0.55×change_views
```

Then `PERCENT_RANK`-ed over the measured-today cohort.

Every term is **change**: it requires two observations bracketing a day. That is the bridge
back to C1 — a rate you did not measure today is *uncomputable*, not zero — and it is why
acceleration ranks only the narratives visited on `calc_date` (D3).

**On gained videos, we do not distinguish new from discovered.** `change_video_count` is
simply the change in how many videos we know the narrative has. If the scraper links a video
today that it had not linked yesterday, that counts as growth — whether the video was
uploaded this morning or is an old one the scraper only just found. This is consistent with
C1, not a violation of it: the *measured quantity* is the narrative's video count, and we
have two honest observations of it (yesterday's count, today's count). We are not claiming to
have watched the individual video appear; we are reporting that the narrative's footprint in
our corpus grew. Distinguishing real-world newness from discovery is neither reliable nor
something we have chosen to gate on. (An earlier revision built a 7-day `uploaded_at` rule to
draw that line; it was dropped 2026-07-20, and with it the measurement whose value we were
waiting on.)

### Acceleration is a plain ratio — not size-adjusted

Acceleration is growth relative to the narrative's own baseline, and that is all. It is
**not** stratified by size, residualised against composite, or given a tuned exponent. This
is not a separate decision — it is what "acceleration is a rate" already means:

- Large narratives rarely double, and when one does that is *genuinely* viral and *should*
  be rare. The mild anti-correlation between the axes (a big narrative cannot post a big
  percentage move; size also proxies for an older, slower lifecycle stage) is **real signal,
  not an artifact to engineer away**. Size-adjusting would manufacture viral narratives on
  days nothing went viral.
- A plain ratio stays explainable — "it grew X% in a day" — which matters when a person has
  to reason about a badge.

For the record, three rejected size adjustments: ranking within size strata (redefines
`viral` as "fast for its size" — we don't want that), a tuned `Δ/prev^α` exponent (same),
and `ln(cur/prev)` (does *nothing* to the correlation — Spearman is rank-based and `ln` is
monotonic, so ranks are identical; relevant only because `feat/unify-virality-windows` uses
log growth and is earmarked for salvage under D4).

**And a fourth, which does not look like a size adjustment: a coverage gate.** Requiring
`coverage >= k` before a narrative may accelerate was proposed to fix an understatement by
(1 − coverage) that turns out not to exist — a video with no `video_stats` row mostly did not
*change*, so zero is its correct contribution, not a missing one, and median measured
acceleration is flat across a hundredfold range of coverage (0.00068 at <1% vs 0.00080 at
≥50%). It is also a size filter wearing a disguise: coverage correlates **−0.61** with size,
so the gate drops the largest narratives first — exactly the population `viral` and
`consolidated` exist to describe. Do not reintroduce it without evidence against all three
points.

### Level vs rate, side by side

| | composite (state) | acceleration (rate) |
|---|---|---|
| question | how far has it spread? | how fast is spread changing? |
| evidence needed | ≥1 measurement, ever | two measurements bracketing `calc_date` |
| carry-forward | valid — a level persists | invalid — a rate needs both endpoints |
| cohort (D3) | every narrative with any stats (~22k) | only narratives visited on `calc_date` |
| unmeasured narrative | keep — its size is still known | exclude — its rate is uncomputable |

---

## Why the current system produces noise

The problem, from the 2026-07-16 production run: narratives carry a badge when nothing is
happening. 289 narratives were badged `early_surge` at acceleration percentile exactly 0.0 —
a combination the classifier cannot even emit. That particular case was the `calculated_at`
staleness bug (`f552c0d`), but fixing staleness only removes stale badges; the *design* still
promotes noise, and it does so by breaking the two core decisions.

### C1 broken: the two cohort predicates are exactly backwards

| axis | C1 requires | the code actually does |
|---|---|---|
| composite (state) | measured **at least once** (~22k) | `videos.updated_at` within 24h of `NOW()` → **visited today** (~2–3k) |
| acceleration (rate) | measured **today** | any snapshot on or before `calc_date` → **ever measured** (~22k) |

Each axis wears the other's cohort rule, and that single swap produces both headline
pathologies:

- **Composite's pool is volatile and scraper-driven** (3116 → 2078 narratives overnight)
  because it is filtered for a freshness a *level* does not need. Its pool comes from
  `get_prevalent_narratives_summary` — a *dashboard pagination query*
  (`core/narratives/repo.py:1595`, "optimized for dashboard display"), predicate
  `videos.updated_at >= NOW() - 24h`, ordered by how many of the narrative's videos were
  touched. "Prevalent" means *recently and heavily scraped*, not *large* or *important*.
- **Acceleration is 93% zeros** because it is ranked over ~20k narratives nobody measured
  that day. Ties share the lowest rank, so all of them land at percentile 0.0 and the
  smallest non-zero percentile is 0.9296 — the axis carries one bit. Every horizontal
  boundary (0.4, 0.5, 0.8) selects the identical set.

### C2 broken: change lives on the wrong axis, and the rate axis cancels its own growth

**The composite/level axis smuggles in a growth term.** Today's composite is 20%
`velocity_score = views_gained_last_2_days / total_views` — a *change* measure sitting on the
*state* axis. For a narrative that was not revisited the carried-forward delta is zero, so
velocity reads 0 for dormant narratives; expanding composite's pool to the full corpus (D3)
would put ~19k narratives at velocity 0 and turn the term binary. A change measure cannot
live on the level axis. It is a category error, and it is what makes the axes *look*
independent when they are not.

**The rate axis is built so it cancels the growth it is meant to measure.** *This is the
largest defect in the document.*

`engagement_score = (likes + 5×comments) / views` is a **rate**. Differencing a rate against
a *level's* growth means that whenever views grow faster than engagement — which is exactly
what spread looks like, since views outpace likes — `change_engagement` goes **negative**.
And at weight 0.40 against views' 0.25, it wins:

```
likes flat, views grow by g:
  change_engagement = 1/(1+g) − 1 = −g/(1+g)  ≈ −g
  acceleration      = 0.40(−g) + 0.25(g) = −0.15g   →  negative for ANY g  →  max(0,…) → 0
```

| with likes flat | chg_eng | chg_views | acceleration |
|---|---|---|---|
| +10% views | −0.091 | 0.100 | **0** |
| +50% views | −0.333 | 0.500 | **0** |
| doubles views | −0.500 | 1.000 | 0.05 |

A narrative can grow **50% in a day and score exactly zero**. Measured on 2026-07-16 data
(n=2237): **679 of the 779 zeros are narratives that grew in views and were floored** — only
86 are genuinely flat; one erased grower grew 35.8% in a day. Spearman(`change_views`,
`change_engagement`) = **−0.427**: the term is substantially a negated copy of the term it is
added to. Consequence — **the acceleration axis has never measured growth**; it measures
video *acquisition*, because `change_video_count` is the only term that cannot be cancelled.

This one line retroactively explains the rest: "the median mover grew 0.46%, so movers are
noise" — the real movers were floored out *before* the mover set was measured. The huge zero
block, the binary axis, the scraper-discovery narratives owning the top: all downstream of
it. D6 fixes it.

### The mechanism behind C1: `video_stats` is a change log, not a measurement log

`update_video` (`core/videos/repo.py:103`) bumps `videos.updated_at` **unconditionally** on
every scrape, but writes a `video_stats` row **only `if stats_changed`**. So:

- `videos.updated_at` is the **visit record** — "we looked."
- `video_stats` is a **change log** — "something moved."

The absence of a `video_stats` row on a day therefore fuses two distinct states — *we did not
look* and *we looked and nothing moved* — which are precisely the two states C1 must keep
apart. This is why C1's acceleration predicate reads `videos.updated_at`, not `video_stats`
(see D0's mechanism note). Three states — unmeasured, flat, declining — must not arrive at
the ranking as one number.

---

## The decisions, as consequences

### D0 — Rank only what we measured  *(this **is** C1)*

Composite is a level, ranked over every narrative measured **at least once**. Acceleration is
a rate, ranked only over narratives measured **on `calc_date`**. The two pools differ because
the two axes require different evidence — not as a design choice needing its own defence.

**The predicates, and which column answers which question.**

- **Acceleration:** ≥1 of the narrative's videos was **provably visited** on `calc_date`.
  This is a *union of two kinds of evidence*, because neither alone suffices:

  ```
  visited(video, D)  ⟸  ∃ video_stats row on D          -- permanent, append-only, never decays
                     ∨  videos.updated_at::date = D      -- ephemeral, overwritten by the next scrape
  ```

  A `video_stats` row **proves** a visit and is permanent; `updated_at` catches the visits
  that changed nothing but survives only until the next scrape overwrites it. The union
  misses exactly one case: *visited, nothing changed, and re-visited since.*
- **Composite:** ≥1 video with any `video_stats` row. Since `create_video` always writes one,
  this reduces in practice to *the narrative has at least one video.*
- **Birth is not acceleration.** The narrative must also have had videos **the day before**.
  One with none was *created* on `calc_date`; the inner join to the previous day's state
  excludes it. The bright line sits at the *narrative*, not the video — a narrative that
  merely gained its first-ever measured video still accelerates. *(Settled as O5.5.)*

**Visited-but-flat is not unmeasured.** A narrative that was visited but did **not** grow
stays in the acceleration cohort, ranked at zero — that is required, not a compromise:
`consolidated` means big *and flat*, so a large narrative that genuinely stopped growing has
`accel = 0` by definition. Excluding all zeros would drop those narratives out of the taxonomy.
What we exclude is the *unmeasured*, which merely wear a zero because the carried-forward
snapshot is identical on both days. A tie-block of honest zeros remains at the bottom of the
axis, sized by however many visited narratives are genuinely flat; its size is unknown and
only measurable going forward (it needs the visit predicate, which is forward-only).

**Accepted cost — the cohort decays, unevenly.** The `updated_at` half holds only the *last*
visit, so it is overwritten by the next scrape:

| | evidence | on a past `calc_date` |
|---|---|---|
| **movers** (≥1 video changed that day) | `video_stats` row — append-only | **recovered in full** |
| **flat** (visited, nothing changed) | `updated_at` only | **erodes** once every video is re-scraped |

The decay eats the honest zeros first — exactly the population D0 keeps for `consolidated`,
and exactly the tie block that D1's boundaries are drawn against. So: raw `accel` for movers
is inspectable historically, but **`accel_pct` is not** (losing zeros shrinks the denominator, every
survivor ranks too high), boundaries **cannot** be chosen from a historical run, and a weights
change cannot be re-scored against history — only new days can. There is also a **day-boundary
race**: the job scores yesterday but reads `updated_at` today, so any *unchanged* video
re-visited between midnight and the moment the acceleration phase reads is dropped. Phase 1
runs first and D3 makes it ~11× longer, so the window is however long phase 1 takes.
**Cheap mitigation: capture the visit set as the first statement of the run, before phase 1.**
Do this even if nothing else changes.

A `video_visits(video_id, visited_at)` table would remove every one of these for the price of
a migration, by making the flat half as durable as the mover half. Deferred, not rejected —
revisit if O6 shows the sweep clusters near midnight, or the first time an inability to
re-score history blocks a decision. **It only starts paying from the day it ships.**

### D1 — Four labels, and the regions they occupy  *(from C2)*

The two axes give four corners of meaning: small-but-climbing is `early_surge`,
big-and-climbing is `viral`, big-and-flat is `consolidated`, and the broad middle is
`trending`. `consolidated` is the genuinely new idea — the old taxonomy had nowhere to put a
large narrative that stopped growing, so `viral` absorbed it and a "plateaued-but-popular"
branch papered over the gap. Replaces the current `viral`/`early_surge`/`alert`/`watch`/`none`.

The regions are **rectangles, not quadrants**, read off the source diagram (`Captura de
pantalla 2026-07-16 122522 (2).png`) and evaluated **in this order** — `viral` is carved out
of the `trending` box:

```
viral         composite_pct >= 0.80  AND  accel_pct >= 0.80
early_surge   composite_pct <= 0.40  AND  accel_pct >= 0.50
consolidated  composite_pct >= 0.50  AND  accel_pct <= 0.40
trending      composite_pct >= 0.40  AND  accel_pct >= 0.40
(no badge)    everything else
```

Two consequences a quadrant reading gets wrong. **`early_surge` is for SMALL narratives
only** (`composite <= 0.40`) — not "anything climbing"; a large climber is `viral` if it
clears 0.80 on both axes, `trending` otherwise, and that cap is the whole point of the label.
And **the four do not tile the plane**: small *and* flat, bottom-left, gets no badge.

**Measured** (2026-07-16, D6 weights, n=2237):

| label | n | share | gained videos |
|---|---|---|---|
| `viral` | 120 | 5.4% | 92% |
| `early_surge` | 213 | 9.5% | 64% |
| `consolidated` | 681 | 30.4% | 0% |
| `trending` | 958 | 42.8% | — |
| *(no badge)* | 265 | 11.8% | — |

1972 of 2237 badged (88%). `consolidated` has a median `accel` of 0.00001 — genuinely flat,
which is what it should mean, and only true since D6's reweighting.

Every edge is axis-aligned, which is what makes D3's differing pools safe. The diagram's one
apparent exception — the grey diagonal `f`, J(0, 0.5) → L(0.5, 0), i.e. `composite + accel =
0.5` — is a construction line: every badged region already forces `composite + accel >= 0.5`,
so the triangle below `f` lies entirely inside the no-badge region and cuts nothing
(confirmed: 62 narratives fall below `f`, all 62 already unbadged, zero exceptions).

*Note on shared edges:* `trending`'s floors (0.40/0.40) coincide with `early_surge`'s
composite ceiling and `consolidated`'s acceleration ceiling, read from the diagram's adjacent
points O and P. If they are in fact distinct, a narrow unbadged band would open between the
regions. The simulation artifact exposes all eight edges separately so this can be
re-examined without touching the query.

### D2 — Axes are percentiles, not absolute values  *(from C1)*

The scraper's coverage is imperfect and evolving, so an absolute magnitude cannot be trusted
to mean the same thing over time — an absolute bar drifts with coverage and needs continual
retuning. Percentiles are self-calibrating, and the labels are relative claims by nature
("the most viral" is top-N, not a threshold). **Accepted cost:** a percentile fires for a
fixed fraction of the corpus, so alert volume cannot fall on a quiet day — which is exactly
why C1 matters, since ranking cannot manufacture signal where there is none.

### D3 — The two axes rank over deliberately different pools  *(from C1 + C2)*

Composite requires one measurement ever; acceleration requires one today; so the pools
differ. Composite is ranked over **every narrative with video stats**, carry-forward.
Acceleration is ranked only over narratives **visited on `calc_date`** (`videos.updated_at`,
per D0's mechanism — *not* `video_stats`).

**Second-order benefit:** if composite were ranked only among today's revisited narratives, a
narrative's "how big am I" rank would swing day to day depending on *who else happened to get
scraped* — scraper noise injected straight into the level axis. That is the observed 3116 →
2078 turnover. Ranking over all narratives gives a stable, scraper-independent answer, and
adopting it deletes the dashboard-pagination query (and its `NOW()` anchor and wrong-table
filter) from the pipeline entirely.

**Why mixing pools is safe:** every region boundary is axis-aligned (`composite >= 0.8`,
`accel >= 0.5`) — a percentile is only ever compared to a constant on its own axis, never to
the other axis's percentile, so the two pools never need a shared denominator (D1 proves the
one apparent exception, the grey diagonal, is a construction line).

**Implementation cost:** ~22.4k narratives vs ~2.1k — **~11×**. Phase 1 is per-narrative
(each opens two nested transactions and issues three inserts,
`core/narratives/service.py:444-536`) — ~88k queries across ~44k transactions at 22k.
**Phase 1 must become a bulk query before D3 is shippable.**

### D4 — Acceleration is growth *per elapsed day*  *(from C2)*

A rate is per unit time or it is not a rate. Today acceleration is `(current − previous) /
previous` with **no division by elapsed time**, so a video last scraped four days ago
contributes four days of growth as if it happened in one — and ranks high for having gone
*unmeasured*, not for growing. Given D2, this matters more than usual: percentiles sort by
measurement error rather than washing it out, and this error concentrates at the top.
Normalise by each video's own elapsed gap. Prior art: `feat/unify-virality-windows`
implements per-day log growth divided by each video's gap — salvage rather than rewrite.

**With a staleness cap of 7–14 days on the baseline.** Per-day normalisation can otherwise
launder an old surge into today's rate — a video last seen three months ago contributes a
quarter's growth, divided down but still anchored to a baseline that means nothing now.
Measured 2026-07-15: 96.8% of the cohort is within 7 days of its baseline, 99.2% within 14, so
a cap in that range removes the laundering case for ~3% of the cohort and touches nothing
else. Cheap; take it. *(Settled as O5.4.)*

### D5 — `viral` requires both axes at once  *(from C2)*

`viral` means high composite **and** high acceleration — big *and* still climbing, not a
statement about size alone. A narrative that is large but no longer growing is precisely
`consolidated`; keeping `viral` a conjunction is what gives `consolidated` a reason to exist.
This is only coherent because unmeasured narratives are excluded (D0): on the current
definition the conjunction is nearly empty, which would force `viral` to collapse into a
size-only band.

The conjunction is *rare by construction* because the axes are mildly anti-correlated (large
narratives cannot post large percentage growth — real signal, per C2's no-size-adjustment
note). That anti-correlation is measured **among movers only**; the full-cohort Spearman is
not a usable number, since half the population is tied at zero and the figure sign-flips
across a single day (−0.173 → +0.198) — it measures the scraper's schedule, not narratives.

The rarity is intended, not a defect. With the D6 composite, top-20% × top-20% lands ~26
narratives on 2026-07-16 (perfect independence would give ~55); top-10% × top-10% lands ~0.
The exact boundary is D1's.

### D6 — All change-measurement moves to the acceleration axis  *(from C2)*

```
composite    = engagement_pct×0.625 + reach_pct×0.375     (velocity dropped)
acceleration = 0.10×change_engagement + 0.35×change_video_count + 0.55×change_views
                    (was 0.40)                                         (was 0.25)
```

**One decision, two halves — neither coherent alone.** Composite evicts its growth term;
acceleration is reweighted so it can actually carry growth. Evicting velocity while
acceleration's engagement weight still cancels growth would leave the system measuring growth
on *neither* axis. The result is two axes that mean cleanly different things — **composite is
pure state, acceleration is pure rate** — which is C2.

**Half one — composite drops `velocity`.** Velocity is a change measure; C2 forbids it on the
state axis; the current double-counting of growth is what makes the axes *look* independent
(see the C2-broken section). The 0.625/0.375 weights are the current 0.50/0.30 rescaled
proportionally — the exact split is a free choice, not a finding.

**Half two — acceleration is reweighted; engagement is demoted to a modifier.** Engagement
*change* is a real quality signal (a narrative whose engagement is rising should rank above
one whose engagement is falling), and the rate semantics are the right way to ask it — but a
modifier that outweighs the thing it modifies is not a modifier. The only hard constraint is
`w_engagement < w_views` (0.40 > 0.25 is what produced the defect). 0.10/0.35/0.55 is the
proposal; the split is otherwise taste.

**Measured effect** — both weightings run against 2026-07-16, n=2237, identical cohort, every
`change_*` component byte-identical (a clean A/B of the weights alone):

| | 0.40/0.35/0.25 | 0.10/0.35/0.55 |
|---|---|---|
| scored exactly 0 | 771 | **150** |
| of those, genuinely flat | 86 | **86** |
| real growers erased by the floor | 679 | **43** |
| best `accel` reachable by real growth | 0.840 | **1.453** |

**636 narratives that genuinely grew are recovered.** The zero block shrinks to 86 truly flat
plus 43 growing at ~1e-6. That is D0 working as intended for the first time, and it is what
lets `consolidated` finally mean "big and flat" rather than "grew, but the rate fell."

**What it does not fix, deliberately:** badge *volume* (2237 either way — `accel_pct >= 0.50`
selects half the cohort whatever you rank; volume is set by boundaries, not weights), and
**acquisition dominance** — under D1's geometry, **247 of the 333 firing narratives** (`viral` +
`early_surge`, 74% of the alert stream, 92% of `viral`) got there by gaining videos. That is
`change_video_count` doing its job: a narrative that gained a 750k-view video *did* grow. Per
C2's no-new-vs-old rule, we count it and do not second-guess it.

**Supersedes** `feat/unify-virality-windows`'s reweighting of `change_video_count`
(0.35 → 0.10), which targeted the right symptom (noise at the top) from the wrong term — the
video-count weight was never the problem, the engagement weight was.

---

## Open questions

Five remain, none of them alert conditions. The numbering is not contiguous: closed questions
have been deleted and their content moved into the decision that now owns it — **O2/O3** into
D1 (volume and boundaries are the region geometry), **O5.4** into D4 (the 7–14 day baseline
cap), **O5.5** into D0 (birth is not acceleration), **O5.1/O5.2** into C2's rejected-adjustments
list (the coverage-bias premise was false, so there is nothing to threshold). Gaps in the
numbering mean answered, not forgotten.

| | question | state |
|---|---|---|
| **O6** | the scraper's revisit strategy | **the critical path** — not answerable from this database |
| **O5.3** | denominator scope | gated on O6 |
| **O5.6** | aggregation | the same decision as O5.3 |
| **O7** | staleness bound for composite | needs one query |
| **O8** | should `max(0, …)` stay | de-escalated by D6 |
| **O4** | frontend / API impact | implementation coordination |

**O6 — The scraper's revisit strategy.** The highest-value open question, not answerable from
this database. **(a) Does the revisit strategy prioritise active videos?** — decides O5.3/O5.6
and explains why coverage dilution is undetectable; until known, acceleration is
*uninterpretable* rather than merely noisy. **(b) When does the daily sweep run?** — D0's
acceleration predicate reads `updated_at` today for a `calc_date` of yesterday, so a sweep
near midnight could drop a large, biased slice of the cohort and force the `video_visits`
log. A question for whoever owns the scraper; the real critical path.

**O5.3 — Denominator scope.** `Δ(refreshed) / prev(all)` (today's; biased down by 1−coverage)
vs `Δ(refreshed) / prev(refreshed)` (unbiased if refreshed videos are representative, biased
*up* if the revisit strategy prioritises active videos). Which is right depends on O6.

**O5.6 — Aggregation.** D4 wants each video divided by its own gap, but acceleration is
defined on narrative-level sums. The clean form avoids averaging ratios:
`narrative_daily_growth = Σ(video's daily view gain) / Σ(baseline views of those videos)` —
numerator and denominator cover the same videos, which is O5.3's second option, so O5.3 and
O5.6 are one decision.

**O7 — Staleness bound for composite.** D0 admits any narrative "measured at least once" to
the composite pool with no upper bound on age. A narrative last measured six months ago has a
well-defined but six-month-old state — should it rank against one measured yesterday? The
composite-side analogue of D4's baseline cap. Needs one query (the staleness distribution of
the never-visited ~19k is unknown).

**O8 — Should `max(0, …)` stay?** *De-escalated by D6* — the floor now touches 43 narratives
(all growing ~1e-6), not 679, so it is no longer urgent; it was never the disease, only what
made the `change_engagement` defect fatal. It still merges decliners with genuinely-flat
narratives, which is the class of merge C1 forbids — and under D0 a flat narrative *should*
rank above a shrinking one (`consolidated` above a loser, not tied with it). Decide it
explicitly rather than inheriting it (`core/narratives/service.py:598-603`).

**O4 — Frontend and API impact.** `narrative_alert_level` is a Postgres enum and the labels
are user-facing. Renaming `alert`/`watch` to `trending`/`consolidated` needs a migration and
coordination with consumers.

---

## Dependencies

- `f552c0d` (`fix/indicators-calc-date-stamp`) — must land first; without it the classifier
  reads the previous run's indicators.
- `feat/unify-virality-windows` — contains the per-day growth work D4 needs.

## Validation still owed

**`viral` must be shown to fire at all.** On 2026-07-15, top-10% × top-10% yields 0 and
top-20% × top-20% yields 26. C2 accepts that `viral` is rare, but *structurally always zero*
would be a dead label. A genuinely viral narrative — large and doubling — would rank top on
both axes and fire, so the 0 plausibly means "nothing went viral that day." That is an
inference from one day. **Before shipping, confirm over ~2 weeks that `viral` fires sometimes
at the chosen boundaries.**

**Composite's expanded pool (D3) has not been simulated.** "Measured at least once" is
answerable from existing data — every composite percentile in this document is a rank out of
~3116 and will move under D3, since adding ~19k dormant narratives changes where the active
ones land, including the `viral` grid. Worth doing; it needs no new instrumentation.
Acceleration's half simulates only *partially* (movers survive on a past day, `accel_pct` and
the tie block do not — see D0's decay note), and D4's per-day normalisation has not been
simulated at all.

