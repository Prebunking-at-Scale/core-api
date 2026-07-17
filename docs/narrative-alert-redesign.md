# Narrative alert system — redesign

**Status:** design in progress. Decisions below are settled; open questions at the end
are not. No implementation yet.

**Revised 2026-07-17.** D0 was added and now derives D3, D5 and D7, which were previously
argued independently. Several measurements in this document were retracted in the same
pass — see *Retractions* under "Validation still owed" before citing any number here.

**Branch:** `redesign/narrative-alert-system`, based on `fix/indicators-calc-date-stamp`
(the classifier on `main` reads the previous run's indicators, so any evaluation done
there measures the bug rather than the design).

---

## Problem

Alerts are noise: narratives carry a badge when nothing is happening to them. The
clearest symptom, from the 2026-07-16 production run — 289 narratives badged
`early_surge` while sitting at acceleration percentile exactly 0.0, a combination the
classifier cannot emit. That particular case was the `calculated_at` bug (fixed in
`f552c0d`), but fixing it only removes the staleness. The underlying design still
promotes noise, for reasons measured below.

## How the indicators are computed today

### Composite — four steps

Per narrative, from its **all-time cumulative** totals (`service.py:444-508`):

```
engagement_score = (likes×1 + comments×5) / views          # a rate
reach_score      = min(views / avg_views_all, 10) / 10     # size, capped at 10× average
velocity_score   = views_gained_last_2_days / total_views  # a fraction of its own total
```

Each of the three is then `PERCENT_RANK`-ed across the cohort
(`get_all_virality_percentiles_for_date`), blended (`service.py:542`):

```
composite = engagement_pct×0.50 + reach_pct×0.30 + velocity_pct×0.20
```

…and the blend is `PERCENT_RANK`-ed **again** by `_attach_percentiles` to give
`composite_pct`.

### Acceleration — one step

Carry-forward totals as-of `calc_date` vs as-of `calc_date − 1`
(`service.py:559-624`, `get_bulk_narrative_stats_comparison`):

```
change_engagement  = (eng_today   − eng_prev)   / eng_prev     capped at 5.0
change_video_count = (vc_today    − vc_prev)    / vc_prev      capped at 5.0
change_views       = (views_today − views_prev) / views_prev   capped at 5.0

acceleration = max(0, 0.40×change_engagement
                    + 0.35×change_video_count
                    + 0.25×change_views)
```

Then `PERCENT_RANK`-ed over its cohort.

## Defects in the current computation

Beyond the cohort mismatch (D3) and the missing time normalisation (D4):

**The two axes describe different moments.** Acceleration is computed *for* `calc_date`.
Phase 1 ignores `calc_date` entirely — `get_narrative_stats` takes no date parameter and
returns all-time-to-now, and the velocity window is anchored to `NOW() - 2 days`
(`repo.py:1781`). `calc_date` only sets the row's timestamp. So composite describes *now*
while acceleration describes *yesterday*, and the classifier plots one against the other.
Corollary: the `--calc-date` backfill flag is broken for composite — it computes today's
numbers and stamps them with the requested date.

**Composite is a blend of ranks, then ranked again.** Three uniform[0,1] variables
averaged gives a bell curve by CLT — hence the observed p50 = 0.51, max = 0.91, and the
fact that nothing reaches 0 or 1. The second ranking is why `composite_pct` is uniform
while `composite` is not. Whether the double ranking is intended is unresolved.

**`video_stats` is a change log, not a measurement log.** `update_video`
(`core/videos/repo.py:103`) bumps `videos.updated_at` unconditionally but writes a
`video_stats` row only `if stats_changed` — where `stats_changed` also includes
`channel_followers`. So a video re-scraped with unmoved numbers leaves **no trace in
`video_stats`**, and the absence of a row on a day means *either* nobody looked *or*
somebody looked and nothing moved. Carry-forward then renders both as an identical
`accel = 0`, and `max(0, ...)` folds declines into the same value. Three distinct states —
unmeasured, flat, declining — arrive at the ranking as one number. **This is the root
defect; D0 exists to fix it.**

> ~~**Acceleration is biased downward by partial coverage.**~~ **Retracted 2026-07-17.**
> The claim was that the numerator covers only refreshed videos while the denominator
> covers all of them, so a narrative measured through 5% of its videos reports ~5% of its
> actual growth — and that this "may invalidate the 'movers are noise' reading", since
> 0.46% measured would be ~9% actual.
>
> **The premise is largely wrong.** Carried-forward videos contribute zero to the
> numerator — true — but that only *understates* growth if those videos actually grew.
> Most have no `video_stats` row precisely **because they did not change**, so zero is
> their correct contribution, not a missing one. The residual bias comes only from videos
> genuinely not visited, which `video_stats` cannot identify at all (see the change-log
> defect above).
>
> **Measured, and the effect is absent.** Across a hundredfold range of coverage, median
> measured acceleration is flat: 0.00068 at <1% coverage vs 0.00080 at ≥50% — despite
> low-coverage narratives also being the large ones (coverage correlates −0.61 with size)
> and large narratives growing slower in percentage terms, both of which should drag the
> top row down. Correcting per narrative rather than dividing median-by-median moves the
> mover median from 0.46% to **0.44%**. The "movers are noise" reading **survives**.
> See O5.1, also resolved.

**Reach mixes data sources.** Numerator from `video_stats` snapshots; denominator
(`get_average_views_for_all_narratives`) from the `videos` table's own `views` column
(`repo.py:1722`). Two separately-maintained sources divided by each other.

**`claim_narratives` has no timestamp.** The table is `(claim_id, narrative_id)` with no
`created_at` (`migrations/7`), so "which videos did this narrative have yesterday" is
unanswerable. `video_claims` does carry `created_at`, but claim creation is not the same
event as narrative linking. This forecloses distinguishing "the narrative gained a
video" from "the scraper discovered an old video" — see O5.5.

**35% of acceleration tracks the scraper — and it dominates the top of the axis.**
`change_video_count` compares videos *known* as of each date, and under carry-forward
"known" means "has any snapshot on or before D". A video first scraped on D counts as new
growth, so a narrative that gained nothing but got swept scores acceleration.
`feat/unify-virality-windows` reweighted this term from 0.35 to 0.10 for exactly this
reason.

This is **the same defect as the zeros, one term over**: absence of a record read as a
fact about the world. We never observed the video not existing; we had not looked. D0
forbids it by identical logic — a change needs two observations, and a video first seen
on `calc_date` has one.

Measured on 2026-07-15, the effect is not marginal — **it is the whole top of the axis**:

| | n | median accel | max |
|---|---|---|---|
| movers with a refreshed baselined video | 807 | 0.0006 | 2.09 |
| movers with **no** baselined refresh (new videos only) | 576 | **0.0296** | **3.37** |

Fifty times the median, and the cohort maximum is one of them. The five largest have 1–3
baselined videos and a few hundred baseline views — tiny narratives that got swept. Since
alerts fire from the top of this axis, this population *is* the alert stream. See O5.5,
which D0 promotes from an open option to a forced choice.

**Data quality:** 2 of 3116 narratives have more likes than views (438 likes on 16
views; 23k likes on 13k views). They take the top two engagement ranks
(`engagement_pct` 1.0 and 0.9997) but their reach and velocity are ~0, so composite
lands mid-axis and they are not badged. Minor, but worth a fix upstream.

### Checked and sound: engagement_score

`engagement_score` being a ratio raised the concern that it measures "few views" rather
than "engaging", since a video with 1 view and 1 comment scores 6.0. **Measured, and
false.** Spearman against views is −0.086, and the median is flat across four orders of
magnitude of size (0.071 at <100 views, 0.048 at >1M). Only 9 narratives in 3116 have
under 100 views — the cohort is overwhelmingly large narratives, so the small-denominator
case barely occurs. Composite also uses engagement's *percentile*, so outlier magnitudes
distort nothing. Engagement belongs on the composite axis and is measured reasonably.

## What we measured

> ⚠ **Some figures in this section were retracted on 2026-07-17** — notably the −0.173
> correlation and everything computed on the `accel > 0` "mover" cohort. The structural
> findings (the pool mismatch, the binary axis, composite's shape) replicated on a second
> day and stand. **Read *Retractions* at the end before citing anything here.**

All figures from the 2026-07-16 00:05 run (scoring 2026-07-15), over the 3116
narratives that have **both** indicators and are therefore classifiable.

Caveats worth carrying: this is **one day's data**, and the `alert_level` column in that
snapshot is unreliable (stale badges from the `calculated_at` bug). The *indicator
values* are fresh and are what the numbers below rest on.

### The two axes are ranked over different populations

Recovered empirically from the granularity of the stored percentiles
(`_percent_ranks` divides by `N-1`, so the smallest gap reveals `N`):

| Axis | Implied cohort |
|---|---|
| `composite_pct` | 3116 — the classifiable set |
| `accel_pct` | 22321 — every narrative with any video stats |

`composite_virality` is computed only for the prevalent set;
`acceleration_rate` is computed for everything `get_bulk_narrative_stats_comparison`
returns. Both then call the same `_attach_percentiles`, on very different cohorts. The
result is that a rank-out-of-3116 and a rank-out-of-22321 are compared as if they were
the same kind of number.

### The acceleration axis is effectively binary

93.0% of the 22321-narrative acceleration pool has a value of exactly 0. Ties share the
lowest rank, so all of them sit at percentile 0.0 and the smallest non-zero percentile
observed is **0.9296**. Nothing exists between the two.

Consequence: every horizontal boundary in the proposed geometry — 0.4, 0.5, 0.8 — is
*the same cut*. Each selects the identical 1384 narratives, which is exactly the set
with `accel > 0`. The axis carries one bit.

Ranking over the matched 3116 cohort improves this (usable range goes from the top 7%
to the top 44%) but does not fix it: 1732 narratives (55.6%) are still exactly zero, so
the bottom half of the axis stays empty and 0.4 and 0.5 remain the same cut.

### Axis distributions (n=3116)

| | min | p10 | p50 | p90 | p99 | max |
|---|---|---|---|---|---|---|
| `composite` | 0.0202 | 0.2609 | 0.5098 | 0.7080 | 0.8087 | 0.9105 |
| `accel` | 0.0000 | 0.0000 | 0.0000 | 0.0372 | 0.6779 | 3.3667 |

Composite is well spread and usable as-is. Acceleration is not: over half the cohort is
zero, and among the 1384 that are non-zero, the median grew **0.46%** and p25 grew
**0.029%**.

### The axes are anti-correlated — *disputed as of 2026-07-17*

> **Status of the three numbers in this section:**
>
> - **Full cohort, −0.173 — RETRACTED.** It is **+0.198** on 2026-07-16. See *Retractions*.
> - **Movers with velocity, −0.015 — holds.** −0.080 on 2026-07-16. Both ≈ 0.
> - **Movers without velocity (D7), −0.172 — suspect, not retracted.** It was computed on
>   the `accel > 0` cohort, which overlaps D0's real cohort by only 58%, and it could not
>   be re-checked on 2026-07-16 because rebuilding the D7 composite needs the
>   engagement/reach/velocity component scores and that day's pull carries only the blend.
>   **Re-run the component pull for a second day before D7 or D8 leans on it.**
>
> The reasoning below is sound; the specific magnitude is what is in doubt.

Over the full cohort, Spearman between the axes is **−0.173**. Restricted to the 1384
narratives that actually moved (the cohort D5 leaves us), it is **−0.015** — apparently
independent.

**That apparent independence is an artifact and must not be relied on.** The current
composite is 20% `velocity_score`, which is itself a growth-rate measure
(`views_gained_last_2_days / total_views`). Composite looked independent of acceleration
because composite *contained a growth term*. Rebuild composite without velocity (D7) and
the real relationship returns:

| composite used | Spearman vs accel, among movers |
|---|---|
| with velocity (current) | −0.015 |
| **without velocity (D7)** | **−0.172** |

The reconstruction was verified: rebuilding the current composite from the raw
engagement/reach/velocity scores reproduces the stored values to a max absolute error of
0.000000, so this is not a methodology artifact.

The honest structural fact: **large narratives cannot post large percentage growth**,
because the denominator forbids it. Size also proxies for lifecycle stage — big
narratives are usually old, and old content grows slowly in percentage terms. Under D7
and D8 this is accepted as signal rather than engineered away (see D8).

### What `viral` costs, given the anti-correlation

With the D7 composite, within the mover cohort:

| | acc top 20% | acc top 10% | acc top 5% |
|---|---|---|---|
| **comp top 20%** | **26** | 7 | 2 |
| **comp top 10%** | 4 | 0 | 0 |
| **comp top 5%** | 1 | 0 | 0 |

(For reference, perfect independence at top-20% × top-20% would give ~55; we get 26.)

`viral` is therefore rare, and D8 accepts that deliberately. The boundary choice is O2 —
top-20% × top-20% yields 26 on this day, top-10% × top-10% yields 0.

---

## Decisions

### D0 — Rank only what we measured

**Acceleration ranks only narratives we measured on `calc_date`. Composite ranks only
narratives we measured at least once.**

Composite is a *level*; acceleration is a *rate*. A level is known from any past
measurement — not re-scraping a narrative today does not make its size unknown. A rate
requires two observations bracketing the period; without a measurement on the day it is
not zero, it is **uncomputable**, and a narrative we did not look at must not be ranked
as the least-accelerating one.

The two axes therefore rank over different pools. That is a consequence of the two axes
requiring different evidence, not a design choice needing its own justification. D3, D5
and D7 all follow from this rule and are stated below as its consequences.

**The current system has the two criteria exactly backwards:**

| axis | D0 requires | the code actually does |
|---|---|---|
| composite | measured **at least once** (~22k) | `videos.updated_at` within 24h of `NOW()` → **visited today** (~2–3k) |
| acceleration | measured **today** | any snapshot on or before `calc_date` → **ever measured** (~22k) |

Each axis wears the other's cohort rule, and that single swap produces both headline
pathologies. Composite's cohort is volatile and scraper-driven (3116 → 2078 overnight)
because it is filtered for freshness it does not need. Acceleration is 93% zeros because
it is computed over ~20k narratives nobody measured that day. One mistake, seen from two
ends.

**Mechanism — which column answers which question.**

`videos.updated_at` is the **visit record**: `update_video` (`core/videos/repo.py:103`)
bumps it unconditionally on every scrape.

`video_stats` is **not** a measurement log, it is a **change log**: the same function
writes a stats row only `if stats_changed`. So the absence of a `video_stats` row on a
day means *either* we did not look *or* we looked and nothing moved — precisely the two
states D0 forbids fusing. (`get_bulk_narrative_stats_comparison`'s own docstring says as
much — "unchanged data is not re-recorded" — the clause was simply never connected to
this decision.)

The predicates are therefore:

- **Acceleration:** ≥1 of the narrative's videos was **provably visited** on `calc_date` —
  which is a *union of two kinds of evidence*, because neither alone is sufficient:

  ```
  visited(video, D)  ⟸  ∃ video_stats row on D          -- permanent, never decays
                     ∨  videos.updated_at::date = D      -- ephemeral, overwritten by the next scrape
  ```

  A `video_stats` row **proves** a visit (`create_video` always writes one;
  `update_video` writes one when the numbers moved), and `video_stats` is append-only, so
  that evidence is as good in six months as it is today. `updated_at` catches the visits
  that changed nothing — but only until the next scrape overwrites it.

  The union misses exactly one case: *visited, nothing changed, and re-visited since*.
- **Composite:** ≥1 video with any `video_stats` row. Since `create_video` always writes
  one, this reduces in practice to *the narrative has at least one video*.

Using `updated_at::date = calc_date` also removes the sliding-`NOW()` defect: the current
`updated_at >= NOW() - 24h` re-evaluates `NOW()` per batch transaction, so the cohort
moves underneath the pagination mid-run. A fixed date does not.

**Accepted cost — the cohort decays, and it decays unevenly.** The `updated_at` half of
the predicate holds only the *last* visit, so it is overwritten by the next scrape. That
does not make a past day uncomputable — the `video_stats` half is permanent — but it
splits the cohort into a durable part and a perishable one:

| | evidence | on a past `calc_date` |
|---|---|---|
| **movers** (≥1 video changed that day) | `video_stats` row — append-only | **recovered in full** |
| **flat** (visited, nothing changed) | `updated_at` only | **erodes**; gone once every video is re-scraped |

**The decay is not neutral — it eats the honest zeros first**, which are precisely what
D5 keeps for `consolidated` and precisely what sizes the tie block O3 needs. So:

- **Raw `accel` for movers survives on a past day.** The mover list is inspectable
  historically; that is more than previously stated here.
- **`accel_pct` does not.** Losing the zeros shrinks the denominator, so every surviving
  narrative ranks too high. **Boundaries cannot be chosen from a historical run.**
- **No re-scoring.** A weights change cannot be re-evaluated against history at the
  percentile level; only new days can. Every tuning iteration costs a wait.
- **A missed or failed run loses that day's flat population permanently.**
- **A race at the day boundary.** The job scores yesterday but reads `updated_at` today,
  so any *unchanged* video re-visited between midnight and the moment the acceleration
  phase reads is dropped. At a 00:05 start that is a 5-minute window — but **phase 1 runs
  first, and D3 makes phase 1 ~11× longer**, so the real window is however long phase 1
  takes. **Mitigation, and it is cheap: capture the visit set as the first statement of
  the run, before phase 1**, which bounds the loss to the job's start offset regardless of
  how slow phase 1 gets. Do this even if nothing else here changes.

A `video_visits(video_id, visited_at)` table would remove every one of these for the price
of a migration, by making the flat half as durable as the mover half. Deferred, not
rejected — revisit if O6 shows the sweep clusters near midnight, or the first time an
inability to re-score history blocks a decision. **It only starts paying from the day it
ships, so the decision has a clock on it.**

### D1 — Four labels

`early_surge`, `trending`, `viral`, `consolidated`, replacing the current
`viral` / `early_surge` / `alert` / `watch` / `none`.

The quadrant model these express: small-but-climbing is early surge, big-and-climbing is
viral, big-and-flat is consolidated, and trending is the broad middle. `consolidated` is
the genuinely new idea — the old taxonomy had nowhere to put a large narrative that has
stopped growing, so `viral` absorbed it and a "plateaued-but-popular" branch existed to
paper over the gap.

### D2 — Axes are percentiles, not absolute values

**Rationale.** The video revisiting strategy is imperfect and evolving, so absolute
magnitudes cannot be trusted to mean the same thing over time; an absolute bar would
drift with scraper coverage and need continual retuning. Percentiles are
self-calibrating. The labels are also relative claims by nature: "the most viral" is a
top-N statement, not a threshold statement.

**Accepted cost.** A percentile fires for a fixed fraction of the corpus by
construction, so alert volume cannot fall on a quiet day. D4 and D5 exist to ensure the
thing being ranked is real, since ranking cannot manufacture signal where there is none.

### D3 — The two axes rank over *deliberately different* pools

**A consequence of D0, not an independent decision.** Composite requires one measurement
ever; acceleration requires one today; so the pools differ. Nothing further needs arguing.

**Composite** is ranked over **every narrative with video stats**, using each
narrative's last known measurement (carry-forward). **Acceleration** is ranked only over
narratives whose videos were **visited on `calc_date`** (`videos.updated_at::date`, per
D0's mechanism note — *not* `video_stats`, which cannot answer this).

The second-order benefit is worth keeping in view: if composite were ranked only among
today's revisited narratives, a narrative's "how big am I" rank would shift day to day
depending on **who else happened to get scraped** — scraper noise injected directly into
the composite axis, which is the thing D2's rationale is trying to keep out. That is not
a hypothetical; it is the observed 3116 → 2078 turnover. Ranking over all narratives
gives a stable, scraper-independent answer.

**D3 is worth more than the cost line below suggests.** Composite's current pool comes
from `get_prevalent_narratives_summary` — a *dashboard pagination query*
(`core/narratives/repo.py:1595`, docstring: "optimized for dashboard display"), whose
predicate is `videos.updated_at >= NOW() - 24h` ordered by how many of the narrative's
videos were touched, paginated. "Prevalent" therefore means *recently scraped, ranked by
how much of it got scraped* — not large, not important. Adopting D3 deletes that query
from the pipeline and takes the `NOW()` anchor, the wrong-table filter and the
pagination instability with it. It is not merely a better ranking pool.

**Implementation cost (revised).** ~22.4k narratives vs ~2.1k on 2026-07-16 — **~11×**,
not the 7× estimated earlier. Phase 1 is per-narrative: each opens a transaction, opens a
*second* nested transaction for `get_narrative_stats`, then issues three separate inserts
(`core/narratives/service.py:444-536`). At 22k that is ~88k queries across ~44k
transactions. **Phase 1 must become a bulk query before D3 is shippable.**

**Why mixing pools is safe here:** every region boundary is axis-aligned. Each
percentile is only ever compared to a constant on its own axis (`composite >= 0.8`,
`accel >= 0.5`), never to the other axis's percentile. They never need a shared
denominator.

> *This supersedes an earlier version of D3 which required a single shared cohort. That
> was a misdiagnosis: the acceleration axis's compression into its top 7% is caused by
> the zeros (D5), not by the pool sizes differing. Under this D3, D3 and D5 collapse
> into one statement — acceleration is computed only where measurable, and that set is
> its pool.*
>
> *Caveat: this assumes the grey diagonal in the source diagram is a construction line.
> If it is a real boundary it compares the two axes to each other, and the pools would
> have to match after all. Unresolved.*

### D4 — Acceleration is growth *per elapsed day*

Today acceleration is `(current - previous) / previous` where `previous` is the last
snapshot on or before the previous day, with **no division by elapsed time**. A video
last scraped four days ago therefore contributes four days of accumulated growth as if
it occurred in one, and ranks high for having gone unmeasured rather than for growing.

Given D2, this matters more than it would otherwise: percentiles do not wash measurement
error out, they sort by it, and this error concentrates precisely at the top of the
ranking. Normalising by each video's own elapsed gap makes a narrative measured after
four days comparable to one measured after one.

Prior art: `feat/unify-virality-windows` already implements per-day log growth with each
video divided by its own gap. Salvage rather than rewrite.

### D5 — Unmeasured narratives are excluded from the cohort, not ranked at zero

**A consequence of D0** — the acceleration half of it, stated as a cohort predicate.

**The predicate:** a narrative is in the acceleration cohort if **at least one of its
videos has `videos.updated_at::date = calc_date`**. Not visited means excluded; visited
means it stays.

> **Corrected 2026-07-17.** This predicate previously read "*at least one of its videos
> has a `video_stats` row recorded on `calc_date`*", justified by: "*the two cases are
> distinguishable — `video_stats.recorded_at` makes this a plain `EXISTS`. This is a
> measurement question, not an inference.*" **That was false and D5 was unimplementable
> as written.** `video_stats` is a change log (D0's mechanism note): a video re-scraped
> with unmoved numbers writes no row. So the `EXISTS` answers "did any video's stats
> *change* today", and its absence fuses "we did not look" with "we looked and nothing
> moved" — the two cases D5 exists to separate. The visit is recorded in
> `videos.updated_at`, which is what the predicate now uses.

The narratives sitting at exactly zero are largely ones whose videos were not visited, so
the carried-forward snapshot is identical on both days. That is missing data wearing a
zero. Ranking them as "the least accelerating" asserts something the data does not
support, and it is what compresses the usable axis into a sliver.

**Narratives that were visited but did not grow stay in, ranked at zero.** That is not a
compromise — it is required. `consolidated` means big *and flat*, so a large narrative
that genuinely stopped growing has `accel = 0` by definition. Excluding all zeros would
drop precisely those narratives out of the taxonomy, producing a discontinuity where
growing 0.01% earns `consolidated` and growing 0.00% earns no label at all.

**Consequence:** a tie-block of honest zeros remains at the bottom of the acceleration
axis, sized by however many visited narratives are genuinely flat. That is fine for
`consolidated` (they *should* share the bottom rank) but it means the smallest non-zero
rank sits at the block's height — so a low boundary is only meaningful if the block is
smaller than the boundary. **The block's size is unknown and unmeasurable from existing
data** (it needs the visit predicate, which is forward-only per D0), so this cannot be
settled until visit data accumulates. See O3.

**Not addressed by D5:** partial coverage — but this now looks like a non-problem; see
the coverage-bias defect above and O5.1, both of which were retracted on 2026-07-17.

### D6 — `viral` requires both axes at once

`viral` means high composite **and** high acceleration — big *and* still climbing. It is
not a statement about size alone.

This is only coherent because of D5. On the current definition the conjunction is empty
(0 narratives at top-1% × top-1%), which would have forced `viral` to collapse into a
size-only band. Excluding unmeasured narratives makes the axes independent (−0.015) and
the conjunction lands at 9 narratives for top-10% × top-10% — see the evidence above.

A narrative that is large but no longer growing is precisely `consolidated`. Keeping
`viral` a conjunction is what gives `consolidated` a reason to exist.

### D7 — Composite drops `velocity`; engagement and reach are reweighted

```
composite = engagement_pct×0.625 + reach_pct×0.375
```

The weights are the current 0.50 / 0.30 rescaled proportionally, preserving their 5:3
balance. (The exact split is a free choice; proportional is the default, not a finding.)

**Dropping velocity is a consequence of D0, not a weighting judgement.** Velocity is a
*change* measure; D0 says changes may only be ranked where measured on `calc_date`;
composite's pool is "measured at least once". A change measure cannot live on that axis.
It is a category error, and the rest of this section is the empirical confirmation rather
than the argument.

**Rationale.** D3 ranks composite over every narrative with stats, using carry-forward.
That works for engagement and reach, which are **state** — still known from the last
measurement. It does not work for `velocity_score`
(`views_gained_last_2_days / total_views`), which is a **change** measure: for a
narrative that was not revisited, the carried-forward delta is zero, so velocity reads
0. `get_narrative_stats_delta_for_period`'s own docstring confirms it ("A video not
re-scraped during the window has current == baseline, so its delta is zero").

Expanding composite's pool to ~22k with velocity still in it would put ~19k narratives
at velocity 0, tie them all at rank 0, and turn the velocity term binary — reproducing
inside composite the exact pathology D5 removes from acceleration. For a dormant
narrative velocity is *unknown*, not zero, by the same argument D5 makes.

Dropping it makes composite pure state, and moves *all* change-measurement onto the
acceleration axis where D4 and D5 handle it properly. The two axes then mean cleanly
different things: level versus rate.

**Known consequence:** this is what restores the −0.172 anti-correlation (see evidence
above). Velocity was masking it by smuggling a growth term into the level axis. D8
accepts that.

### D8 — Acceleration stays a plain ratio; no size adjustment

Acceleration remains growth relative to the narrative's own baseline (with D4's per-day
normalisation). It is **not** size-adjusted — no stratifying by size, no residualising
against composite, no tuned exponent.

**Rationale.** It is true and meaningful that large narratives rarely double. When one
does, that is genuinely viral, and it *should* be rare. Engineering the anti-correlation
away would manufacture viral narratives on days there aren't any. Acceleration also
keeps a simple, explainable definition — "it grew by X%" — which matters when a person
has to reason about a badge.

**Accepted consequence:** `viral` is rare, possibly zero on many days. That is intended.
See the validation note about confirming it is not *structurally* always zero.

Rejected alternatives, for the record:

- **Rank acceleration within size strata** (bin by `prev_views`, rank within bin). Would
  make the axes independent by construction and redefine `viral` as "growing unusually
  fast *for its size*". Rejected: the size relationship is real signal, not an artifact.
- **Tune the exponent** `Δ / prev^α` (α=1 is today's ratio, α=0 is absolute delta; some
  α gives ~zero correlation). Rejected with the above.
- **Note for anyone revisiting this:** `ln(cur/prev)` does *not* address the
  correlation. Spearman is rank-based and `ln` is monotonic, so log growth has identical
  ranks and identical −0.172. It changes the distribution's shape only. Relevant because
  `feat/unify-virality-windows` uses log growth and is earmarked for salvage under D4.

---

## Open questions

**O2 — Daily volume per label.** Roughly how many narratives should carry each badge on
a normal day? This is the only free parameter left once D1–D8 are fixed, and it cannot
be derived from the data. The `viral` grid above is the shape of the answer: with the D7
composite, top-20% × top-20% gives 26 and top-10% × top-10% gives 0. D8 accepts that
`viral` is rare, so the question is how rare. Needed before any boundary numbers are
final.

**O3 — Region boundaries.** The geometry (a 2×2-ish partition of the percentile plane
with a `viral` box carved out of the top-right) is agreed in shape, and D6 confirms the
`viral` box stays a box rather than becoming a band. The specific boundary values are
provisional and depend on O2.

**O3 now blocks on instrumentation, not analysis.** A boundary is only meaningful if it
sits above the honest-zero tie block (D5), and that block's size cannot be computed from
existing data — it needs the visit predicate, which D0 makes forward-only. **No boundary
number can be chosen until visit data accumulates.**

> **Retracted 2026-07-17:** an earlier revision put the tie block at 42.5% of the cohort
> (596 of 1403) and concluded `y = 0.4` "fails, narrowly". Both numbers came from the
> mislabelled split — see *Retractions*. The true block size is unknown.

**O4 — Frontend and API impact.** `narrative_alert_level` is a Postgres enum and the
labels are user-facing. Renaming `alert`/`watch` to `trending`/`consolidated` needs a
migration and coordination with consumers.

**O5 — the measurement rules.** D0 settles the top-level predicate (≥1 video with
`videos.updated_at::date = calc_date`). These remain:

**O5.1 — Coverage threshold. RESOLVED 2026-07-17: no threshold.** The question was
whether the predicate should be `coverage >= k` rather than all-or-nothing, since
acceleration was thought biased down by (1 − coverage). Three reasons to close it:

1. **The premise is largely false** — see the retracted coverage-bias defect. A video
   with no stats row mostly did not *change*, so zero is its correct contribution.
2. **The effect is not detectable.** Median measured acceleration is flat across a
   hundredfold range of coverage (0.00068 at <1% vs 0.00080 at ≥50%).
3. **It would be a size filter in disguise.** Coverage correlates **−0.61** with
   narrative size, so `coverage >= k` systematically drops the largest narratives —
   exactly the population `viral` and `consolidated` exist to describe. That is a bad
   trade for a bias that cannot be measured.

**O5.2 — How coverage is measured. MOOT** — follows O5.1. (For the record, the
views-weighted measure was the right one and is what the O5.1 numbers use.)

**O5.3 — Denominator scope.** Two options, neither free:
- `Δ(refreshed) / prev(all)` — today's behaviour. Biased down by (1 − coverage).
- `Δ(refreshed) / prev(refreshed)` — the growth rate among videos we actually looked at,
  applied to the narrative. Unbiased *if* refreshed videos are representative; biased
  **upward** if the revisit strategy prioritises active videos.

Which is right depends on how the revisit strategy selects videos — a question about the
scraper, not about this data.

**O5.4 — Maximum baseline staleness. RESOLVED 2026-07-17: cap at 7–14 days.** The worry
was that D4's per-day normalisation launders an old surge into current acceleration — a
video scraped after 50 days whose surge happened on day 40 reports `(large growth)/50` as
*today's* rate. Real, but rare. Measured on 2026-07-15:

| avg baseline age | p10 | p50 | p90 | p99 | max |
|---|---|---|---|---|---|
| days | 1.0 | 1.0 | 4.0 | 11.0 | 91.0 |

96.8% of the cohort is within 7 days, 99.2% within 14. A cap anywhere in that range
removes the laundering case for ~3% of the cohort. Cheap; take it.

**O5.5 — Videos with no baseline. D0 forces this: add `claim_narratives.created_at`, or
drop the term.** A video first scraped on `calc_date` has one observation, and a change
needs two — so under D0 it cannot contribute to acceleration. But `change_video_count`
currently counts it as growth, which is why the 576 no-baseline narratives sit at the top
of the axis (see the scraper defect above).

The signal is worth keeping if it can be had honestly: a narrative genuinely gaining a
video is probably the strongest `early_surge` evidence available. But `claim_narratives`
has no `created_at` (see the schema defect above), so "gained a video" and "we discovered
an old video" are indistinguishable. Previously this listed three options; **D0 removes
the first.** What remains:

- **Migrate `claim_narratives` to add `created_at`** — measures linkage properly, forward
  only.
- **Drop no-baseline videos from acceleration** — safe, loses the new-video signal.

There is no third path where we keep counting things we did not measure.

**O5.6 — Aggregation.** D4 wants each video divided by its own gap, but acceleration is
defined on narrative-level sums today. Combining per-video *rates* into a narrative rate
needs a rule. The clean form avoids averaging ratios:

```
narrative_daily_growth = Σ(video's daily view gain) / Σ(baseline views of those videos)
```

Numerator and denominator then cover the same videos — which is O5.3's second option, so
O5.3 and O5.6 are one decision, not two.

**O6 — The scraper's revisit strategy.** *New, 2026-07-17, and the highest-value open
question now that O5.1 is closed.* Two things depend on it and neither is answerable from
this database:

- **Does the revisit strategy prioritise active videos?** This decides O5.3/O5.6 (whether
  `Δ(refreshed)/prev(refreshed)` is biased upward), and it is the leading explanation for
  why coverage dilution is undetectable (O5.1). Until it is known, acceleration is
  *uninterpretable* rather than merely noisy.
- **When does the daily sweep run?** D0's acceleration predicate reads `updated_at` today
  for a `calc_date` of yesterday, so every video re-visited between midnight and the
  moment phase 2 reads is silently dropped from the cohort. Phase 1 runs first and D3
  makes it ~11× longer, so that window is however long phase 1 takes. If the sweep starts
  near midnight the loss could be large and biased, and D0's cheap `updated_at` mechanism
  would have to give way to a `video_visits` log.

This is a question for whoever owns the scraper, and it is the real critical path.

**O7 — Staleness bound for composite.** *New, 2026-07-17.* D0 admits any narrative
"measured at least once" to the composite pool, with no upper bound on how old that
measurement is. A narrative last measured six months ago has a well-defined state — a
six-month-old one. Should it be ranked against a narrative measured yesterday? This is
the composite-side analogue of O5.4, which only ever asked about acceleration baselines.
Not answerable from the pulls so far: `narrative_coverage.txt` reports baseline age only
for refreshed videos, so the staleness distribution of the never-visited ~19k is unknown.
Needs one query.

**O8 — Should `max(0, ...)` stay?** *New, 2026-07-17.* "Measured and declined" is a
measurement; flooring it to zero merges decliners with genuinely flat narratives, which is
the class of merge D0 forbids. The floor was added deliberately
(`core/narratives/service.py:598-603`) to stop decliners going negative and pushing flat
narratives up the ranking — but under D0 that reordering is *correct*: a flat narrative is
genuinely accelerating more than a shrinking one, and `consolidated` (big and flat) should
rank above a narrative losing views rather than tie with it. Removing the floor changes
what the bottom of the axis means, so decide it explicitly rather than inheriting it.

---

## Dependencies

- `f552c0d` (`fix/indicators-calc-date-stamp`) — must land first; without it the
  classifier reads the previous run's indicators.
- `feat/unify-virality-windows` — contains the per-day growth work D4 needs.

## Validation still owed

**`viral` must be shown to fire at all.** On 2026-07-15, top-10% × top-10% yields 0 and
top-20% × top-20% yields 26. D8 accepts that `viral` is rare, but *structurally always
zero* would be a dead label. A genuinely viral narrative — large and doubling — would
rank top on both axes and fire, so the 0 plausibly means "nothing went viral that day"
rather than "this cannot fire". That is an inference from one day. **Before shipping,
confirm over ~2 weeks that `viral` fires sometimes at the chosen boundaries.**

### Retractions — 2026-07-17

Read these before citing any number in this document.

**The mover-cohort figures still use the wrong cohort, but not for the stated reason.**
The −0.172, the `viral` grid and the n=1384 were produced by filtering to `accel > 0`,
which is not D0's predicate. The earlier claim was that the real cohort is "larger than
1384 by however many narratives are refreshed-and-flat." **It isn't.** Measured against
`narrative_coverage.txt` on 2026-07-15, the proxy makes two errors that cancel in the
count: it drops 596 narratives it should keep and admits 576 it should not. The cohorts
come out the same size (1383 vs 1403) and **overlap on only 807 narratives — 58%**. Same
size, 42% different population, and no summary statistic reveals it. Every mover-cohort
number still needs recomputing; the size just will not warn you.

**The split's labels were wrong.** What was reported as "596 refreshed-and-flat / 1135
never-refreshed" is not that at all, because `video_stats` is a change log:

| reported as | actually is | n |
|---|---|---|
| "refreshed and flat — D5 keeps" | had ≥1 stats *change*, net effect ≤ 0 → **decliners floored to zero** | 596 |
| "never refreshed — D5 drops" | no stats change: **unmeasured *and* genuinely flat, fused** | 1135 |

The 1135 are mostly narratives that *were* re-scraped — that is why they are in the
composite pool at all — whose numbers did not move. D5's two target cases are the two
fused inside them.

**The tie block is not 42.5%.** See O3.

**The coverage-bias defect and O5.1** are retracted and resolved respectively; see above.

**The full-cohort −0.173 correlation should never have been a finding.** On 2026-07-16 it
is **+0.198** — a sign flip and a 0.37 swing in one day. It is computed over a population
about half of which is tied at exactly zero, and those zeros are mostly narratives nobody
re-scraped, so the statistic measures *whether the narratives that went unmeasured that
day happened to be big or small*. That is a property of the scraper's schedule, not of the
narratives. Its instability is itself evidence for D0. Among movers the picture is stable
and boring: −0.015 → −0.080.

### Replicated on a second day (2026-07-16)

The structural claims hold; only the magnitudes moved.

| | 07-15 | 07-16 |
|---|---|---|
| classifiable cohort | 3116 | 2078 |
| implied `composite_pct` cohort | 3116 | 2078 |
| implied `accel_pct` cohort | 22321 | 22376 |
| smallest non-zero `accel_pct` | 0.9296 | 0.9347 |
| `composite` p50 / max | 0.510 / 0.911 | 0.512 / 0.947 |
| `accel` exactly zero | 55.6% | 47.0% |

The pool mismatch, the binary acceleration axis and composite's shape all reproduce. The
zero fraction moved 8.6 points, so do not lean on 55.6% as a constant.

**The `calculated_at` bug is still live in production.** `f552c0d` is not deployed: 47 of
2026-07-16's 149 `early_surge` badges sit at acceleration percentile exactly 0.0 — a
combination the classifier cannot emit. All 430 narratives that became classifiable that
run carry a NULL badge, exactly and only those 430, which is precisely what the bug
predicts (no previous-run rows → unclassifiable → nulled). The 75 narratives badged
`viral` have a median acceleration of 0.00016.

### What can and cannot be simulated

D0 splits the simulation cleanly in two, and this is the practical upshot of the whole
2026-07-17 pass.

**Composite's half can be simulated today.** "Measured at least once" is answerable from
existing data — `create_video` always writes a `video_stats` row, so every video has a
first measurement on record. D3's expanded pool can therefore be evaluated against any
historical day, right now, with no new instrumentation. Worth doing: every composite
percentile in this document is a rank out of ~3116 and will move under D3, since adding
~19k dormant narratives changes where the active ones land — including the `viral` grid.

**Acceleration's half simulates *partially*, and degrades rather than failing cleanly.**
D0's predicate unions permanent evidence (a `video_stats` row on `calc_date`) with
ephemeral evidence (`updated_at`), so on a past day the **movers survive in full** and the
**visited-but-flat population erodes**. Consequences:

- **Raw `accel` and the mover list are inspectable on a historical day.** Useful — this is
  how you eyeball whether the detections look sensible.
- **`accel_pct` is not.** The ranking loses its zeros, so everything ranks too high.
  **O2 and O3's boundaries cannot be chosen from history**, only from `calc_date = today`
  or from days accrued going forward.
- The erosion is biased toward exactly the population D5 exists to keep, so the tie block
  O3 needs is the *least* recoverable thing in the design.

If that becomes intolerable, the `video_visits` table in D0 is the escape hatch — and it
only starts paying from the day it ships.

**D4 has not been simulated either.** Per-day normalisation needs each video's elapsed
gap, which no pull so far carries. D4 changes what acceleration measures, so it will move
the distributions and the correlation — in either direction.

**`viral` must still be shown to fire.** See above; unchanged, and now unfalsifiable from
history for the same reason acceleration is.
