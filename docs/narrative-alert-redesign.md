# Narrative alert system — redesign

**Status:** design in progress. Decisions below are settled; open questions at the end
are not. No implementation yet.

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

**Acceleration is biased downward by partial coverage.** A narrative is many videos, and
on any day only some are refreshed. The numerator (delta) covers only the refreshed
videos — carried-forward videos contribute exactly zero — while the denominator covers
*all* of them:

```
today_total = refreshed videos (fresh) + the rest (carried forward)
prev_total  = all videos as of calc_date - 1
delta       = growth of the refreshed videos only

acceleration = delta / prev_total(ALL videos)
```

So a narrative measured through 5% of its videos reports roughly 5% of its actual
growth. This is not missing data — it is present data carrying a wrong number, and
nothing downstream can distinguish it from a narrative that genuinely grew that little.

**This may invalidate the "movers are noise" reading.** The finding that the median
mover grew 0.46% and p25 grew 0.029% is used throughout this document. If the median
mover's coverage is ~5%, then 0.46% measured is ~9% actual, and those narratives are
real movers seen through a keyhole rather than noise. The measurement is sound; the
interpretation may not be. **Unresolved — see O5.1 and the validation notes.**

**Reach mixes data sources.** Numerator from `video_stats` snapshots; denominator
(`get_average_views_for_all_narratives`) from the `videos` table's own `views` column
(`repo.py:1722`). Two separately-maintained sources divided by each other.

**`claim_narratives` has no timestamp.** The table is `(claim_id, narrative_id)` with no
`created_at` (`migrations/7`), so "which videos did this narrative have yesterday" is
unanswerable. `video_claims` does carry `created_at`, but claim creation is not the same
event as narrative linking. This forecloses distinguishing "the narrative gained a
video" from "the scraper discovered an old video" — see O5.5.

**35% of acceleration tracks the scraper.** `change_video_count` compares videos *known*
as of each date, and under carry-forward "known" means "has any snapshot on or before D".
A video first scraped on D counts as new growth, so a narrative that gained nothing but
got swept scores acceleration. `feat/unify-virality-windows` reweighted this term from
0.35 to 0.10 for exactly this reason.

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

### The axes are anti-correlated, and that is real

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

**Composite** is ranked over **every narrative with video stats**, using each
narrative's last known measurement (carry-forward). **Acceleration** is ranked only over
narratives whose videos were **revisited on `calc_date`**.

**Rationale — composite is a state, acceleration is a change.** A narrative's size and
engagement are still known from its last measurement; not re-scraping it today does not
make it unknown, so carry-forward is legitimate. Acceleration needs two measurements;
without a fresh one it is genuinely uncomputable. Different epistemics, so different
pools is the correct answer rather than a compromise.

There is a second reason. If composite were ranked only among today's revisited
narratives, a narrative's "how big am I" rank would shift day to day depending on **who
else happened to get scraped** — scraper noise injected directly into the composite
axis, which is the thing D2's rationale is trying to keep out. Ranking over all
narratives gives a stable, scraper-independent answer.

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

**The predicate:** a narrative is in the acceleration cohort if **at least one of its
videos has a `video_stats` row recorded on `calc_date`**. Zero refreshed videos means
excluded; one or more means it stays.

The 55.6% sitting at exactly zero are largely narratives whose videos were not
re-measured, so the carried-forward snapshot is identical on both days. That is missing
data wearing a zero. Ranking them as "the least accelerating" asserts something the data
does not support, and it is what compresses the usable axis into a sliver.

**Narratives that were refreshed but did not grow stay in, ranked at zero.** That is not
a compromise — it is required. `consolidated` means big *and flat*, so a large narrative
that genuinely stopped growing has `accel = 0` by definition. Excluding all zeros would
drop precisely those narratives out of the taxonomy, producing a discontinuity where
growing 0.01% earns `consolidated` and growing 0.00% earns no label at all.

The two cases are distinguishable — `video_stats.recorded_at` makes "was any video
refreshed on `calc_date`" a plain `EXISTS`. This is a measurement question, not an
inference.

**Consequence:** a tie-block of honest zeros remains at the bottom of the acceleration
axis, sized by however many refreshed narratives are genuinely flat. That is fine for
`consolidated` (they *should* share the bottom rank) but it means the smallest non-zero
rank sits at the block's height — so the `y = 0.4` boundary is only safe if the block is
smaller than 40% of the cohort. Unverified; see O3 and the validation notes.

**Not addressed by D5:** partial coverage. See the coverage-bias defect above and O5.1 —
D5 as scoped is all-or-nothing, and says nothing about the narrative whose acceleration
is real but measured through 5% of its videos.

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

Note that the boundaries must now be read against the **mover cohort** (D5), not the
full corpus: "top 10% of narratives that moved" is 138 narratives, not 312.

**O4 — Frontend and API impact.** `narrative_alert_level` is a Postgres enum and the
labels are user-facing. Renaming `alert`/`watch` to `trending`/`consolidated` needs a
migration and coordination with consumers.

**O5 — the measurement rules.** D5 settles the top-level predicate (≥1 video refreshed
on `calc_date`). These remain:

**O5.1 — Coverage threshold.** Coverage is a continuum, not a flag, and acceleration is
biased down by (1 − coverage). D5 as scoped is all-or-nothing: it drops narratives at 0%
coverage and keeps everything else, including the narrative measured through 5% of its
videos whose number is wrong by 20×. Should the predicate instead be `coverage >= k`,
and what is `k`? **This is the highest-value open question** — it also decides whether
the "movers are noise" reading survives.

**O5.2 — How coverage is measured.** By video count, or views-weighted? Views-weighted
is the recommendation: one refreshed video holding 99% of the narrative's views is 99%
covered, not 1%. It is the share that actually drives the number.

**O5.3 — Denominator scope.** Two options, neither free:
- `Δ(refreshed) / prev(all)` — today's behaviour. Biased down by (1 − coverage).
- `Δ(refreshed) / prev(refreshed)` — the growth rate among videos we actually looked at,
  applied to the narrative. Unbiased *if* refreshed videos are representative; biased
  **upward** if the revisit strategy prioritises active videos.

Which is right depends on how the revisit strategy selects videos — a question about the
scraper, not about this data.

**O5.4 — Maximum baseline staleness.** D4 divides by elapsed days, which assumes growth
was even across the gap. A video scraped after 50 days whose surge happened on day 40
reports `(large growth)/50` as *today's* rate — per-day normalisation launders an old
surge into current acceleration. There should be a maximum gap past which a baseline is
unusable. Value unknown; needs the staleness distribution.

**O5.5 — Videos with no baseline.** A video first scraped on `calc_date` has nothing to
compare against. Narrative growth, or scraper discovery? **The schema forecloses telling
them apart** (see the `claim_narratives` defect above). Options: treat first-scrape as
growth (wrong whenever old content is discovered); drop no-baseline videos (safe, but
blind to new-video growth — arguably the strongest early-surge signal available); or
migrate `claim_narratives` to add `created_at`, which fixes it correctly but only
forward.

**O5.6 — Aggregation.** D4 wants each video divided by its own gap, but acceleration is
defined on narrative-level sums today. Combining per-video *rates* into a narrative rate
needs a rule. The clean form avoids averaging ratios:

```
narrative_daily_growth = Σ(video's daily view gain) / Σ(baseline views of those videos)
```

Numerator and denominator then cover the same videos — which is O5.3's second option, so
O5.3 and O5.6 are one decision, not two.

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

**Every mover-cohort figure in this document uses the WRONG cohort.** The −0.172, the
`viral` grid, the n=1384 — all were produced by filtering to `accel > 0`. That is not
D5's predicate. `accel > 0` also drops narratives that *were* refreshed and genuinely
did not grow, which D5 explicitly keeps. The real cohort is larger than 1384 by however
many narratives are refreshed-and-flat, and it contains honest zeros that the simulation
excluded. **Every one of these numbers must be recomputed against the real predicate
before any boundary is chosen.**

The split of the 1732 zeros into refreshed-and-flat versus never-refreshed is unknown
and is the single number that unblocks this. `narrative_coverage.txt` in the repo root
pulls it, along with the coverage distribution O5.1 needs and the baseline-staleness
distribution O5.4 needs.

**D3's expanded composite pool has not been simulated.** Composite is currently computed
only for the prevalent set (~3116); D3 requires it for all ~22k. Every composite
percentile in this document is a rank out of 3116 and will change under D3 — including
the `viral` grid, since adding ~19k dormant narratives moves where the active ones rank.
This also has a real cost: phase 1 currently batches over prevalent narratives only, so
D3 means computing composite for 7× more narratives.

**D4 has not been simulated at all.** Per-day normalisation needs each video's elapsed
gap, which no pull so far carries. D4 changes what acceleration measures, so it will
move the distributions and the correlation — in either direction.

**Every number comes from a single run** (2026-07-15). Before boundaries are fixed, the
measurements should be repeated across several days to confirm the distributions are
stable — particularly the 55.6% zero fraction and the −0.172 correlation, both of which
the design now leans on heavily.
