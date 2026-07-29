# Lead Decisions Report — 2026-07-29

## UPDATE: Target list by messaging recency

New files: **`target_list_2026-07-29.csv`** (all 19,127, tiered) and **`target_list_2026-07-29_actionable.csv`** (the 8,773 worth touching, sorted so the hottest are at the top). Every row now shows **when they last replied**, **what they said**, and **when we last messaged them**.

| Tier | Count | Who | Why this order |
|---|---|---|---|
| **T1 CALL TODAY — hot reply** | **9** | Replied *interested* within the last 14 days | The conversation is still alive. Several named amounts: Thomas Turbiak ($50,000), Scott Tene ($100k), Robert Cassell ($500k LOC, 36 mo). Jacques Houssou asked for the term sheet yesterday. |
| **T2 REVIVE** | 57 | 41 said interested 15+ days ago then went quiet; 16 gave an unclear reply in the last 30 days | A human engaged, and they reappeared in today's data. Read the thread first — some are "who is this?" and need a soft reintro, not a pitch. |
| T3 RE-OPEN | 0 | Cool-off expired (31–120d since we messaged) + new activity | Empty today because almost everyone we've messaged was messaged in the last 30 days. This tier will fill as time passes. |
| **T4 FRESH** | 2,419 | Never messaged by us, came via funding-request source, owner + direct mobile | Untouched, asked for funding, reachable. The best cold-call pool. |
| T5 FRESH (weaker) | 6,288 | Never messaged, intent source, but wrong contact or no mobile | Email/enrich, don't burn call time. |
| SKIP — cool-off | 1,342 | We messaged/worked them within 30 days | Touching again now double-contacts them. |
| T7 background | 8,956 | Never messaged, no funding ask | Pool only; wait for a signal. |
| EXCLUDED | 41 | Opt-outs, funded deals, do-not-issue | Off limits. |

**Today's calling order: the 9 T1 rows, then the 41 old-interested T2 rows, then start on T4.**

---


**Input:** 19,127 incoming records from today's cycle that matched something we already have (pool or old records). Every one matched on a strong ID — email, phone, or both — so **Match = SAME** for all of them. No name-only guesses were needed.

**Output file:** `lead_decisions_2026-07-29.csv` (same folder). Sorted so the A list is at the top. Each row has Match / Action / Priority / Why / Missing.

## The bottom line

| Priority | Action | Count | What to do |
|---|---|---|---|
| **A** | CALL NOW | **2,469** | Work today |
| B | EMAIL THIS WEEK | 6,304 | This week |
| B | SEND FOR MORE INFO | 16 | Enrich first |
| C | EMAIL THIS WEEK | 6,451 | Low-pressure, later |
| C | HOLD | 3,640 | Do nothing yet |
| C | MERGE WITH EXISTING | 206 | Just update the record |
| C | DROP | 41 | Remove from outreach |

## The A list (2,469) — work today

Two groups, in this order:

1. **50 leads replied to our texts and showed interest** (TextTorrent replies, verdict "replied_interested"). These are the very top of the file — a real person answered us and said yes-ish. Examples: Shawn Lawton (Lawton Business Solutions, owner), Jason Lurie (owner), Angela Harvey (Harvey Counseling Services). Call these first.
2. **2,419 leads came in through a funding-request source** (merchant cash advance / quick business / B2C loan files), are the **decision maker** (owner, founder, CEO, president), and have a **direct mobile**. They asked; we are not guessing; we can reach them.

## The B list (6,320) — this week

- **32** replied to us before but not a clear yes ("replied_later" / "replied_other"). Read the thread before touching them.
- **6,272** came from a funding-intent source with a valid email or phone, but they are not clearly the decision maker or have no direct mobile. Email first.
- **16** have intent but no reliable way to reach them — send for enrichment before anyone spends call time.

## The C list — later or never

- **6,451** appear in 3+ unrelated sources or just showed up in a new source (the story is corroborated, which builds trust), but **nobody asked for funding** — low-pressure email only.
- **3,640 HOLD:**
  - 1,341 were **worked by a rep in the last 30 days** — touching them again now would double-contact.
  - 2,299 matched but have **no intent signal and nothing changed** — we would just be guessing.
- **206 MERGE WITH EXISTING:** seen on 2+ previous days from the **same source with nothing changed**. That is the vendor's file refreshing, not a lead. Update the record, no outreach.
- **41 DROP:**
  - 34 **opted out** ("do not contact me again") — final, overrides everything.
  - 7 are **already funded deals or marked do-not-issue**.

## How the rules were applied (in your order of weight)

1. **Intent** — source file tells us if they asked: `merchant_cash_advances`, `quick_business`, `b2c` = they requested funding. The B2B estate/construction/capital lists = we are guessing.
2. **Engagement** — TextTorrent reply history (retarget files from 2026-07-28) trumps everything except opt-outs: interested reply → A, any reply → B, opt-out → DROP.
3. **Repetition rule** — leads seen on multiple prior days (from `rds_lead_overlaps`) with **no new source** and no change = noise → merge, don't work. A lead that reappeared **in a new source today** got a boost instead.
4. **Reachability** — direct mobile + decision-maker title = call; validated email only = email; nothing validated = enrich.
5. **Recently worked** — anything a rep touched in the last 30 days (from `recurring_worked_leads`) is on HOLD regardless of everything else.

## What's missing (would change answers)

- **Timing signals we don't have yet:** funding rounds, hiring pace, filings. Today's data can't tell "raised 18-24 months ago" from "just raised." If you add an enrichment source for that (the Step 6 pilot Lambda could feed this), a chunk of the C "no signal" pile would re-rank.
- **Reply threads for the 32 unclear replies** — reading them manually would sort them into A or DROP quickly.
- **Fit filters** (your stage, ticket size, sector, country) were not applied because they aren't defined in the data — tell me the criteria and I'll add a fit column.
