// ============================================================
// FIDATO MIS SERVER v2.11.0-s6.9 - SALES BOOKING MODULE WIRED: separate ./sales.js handles unit bookings ("book <unit> <customer>" from accountants in the expense group or DM) -> tracker-API lookup + price menu with delta vs current list -> broker/brokerage (%% and absolute both shown) -> advance (amount/mode/account from LEDGER_ACCOUNTS) -> agent preview -> posted to APPROVAL group -> M+S both-yes (same swipe rail; sales handler runs FIRST in the dispatcher and consumes only its own quoted posts) -> agent RE-CONFIRMS ("confirm <unit>") -> POST booking to the capital-tracker web app (unit flips Sold, cover sheet created+seeded, advance in tranche 1). "edit <unit>" re-opens fields and re-posts for approval with verdicts reset. Registry: wa_auth/sales_pending.json. Env: TRACKER_API_URL + TRACKER_API_SECRET (Railway). Offline-tested (sales.js --test). Prior: v2.11.0-s6.8 - APPROVAL DIGEST: 3 SEPARATE MESSAGES + HIDE ALREADY-DONE. buildApprovalReminderDigest now returns {messages:[...]} - up to THREE messages posted to the capital approval group: "APPROVALS NEEDED - M" (S already approved, mentions M only), "APPROVALS NEEDED - S" (M approved, mentions S only), and "APPROVALS NEEDED - M & S" (neither yet, mentions both). Empty buckets are not posted. Numbering is CONTINUOUS across the three so a reply "5 yes" still maps; sendApprovalReminderDigest posts each, tags each items msgId, and saves one digest_map with msgIds[] so a bare verdict quoting any one message applies only to that messages slice. NEW filter alreadyDone(e) drops entries that are already fully approved (findApprovedEvent), already paid (paidStatsForItem>0), or are the bots own "Approved (M+S)" confirmation echo (regex on label) - so paid/approved items no longer pollute the pending list. Held items fold into whichever bucket still needs that persons yes, tagged (on hold). Preview .text kept (messages joined). Offline-tested. Prior: v2.11.0-s6.7 - OUTFLOW SUMMARY SHOWS APPROVED-BUT-UNPOSTED + EVENT-STORE-SOURCED CATCH-UP. buildPaymentsSummary adds a third pass over event-store approved events with no posted record and no paid event (mirrors the s6.3 buildOutflowLog pass) so the in-group summary is the COMPLETE approved universe, not just posted (pp.recent) items. Capital-IN raises are excluded NARROWLY via isCapitalInflowLabel (/contribution of rs|inr|rupee/) so a mis-posted "MM/SM Contribution of Rs 2.28 Crore" never shows as a payable outflow, while a contribution REPAYMENT ("SM EXCESS contribution old" = paid back to SM) correctly STAYS in the payments list. Approved-but-unposted items listed in the summary are now payable by number: the bare-number handler synthesizes+registers a posted record from the approved event when none exists. The one-time catch-up (catchUpApprovedToOutflow) is RE-SOURCED from the event store (new listApprovedFromEventStore) instead of the lossy chat-history audit (listApprovedForOutflow), so the approved-but-unposted backlog clears in one run. Diagnosis: approvals were recorded but never bridged because OUTFLOW_POST_ENABLED was OFF; the summary read only posted items so approved-unposted were invisible; the old catch-up re-derived approved from chat history (saw only 7) so it never reached the ~30 stranded. Toggle now ON; this patch surfaces + clears the rest. Offline-tested. Prior: v2.11.0-s6.6 - PROMOTER CONTRIBUTION ACCOUNTS. The inflow group now recognises a CONTRIBUTION (MM/SM putting money into a company) as a third kind alongside inflow/transfer. parseInflowOpening routes to 'contribution' when a contribution word (contribution/capital/put in/infused/invested) is present OR the promoter (MM/SM by token or name) is the named source; a plain "drawing" is never a contribution. New IO step 'promoter' asks MM or SM if it wasn't parsed from the line. On confirm it writes an IN row (Person=promoter, tag "Promoter Contribution") AND records a 'contribution' event {promoter,entity,amount,date}. Outflow side: when a paid item's label is a contribution repayment (detectContributionRepayment = a repay/return/payback verb + MM/SM, or an explicit contribution/capital context; "MM Drawing" is excluded), the paid flow also records a 'contrepay' event that knocks it off that promoter's account. buildContributionStatement nets contributions(+) - repayments(-) per promoter, capturing a SILENT per-entity breakdown (byEntity) while the headline is the combined figure (Option A: personal drawings are NOT subtracted). Read it in the inflow group with "MM account" / "SM account" / "contributions", or GET /api/contributions. Offline-tested: 54 assertions (routing / prefill / row / repayment-detect / statement-math + full contribution flow + inflow regression). Prior: v2.11.0-s6.5 - NATURAL INFLOW/TRANSFER TRIGGERS. parseInflowOpening no longer requires the keyword to be the FIRST word; it fires on intent in ANY word order, gated by a money-like amount so chatter never logs. Triggers only when the message has a real amount (a unit lakh/lac/lk/cr/k/l, an Rs/INR/rupees marker, or magnitude >= 1000) AND an intent signal -> transfer if a transfer verb (transfer/trf/tfr/moved/shifted) OR an account->account "from X to Y" (both resolve to accounts), else inflow if an inflow verb (received/recd/rcvd/got/credited/deposited/inflow/collected/collection/came in) OR a "from <payer>" / "by <payer>". So "5 lakh received from Rajesh", "got 5L from Rajesh", "5L from Rajesh", "credited 250000 from ABC" all log now, while "ok" / "thanks" / "got it 100%" / "call me at 5" / "received the documents" stay silent. parseRoughAmount now also reads "5L"/"5 L" as 5 lakh (added l/lk units with a word-boundary guard so 5kg/5liters are not misread). Slot pre-fill (amount/mode/payer/into-acct/from-acct/to-acct) unchanged, just order-independent. Offline-tested (39 assertions). Prior: v2.11.0-s6.4 - QUIET SINGLE VERDICTS. A lone promoter YES no longer posts a group receipt (the old "recorded M's approval" line read like the expense was already fully approved). New perVerdictNotice(v,who,label,amount): YES -> null (silent; only the combined "Approved (M+S)" line announces it, and that already fires solely when BOTH M+S are yes); NO -> posts "rejected by <who>"; HOLD -> posts "put on hold by <who>". The main verdict branch calls it; the re-approval branch drops its lone receipt too (its own rejection + both-yes lines already cover it). The M+S approval GATE is UNCHANGED — approved events still mint only on both-yes, so nothing is treated as approved on a single verdict. Offline-tested (5 assertions). Prior: v2.11.0-s6.3 - DASHBOARD MARK-PAID (manual reconciliation). The Payments-log dashboard (/api/outflow-log) now lists the COMPLETE approved universe: buildOutflowLog adds a third pass over event-store 'approved' events with no posted record and no paid event, surfaced as status 'approved' (purple pill) so approved-but-unposted items appear alongside posted/part-paid/paid/closed. Each row with a balance (approved/posted/part-paid, not closed, not fully paid) gets a green Mark-paid button -> GET /api/paid-mark?id=...[&amount=...] -> markItemPaid(): records a 'paid' event for the remaining balance (mode 'Manual', date IST today), NO ledger write (a books-reconciliation mark, not a disbursement), reversible via the existing Mark-unpaid (/api/paid-undo). Optional &amount records a partial (capped at balance). Lets M tick every approved/pending item against his books once and start the live tracking afresh. Offline-tested: 22 assertions (full / partial / over-cap / closed / already-paid / approved-unposted + pending-row surfacing). Prior: v2.11.0-s6.2 - PAYABLE CODE (stage 2c+2d): AMOUNT-STEP 3-WAY BRANCH + OVER-APPROVAL RE-APPROVAL. paidFlowAdvance amount step now splits on typed amount vs remaining balance: == balance settles as before; < balance asks part-vs-reduced (new 'partask' step: 1=part payment rest still due; 2=final/reduced -> sets session.closeAfter so the outer handler recordClosedEvent's the item and writes off the unpaid remainder, no ledger row for the write-off); > balance is BLOCKED (no answers.amount, no write) and routed to a pick-list reason (new 'overreason' step over REAPPROVAL_REASONS = Price revised / Extra work / GST taxes / Measurement change / Other; Other -> 'overnote' free-text), then reapprovalSignalFrom packages {itemId,code,label,approved,paidSoFar,balance,thisInstalment,attempted=paidSoFar+thisInstalment,reason,isFinal=paidSoFar>0}. Outer handlePaidFlow, on out.reapproval, posts buildReapprovalMessage (two auto-selected variants: fresh/mid-stream shows approved/now-being-paid/increase; final-instalment shows already-paid history + total-would-become + over delta) to APPROVAL_GROUP_JID, registerReapproval()s it keyed by the posted msgId (wa_auth/reapproval_pending.json), tells the accountant it is blocked pending M+S, ends the session (no write). handlePromoterVerdicts gains a re-approval branch AHEAD of the EXPENSE-REQUEST swipe branch: a bare ok/yes/no swipe on a pending re-approval post records the M/S verdict on the registry entry; both-yes calls liftPayableAmount(itemId,attempted,code) which raises paid_posted rec.amount (what newPaidSession + the summary read) so the higher figure becomes payable, records a 'reapproved' audit event, confirms in-group; a no marks it rejected and leaves the approved figure unchanged. Offline-tested: 37 assertions on the amount branch/signal/message + 9 on liftPayableAmount. BACKEND-ONLY P-code unchanged. ROLLOUT: keep LEDGER_WRITE_TAB on the copy tab until this is live so the over/under guards gate real writes from day one. Prior: v2.11.0-s6.1 - PAYABLE CODE (stage 2a+2b of the build). mintPayableCode -> P-YYMMDD-NNN with a DAILY-RESET counter derived from existing approved-event codes for that IST day (single-threaded store, no double-issue). recordApprovedEvent now MINTS + stores `code` on the approved event at full M+S approval (idempotent: re-approval keeps the existing code, adds no dup). payableCodeFor(itemId) resolves it. assemblePaymentRow ledger Notes tag switched from [bot:<id>] to [bot:<P-code>#seq] (falls back to <id> for TEST/inflow/transfer rows that have no code) so bot-written ledger rows reconcile by EXACT JOIN on the code instead of fuzzy matching. One-time back-fill buildPayableCodeBackfill + GET /api/payable-code-backfill[?commit=1] codes the existing approved events in seq order using their recorded `at` date (the 30 historical ones batch as P-260620-NNN since the s5.20 catch-up recorded them on 20 Jun); dry-run by default, idempotent on commit. Offline-tested (14 assertions). BACKEND-ONLY: code is never shown to accountants. STILL TO BUILD (stage 2c+2d): amount-step 3-way branch (=balance settle / <balance part-vs-reduced prompt / >balance BLOCK + no write) and re-approval bounce to the approval group (pick-list reason; mid-stream + final-instalment message variants) with the M+S unlock that lifts the Payable amount. Prior: v2.11.0-s6.0 - CATEGORISED PAYMENTS SUMMARY (STAGE 1 of the Payable-code build). buildPaymentsSummary/formatPaymentsSummary rewritten into three labelled blocks: a header scorecard (Approved-due / Outstanding / Paid-to-date + last-Nd / Part-paid-balance / Closed); APPROVED-NOT-PAID (fresh items, sorted BIGGEST AMOUNT FIRST, NO cap, numbered for reply-to-pay); PART-PAID (paid/of/balance, numbered for next instalment); and PAID-last-N-days (CALENDAR window PAID_WINDOW_DAYS=3 = today + N-1 prior, computed via payDayIndex/payDateLabel on an IST day-index so it does not wobble by time-of-day; shows amount+date, check-marked not numbered). Footer self-checks Paid+Outstanding[+Closed]=Approved. NO P- code shown to accountants. Trigger unchanged: 'summary'/'status' in the outflow group; numbered map (fresh+part-paid only) still drives reply-number. STILL TO BUILD (next stages): P-YYMMDD-NNN daily-reset code minted at full M+S approval + [bot:P-] ledger tag; bot writes REAL Ledger (repoint LEDGER_WRITE_TAB); amount-step 3-way branch (=balance settle / <balance part-vs-reduced prompt / >balance BLOCK + no ledger write); re-approval bounce to approval group with pick-list reason (mid-stream AND final-instalment message variants, auto-selected); test/[See image]/duplicate cleanup of paid_posted.json; 'paid all' full-history command. Prior: v2.10.0-s5.21 - INFLOW + TRANSFER group (one group, INFLOW_GROUP_JID, default 120363429672822928@g.us). Routed by first word: "received …" → INFLOW (writes an IN row; Tag = receivable code from LEDGER_INFLOW_TAGS so the Site+Projections receivables SUMIFS pick it up; Description = payer; bankAc = account received into; questions amount→date→mode→head→tag→entity→into-account→from-whom→CONFIRM). "transfer …" → TRANSFER (writes a TRANSFER row; bankAc = from, transferTo = to, IN/OUT = TRANSFER so it's excluded from IN/OUT day totals; questions amount→date→mode→from-acct→to-acct→entity→CONFIRM). The opening line is parsed for amount (handles "5 lakh"/"1.5 cr"), mode keyword, transfer from/to accounts, and inflow payer/into-account; pre-filled slots are skipped and only the missing answers are asked; CONFIRM always shows the full row preview so any misread is caught before writing. Reuses the existing question validators, writeRowToLedger (same dry-run / LEDGER_WRITE_ENABLED / LEDGER_WRITE_TAB / new-day-block gating + [bot:<id>] dedupe), day-block creation, and accountant auth. New: handleInflowFlow adapter wired into the message dispatcher ahead of handlePaidFlow; sessions namespaced (#io) so they never collide with an outflow paid session. Simple logger for now (no event-store tracking / summary / part-receipt / unit register — those are the later layers). NOTE: the receivable tags must be added to the Ledger Tag dropdown; transfer rows leave Head blank. Prior: v2.10.0-s5.16 — corrected Tag and Head to the VERIFIED live dropdowns (screenshots 17 Jun). TAG is now the project cost-code set (FBD-Contractor/Steel/RMC/Exterior/STP/Road/SCO/Electricity/Diesel/Other, VRN-Contractor/Steel/RMC/Electricity/Site/Other, FBD/VRN Floor/Plot/Other-Collection, and a blank '—'); the old generic Tag values (Office/Legal/Salary/Directors-SM/-MM/Loan) were never real Tags — they are HEADS. HEAD is now the verified expense-nature set (Capital Site, Vrindavan, Office GK-1, Legal, Directors, Salary, Loan, Site, Drawing, Office Exp, Legal Exp, Diesel, Electricity, Other, Noida 153, Noida TS-3) and the Head step (4/7) is now a VALIDATED picklist like Tag/Account/Entity (number / "ok" guess / exact name; off-list rejected) — no longer free-form. Realigned the guessers: aiGuessTag prompt updated to project cost-codes only (Head nature lives in Head; off-list→dash); aiGuessHead now constrained to LEDGER_HEADS; guessHeadFallback returns only in-list heads; guessTagAndPerson rewritten to infer a valid project code (proj×category, else the project's -Other, else none) + promoter Person (SM/MM) from the description/entity, and the tag guess is guarded to in-list. Removed the now-dead TAG_QUICK. Prior: v2.10.0-s5.15 — FULL numbered pickers for Mode, Tag and Head in the paid flow. Mode (3/7) and Tag (5/7) now render the COMPLETE list vertically (Tag = all 19 LEDGER_TAGS, not just the 6 quick picks; a tag number now indexes the full list); Head (4/7) gains a numbered quick-pick list (LEDGER_HEADS, PROVISIONAL) shown alongside the AI guess — "ok"=guess, a number picks from the list, or type any Head (Head stays free-form). paidModeMenu/paidTagMenu now use the shared vertical paidNumberedMenu; added paidHeadMenu. No model/flow changes otherwise. Prior: v2.10.0-s5.14 — PART-PAYMENT TRACKING + interactive two-group summary + close/cancel. The paid flow now allows MANY instalments per approved item: each completed flow records its own paid event (seq-aware dedupe key paid:<id>#<seq>) and its own ledger row tagged [bot:<id>#<seq>] (distinct + idempotent per planLedgerWrite). Per item we derive approved (posted/due), paid (Σ instalments), balance; states fresh/part-paid/settled. The "amount paid?" step now defaults "ok" to the remaining BALANCE, not the full approved. Question order swapped: …tag → 6/7 ENTITY → 7/7 ACCOUNT → CONFIRM (was account then entity). buildPaymentsSummary is now a CUMULATIVE roll-up: Paid = actual cash out; identity Paid + Outstanding + Closed = Approved (Closed term shown only when present). formatPaymentsSummary renders two labelled groups — ⚪ FRESH (not yet paid) on top, 🟡 PART-PAID below — with CONTINUOUS numbering (Fresh first), oldest-approved first; settled items drop off. The summary persists its number→item map (paid_summary_map.json) so in the outflow group a bare NUMBER logs the next instalment for that item (balance pre-filled, instalment-aware "paid so far / balance"), and CLOSE <n> / CLOSE <n> yes closes/cancels an item as-is (accountants allowed): under-settled → remaining written off, never-paid → fully cancelled; closing writes NO ledger row. Reopen via /api/payment-reopen?id=. buildOutflowLog sums instalments and reports status posted/part-paid/paid/closed (+ dashboard pill CSS). Prior: v2.10.0-s5.13 — added /api/outflow-post-dummy (lock-protected): posts a ⟨TEST⟩-tagged PAYMENT DUE item via the SAME postApprovedToOutflow path as the real bridge (force-bypasses the OUTFLOW_POST toggle so it works while posting is OFF; registered in paid_posted.json so a "paid" reply matches back and it shows in the summary). Item id test-<ts> => any ledger row it produces is tagged [bot:test-…] for cleanup. Params: label, amount, optional entity/account. Also added an on-demand summary/status command in the outflow group (works for accountants AND M/S, who are whitelisted): replies with Approved(due)/Paid/Yet-to-pay counts + ₹ totals, yet-to-pay itemised, in the agreed layout. buildPaymentsSummary is a pure status-partition of the POSTED universe valued at the posted/approved (due) amount, so Approved = Paid + Yet-to-pay always reconciles (self-checking); formatPaymentsSummary renders it; both offline-tested. Held / Do-not-pay deferred (no way to set them yet). Prior: v2.10.0-s5.12 — added /api/ledger-test-write (lock-protected rehearsal trigger): builds a row via the same assemblePaymentRow and fires the same gated writeRowToLedger, tagged [bot:test-<ts>] in col L. Lets a same-day or back-dated write be fired from the browser to verify the executor on Copy of Ledger without WhatsApp. Confirmed (live) the new-day clone target: date breaker =DATE(y,m,d) and DAY TOTAL formulas are all RELATIVE (C=SUMIFS IN ref A<daterow>, G=SUMIFS OUT, J=C-G) so copyPaste repoints them; bot clones the 2-row date+total breaker (no repeated header, matching the newest day). entity list (step 7/7) corrected to the verified Ledger Entity dropdown (21 entries incl Fidatocity Homes, Others (combined), MM, SM). Account list (step 6/7) and entity list both validate typed input against the dropdowns. 7-question paid flow fills cols B (entity) and J (account), overriding the request lines. paid flow is now 7 questions: added "7/7 Which entity/company?" (numbered list compiled from the workbook Company/Entity column; typed value validated/resolved, warns on mismatch). Chosen entity fills col B, overriding the request Company line. NOTE: the entity list is PROVISIONAL (no confirmed dropdown) — confirm/correct it. Prior: 6th account question fills col J. paid flow is now 6 questions: added "6/6 Paid from which account?" (full numbered list of the 22 Ledger Bank A/C accounts; a typed account is validated/resolved against that list, warns on mismatch). The chosen account fills col J, overriding the request From line (which only ever held the company). Rest of the flow unchanged. Delete now clears the WHOLE thread for an expense: the paid flow records every message id (the PAYMENT DUE post, each bot Q&A prompt, and each accountant reply) against the item, and unpost deletes them all (best-effort; the bot can only delete-for-everyone its own messages unless it is group admin). outflow payments log (/api/outflow-log: every item pushed to the group joined with paid status) + testing controls: mark a paid item back to unpaid (/api/paid-undo, removes the paid event) and delete a pushed item from the dashboard + its group message (/api/outflow-unpost?deleteMsg=1); both wired as buttons on the queue dashboard. parseSheetDate now reads weekday-suffixed breaker dates ("09 Jun 2026, Tuesday") and isLedgerNum strips the rupee symbol, so breaker vs transaction rows classify correctly on the LIVE sheet (verified against Copy of Ledger). new-day block creation by cloning the previous breaker (preserves formatting + SUMIFS/NET formulas), chronologically placed (newest=bottom, back-dated=mid-sheet); tighter txn-row detection (numeric col G) so breaker rows are not misread. bot writes Ledger col A as dd/mm/yyyy real date so the breaker =SUMIFS day-totals match it. configurable write tab LEDGER_WRITE_TAB (default Ledger) so a same-workbook copy tab can be the rehearsal target; reads/reports stay on real Ledger. backfill sourced from buildApprovalAudit().fullyApproved (FAST — the s5.1 reconciliation source ran the AI matcher per item and hung the page). Queue page now has a timeout + visible errors. Company/From recovered from request body; does not auto-exclude already-paid (push selectively). list approved items + manual per-item push + one-time catch-up sweep (event-store sourced, idempotent, force-bypasses the auto toggle), with an interactive /api/outflow-queue page behind the lock. Also fixed /health version (was stale s1). STAGE 4 LEDGER WRITE dry-run is a live lock-protected panel toggle: pure planLedgerWrite (right day-block, dedupe by [bot:id], insert position) + gated executor writeRowToLedger. Read/write Sheets scope only when LEDGER_WRITE_ENABLED; LEDGER_WRITE_DRYRUN computes+logs the plan but writes nothing. Default still capture-only. STAGE 3 THE BRIDGE (outflow toggle is a live lock-protected panel switch): on full M+S approval, post the approved item (entity/account/desc parsed from the EXPENSE REQUEST, threaded via the digest map) into the outflow group and register it so a "paid" reply matches back. Toggle OUTFLOW_POST_ENABLED (default OFF), idempotent per expense id. Then STAGE 2 paid-flow Q&A logs it. Still capture-only, NO Sheet write. Base: v2.10.0-s2.3. Tag step now AI-first (aiGuessTag, constrained/validated to LEDGER_TAGS so it can disambiguate e.g. Vrindavan vs Faridabad diesel) with the rule guessTagAndPerson as offline/off-list fallback. Rest of STAGE 2 unchanged. Capture-only, NO Sheet write. Base: v2.10.0-s1.
// v2.8.16 — clear stale Chromium SingletonLock on boot so redeploys self-heal (volume no longer causes 'profile in use' Code 21)
// Adds: smart first-message parsing (extracts company/account from free-form text),
//       multi-amount detection that forces one-at-a-time discipline,
//       vision read confirmation before posting (kills filename-misread bugs),
//       SHOW + EDIT mid-flow commands.
// All v2.6 features preserved unchanged (reverse-scan, MORE commands, top-N report).
// ============================================================
const express = require('express');
const { google } = require('googleapis');
const fetch = require('node-fetch');
const cron = require('node-cron');
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const qrcode = require('qrcode');
const puppeteer = require('puppeteer');
const fs = require('fs');
const initSales = require('./sales'); // v2.11.0-s6.9 sales booking module
const app = express();
app.use(express.json());
const CONFIG = {
  SHEET_ID: process.env.SHEET_ID || '1JDoDEk2smAJu0S3RO1WLPZ4MzGZD-_Kn1pP9K8U0J5w',
  GOOGLE_CREDENTIALS: process.env.GOOGLE_CREDENTIALS,
  WHATSAPP_GROUP_JID: process.env.WHATSAPP_GROUP_JID || '120363425432126351@g.us',
  APPROVAL_GROUP_JID: process.env.APPROVAL_GROUP_JID || '120363408304471879@g.us',
  PAYMENT_OUTFLOW_GROUP_JID: process.env.PAYMENT_OUTFLOW_GROUP_JID || '120363425603031556@g.us',
  INFLOW_GROUP_JID: process.env.INFLOW_GROUP_JID || '120363429672822928@g.us',
  BOT_ENABLED: process.env.BOT_ENABLED !== 'false',
  CLAUDE_API_KEY: process.env.CLAUDE_API_KEY,
  PORT: process.env.PORT || 3000,
  MM_PHONE: '919873095398',
  SM_PHONE: '919873429794',
  ACCOUNTANT_PHONES: ['919873574112','919873574180','919873574192','919873574103','919773592304'],
  TEST_PHONES: ['917838537000'],
  LID_WHITELIST: ['86960253214761@lid'],
  MM_NAMES: ['madhur', 'madhur mittal'],
  SM_NAMES: ['sumit', 'sumit mittal'],
};

// ── v2.6 NEW: Reverse-scan + report tuning constants ─────────────────────────
// Floor amount below which Ledger OUT entries are ignored (treated as petty cash).
var REVERSE_SCAN_MIN_AMOUNT = parseInt(process.env.REVERSE_SCAN_MIN_AMOUNT) || 50000;
// Reverse-scan window in days (default 3).
var REVERSE_SCAN_WINDOW_DAYS = parseInt(process.env.REVERSE_SCAN_WINDOW_DAYS) || 3;
// Top-N caps for the daily report (each section shows this many; rest rolled up).
var REPORT_TOP_N = parseInt(process.env.RECON_TOP_N) || 3;
var STALE_TOP_N = parseInt(process.env.STALE_TOP_N) || 3;
// "Recent" window for stale section in hours (default 72h).
var STALE_RECENT_HOURS = parseInt(process.env.STALE_RECENT_HOURS) || 72;

// Recurring-vendor patterns — Ledger entries matching these are suppressed from
// the "payment without approval" anomaly list. Tune by adding patterns.
var RECURRING_PATTERNS = [
  /bank charges?/i, /\btds\b/i,
  /(mm|sm)\s+drawing/i, /\bmm\s+pdc\b/i, /\bsm\s+pdc\b/i, /drawing\s+(mm|sm)/i,
  /\bpf\b/i, /\besic?\b/i,
  /cash\s*withdrawal/i, /internal transfer/i, /\bcontra\b/i,
  /electricity\s+bill/i, /water\s+bill/i, /property\s+tax/i,
];
// v2.8 Module 6: salary and GST removed from suppression — they always need an
// approval, so an unapproved salary/GST payment is flagged like any other.
// EMI rule: "auto debit" in the description => informational auto-debit (suppressed,
// highlighted separately). EMI WITHOUT "auto debit" => manual payment that needed
// approval => flagged if no approval found.
var EMI_PATTERN = /\bcar\s*emi\b|\bhome\s*loan\b|\bemi\b/i;
var AUTO_DEBIT_PATTERN = /auto\s*-?\s*debit|\becs\b|\bnach\b|standing\s+instruction/i;
function isAutoDebitEntry(le){
  if(!le) return false;
  var text = ((le.description||'') + ' ' + (le.head||'') + ' ' + (le.tag||'') + ' ' + (le.person||'')).toLowerCase();
  return AUTO_DEBIT_PATTERN.test(text);
}
function isRecurringPattern(le) {
  if(!le) return false;
  var text = ((le.description||'') + ' ' + (le.head||'') + ' ' + (le.tag||'') + ' ' + (le.person||'')).toLowerCase();
  if(EMI_PATTERN.test(text)) return AUTO_DEBIT_PATTERN.test(text);
  for(var i=0; i<RECURRING_PATTERNS.length; i++){
    if(RECURRING_PATTERNS[i].test(text)) return true;
  }
  return false;
}

// ── Google Sheets ─────────────────────────────────────────────────────────────
var sheetsApi = null;
function initGoogleSheets() {
  if (!CONFIG.GOOGLE_CREDENTIALS) { console.log('No GOOGLE_CREDENTIALS.'); return; }
  try {
    var creds = JSON.parse(CONFIG.GOOGLE_CREDENTIALS);
    // v2.10.0-s4: request the read/write scope only when ledger writing is enabled.
    // While off (and during dry-run), stay read-only so a bug literally cannot write.
    var scope = (process.env.LEDGER_WRITE_ENABLED === 'true')
      ? 'https://www.googleapis.com/auth/spreadsheets'
      : 'https://www.googleapis.com/auth/spreadsheets.readonly';
    var auth = new google.auth.GoogleAuth({ credentials: creds, scopes: [scope] });
    sheetsApi = google.sheets({ version: 'v4', auth: auth });
    console.log('Google Sheets API initialized ('+(scope.indexOf('readonly')>=0?'read-only':'READ/WRITE')+').');
  } catch (e) { console.error('Sheets init failed:', e.message); }
}
async function readSheet(range) {
  if (!sheetsApi) throw new Error('Google Sheets not initialized');
  var r = await sheetsApi.spreadsheets.values.get({ spreadsheetId: CONFIG.SHEET_ID, range: range });
  return r.data.values || [];
}
// ── WhatsApp ──────────────────────────────────────────────────────────────────
var waClient = null, waReady = false, latestQR = null, latestQRDataUrl = null;
function clearStaleChromiumLocks() {
  // v2.8.16: with a persistent volume, a redeploy leaves the previous container's
  // Chromium lock files behind. The new container then refuses to launch
  // ("profile appears to be in use", Code 21). These lock files are safe to
  // delete on boot — they are NOT the session; the WhatsApp auth lives in
  // ./wa_auth/session/Default and is preserved. This self-heals every redeploy.
  var sessionDir = './wa_auth/session';
  var lockNames = ['SingletonLock','SingletonSocket','SingletonCookie'];
  try {
    lockNames.forEach(function(name){
      var p = sessionDir + '/' + name;
      try { if (fs.existsSync(p) || fs.lstatSync(p)) { fs.rmSync(p, { force: true }); console.log('[WA] cleared stale lock', name); } }
      catch(e){ /* lstat throws if absent — ignore */ }
    });
    // Also clear the Default-profile lock if present.
    ['./wa_auth/session/Default/SingletonLock'].forEach(function(p){
      try { fs.rmSync(p, { force: true }); } catch(e){}
    });
  } catch(e) { console.error('[WA] lock cleanup:', e.message); }
}
function createWhatsAppClient() {
  clearStaleChromiumLocks();
  waClient = new Client({
    authStrategy: new LocalAuth({ dataPath: './wa_auth' }),
    puppeteer: { headless: true, args: ['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage','--disable-accelerated-2d-canvas','--no-first-run','--no-zygote','--single-process','--disable-gpu','--disable-extensions'] },
  });
  waClient.on('qr', function(qr) { latestQR = qr; qrcode.toDataURL(qr, function(err, url) { if (!err) latestQRDataUrl = url; }); console.log('QR generated.'); });
  waClient.on('ready', function() { waReady = true; latestQR = null; latestQRDataUrl = null; console.log('WhatsApp ready!'); });
  waClient.on('authenticated', function() { console.log('WhatsApp authenticated.'); });
  waClient.on('auth_failure', function(msg) { console.error('Auth failure:', msg); waReady = false; });
  waClient.on('disconnected', function(reason) {
    console.log('[WA] Disconnected:', reason);
    waReady = false;
    if (!global._waLogoutLog) global._waLogoutLog = [];
    var now = Date.now();
    if (reason === 'LOGOUT') {
      global._waLogoutLog.push(now);
      global._waLogoutLog = global._waLogoutLog.filter(function(t){ return now - t < 5*60*1000; });
      console.log('[WA] LOGOUT count in 5min window:', global._waLogoutLog.length);
      if (global._waLogoutLog.length >= 3) {
        console.log('[WA] 3+ LOGOUTs in 5min — clearing wa_auth and restarting fresh');
        global._waLogoutLog = [];
        setTimeout(function() {
          try { if (fs.existsSync('./wa_auth')) { fs.rmSync('./wa_auth', { recursive: true, force: true }); console.log('[WA] wa_auth cleared'); } } catch(e) { console.error('[WA] Clear failed:', e.message); }
          createWhatsAppClient();
        }, 5000);
        return;
      }
    }
    setTimeout(function() {
      try { waClient.initialize().catch(function(e) { console.error('[WA] reinit failed:', e.message); }); }
      catch(e) { console.error('[WA] reinit threw:', e.message); }
    }, 10000);
  });
  waClient.on('message', function(msg) {
    sales.handleSalesMessage(msg).then(function(handledSales){   // v2.11.0-s6.9 sales first
      if(handledSales) return;
      return handleInflowFlow(msg).then(function(handledInflow){
        if(handledInflow) return;
        return handlePaidFlow(msg).then(function(handledPaid){
          if(handledPaid) return;
          return handlePromoterVerdicts(msg).then(function(handled){
            if(!handled) return handleAccountantDM(msg);
          });
        });
      });
    }).catch(function(e){ console.error('[Msg handler]', e.message); });
  });
  waClient.initialize().catch(function(e) { console.error('WA init failed:', e.message); });
}
// ── Puppeteer ─────────────────────────────────────────────────────────────────
var browserInstance = null;
async function htmlToImage(html, width, height) {
  if (!browserInstance) { browserInstance = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage'] }); }
  var page = await browserInstance.newPage();
  await page.setViewport({ width: width || 800, height: height || 600 });
  await page.setContent(html, { waitUntil: 'networkidle0' });
  var body = await page.$('body');
  var box = await body.boundingBox();
  var shot = await page.screenshot({ clip: { x: 0, y: 0, width: box.width, height: box.height }, type: 'png' });
  await page.close();
  return shot;
}
// ── Helpers ───────────────────────────────────────────────────────────────────
function parseSheetDate(val) {
  if (!val) return null; if (val instanceof Date) return val;
  var s = val.toString().trim(), p = s.split(/[\/\.\-]/);
  if (p.length === 3) { var d=parseInt(p[0]),m=parseInt(p[1]),y=parseInt(p[2]); if(y<100)y+=2000; if(d>0&&d<=31&&m>0&&m<=12) return new Date(y,m-1,d); }
  var cleaned = s.replace(/,\s*[A-Za-z]+\.?\s*$/,'').trim();      // strip a trailing weekday, e.g. "09 Apr 2026, Thursday"
  if (/^-?\d+(\.\d+)?$/.test(cleaned)) return null;               // a bare number is not a date (avoid 208287 -> year)
  if (!/\b\d{4}\b/.test(cleaned)) return null;                    // require a 4-digit year so "14 Jan" titles don't become phantom dates
  var x = new Date(cleaned); return isNaN(x.getTime()) ? null : x;
}
function parseAmount(val) { if(typeof val==='number')return val; if(!val)return 0; var n=parseFloat(String(val).replace(/,/g,'').replace(/[^0-9.\-]/g,'')); return isNaN(n)?0:n; }
function formatINR(num) {
  if(!num)return '0'; var neg=num<0; num=Math.abs(Math.round(num)); var s=num.toString();
  var l=s.substring(s.length-3), o=s.substring(0,s.length-3);
  if(o!=='')l=','+l; return (neg?'-':'')+o.replace(/\B(?=(\d{2})+(?!\d))/g,',')+l;
}
// ── Sender identification ─────────────────────────────────────────────────────
async function identifySender(rawSender) {
  var role = 'unknown', contactName = '';
  try {
    var contact = await waClient.getContactById(rawSender);
    if (contact) {
      contactName = (contact.pushname || contact.name || contact.shortName || '').toLowerCase().trim();
      for (var i = 0; i < CONFIG.MM_NAMES.length; i++) {
        if (contactName === CONFIG.MM_NAMES[i] || contactName.indexOf(CONFIG.MM_NAMES[i]) >= 0) {
          var isSM = false;
          for (var s = 0; s < CONFIG.SM_NAMES.length; s++) { if (contactName === CONFIG.SM_NAMES[s]) { isSM = true; break; } }
          if (!isSM) { role = 'mm'; break; }
        }
      }
      if (role === 'unknown') {
        for (var j = 0; j < CONFIG.SM_NAMES.length; j++) {
          if (contactName === CONFIG.SM_NAMES[j] || contactName.indexOf(CONFIG.SM_NAMES[j]) >= 0) { role = 'sm'; break; }
        }
      }
    }
  } catch (e) { /* skip */ }
  return { role: role, contactName: contactName };
}
// ── Response parsing ──────────────────────────────────────────────────────────
function parseResponse(text) {
  if(!text)return 'pending'; var l=text.toLowerCase().trim();
  var qWords=['advance to','for whom','kis ke liye','kya','kab','kaun','kitna'];
  for(var q=0;q<qWords.length;q++){if(l.indexOf(qWords[q])>=0)return 'question';}
  if((l.indexOf('?')>=0)&&l.length<40)return 'question';
  var yesExact=['yes','ok','okay','o','approved','done','haan','ha','han','theek hai','thik hai','kar do','karo','y','yep','yea','yeah','sure','fine','agreed','confirmed','sahi hai','bilkul'];
  for(var i=0;i<yesExact.length;i++){if(l===yesExact[i])return 'yes';}
  if(l.indexOf('\u{1F44D}')>=0||l.indexOf('\u2705')>=0||l.indexOf('\u{1F44C}')>=0)return 'yes';
  var yesContains=['ok','okay','approved','haan','theek','kar do','go ahead','proceed','done'];
  for(var ic=0;ic<yesContains.length;ic++){if(l.indexOf(yesContains[ic])>=0)return 'yes';}
  var no=['no','nahi','nah','rejected','cancel','mat karo','nope','deny','denied','reject','nhi','mat','band karo'];
  for(var j=0;j<no.length;j++){if(l===no[j]||l.indexOf(no[j])>=0)return 'no';}
  if(l.indexOf('\u274C')>=0||l.indexOf('\u{1F44E}')>=0)return 'no';
  var hold=['hold','wait','ruko','later','baad mein','not now','pending','rukko','abhi nahi','bad me','kal'];
  for(var k=0;k<hold.length;k++){if(l===hold[k]||l.indexOf(hold[k])>=0)return 'hold';}
  return 'other';
}
// ── Expense message parsing ───────────────────────────────────────────────────
function extractLineAmount(line, strict) {
  if(!line) return 0;
  var am=line.match(/(\d[\d,]*\.?\d*)\s*(?:lakhs?|lacs?|l\b|cr|crore)/i);
  if(am){var a=parseFloat(am[1].replace(/,/g,'')); return /cr|crore/i.test(am[0])?a*10000000:a*100000;}
  var km=line.match(/(\d[\d,]*\.?\d*)\s*k\b/i);
  if(km) return parseFloat(km[1].replace(/,/g,''))*1000;
  var tm=line.match(/(\d[\d,]*\.?\d*)\s*(?:thousands?|hazaar|hazar)\b/i);
  if(tm) return parseFloat(tm[1].replace(/,/g,''))*1000;
  var hm=line.match(/(\d[\d,]*\.?\d*)\s*hundreds?\b/i);
  if(hm) return parseFloat(hm[1].replace(/,/g,''))*100;
  var pm=line.match(/(\d[\d,]{3,})\s*\/\s*\-?/);
  if(pm) return parseFloat(pm[1].replace(/,/g,''));
  var rm=line.match(/(?:rs\.?\s*|inr\s*|\u20B9\s*)(\d[\d,]*\.?\d*)/i);
  if(rm) return parseFloat(rm[1].replace(/,/g,''));
  if(strict) return 0;
  var lm = line.match(/\b(\d{1,3}(?:,\d{2,3}){1,3})\b/);
  if(lm){
    var v = parseFloat(lm[1].replace(/,/g,''));
    if(v >= 100 && v < 1000000000) return v;
  }
  if(/\d{1,2}[\-\/](?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|\d{1,2})[\-\/]\d{2,4}/i.test(line)) return 0;
  var lm2 = line.match(/\b(\d{4,9})\b/);
  if(lm2){
    var v2 = parseFloat(lm2[1]);
    if(v2 >= 100 && v2 < 1000000000){
      if(lm2[1].length === 10 && /^[6-9]/.test(lm2[1])) return 0;
      if(lm2[1].length === 4 && v2 >= 1900 && v2 <= 2100){
        if(!/(amount|paid|payment|approve|due|invoice|bill|expense|cost|fee|rs|inr|\u20B9|\$|lac|lakh|cr|crore|k\b)/i.test(line)) return 0;
      }
      return v2;
    }
  }
  return 0;
}
function extractLineVendor(line) {
  return line
    .replace(/(\d[\d,]*\.?\d*)\s*(?:lakhs?|lacs?|l\b|cr|crore)/i, '')
    .replace(/(\d[\d,]*\.?\d*)\s*k\b/i, '')
    .replace(/(\d[\d,]{3,})\s*\/\s*\-?/, '')
    .replace(/(?:rs\.?\s*|inr\s*|\u20B9\s*)(\d[\d,]*\.?\d*)/i, '')
    .replace(/^\s*(please approve|kindly approve|approve|for|to|on account of|on account|payment to|pay to)\s*/i, '')
    .replace(/\s+/g,' ').trim();
}
// v2.7 NEW: clean up a free-form expense message into a tidy "Details" string.
// Drops field-prefix lines (Amount:/Company:/From:/etc), strips the approval
// preamble ("kindly approve 10 lakh rs for"), removes leftover amount/currency
// tokens, and collapses whitespace. Keeps the meaningful purpose text.
function cleanDetails(body) {
  if(!body) return '';
  var lines = body.split('\n').map(function(l){ return l.trim(); }).filter(Boolean);
  var kept = [];
  lines.forEach(function(line){
    // Skip lines that are purely a structured field we capture elsewhere
    if(/^\s*(amount|company|from|account|a\/c|bank)\s*[:\-]/i.test(line)) return;
    kept.push(line);
  });
  var text = kept.join(' ');
  // Strip leading approval-request phrases (repeat to catch "please kindly approve")
  for(var i=0;i<3;i++){
    text = text.replace(/^\s*(please|kindly|pls|plz|request(?:ing)?(?:\s+(?:you|u))?|i\s+request|sir|ji)\s+/i, '');
    text = text.replace(/^\s*(approve|approval|pay|payment|release|sanction)\s+/i, '');
  }
  // Remove amount + unit tokens. Units ordered LONGEST-FIRST so "crore" is not
  // partially matched as "cr" (which used to leave "ore"). Covers crore(s)/cr,
  // lakh(s)/lac(s), thousand(s), hundred(s), hazaar/hazar (Hindi), k, l.
  // Repeated twice to catch compound amounts like "2 crore 50 lakh".
  for(var u=0;u<2;u++){
    text = text
      .replace(/(?:rs\.?|inr|\u20B9)\s*\d[\d,]*\.?\d*\s*(?:crores?|cr|lakhs?|lacs?|thousands?|hundreds?|hazaar|hazar|k|l)?\/?\-?/gi, '')
      .replace(/\b\d[\d,]*\.?\d*\s*(?:crores?|cr|lakhs?|lacs?|thousands?|hundreds?|hazaar|hazar)\b/gi, '')
      .replace(/\b\d+\.?\d*\s*l\b/gi, '')
      .replace(/\b\d[\d,]*\.?\d*\s*k\b/gi, '')
      .replace(/\b\d{1,3}(?:,\d{2,3}){1,3}\s*\/?\-?/g, '')
      .replace(/\b\d{4,9}\b\s*\/?\-?/g, '');
  }
  text = text.replace(/\brs\b\.?/gi, '');
  // Tidy connector words left dangling at the start ("for legal expenses" -> "legal expenses")
  for(var j=0;j<3;j++){
    text = text.replace(/^\s*(payment|paid|pay)\s+/i, '');
    text = text.replace(/^\s*(for|to|towards|of|on account of|on account|against)\s+/i, '');
  }
  // Collapse whitespace and stray punctuation
  text = text.replace(/\s{2,}/g, ' ').replace(/\s+([,.])/g, '$1').replace(/^[\s,.\-]+|[\s,.\-]+$/g, '').trim();
  // Capitalize first letter for a tidy look
  if(text.length > 0) text = text.charAt(0).toUpperCase() + text.slice(1);
  return text.substring(0, 250);
}
function parseExpenseMessage(body) {
  if(!body) return [{vendor:'',amount:0}];
  // v2.8.13: strip a leading WhatsApp @number mention (e.g. "@77975215149179 ")
  // so free-text "pay" requests posted straight to the group still parse cleanly.
  body = body.replace(/^\s*@\d{6,}\s*/,'').trim();
  // v2.8: the accountant bot posts a structured "*EXPENSE REQUEST*" block.
  // Read its Details:/Amount: lines instead of grabbing the header as vendor.
  if(/^\s*\*?EXPENSE REQUEST\*?/i.test(body)){
    var dl = body.match(/Details:\s*(.+)/i);
    var subRe = /^\s*-\s*(.+?)\s+Rs\.?\s*([\d,\.]+)/gim, subM, subs=[];
    while((subM = subRe.exec(body))!==null){ subs.push({vendor:subM[1].trim(), amount:parseAmount(subM[2])}); }
    if(subs.length>1) return subs;
    var am = body.match(/Amount:\s*Rs\.?\s*([\d,\.]+)/i);
    var vend = dl ? dl[1].trim() : '';
    var amt = am ? parseAmount(am[1]) : 0;
    if(!amt){ // fall through to scan if Amount line missing
      var t2=0; body.split('\n').forEach(function(l){ t2+=extractLineAmount(l,false); });
      amt=t2;
    }
    return [{vendor:vend.substring(0,150), amount:amt}];
  }
  var lines=body.split('\n').map(function(l){return l.trim();}).filter(Boolean);
  var itemLines=[];
  for(var i=0;i<lines.length;i++){
    var a=extractLineAmount(lines[i], true);
    if(a>0){var v=extractLineVendor(lines[i]); if(v&&v.length>1)itemLines.push({vendor:v,amount:a});}
  }
  if(itemLines.length>1) return itemLines;
  // v2.8.13: handle inline sums like "pay 31,200+5,000" on a single line — add the
  // parts rather than capturing only the first number.
  var total=0;
  for(var j=0;j<lines.length;j++){
    var plusMatch = lines[j].match(/(\d[\d,]*)\s*\+\s*(\d[\d,]*)/);
    if(plusMatch){
      total += parseAmount(plusMatch[1]) + parseAmount(plusMatch[2]);
    } else {
      total += extractLineAmount(lines[j], false);
    }
  }
  var rawVendor = extractLineVendor(lines[0]) || lines[0].substring(0,150);
  var vendor = (typeof cleanDetails==='function') ? (cleanDetails(rawVendor)||rawVendor) : rawVendor;
  return [{vendor:vendor,amount:total}];
}
// v2.10.0-s3: pull the structured fields (Company / From / Details) out of an EXPENSE
// REQUEST body so the approved item can carry entity + paying account + description into
// the outflow post and the final ledger row. Free-text expenses (no Company/From) return
// blanks, which the accountant sees in the posted message.
function parseExpenseFields(body){
  var out = { entity:'', bankAc:'', description:'' };
  if(!body) return out;
  var mC = body.match(/^\s*Company:\s*(.+)$/im); if(mC) out.entity = mC[1].trim();
  var mF = body.match(/^\s*From:\s*(.+)$/im);    if(mF) out.bankAc = mF[1].trim();
  var mD = body.match(/^\s*Details:\s*(.+)$/im); if(mD) out.description = mD[1].trim();
  return out;
}
// ── Vision (image + PDF) ──────────────────────────────────────────────────────
const visionCache = new Map();
async function extractFromImage(media, msgId) {
  if (msgId && visionCache.has(msgId)) return visionCache.get(msgId);
  if (!CONFIG.CLAUDE_API_KEY) { console.error('[Vision] CLAUDE_API_KEY missing'); return null; }
  if (!media || !media.data) return null;
  var mime = (media.mimetype || '').toLowerCase();
  if (mime.indexOf('image/') !== 0 && mime !== 'application/pdf') return null;
  try {
    var isPDF = mime === 'application/pdf';
    var mediaBlock = isPDF
      ? { type: 'document', source: { type: 'base64', media_type: 'application/pdf', data: media.data } }
      : { type: 'image', source: { type: 'base64', media_type: mime, data: media.data } };
    var prompt = isPDF
      ? 'This PDF is attached to an expense approval request. It is likely an invoice, PO, bill, or payment challan. Extract: (1) vendor/payee name, (2) total amount in INR as a number, (3) brief purpose max 10 words, (4) amountCount = how many DISTINCT separate payable amounts/invoices appear (1 for a single bill, 2+ if multiple separate bills or payment items). Do NOT treat a single itemized invoice as multiple — that is amountCount 1 with the grand total. Reply ONLY with JSON on one line: {"vendor":"","amount":0,"purpose":"","imageType":"invoice","confidence":"high","amountCount":1}.'
      : 'This image is attached to an expense approval request. Classify it: imageType = "cheque" (any bank cheque even cancelled), "invoice" (printed bill), "receipt", "screenshot", or "other". For CHEQUES: set vendor to "" and amount to 0 — they are shared as bank reference only, not expense amounts. For printed INVOICES/RECEIPTS: extract vendor, total amount in INR, and purpose. Also set amountCount = how many DISTINCT separate payable amounts/invoices appear in the image (1 for a single bill, 2+ if it shows multiple separate bills or a list of multiple payments). A single itemized invoice with line items is amountCount 1 (the grand total). Set confidence to "high" if clearly printed, "low" if handwritten or blurry. Reply ONLY with JSON on one line: {"vendor":"","amount":0,"purpose":"","imageType":"cheque","confidence":"low","amountCount":1}.';
    var resp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': CONFIG.CLAUDE_API_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ model: 'claude-sonnet-4-6', max_tokens: 300, messages: [{ role: 'user', content: [mediaBlock, { type: 'text', text: prompt }] }] })
    });
    if (!resp.ok) { console.error('[Vision] API error', resp.status); return null; }
    var data = await resp.json();
    var text = '';
    if (data.content) { for (var i=0;i<data.content.length;i++) { if(data.content[i].type==='text'){text=data.content[i].text;break;} } }
    if (!text) return null;
    text = text.replace(/```json|```/g, '').trim();
    var parsed; try { parsed = JSON.parse(text); } catch(e) { var m=text.match(/\{[^}]*\}/); if(m){try{parsed=JSON.parse(m[0]);}catch(e2){return null;}} else return null; }
    var result = { vendor:(parsed.vendor||'').toString().substring(0,150), amount:parseAmount(parsed.amount), purpose:(parsed.purpose||'').toString().substring(0,200), imageType:parsed.imageType||'other', confidence:parsed.confidence||'low', amountCount:parseInt(parsed.amountCount)||1 };
    if (msgId) visionCache.set(msgId, result);
    console.log('[Vision] Parsed', msgId, '->', JSON.stringify(result));
    return result;
  } catch (e) { console.error('[Vision] Exception:', e.message); return null; }
}
// ── Approval audit ────────────────────────────────────────────────────────────
async function fetchApprovalMessages(days) {
  if(!waReady||!waClient)throw new Error('WhatsApp not connected');
  var chat = await waClient.getChatById(CONFIG.APPROVAL_GROUP_JID);
  var allMessages = [];
  var limits = [100,200,500,1000];
  for(var i=0;i<limits.length;i++){
    try{allMessages=await chat.fetchMessages({limit:limits[i]}); if(allMessages.length<limits[i])break;}catch(e){break;}
  }
  var cutoff=new Date(); cutoff.setDate(cutoff.getDate()-(days||15));
  return allMessages.filter(function(m){return new Date(m.timestamp*1000)>=cutoff;});
}
async function buildApprovalAudit(days) {
  var messages = await fetchApprovalMessages(days||15);
  var expenses = [], replyMap = {};
  var questionMessages = {};
  var answerMap = {};
  var digestQuotedApprovals = []; // v2.8.6: swipe-replies that quoted a digest/reminder
  for(var i=0;i<messages.length;i++){
    var msg=messages[i];
    var rawSender=msg.author||msg.from||'';
    var msgDate=new Date(msg.timestamp*1000);
    var body=(msg.body||'').trim();
    var hasMedia=msg.hasMedia||false;
    var senderInfo = await identifySender(rawSender);
    var thisMsgId = msg.id._serialized||msg.id.id;
    var quotedMsgId=null, quotedBody=null;
    if(msg.hasQuotedMsg){try{var q=await msg.getQuotedMessage();quotedMsgId=q.id._serialized||q.id.id;quotedBody=q.body||'';}catch(e){}}
    // v2.8.5: if the reply quotes one of the bot's own digest/reminder messages, the
    // promoter is approving the expense(s) that message was ABOUT — redirect the
    // quotedMsgId to the real expense id so a swipe-"ok" on the reminder registers.
    if(quotedMsgId && quotedBody && /PENDING APPROVALS|🔔|\[BOT REMINDER\]|⚡\s*URGENT|URGENT\s*—\s*approval needed/i.test(quotedBody)){
      // Defer resolution: stash the approval with the quoted digest text, and after all
      // expenses are built, match it to a live expense by content (id-drift safe).
      digestQuotedApprovals.push({ role: senderInfo.role, resp: parseResponse(body), raw: body, date: msgDate, name: senderInfo.contactName, quotedBody: quotedBody });
      quotedMsgId = null; // don't process via the normal id path
    }
    if(quotedMsgId){
      var resp=parseResponse(body);
      if(questionMessages[quotedMsgId]){
        var qInfo = questionMessages[quotedMsgId];
        var answerFromOtherPromoter = (qInfo.role==='mm' && senderInfo.role==='sm') || (qInfo.role==='sm' && senderInfo.role==='mm');
        var answerFromAccountant = senderInfo.role!=='mm' && senderInfo.role!=='sm';
        if(answerFromOtherPromoter || answerFromAccountant){
          var promoterReplyShort = answerFromOtherPromoter && (parseResponse(body)==='yes' || parseResponse(body)==='no' || parseResponse(body)==='hold');
          if(!promoterReplyShort){
            answerMap[qInfo.expenseId] = {
              role: qInfo.role,
              question: qInfo.question,
              questionDate: qInfo.date,
              answer: body,
              answerDate: msgDate,
              answerBy: senderInfo.contactName || rawSender,
              answerByRole: answerFromOtherPromoter ? (senderInfo.role==='mm' ? 'M' : 'S') : 'accountant'
            };
            continue;
          }
        }
      }
      if(!replyMap[quotedMsgId])replyMap[quotedMsgId]={mm:null,sm:null};
      if(senderInfo.role==='mm'){
        replyMap[quotedMsgId].mm={response:resp,date:msgDate,raw:body,name:senderInfo.contactName,msgId:thisMsgId};
        if(resp==='question'){
          questionMessages[thisMsgId] = { expenseId: quotedMsgId, role: 'mm', question: body, date: msgDate, name: senderInfo.contactName };
        }
      } else if(senderInfo.role==='sm'){
        replyMap[quotedMsgId].sm={response:resp,date:msgDate,raw:body,name:senderInfo.contactName,msgId:thisMsgId};
        if(resp==='question'){
          questionMessages[thisMsgId] = { expenseId: quotedMsgId, role: 'sm', question: body, date: msgDate, name: senderInfo.contactName };
        }
      }
    } else {
      // v2.8: never ingest the bot's own posts as expenses (digest, urgent, reminder).
      if(body.indexOf('[BOT REMINDER]')===0){continue;}
      if(body.indexOf('🔔')===0 || /PENDING APPROVALS/i.test(body)){continue;}
      if(/^\s*\*?URGENT\b/i.test(body) && /please review/i.test(body)){continue;}
      if(/^\s*Recorded from [MS]:/i.test(body)){continue;}
      if(senderInfo.role!=='mm'&&senderInfo.role!=='sm'){
        var msgId=thisMsgId;
        var parsedItems=parseExpenseMessage(body);
        var vendor=parsedItems[0].vendor, amount=parsedItems[0].amount, purpose='', visionResult=null;
        var subItems=parsedItems.length>1?parsedItems:null;
        if(hasMedia){
          try{
            var media=await msg.downloadMedia();
            if(media&&media.data){
              visionResult=await extractFromImage(media,msgId);
              if(visionResult){
                var isCheque=visionResult.imageType==='cheque', isLow=visionResult.confidence==='low';
                if(!isCheque){
                  if(amount===0 && visionResult.amount>0) amount=visionResult.amount;
                  if(!isLow && (!vendor||body.length<15) && visionResult.vendor) vendor=visionResult.vendor;
                  if(!isLow && visionResult.purpose) purpose=visionResult.purpose;
                }
              }
            }
          }catch(e){console.error('[Vision] Failed for',msgId,e.message);}
        }
        // v2.8.17: only capture as an expense if it is a structured EXPENSE REQUEST,
        // OR carries a real amount, OR is media that vision parsed into an amount.
        // Zero-amount free text (leave notes, "From X account" orphans, bare images
        // with no parse) is NOT an expense and is skipped at intake.
        var isExpenseRequest = /^\s*\*?EXPENSE REQUEST\*?/i.test(body);
        var hasParsedMediaAmount = hasMedia && visionResult && visionResult.amount > 0;
        var capture = isExpenseRequest || amount > 0 || hasParsedMediaAmount;
        if(capture){
          expenses.push({id:msgId,date:msgDate,body:body||(hasMedia?'[Image attached]':'[Empty]'),sender:senderInfo.contactName||rawSender,vendor:vendor||(hasMedia?'[See image]':''),amount:amount,purpose:purpose,subItems:subItems,hasMedia:hasMedia,visionParsed:visionResult?true:false,mmApproval:null,smApproval:null,status:{mm:'pending',sm:'pending'},queryAnswer:null});
        }
      }
    }
  }
  // v2.8.11: resolve bot-post-quoted replies against the actual expenses now that
  // they're all built. Parse the quoted post into its item(s); resolve each item by
  // label words + amount. Digest quotes apply to every item they listed; single-item
  // posts (reminder/urgent) must resolve to exactly one expense or are skipped.
  // Only clear verdicts (yes/no/hold) are applied retroactively from history.
  var DQ_STOP = ['the','and','for','with','from','this','that','please','approve','kindly','payment','amount','pending','approvals','approval','request','requests','needs','reply','number','yes','hold','later','reject','rs','inr','total','both'];
  function dqMatchExpenses(label, amount){
    var w = (label||'').toLowerCase().replace(/…\s*$/,'').split(/[^a-z0-9]+/).filter(function(x){ return x.length>=4 && DQ_STOP.indexOf(x)<0; });
    if(!w.length) return [];
    var cands = expenses.filter(function(e){
      var ev = ((e.vendor||'')+' '+(e.body||'')).toLowerCase();
      return w.some(function(word){ return ev.indexOf(word)>=0; });
    });
    if(cands.length>1 && amount>0){
      var nar = cands.filter(function(e){ return e.amount===amount; });
      if(nar.length) cands = nar;
    }
    return cands;
  }
  digestQuotedApprovals.forEach(function(dq){
    if(dq.role!=='mm' && dq.role!=='sm') return;
    if(dq.resp!=='yes' && dq.resp!=='no' && dq.resp!=='hold') return;
    var post = parseBotPostItems(dq.quotedBody);
    var resolvedIds = [];
    if(post && post.items && post.items.length){
      post.items.forEach(function(qi){
        var c = dqMatchExpenses(qi.label, qi.amount);
        if(c.length===1 && resolvedIds.indexOf(c[0].id)<0) resolvedIds.push(c[0].id);
      });
    } else {
      // Fallback (unparseable bot post): whole-body words, require exactly one match.
      var qWords = (dq.quotedBody||'').toLowerCase().split(/[^a-z0-9]+/).filter(function(w){ return w.length>=4 && DQ_STOP.indexOf(w)<0; });
      if(qWords.length){
        var matches = expenses.filter(function(e){
          var ev = ((e.vendor||'')+' '+(e.body||'')).toLowerCase();
          return qWords.some(function(w){ return ev.indexOf(w)>=0; });
        });
        if(matches.length>1){
          var amtM = (dq.quotedBody||'').match(/rs\.?\s*([\d,]+)/ig) || [];
          var qAmts = amtM.map(function(s){ return parseAmount(s); }).filter(function(n){ return n>0; });
          if(qAmts.length){
            var narrowed = matches.filter(function(e){ return qAmts.indexOf(e.amount)>=0; });
            if(narrowed.length===1) matches = narrowed;
          }
        }
        if(matches.length===1) resolvedIds.push(matches[0].id);
      }
    }
    resolvedIds.forEach(function(eid){
      if(!replyMap[eid]) replyMap[eid]={mm:null,sm:null};
      if(dq.role==='mm' && !replyMap[eid].mm) replyMap[eid].mm={response:dq.resp,date:dq.date,raw:dq.raw,name:dq.name,viaDigest:true};
      if(dq.role==='sm' && !replyMap[eid].sm) replyMap[eid].sm={response:dq.resp,date:dq.date,raw:dq.raw,name:dq.name,viaDigest:true};
    });
  });
  for(var j=0;j<expenses.length;j++){
    var rep=replyMap[expenses[j].id];
    if(rep){expenses[j].mmApproval=rep.mm;expenses[j].smApproval=rep.sm;expenses[j].status.mm=rep.mm?rep.mm.response:'pending';expenses[j].status.sm=rep.sm?rep.sm.response:'pending';}
    if(answerMap[expenses[j].id]){
      expenses[j].queryAnswer = answerMap[expenses[j].id];
    }
  }
  function descriptionsRelate(visionData, textBody) {
    if(!visionData || !textBody) return false;
    var visionText = ((visionData.purpose||'') + ' ' + (visionData.vendor||'')).toLowerCase();
    var stopWords = ['the','and','for','with','from','this','that','please','approve','kindly','payment','amount','rs','inr','total'];
    var textWords = textBody.toLowerCase().split(/[^a-z0-9]+/).filter(function(w){
      return w.length >= 3 && stopWords.indexOf(w) < 0;
    });
    var visionWords = visionText.split(/[^a-z0-9]+/);
    for(var w=0; w<textWords.length; w++) {
      if(visionWords.indexOf(textWords[w]) >= 0) return true;
    }
    return false;
  }
  var dedupedIds = {};
  for(var di=0; di<expenses.length; di++) {
    var mediaExp = expenses[di];
    if(!mediaExp.hasMedia || !mediaExp.visionParsed || mediaExp.amount <= 0) continue;
    if(mediaExp.mmApproval || mediaExp.smApproval) continue;
    for(var dj=0; dj<expenses.length; dj++) {
      if(di === dj) continue;
      var textExp = expenses[dj];
      if(textExp.hasMedia && (!textExp.body || textExp.body.length < 10)) continue;
      if(textExp.sender !== mediaExp.sender) continue;
      if(Math.abs(textExp.amount - mediaExp.amount) > 1) continue;
      var timeDelta = Math.abs(textExp.date.getTime() - mediaExp.date.getTime());
      if(timeDelta > 10 * 60 * 1000) continue;
      var cached = visionCache.get(mediaExp.id);
      if(cached && !descriptionsRelate(cached, textExp.body)) continue;
      if(!textExp.supportingDocs) textExp.supportingDocs = [];
      textExp.supportingDocs.push({
        filename: mediaExp.body || '[Attachment]',
        amount: mediaExp.amount,
        vendor: cached ? cached.vendor : '',
        purpose: cached ? cached.purpose : (mediaExp.purpose || ''),
        msgId: mediaExp.id
      });
      dedupedIds[mediaExp.id] = textExp.id;
      console.log('[Dedup] Merged', mediaExp.id, '->', textExp.id, '(', mediaExp.amount, ')');
      break;
    }
  }
  var consolidatedExpenses = expenses.filter(function(e){ return !dedupedIds[e.id]; });
  applyVerdictOverrides(consolidatedExpenses); // v2.8: numbered-reply verdicts + state/approvedAmount
  assignClusters(consolidatedExpenses);        // v2.8: silent re-ask clustering
  var result={fullyApproved:[],partialApproval:[],noApproval:[],onHold:[],rejected:[],allExpenses:consolidatedExpenses,totalExpenses:consolidatedExpenses.length,totalMessages:messages.length,fetchedDays:days||15,visionCacheSize:visionCache.size,dedupedCount:Object.keys(dedupedIds).length};
  for(var k=0;k<consolidatedExpenses.length;k++){
    var e=consolidatedExpenses[k],mm=e.status.mm,sm=e.status.sm;
    if(mm==='no'||sm==='no')result.rejected.push(e);
    else if(mm==='hold'||sm==='hold')result.onHold.push(e);
    else if(mm==='yes'&&sm==='yes')result.fullyApproved.push(e);
    else if(mm==='yes'||sm==='yes')result.partialApproval.push(e);
    else result.noApproval.push(e);
  }
  return result;
}
// ── Pending reminders ─────────────────────────────────────────────────────────
function getDaysPending(date) {
  return Math.max(1, Math.floor((new Date() - date) / (1000*60*60*24)));
}
function buildReminderText(expense) {
  var mm=expense.status.mm, sm=expense.status.sm;
  var bothPending=mm==='pending'&&sm==='pending';
  var mmOnly=mm==='pending'&&sm==='yes';
  var smOnly=sm==='pending'&&mm==='yes';
  var queryMM=mm==='question', querySM=sm==='question';
  var queryAnswered = expense.queryAnswer ? true : false;
  var header;
  if((queryMM||querySM) && queryAnswered) header='[BOT REMINDER] - Query answered - awaiting M+S approval';
  else if(queryMM||querySM) header='[BOT REMINDER] - Query unanswered - '+getDaysPending(expense.date)+' day(s) pending';
  else if(bothPending) header='[BOT REMINDER] - Approval needed';
  else if(mmOnly) header='[BOT REMINDER] - M approval needed';
  else if(smOnly) header='[BOT REMINDER] - S approval needed';
  else return null;
  var lines=[header,''];
  var vendor=expense.vendor||expense.body.substring(0,60);
  lines.push(vendor);
  if(expense.subItems&&expense.subItems.length>1){
    var total=expense.subItems.reduce(function(s,it){return s+it.amount;},0);
    lines.push('Amount: Rs.'+formatINR(total)+' total');
    expense.subItems.forEach(function(si){lines.push('  - '+si.vendor+' Rs.'+formatINR(si.amount));});
  } else if(expense.amount>0){
    lines.push('Amount: Rs.'+formatINR(expense.amount));
  }
  var d=expense.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'});
  var t=expense.date.toLocaleTimeString('en-IN',{hour:'2-digit',minute:'2-digit',timeZone:'Asia/Kolkata'});
  lines.push('Requested by: '+expense.sender+' - '+d+', '+t);
  if(expense.supportingDocs && expense.supportingDocs.length > 0){
    var docNames = expense.supportingDocs.map(function(d){ return d.filename; }).join(', ');
    lines.push('Supporting docs: '+docNames);
  }
  lines.push('');
  var mmLabel=mm==='yes'?'M: Ok':mm==='question'?'M: query raised':'M: pending';
  var smLabel=sm==='yes'?'S: Ok':sm==='question'?'S: query raised':'S: pending';
  lines.push(mmLabel+' | '+smLabel);
  if((queryMM||querySM) && queryAnswered){
    var ans = expense.queryAnswer;
    var who = ans.role==='mm'?'M':'S';
    var answerLabel = ans.answerByRole && ans.answerByRole !== 'accountant' ? (ans.answerByRole + ' (' + ans.answerBy + ')') : ans.answerBy;
    lines.push('');
    lines.push(who+' asked:');
    lines.push('"'+ans.question+'"');
    lines.push('');
    lines.push(answerLabel+' answered:');
    lines.push('"'+ans.answer+'"');
    lines.push('');
    lines.push(who==='M'?'Madhur sir, please confirm to approve':'Sumit sir, please confirm to approve');
  }
  else if(queryMM&&expense.mmApproval){lines.push('');lines.push('M asked:');lines.push('"'+expense.mmApproval.raw+'"');lines.push('');lines.push('Please answer M\'s query to proceed');}
  else if(querySM&&expense.smApproval){lines.push('');lines.push('S asked:');lines.push('"'+expense.smApproval.raw+'"');lines.push('');lines.push('Please answer S\'s query to proceed');}
  else if(mmOnly){lines.push('');lines.push('Madhur sir, please swipe-reply Ok to approve');}
  else if(smOnly){lines.push('');lines.push('Sumit sir, please swipe-reply Ok to approve');}
  else{lines.push('');lines.push('Please swipe-reply Ok to approve');}
  return lines.join('\n');
}
async function sendPendingReminders() {
  if(!waReady){console.log('[Reminders] WA not connected');return 0;}
  if(loadSilentMode()){console.log('[Reminders] silent mode ON — skipping group reminders');return 0;}
  try {
    var audit=await buildApprovalAudit(15);
    var toRemind=audit.partialApproval.concat(audit.noApproval).filter(function(e){
      return e.amount>0||(e.subItems&&e.subItems.length>0);
    });
    console.log('[Reminders] Sending',toRemind.length,'reminders');
    var delay=function(ms){return new Promise(function(r){setTimeout(r,ms);});};
    for(var i=0;i<toRemind.length;i++){
      var text=buildReminderText(toRemind[i]);
      if(!text)continue;
      await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID,text);
      await delay(2000);
    }
    console.log('[Reminders] Done');
    return toRemind.length;
  } catch(e){console.error('[Reminders] Error:',e.message);return 0;}
}

// ── v2.7.2 NEW: twice-daily consolidated approval reminder digest ────────────
// Posts ONE message to the approval group listing all pending approvals that are
// 0–14 days old with a real amount. Tags M and S once at the bottom.
// Bypasses silent mode (these are the scheduled digests the user explicitly wants).
var REMINDER_MAX_AGE_DAYS = parseInt(process.env.REMINDER_MAX_AGE_DAYS) || 14;
// v2.7.7 NEW: fresh-start cutoff. Requests posted BEFORE this date are excluded
// from the reminder digest and urgent lists (they remain in the audit + sheet).
// Set via Railway env var REPORT_START_DATE (YYYY-MM-DD, IST). Default: 2026-06-10.
var REPORT_START_DATE = process.env.REPORT_START_DATE || '2026-06-10';
var REPORT_START_MS = new Date(REPORT_START_DATE + 'T00:00:00+05:30').getTime();
// v2.7.3 NEW: detect M/S capital-contribution entries — these are not vendor
// payments needing approval, so they must not appear in the reminder digest.
// Matches "contribution" / "contributiin" (common typo) / "capital" together with
// an M/S / promoter / drawing reference, OR an explicit "MM/SM contribution".
function isContributionEntry(e) {
  var text = ((e.vendor||'') + ' ' + (e.body||'') + ' ' + (e.details||'') + ' ' + (e.purpose||'')).toLowerCase();
  var mentionsContribution = /contribut|capital\s+contribut/.test(text);
  if(!mentionsContribution) return false;
  // Confirm it's tied to the promoters / their own capital, not a vendor named "...contribution"
  var promoterRef = /\bmm\b|\bsm\b|\bm\b|\bs\b|madhur|sumit|partner|promoter|drawing|own/.test(text);
  return mentionsContribution && promoterRef;
}
// ══ v2.8 — verdict state (numbered-reply approvals) ══════════════════════════
var VERDICT_FILE = './wa_auth/verdict_overrides.json';   // {expenseId:{mm:{verdict,amount,raw,at},sm:{...}}}
var DIGEST_MAP_FILE = './wa_auth/digest_map.json';       // {at, items:[{n,id,label,amount,sender}]}
function loadVerdicts(){ try{ if(fs.existsSync(VERDICT_FILE)) return JSON.parse(fs.readFileSync(VERDICT_FILE,'utf8')); }catch(e){} return {}; }
function saveVerdicts(v){ try{ fs.writeFileSync(VERDICT_FILE, JSON.stringify(v,null,1)); }catch(e){ console.error('[Verdicts] save:',e.message);} }
function loadDigestMap(){ try{ if(fs.existsSync(DIGEST_MAP_FILE)) return JSON.parse(fs.readFileSync(DIGEST_MAP_FILE,'utf8')); }catch(e){} return null; }
function saveDigestMap(m){ try{ fs.writeFileSync(DIGEST_MAP_FILE, JSON.stringify(m,null,1)); }catch(e){ console.error('[DigestMap] save:',e.message);} }

// ── v2.9.0 persist-on-event store (Railway volume, append-only) ──────────────
// Records approval/verdict/paid events the MOMENT they happen, so the bot's record
// no longer depends on re-reading WhatsApp history. ADDITIVE ONLY in this build:
// the bot still reads from chat history as before. This store runs in parallel so
// it can be validated against the chat-history audit before any read-path cutover.
var EVENT_STORE_FILE = './wa_auth/event_store.json';   // {version, createdAt, events:[{seq,type,at,...}]}
var _eventStoreCache = null;
function loadEventStore(){
  if(_eventStoreCache) return _eventStoreCache;
  try{ if(fs.existsSync(EVENT_STORE_FILE)){ _eventStoreCache = JSON.parse(fs.readFileSync(EVENT_STORE_FILE,'utf8')); } }catch(e){ console.error('[Events] load:',e.message); }
  if(!_eventStoreCache || !Array.isArray(_eventStoreCache.events)){
    _eventStoreCache = { version:1, createdAt:new Date().toISOString(), events:[] };
  }
  return _eventStoreCache;
}
function _persistEventStore(){
  try{
    if(!fs.existsSync('./wa_auth')) fs.mkdirSync('./wa_auth',{recursive:true});
    fs.writeFileSync(EVENT_STORE_FILE, JSON.stringify(_eventStoreCache));
  }catch(e){ console.error('[Events] save:',e.message); }
}
// Append one event. type: 'verdict' | 'approved' | 'paid'. data: type-specific fields.
// dedupeKey (optional): if an event with the same dedupeKey already exists, skip (idempotency).
function recordEvent(type, data, dedupeKey){
  try{
    var store = loadEventStore();
    if(dedupeKey){
      for(var i=store.events.length-1;i>=0 && i>store.events.length-500;i--){
        if(store.events[i].dedupeKey===dedupeKey) return false; // already recorded
      }
    }
    var ev = { seq: store.events.length+1, type: type, at: new Date().toISOString() };
    if(dedupeKey) ev.dedupeKey = dedupeKey;
    for(var k in data){ if(Object.prototype.hasOwnProperty.call(data,k)) ev[k]=data[k]; }
    store.events.push(ev);
    _persistEventStore();
    return true;
  }catch(e){ console.error('[Events] record:',e.message); return false; }
}
// Convenience recorders for the three event types.
function recordVerdictEvent(itemId, label, amount, role, verdict, amendAmount, raw){
  return recordEvent('verdict', {
    itemId: itemId, label: label, amount: amount,
    role: role, party: (role==='mm'?'M':'S'),
    verdict: verdict, amendAmount: amendAmount||0,
    raw: (raw||'').substring(0,200)
  }, 'verdict:'+itemId+':'+role+':'+verdict+':'+(amendAmount||0));
}
// ── v2.11.0-s6.1: Payable code P-YYMMDD-NNN (daily-reset counter, IST date) ──
// Minted ONCE at full M+S approval and stored on the approved event. It is the durable
// key carried into the ledger ([bot:P-...]) so reconciliation is an exact join, never a
// fuzzy match. Counter is derived from existing approved-event codes for that IST day
// (single-threaded event store => no double-issue). NOT shown to accountants.
function mintPayableCode(atMs, store){
  store = store || loadEventStore();
  var d = new Date((atMs||Date.now()) + 5.5*3600000);   // shift to IST, then read UTC parts
  var yy=String(d.getUTCFullYear()).slice(-2), mm=('0'+(d.getUTCMonth()+1)).slice(-2), dd=('0'+d.getUTCDate()).slice(-2);
  var prefix='P-'+yy+mm+dd+'-', max=0;
  (store.events||[]).forEach(function(e){
    if(e.type==='approved' && e.code && e.code.indexOf(prefix)===0){
      var n=parseInt(e.code.slice(prefix.length),10); if(!isNaN(n) && n>max) max=n;
    }
  });
  return prefix + ('00'+(max+1)).slice(-3);
}
function findApprovedEvent(store, itemId){
  var evs=(store && store.events || []).filter(function(e){ return e.type==='approved' && e.itemId===itemId; });
  return evs.length ? evs[evs.length-1] : null;
}
// The Payable code for an item (null if the approval has no code yet, e.g. a TEST item).
function payableCodeFor(itemId){ var e=findApprovedEvent(loadEventStore(), itemId); return (e && e.code) || null; }
function recordApprovedEvent(itemId, label, amount, atMs){
  var store=loadEventStore();
  if(findApprovedEvent(store, itemId)) return false;   // already approved — keeps its existing code (idempotent)
  var code=mintPayableCode(atMs||Date.now(), store);
  return recordEvent('approved', { itemId:itemId, label:label, amount:amount, code:code }, 'approved:'+itemId);
}
function recordPaidEvent(itemId, label, paidAmount, fields, seq){
  seq = seq || 1;
  return recordEvent('paid', {
    itemId:itemId, label:label, paidAmount:paidAmount, seq:seq,
    date: fields&&fields.date, mode: fields&&fields.mode, head: fields&&fields.head,
    tag: fields&&fields.tag, person: fields&&fields.person, transferTo: fields&&fields.transferTo,
    entity: fields&&fields.entity, bankAc: fields&&fields.bankAc
  }, 'paid:'+itemId+'#'+seq);   // v2.10.0-s5.14: per-instalment dedupe key allows part payments
}
// v2.10.0-s5.14: cumulative paid stats for one item across all its instalments.
// Returns total paid, instalment count (= next seq - 1), and the most recent paid event.
function paidStatsForItem(store, itemId){
  var total=0, count=0, last=null;
  (store && store.events || []).forEach(function(e){
    if(e.type==='paid' && e.itemId===itemId){ total += (e.paidAmount||0); count++; last=e; }
  });
  return { total:total, count:count, last:last };
}
// v2.10.0-s5.14: close/cancel an approved item "as-is" — freezes it at whatever cash has
// actually gone out and stops tracking the rest. Covers under-settlement (₹1L approved, ₹90k
// paid, done) and cancellation (approved, ₹0 paid, won't be paid). One closed event per item.
function recordClosedEvent(itemId, label, approvedAmount, paidAmount){
  return recordEvent('closed', {
    itemId:itemId, label:label, approvedAmount:approvedAmount||0, paidAmount:paidAmount||0,
    writeOff: Math.max(0, (approvedAmount||0)-(paidAmount||0))
  }, 'closed:'+itemId);
}
function isClosed(store, itemId){
  return (store && store.events || []).some(function(e){ return e.type==='closed' && e.itemId===itemId; });
}

// ── v2.10.0 Payment Outflow flow — stage 1 helpers (capture-only, no Sheet write) ──
// Real Ledger taxonomy (from the live sheet audit).
// v2.10.0-s5.16: Tag = the verified Ledger TAG dropdown (project cost-codes), exact order from the
// live sheet (screenshots 17 Jun 2026). '—' is the blank/none option. Validated for the numbered
// picker (step 5) and to constrain a typed tag.
var LEDGER_TAGS = ['FBD-Contractor','FBD-Steel','FBD-RMC','FBD-Exterior','FBD-STP','FBD-Road','FBD-SCO','FBD-Electricity','FBD-Diesel','FBD-Other',
  'VRN-Contractor','VRN-Steel','VRN-RMC','VRN-Electricity','VRN-Site','VRN-Other',
  'FBD-Floor','FBD-Plot','FBD-Other-Collection','VRN-Floor','VRN-Plot','VRN-Other-Collection','—'];
var LEDGER_MODES = ['Chq','Cash','RTGS','NEFT','PDC','Auto'];
// v2.10.0-s5.16: Head = the verified Ledger HEAD dropdown (expense nature), exact order from the
// live sheet (screenshots 17 Jun 2026). Validated for the numbered picker (step 4) like Tag.
var LEDGER_HEADS = ['Capital Site','Vrindavan','Office GK-1','Legal','Directors','Salary','Loan','Site','Drawing','Office Exp','Legal Exp','Diesel','Electricity','Other','Noida 153','Noida TS-3'];
// v2.10.0-s5.17: receivable Tags for the INFLOW flow — the codes the Site+Projections receivables
// SUMIFS sum (Ledger col E, IN rows). These must also exist in the Ledger Tag dropdown so the written
// row isn't flagged invalid. '—' is for non-project inflows (refunds etc.) that hit no receivable line.
var LEDGER_INFLOW_TAGS = ['FBD-Plot-Receivable','FBD-Plot-Construction','FBD-Floor-Receivable','FBD-Floor-Possession','FBD-Floor-Buyback','VRN-Floor','VRN-Plot','\u2014'];
// v2.10.0-s5.9: paying-account list — exact order/contents of the Ledger Bank A/C dropdown.
// Used both for the numbered menu (step 6) and to validate a typed account.
var LEDGER_ACCOUNTS = ['Fidatocity-70%','Fidatocity-30%','Fidato City Homes','Fidatocity AXIS','Trinity JKB','Trinity HDFC','Pitam JKB','Hansaflon JKB','Hansaflon AXIS','Hansaflon HDFC','Hansaflon Buildwell','Dholpur JKB','Trinity Tulsivan','Beatific HDFC','Chahat JKB','Fidato Buildcon','Fidato Maintenance','Maximal JKB','—','MM PDC','SM PDC','PDC'];
// v2.11.0-s6.9: sales booking module (separate file ./sales.js) - unit bookings
// over WhatsApp with M+S approval + agent re-confirm, committing to the
// capital tracker web app. identifySender/waClient resolved lazily (hoisted).
var sales = initSales({
  CONFIG: CONFIG,
  getClient: function(){ return (typeof waClient!=='undefined') ? waClient : null; },
  identifySender: identifySender,
  LEDGER_ACCOUNTS: LEDGER_ACCOUNTS,
  fetch: fetch, fs: fs, authDir: './wa_auth',
  SALES_AGENT_PHONES: ['917838537000'],  // Mukund (observer line) can also raise bookings; Umesh already in ACCOUNTANT_PHONES
  TRACKER_API_URL: process.env.TRACKER_API_URL,
  TRACKER_API_SECRET: process.env.TRACKER_API_SECRET
});
// v2.10.0-s5.11: entity/company list — exact order/contents of the Ledger Entity dropdown (verified).
var LEDGER_ENTITIES = ['Fidatocity - 70%','Fidatocity - 30%','Fidato City Homes','Fidatocity Homes','Trinity Landspace','Pitam','Hansaflon Buildcon','Hansaflon Buildwell','Dholpur Developers','Trinity Tulsivan','Beatific Hospitality','Chahat Garments','Fidato Buildcon','Fidato Maintenance','Maximal Infrastructure','Others (combined)','MM PDC','SM PDC','PDC','MM','SM'];

// Rule-based Tag pre-guess from a description. Returns {tag, person} or null.
function guessTagAndPerson(desc, entity){
  var d = (desc||'').toLowerCase(), e = (entity||'').toLowerCase(), de = d+' '+e;
  // Promoter (Person col) detection for drawings — independent of the project Tag.
  var person='';
  if(/\bsm\b|s\.?m\.?\b|sumit/.test(d)) person='SM';
  else if(/\bmm\b|m\.?m\.?\b|madhur|mummy/.test(d)) person='MM';
  // Project inference: which cost-code family.
  var proj = /vrn|vrindavan|mathura|jait/.test(de) ? 'VRN' : (/fbd|faridabad|ccm/.test(de) ? 'FBD' : '');
  // Category within the project.
  var cat='';
  if(/contractor|labour|mason/.test(d)) cat='Contractor';
  else if(/\brmc\b|concrete/.test(d)) cat='RMC';
  else if(/steel|saria|tmt/.test(d)) cat='Steel';
  else if(/diesel/.test(d)) cat='Diesel';                 // FBD-Diesel only
  else if(/exterior|facade|finishing/.test(d)) cat='Exterior';
  else if(/\bstp\b/.test(d)) cat='STP';
  else if(/\broad\b/.test(d)) cat='Road';
  else if(/\bsco\b/.test(d)) cat='SCO';
  else if(/electric/.test(d)) cat='Electricity';
  else if(/\bsite\b/.test(d)) cat='Site';                 // VRN-Site
  else if(/floor/.test(d)) cat='Floor';
  else if(/plot/.test(d)) cat='Plot';
  var tag='';
  if(proj && cat){
    var cand = proj+'-'+cat;
    if(LEDGER_TAGS.indexOf(cand)>=0) tag=cand;
    else if(LEDGER_TAGS.indexOf(proj+'-Other')>=0) tag=proj+'-Other';   // category not in this project → project's Other
  }
  if(!tag && !person) return null;                        // nothing confident → accountant picks
  return { tag:tag, person:person };
}

// Rule-based Head pre-guess fallback (used if AI guess unavailable). Loose, descriptive.
function guessHeadFallback(desc){
  var d = (desc||'').toLowerCase();
  if(/drawing|draw\b/.test(d)) return 'Drawing';
  if(/salary|payroll|wages/.test(d)) return 'Salary';
  if(/legal exp/.test(d)) return 'Legal Exp';
  if(/legal|advocate|\bca\b|roc|retainer|notary|stamp/.test(d)) return 'Legal';
  if(/loan|interest|emi|repay/.test(d)) return 'Loan';
  if(/diesel/.test(d)) return 'Diesel';
  if(/electric|electricity|\bbill\b|power/.test(d)) return 'Electricity';
  if(/director/.test(d)) return 'Directors';
  if(/office exp/.test(d)) return 'Office Exp';
  if(/office|conveyance|stationery|bank charge/.test(d)) return 'Office GK-1';
  if(/capital|capex/.test(d)) return 'Capital Site';
  if(/noida.*ts.?3|ts.?3/.test(d)) return 'Noida TS-3';
  if(/noida.*153|153/.test(d)) return 'Noida 153';
  if(/vrindavan|vrn|mathura|jait/.test(d)) return 'Vrindavan';
  if(/\bsite\b/.test(d)) return 'Site';
  return 'Other';
}

// AI Head guess via the Claude API (uses the key already wired in). Returns a string or null.
async function aiGuessHead(desc, entity){
  if(!CONFIG.CLAUDE_API_KEY) return null;
  try{
    var prompt = 'You are labelling a real-estate accounting ledger row. Pick the SINGLE best "Head" '+
      '(expense nature) from this exact list and reply with it VERBATIM, nothing else:\n'+
      LEDGER_HEADS.join('\n')+'\n\nDescription: "'+(desc||'')+'"\nEntity: "'+(entity||'')+'"\nHead:';
    var r = await fetch('https://api.anthropic.com/v1/messages', {
      method:'POST',
      headers:{'Content-Type':'application/json','x-api-key':CONFIG.CLAUDE_API_KEY,'anthropic-version':'2023-06-01'},
      body: JSON.stringify({ model:'claude-sonnet-4-6', max_tokens:20, messages:[{role:'user',content:prompt}] })
    });
    var data = await r.json();
    if(data && data.content && data.content[0] && data.content[0].text){
      var head = data.content[0].text.trim().replace(/^["']|["'\.]+$/g,'').trim().toLowerCase();
      for(var i=0;i<LEDGER_HEADS.length;i++){ if(LEDGER_HEADS[i].toLowerCase()===head) return LEDGER_HEADS[i]; }  // validate to list, else null
      console.log('[PaidFlow] aiGuessHead off-list, ignoring:', head);
    }
  }catch(e){ console.error('[PaidFlow] aiGuessHead:', e.message); }
  return null;
}

// AI Tag guess, CONSTRAINED to the real LEDGER_TAGS taxonomy. Unlike Head (free text),
// Tag drives the Dashboard, so this NEVER returns an invented value: the model's answer
// is validated against LEDGER_TAGS and rejected (→ null, caller falls back to the rule
// guess) if off-list. The prompt gives the model the project codes so it can disambiguate
// e.g. diesel-for-Vrindavan (VRN-Site) from diesel-for-Faridabad-CCM (FBD-CCM-Diesel).
async function aiGuessTag(desc, entity){
  if(!CONFIG.CLAUDE_API_KEY) return null;
  try{
    var prompt = 'You are tagging one row of a real-estate accounting ledger. Pick the SINGLE best Tag '+
      'from this exact list and reply with the tag VERBATIM and nothing else:\n'+
      LEDGER_TAGS.join('\n')+'\n\n'+
      'Rules: FBD-* = the Faridabad CCM project; VRN-* = the Vrindavan project. Use the description AND '+
      'entity to infer which project, then pick the matching cost-code (e.g. diesel on the Faridabad site '+
      'is FBD-Diesel; a Vrindavan contractor bill is VRN-Contractor; a Vrindavan site cost with no closer '+
      'code is VRN-Other). These are PROJECT cost-codes only — expense nature (Salary, Legal, Drawing, '+
      'Directors, etc.) belongs in Head, not here. If no project/code clearly fits, reply — (a dash).\n'+
      'Description: "'+(desc||'')+'"\nEntity: "'+(entity||'')+'"\nTag:';
    var r = await fetch('https://api.anthropic.com/v1/messages', {
      method:'POST',
      headers:{'Content-Type':'application/json','x-api-key':CONFIG.CLAUDE_API_KEY,'anthropic-version':'2023-06-01'},
      body: JSON.stringify({ model:'claude-sonnet-4-6', max_tokens:20, messages:[{role:'user',content:prompt}] })
    });
    var data = await r.json();
    if(data && data.content && data.content[0] && data.content[0].text){
      var guess = data.content[0].text.trim().replace(/^["']|["'\.]+$/g,'').trim().toLowerCase();
      for(var i=0;i<LEDGER_TAGS.length;i++){ if(LEDGER_TAGS[i].toLowerCase()===guess) return LEDGER_TAGS[i]; }
      console.log('[PaidFlow] aiGuessTag off-list, ignoring:', guess);
    }
  }catch(e){ console.error('[PaidFlow] aiGuessTag:', e.message); }
  return null;
}

// Assemble the 12-column Ledger row object from the approved item + collected answers.
// Column order: Date, Entity, Head, Description, Tag, IN/OUT, Amount, Mode, Person, Bank A/C, Transfer To, Notes
function assemblePaymentRow(item, answers){
  return {
    date: toLedgerWriteDate(answers.date),       // dd/mm/yyyy real date (so SUMIFS day-totals match)
    entity: answers.entity || item.entity || '', // prefer the entity captured at payment time (request Company is often a placeholder)
    head: answers.head || '',                    // AI-guessed, confirmed
    description: item.description || item.label || '', // auto from request
    tag: answers.tag || '',                      // guessed, confirmed
    inout: 'OUT',                                // approved spend, always OUT
    amount: answers.amount,                      // confirmed paid amount
    mode: answers.mode || '',                    // Chq/Cash/RTGS/NEFT/PDC/Auto
    person: answers.person || '',                // derived from Tag (Directors-SM→SM)
    bankAc: answers.bankAc || item.bankAc || '', // prefer the account captured at payment time (request 'From' is often just the company)
    transferTo: '',                              // always blank (internal transfers manual)
    notes: '[bot:'+(payableCodeFor(item.id)||item.id||'')+(item.seq?('#'+item.seq):'')+']'   // v2.11.0-s6.1: Payable code (falls back to id for TEST/non-coded items)
  };
}
// v2.10.0-s5.17: INFLOW row (IN) — money received. Tag = receivable code (drives the receivables
// SUMIFS); Description carries the payer/source; bankAc = the account it was received INTO.
function assembleInflowRow(ses){
  var A=ses.answers;
  return { date: toLedgerWriteDate(A.date), entity: A.entity||'', head: A.head||'', description: A.fromWhom||'',
    tag: A.tag||'', inout:'IN', amount: A.amount, mode: A.mode||'', person:'', bankAc: A.bankAc||'',
    transferTo:'', notes:'[bot:'+ses.id+']' };
}
// v2.11.0-s6.6: CONTRIBUTION row — promoter (MM/SM) puts money INTO a company. An IN row, tagged so it
// reads clearly, Person = the promoter. The running loan-account tally is driven by the 'contribution'
// EVENT (recorded on confirm), not this row, so the ledger tag need not be a dropdown value.
function assembleContributionRow(ses){
  var A=ses.answers, who=A.promoter||'';
  return { date: toLedgerWriteDate(A.date), entity: A.entity||'', head:'Promoter Contribution', description:'Contribution by '+who,
    tag:'Promoter Contribution', inout:'IN', amount: A.amount, mode: A.mode||'', person: who, bankAc: A.bankAc||'',
    transferTo:'', notes:'[bot:contrib:'+who+':'+ses.id+']' };
}
// Resolve MM / SM from free text (token or known name). Returns 'MM' | 'SM' | null (null if none or ambiguous).
function detectPromoter(text){
  var l=' '+(text||'').toLowerCase()+' ';
  var mm = /\bmm\b/.test(l) || (CONFIG.MM_NAMES||[]).some(function(n){ return n && l.indexOf(n.toLowerCase())>=0; });
  var sm = /\bsm\b/.test(l) || (CONFIG.SM_NAMES||[]).some(function(n){ return n && l.indexOf(n.toLowerCase())>=0; });
  if(mm && !sm) return 'MM';
  if(sm && !mm) return 'SM';
  return null;
}
// Is this outflow item a promoter CONTRIBUTION REPAYMENT (not a personal drawing)? Needs a repay-intent
// AND a promoter, OR an explicit contribution/capital context. "MM Drawing" (no repay word) -> null.
function detectContributionRepayment(label){
  var l=(label||'').toLowerCase();
  var repayVerb = /\b(repay|repaid|repayment|payback|pay\s*back|return(ed)?|refund)\b/.test(l);
  var ctx = /\bcontribution|\bcapital\b/.test(l);
  var p = detectPromoter(label);
  if(!p) return null;
  if(!(ctx || (repayVerb && /\b(mm|sm)\b/.test(l)))) return null;
  if(/\bdrawing/.test(l) && !ctx && !repayVerb) return null;   // a plain drawing is not a repayment
  return { promoter:p };
}
// Promoter loan account = sum of 'contribution' events (+) minus 'contrepay' events (-), per promoter.
// Entity is captured on every event (silent breakdown in byEntity); the headline is the combined figure.
function buildContributionStatement(){
  var store=loadEventStore();
  var acc={ MM:{contributed:0,repaid:0,byEntity:{}}, SM:{contributed:0,repaid:0,byEntity:{}} };
  (store.events||[]).forEach(function(e){
    var p=e.promoter; if(p!=='MM'&&p!=='SM') return;
    var ent=e.entity||'(unspecified)';
    if(e.type==='contribution'){ acc[p].contributed+=e.amount||0; acc[p].byEntity[ent]=(acc[p].byEntity[ent]||0)+(e.amount||0); }
    else if(e.type==='contrepay'){ acc[p].repaid+=e.amount||0; acc[p].byEntity[ent]=(acc[p].byEntity[ent]||0)-(e.amount||0); }
  });
  ['MM','SM'].forEach(function(p){ acc[p].outstanding=acc[p].contributed-acc[p].repaid; });
  return acc;
}
function formatContributionStatement(which){
  var a=buildContributionStatement();
  function block(p){
    var x=a[p];
    return ['*'+p+' \u2014 contribution account*',
      'Contributed:  \u20B9'+formatINR(x.contributed),
      'Repaid:       \u20B9'+formatINR(x.repaid),
      '\u2014\u2014\u2014\u2014\u2014\u2014\u2014\u2014\u2014',
      'Outstanding:  \u20B9'+formatINR(x.outstanding)+'  (company owes '+p+')'].join('\n');
  }
  if(which==='MM'||which==='SM') return block(which);
  return block('MM')+'\n\n'+block('SM');
}
// v2.10.0-s5.17: TRANSFER row — money moved between own accounts. bankAc = FROM, transferTo = TO.
// No Head/Tag (not income or expense); IN/OUT = TRANSFER so it's excluded from the IN/OUT day totals.
function assembleTransferRow(ses){
  var A=ses.answers;
  return { date: toLedgerWriteDate(A.date), entity: A.entity||'', head:'', description:'Transfer '+(A.fromAcct||'')+' \u2192 '+(A.toAcct||''),
    tag:'\u2014', inout:'TRANSFER', amount: A.amount, mode: A.mode||'', person:'', bankAc: A.fromAcct||'',
    transferTo: A.toAcct||'', notes:'[bot:'+ses.id+']' };
}
function rowToArray(r){
  return [r.date, r.entity, r.head, r.description, r.tag, r.inout, r.amount, r.mode, r.person, r.bankAc, r.transferTo, r.notes];
}

// ── Date-block write logic (TOGGLED OFF by default; built for later) ─────────
// Writing to the Ledger requires this flag AND the read/write Sheets scope.
var LEDGER_WRITE_ENABLED = (process.env.LEDGER_WRITE_ENABLED === 'true');
var NEWDAY_BLOCK_CREATE_ENABLED = (process.env.NEWDAY_BLOCK_CREATE_ENABLED === 'true');
// Convert dd.mm.yy / today → the Ledger's column-A short date string dd.mm.yy
function toLedgerDate(input){
  var d;
  if(!input || /today/i.test(input)){ d = new Date(); }
  else {
    var m = input.match(/(\d{1,2})[\/\.\-](\d{1,2})(?:[\/\.\-](\d{2,4}))?/);
    if(m){ var yr = m[3]?(m[3].length===2?2000+parseInt(m[3]):parseInt(m[3])):new Date().getFullYear();
      d = new Date(yr, parseInt(m[2])-1, parseInt(m[1])); }
    else d = new Date();
  }
  var dd=('0'+d.getDate()).slice(-2), mm=('0'+(d.getMonth()+1)).slice(-2), yy=String(d.getFullYear()).slice(-2);
  return dd+'.'+mm+'.'+yy;
}
// v2.10.0-s5.4: the date written into Ledger col A MUST be a real date in dd/mm/yyyy so the
// day-total breaker's =SUMIFS(G:G,A:A,<date>,...) actually matches it (dd.mm.yy reads as text
// and the totals silently miss the row). Reads/matching still accept both formats.
function toLedgerWriteDate(input){
  var d = parseSheetDate(input) || new Date();
  var dd=('0'+d.getDate()).slice(-2), mm=('0'+(d.getMonth()+1)).slice(-2), yyyy=d.getFullYear();
  return dd+'/'+mm+'/'+yyyy;
}
// Find the row index of a date block's header in the Ledger (returns {headerRow, dayTotalRow} or null).
// Reads the sheet; pure read, safe. Caller decides insert position.
async function findDateBlock(ledgerValues, shortDate){
  // ledgerValues: 2D array of the Ledger sheet. Date header rows contain the long date;
  // transaction rows carry shortDate in column A. We locate the contiguous block.
  var firstRow=-1, lastRow=-1;
  for(var i=0;i<ledgerValues.length;i++){
    var colA = (ledgerValues[i][0]||'').toString().trim();
    if(colA===shortDate){ if(firstRow<0) firstRow=i; lastRow=i; }
  }
  if(firstRow<0) return null;
  return { firstTxnRow:firstRow, lastTxnRow:lastRow };
}

// ── v2.10.0-s4: LEDGER WRITE — planner (pure) + dry-run/gated executor ────────
// The planner decides WHAT and WHERE to write, with zero I/O, so the scary part
// (right day-block, right row, no double-write) is unit-testable offline against a
// synthetic ledger array. The executor only touches Google Sheets when LEDGER_WRITE_ENABLED
// is on; with LEDGER_WRITE_DRYRUN it computes the plan and logs it but writes nothing.
// v2.10.0-s4.1: dry-run is a RUNTIME switch flippable from the locked control panel
// (saved to ./wa_auth/ledger_dryrun.json, read live). The LEDGER_WRITE_DRYRUN env var is
// the boot default until the dashboard toggle is set. NOTE: LEDGER_WRITE_ENABLED stays an
// env var — it also controls the OAuth scope at startup (read-only vs read/write), which
// can't change at runtime — so going truly live still requires setting it + redeploy.
var LEDGER_WRITE_DRYRUN = (process.env.LEDGER_WRITE_DRYRUN === 'true');
var LEDGER_DRYRUN_STATE_FILE = './wa_auth/ledger_dryrun.json';
function loadLedgerDryrun(){
  try{ if(fs.existsSync(LEDGER_DRYRUN_STATE_FILE)){ return JSON.parse(fs.readFileSync(LEDGER_DRYRUN_STATE_FILE,'utf8')).enabled===true; } }catch(e){}
  return LEDGER_WRITE_DRYRUN;   // env-var default until the dashboard toggle overrides it
}
function saveLedgerDryrun(on){ try{ if(!fs.existsSync('./wa_auth')) fs.mkdirSync('./wa_auth',{recursive:true}); fs.writeFileSync(LEDGER_DRYRUN_STATE_FILE, JSON.stringify({enabled:!!on, at:new Date().toISOString()})); }catch(e){ console.error('[Ledger] dryrun toggle save:',e.message); } }
// v2.10.0-s5.3: which TAB writes target. Default the real 'Ledger'. For a same-workbook
// rehearsal, set LEDGER_WRITE_TAB to your copy tab's exact name (e.g. 'Copy of Ledger').
// Only the WRITE path uses this; all reads/reports stay on the real 'Ledger' tab.
var LEDGER_WRITE_TAB = process.env.LEDGER_WRITE_TAB || 'Ledger';
function a1Tab(name){ return /^[A-Za-z0-9_]+$/.test(name) ? name : ("'"+String(name).replace(/'/g,"''")+"'"); } // quote tab names with spaces
// A real transaction row has a NUMBER in col G (amount). Breaker/header/day-total rows do not,
// even though a breaker's top row carries a date in A and the label "OUT" in F — so col G is the
// reliable discriminator (learned from the live sheet).
function isLedgerNum(v){ if(v===0||v==='0') return true; if(v===''||v===null||v===undefined) return false; var s=String(v); if(!/[0-9]/.test(s)) return false; var n=parseFloat(s.replace(/[^0-9.\-]/g,'')); return !isNaN(n); }
// Pure planner. values: 2D array of the target tab A:L. row: assembled 12-col row. cfg:{newDay, tab}.
// A transaction row is one with BOTH a parseable date in col A and a value in col F (IN/OUT) —
// header rows and day-total rows lack col F, so they're skipped (matches getLedgerData).
function planLedgerWrite(values, row, cfg){
  cfg = cfg || {};
  var tab = cfg.tab || 'Ledger';
  values = values || [];
  var rowArray = rowToArray(row);
  var idm = (row.notes||'').match(/\[bot:([^\]]+)\]/);
  var botId = idm ? idm[1] : '';
  // 1) dedupe — never write the same bot row twice (idempotency)
  if(botId){
    for(var i=0;i<values.length;i++){
      if((values[i] && (values[i][11]||'').toString()).indexOf('[bot:'+botId+']')>=0){
        return { action:'skip-dup', reason:'a row tagged [bot:'+botId+'] already exists at sheet row '+(i+1), existingRow:i+1, rowArray:rowArray };
      }
    }
  }
  // 2) classify rows. txn = parseable date in A AND a number in G. breaker-top = date in A, no number in G.
  var txns=[], breakers=[];
  for(var j=0;j<values.length;j++){
    var A = values[j] && values[j][0], G = values[j] && values[j][6];
    var d = parseSheetDate(A); if(!d) continue;
    var key = d.toISOString().split('T')[0], rec = { idx:j, key:key, t:d.getTime() };
    if(isLedgerNum(G)) txns.push(rec); else breakers.push(rec);   // breakers/txns gathered in row order = date order
  }
  var target = parseSheetDate(row.date);
  if(!target) return { action:'no-date', reason:'could not parse the row date "'+row.date+'"', rowArray:rowArray };
  var targetKey = target.toISOString().split('T')[0], targetT = target.getTime();
  // 3) block already exists for this date (anywhere in the sheet — handles back-dated dates too)?
  var lastTxn=-1, dayCount=0;
  for(var k=0;k<txns.length;k++){ if(txns[k].key===targetKey){ lastTxn=txns[k].idx; dayCount++; } }
  if(lastTxn>=0){
    var insertArrayIndex = lastTxn + 1, sheetRow = insertArrayIndex + 1;
    return { action:'insert', insertArrayIndex:insertArrayIndex, sheetRow:sheetRow,
      a1Range:a1Tab(tab)+'!A'+sheetRow+':L'+sheetRow, dayCount:dayCount, rowArray:rowArray,
      reason:'insert into the '+row.date+' block (after its '+dayCount+' row(s)) at sheet row '+sheetRow };
  }
  var bMatch=null; for(var b0=0;b0<breakers.length;b0++){ if(breakers[b0].key===targetKey){ bMatch=breakers[b0]; break; } }
  if(bMatch){
    var insB = bMatch.idx + 2, srB = insB + 1;       // header exists but no rows: insert just below the 2-row breaker
    return { action:'insert', insertArrayIndex:insB, sheetRow:srB, a1Range:a1Tab(tab)+'!A'+srB+':L'+srB, dayCount:0, rowArray:rowArray,
      reason:'header for '+row.date+' exists but has no rows yet; insert first row at sheet row '+srB };
  }
  // 4) NEW DAY (no block). Need an existing breaker to clone, and the NEWDAY toggle on.
  if(!breakers.length) return { action:'newday-blocked', reason:'no existing breaker to clone (empty ledger); not creating a block', rowArray:rowArray };
  if(!cfg.newDay)       return { action:'newday-blocked', reason:'no '+row.date+' block and NEWDAY_BLOCK_CREATE_ENABLED is off; not writing', rowArray:rowArray };
  // chronological insert point: before the first breaker dated later than target; else after the last txn (bottom, above any grand-total footer)
  var later = breakers.filter(function(b){ return b.t > targetT; });
  var t = later.length ? later[0].idx : ((txns.length ? txns[txns.length-1].idx : values.length-1) + 1);
  // clone the nearest breaker above the insert point; if none (target older than all), clone the next one
  var above=null; for(var c=0;c<breakers.length;c++){ if(breakers[c].idx < t) above=breakers[c]; }
  var cloneTop = above ? above.idx : later[0].idx;
  return {
    action:'newday-create',
    insertArrayIndex:t, insertSheetRow:t+1,
    cloneTopArrayIndex:cloneTop, cloneBottomArrayIndex:cloneTop+1,
    newDate:{ y:target.getFullYear(), m:target.getMonth()+1, d:target.getDate() },
    dateA1:a1Tab(tab)+'!A'+(t+1),
    txnSheetRow:t+3, a1Range:a1Tab(tab)+'!A'+(t+3)+':L'+(t+3),
    rowArray:rowArray,
    reason:'no '+row.date+' block → clone breaker at sheet rows '+(cloneTop+1)+'-'+(cloneTop+2)+', insert new block at sheet row '+(t+1)+', write row at sheet row '+(t+3)+(later.length?' (back-dated, mid-sheet)':' (newest day, bottom)')
  };
}
// Resolve the numeric sheetId (gid) of a tab by title — needed for row insertion.
async function getLedgerSheetGid(tabName){
  tabName = tabName || 'Ledger';
  if(tabName==='Ledger' && process.env.LEDGER_SHEET_GID) return parseInt(process.env.LEDGER_SHEET_GID,10);
  var meta = await sheetsApi.spreadsheets.get({ spreadsheetId: CONFIG.SHEET_ID });
  var sheets = (meta.data && meta.data.sheets) || [];
  for(var i=0;i<sheets.length;i++){ if(sheets[i].properties && sheets[i].properties.title===tabName) return sheets[i].properties.sheetId; }
  throw new Error('tab "'+tabName+'" not found in the workbook');
}
// Executor. Gated: writes only when LEDGER_WRITE_ENABLED; dry-run (live, panel-toggle) logs and writes nothing.
async function writeRowToLedger(row){
  var dry = loadLedgerDryrun();   // live panel toggle; also acts as a runtime "pause real writes"
  if(!LEDGER_WRITE_ENABLED && !dry) return { skipped:true, reason:'capture-only (LEDGER_WRITE_ENABLED off, no dry-run)' };
  if(!sheetsApi) return { error:'Sheets not initialized' };
  try{
    var tab = LEDGER_WRITE_TAB;
    var values = await readSheet(a1Tab(tab)+'!A:L');
    var plan = planLedgerWrite(values, row, { newDay: NEWDAY_BLOCK_CREATE_ENABLED, tab: tab });
    if(plan.action==='skip-dup'){ console.log('[Ledger] skip duplicate:', plan.reason); return { skipped:true, dup:true, plan:plan }; }
    if(plan.action==='newday-blocked'){ console.log('[Ledger]', plan.reason); return { skipped:true, plan:plan }; }
    if(plan.action==='no-date'){ console.log('[Ledger]', plan.reason); return { skipped:true, plan:plan }; }
    if(dry){
      if(plan.action==='newday-create'){
        console.log('[Ledger][DRY-RUN] WOULD CREATE NEW DAY for '+row.date+' → clone breaker (sheet rows '+(plan.cloneTopArrayIndex+1)+'-'+(plan.cloneTopArrayIndex+2)+') into a new block at sheet row '+plan.insertSheetRow+', set date =DATE('+plan.newDate.y+','+plan.newDate.m+','+plan.newDate.d+'), write row at '+plan.a1Range+' :: '+JSON.stringify(plan.rowArray)+'  ['+plan.reason+']');
      } else {
        console.log('[Ledger][DRY-RUN] WOULD WRITE → '+(plan.a1Range||'(n/a)')+' :: '+JSON.stringify(plan.rowArray)+'  ['+plan.reason+']');
      }
      return { dryRun:true, plan:plan };
    }
    if(plan.action==='insert'){
      var gid = await getLedgerSheetGid(tab);
      await sheetsApi.spreadsheets.batchUpdate({ spreadsheetId:CONFIG.SHEET_ID, resource:{ requests:[
        { insertDimension:{ range:{ sheetId:gid, dimension:'ROWS', startIndex:plan.insertArrayIndex, endIndex:plan.insertArrayIndex+1 }, inheritFromBefore:true } }
      ]}});
      await sheetsApi.spreadsheets.values.update({ spreadsheetId:CONFIG.SHEET_ID, range:plan.a1Range, valueInputOption:'USER_ENTERED', resource:{ values:[plan.rowArray] } });
      console.log('[Ledger] inserted row at', plan.a1Range);
      return { written:true, plan:plan };
    }
    if(plan.action==='newday-create'){
      var gidN = await getLedgerSheetGid(tab);
      var t = plan.insertArrayIndex;
      // 1) open 3 rows (breaker-top, breaker-bottom, txn), inheriting format from the row above
      await sheetsApi.spreadsheets.batchUpdate({ spreadsheetId:CONFIG.SHEET_ID, resource:{ requests:[
        { insertDimension:{ range:{ sheetId:gidN, dimension:'ROWS', startIndex:t, endIndex:t+3 }, inheritFromBefore:true } }
      ]}});
      // 2) clone the breaker (2 rows × cols A:L) — copies formats, merges AND the SUMIFS/NET formulas,
      //    whose relative refs auto-re-point to the new block's own rows.
      var src = (plan.cloneTopArrayIndex >= t) ? plan.cloneTopArrayIndex+3 : plan.cloneTopArrayIndex; // source shifts if it was below the insert
      await sheetsApi.spreadsheets.batchUpdate({ spreadsheetId:CONFIG.SHEET_ID, resource:{ requests:[
        { copyPaste:{
            source:{ sheetId:gidN, startRowIndex:src, endRowIndex:src+2, startColumnIndex:0, endColumnIndex:12 },
            destination:{ sheetId:gidN, startRowIndex:t, endRowIndex:t+2, startColumnIndex:0, endColumnIndex:12 },
            pasteType:'PASTE_NORMAL' } }
      ]}});
      // 3) overwrite the cloned breaker's date (top row, col A) with the real new-day date
      await sheetsApi.spreadsheets.values.update({ spreadsheetId:CONFIG.SHEET_ID, range:plan.dateA1, valueInputOption:'USER_ENTERED', resource:{ values:[['=DATE('+plan.newDate.y+','+plan.newDate.m+','+plan.newDate.d+')']] } });
      // 4) write the transaction row beneath the new breaker
      await sheetsApi.spreadsheets.values.update({ spreadsheetId:CONFIG.SHEET_ID, range:plan.a1Range, valueInputOption:'USER_ENTERED', resource:{ values:[plan.rowArray] } });
      console.log('[Ledger] created new-day block for', row.date, 'at sheet row', plan.insertSheetRow, '— wrote row at', plan.a1Range);
      return { written:true, createdBlock:true, plan:plan };
    }
    return { skipped:true, plan:plan };
  }catch(e){ console.error('[Ledger] write:', e.message); return { error:e.message }; }
}

// ── v2.10.0 Payment Outflow flow — STAGE 2: paid-flow Q&A state machine ──────
// When an accountant replies "paid" on a posted approved item in the payments
// group, run a 5-question Q&A (amount → date → mode → Head → Tag), assemble the
// 12-col row, require CONFIRM, then recordPaidEvent to the volume store.
// CAPTURE-ONLY: no Sheet write (LEDGER_WRITE_ENABLED stays off). Stage 3 will add
// the approved→post-to-group trigger that fills paid_posted.json (registerPostedApproved).
var PAID_STATE_FILE  = './wa_auth/paid_state.json';   // { sessions: { <authorJid>: {...} } }
var PAID_POSTED_FILE = './wa_auth/paid_posted.json';  // { items:{<postedMsgId>:item}, recent:[item,...] }
var PAID_SESSION_TTL_MS  = 6 * 60 * 60 * 1000;        // abandon a half-finished Q&A after 6h
var PAID_POSTED_FRESH_MS = 36 * 60 * 60 * 1000;       // a bare "paid" binds to a posted item this fresh

function loadPaidState(){ try{ if(fs.existsSync(PAID_STATE_FILE)){ var s=JSON.parse(fs.readFileSync(PAID_STATE_FILE,'utf8')); if(s&&s.sessions) return s; } }catch(e){} return { sessions:{} }; }
function savePaidState(s){ try{ if(!fs.existsSync('./wa_auth')) fs.mkdirSync('./wa_auth',{recursive:true}); fs.writeFileSync(PAID_STATE_FILE, JSON.stringify(s,null,1)); }catch(e){ console.error('[PaidFlow] state save:',e.message); } }
function prunePaidState(s){ var now=Date.now(),n=0; for(var k in s.sessions){ if(!Object.prototype.hasOwnProperty.call(s.sessions,k)) continue; var ses=s.sessions[k]; var t=(ses&&ses.lastAt)?Date.parse(ses.lastAt):0; if(!t || (now-t)>PAID_SESSION_TTL_MS){ delete s.sessions[k]; n++; } } return n; }

function loadPaidPosted(){ try{ if(fs.existsSync(PAID_POSTED_FILE)){ var p=JSON.parse(fs.readFileSync(PAID_POSTED_FILE,'utf8')); if(p){ if(!p.items)p.items={}; if(!p.recent)p.recent=[]; if(!p.byItem)p.byItem={}; return p; } } }catch(e){} return { items:{}, recent:[], byItem:{} }; }
function savePaidPosted(p){ try{ if(!fs.existsSync('./wa_auth')) fs.mkdirSync('./wa_auth',{recursive:true}); fs.writeFileSync(PAID_POSTED_FILE, JSON.stringify(p,null,1)); }catch(e){ console.error('[PaidFlow] posted save:',e.message); } }
// Stage 3 calls this when it posts an approved item into the payments group.
// byItem indexes by expense id so the same approved item is never posted twice.
function registerPostedApproved(postedMsgId, item){
  var p=loadPaidPosted();
  var rec={ postedMsgId:postedMsgId, at:new Date().toISOString(),
    id:item.id, label:item.label, amount:item.amount,
    entity:item.entity||'', bankAc:item.bankAc||'', description:item.description||item.label||'' };
  p.items[postedMsgId]=rec; p.recent.unshift(rec); if(p.recent.length>50) p.recent=p.recent.slice(0,50);
  if(item.id) p.byItem[item.id]=postedMsgId;
  savePaidPosted(p); return rec;
}
// ── v2.10.0-s5.7: outflow log + testing controls (mark unpaid / unpost-delete) ──
// Combined live view of everything pushed to the payments group, joined with paid events.
function buildOutflowLog(){
  var p=loadPaidPosted(), store=loadEventStore();
  var rows=[], seen={};
  (p.recent||[]).forEach(function(rec){
    if(seen[rec.id]) return; seen[rec.id]=true;
    var s=paidStatsForItem(store, rec.id); var approved=rec.amount||0; var paid=s.total; var last=s.last;
    var closed=isClosed(store, rec.id);
    var status = closed ? 'closed' : (paid<=0 ? 'posted' : (paid<approved ? 'part-paid' : 'paid'));
    rows.push({ itemId:rec.id, label:rec.label, amount:approved, paidTotal:paid, balance:Math.max(0,approved-paid), instalments:s.count,
      entity:rec.entity||'', bankAc:rec.bankAc||'', postedAt:rec.at, postedMsgId:rec.postedMsgId, paid:paid>0, closed:closed,
      paidDetails: last?{amount:last.paidAmount,date:last.date,mode:last.mode,head:last.head,tag:last.tag,person:last.person,entity:last.entity,bankAc:last.bankAc,at:last.at}:null,
      status: status });
  });
  // paid events whose item has no posted record (e.g. paid via a manual flow) — still list them
  (store.events||[]).forEach(function(e){
    if(e.type!=='paid' || seen[e.itemId]) return; seen[e.itemId]=true;
    var s2=paidStatsForItem(store, e.itemId);
    rows.push({ itemId:e.itemId, label:e.label, amount:s2.total, paidTotal:s2.total, balance:0, instalments:s2.count, entity:e.entity||'', bankAc:e.bankAc||'',
      postedAt:null, postedMsgId:null, paid:true, closed:isClosed(store,e.itemId),
      paidDetails:{amount:(s2.last||e).paidAmount,date:(s2.last||e).date,mode:(s2.last||e).mode,head:(s2.last||e).head,tag:(s2.last||e).tag,person:(s2.last||e).person,entity:(s2.last||e).entity,bankAc:(s2.last||e).bankAc,at:(s2.last||e).at},
      status:isClosed(store,e.itemId)?'closed':'paid' });
  });
  // v2.11.0-s6.3: approved-but-not-yet-posted, unpaid items — surface as 'approved' (pending) so the
  // log is the COMPLETE approved universe for one-time reconciliation (mark-paid works on these too).
  (store.events||[]).forEach(function(e){
    if(e.type!=='approved' || seen[e.itemId]) return; seen[e.itemId]=true;
    if(isClosed(store, e.itemId)) return;
    if(paidStatsForItem(store, e.itemId).total>0) return;   // any paid already handled above
    rows.push({ itemId:e.itemId, label:e.label, amount:e.amount||0, paidTotal:0, balance:e.amount||0, instalments:0,
      entity:'', bankAc:'', postedAt:null, postedMsgId:null, paid:false, closed:false, paidDetails:null, status:'approved' });
  });
  rows.sort(function(a,b){ var ta=Date.parse((a.paidDetails&&a.paidDetails.at)||a.postedAt||0)||0, tb=Date.parse((b.paidDetails&&b.paidDetails.at)||b.postedAt||0)||0; return tb-ta; });
  return rows;
}
// ── v2.10.0-s5.14: on-demand payments summary (cumulative part-payment model) ──
// Roll-up over the POSTED universe (deduped by item id). Per item: approved (posted/due amount),
// paid (Σ all instalments), balance (approved−paid). Items bucket into FRESH (paid≤0), PART-PAID
// (0<paid<approved), SETTLED (paid≥approved → drops off the action list), or CLOSED (an explicit
// close/cancel — its unpaid slice is written off). Paid is now ACTUAL CASH OUT. The self-checking
// identity becomes  Paid + Outstanding + Closed = Approved  (Closed term omitted when none).
// Fresh + Part-paid get continuous numbering (Fresh first) so a single reply-number maps to one item.
function payDayIndex(e){
  var d=e&&e.date;
  if(d){ var m=String(d).match(/(\d{1,2})\/(\d{1,2})\/(\d{2,4})/); if(m){ var y=+m[3]; if(y<100)y+=2000; return Math.floor(Date.UTC(y,+m[2]-1,+m[1])/86400000); } }
  var at=Date.parse(e&&e.at); return isNaN(at)?null:Math.floor((at+5.5*3600000)/86400000);
}
function payDateLabel(e){
  var d=e&&e.date;
  if(d){ var m=String(d).match(/(\d{1,2})\/(\d{1,2})\/(\d{2,4})/); if(m){ var y=+m[3]; if(y<100)y+=2000; return new Date(Date.UTC(y,+m[2]-1,+m[1])).toLocaleDateString('en-IN',{day:'2-digit',month:'short',timeZone:'UTC'}); } }
  var at=Date.parse(e&&e.at); return isNaN(at)?'':new Date(at).toLocaleDateString('en-IN',{day:'2-digit',month:'short',timeZone:'Asia/Kolkata'});
}
var PAID_WINDOW_DAYS = 3;   // "Paid . last N days" calendar window (today + N-1 prior). Configurable.
// v2.11.0-s6.0: categorised summary — Approved.not-paid (biggest first) / Part-paid / Paid.last-Nd.
// Self-checking identity Paid + Outstanding + Closed = Approved (all-time Paid). No P- code shown.
function buildPaymentsSummary(pp, store, now){
  pp = pp || loadPaidPosted();
  store = store || loadEventStore();
  now = now || Date.now();
  var todayIdx = Math.floor((now + 5.5*3600000)/86400000);
  var cutoffIdx = todayIdx - (PAID_WINDOW_DAYS-1);
  var seen={}, fresh=[], partPaid=[];
  var approvedTotal=0,paidTotal=0,outstandingTotal=0,partBalanceTotal=0,closedCount=0,closedWriteOff=0,settledCount=0;
  var paid3dTotal=0, paid3dByItem={};
  (store.events||[]).forEach(function(e){
    if(e.type!=='paid') return;
    var di=payDayIndex(e);
    if(di!=null && di>=cutoffIdx){
      paid3dTotal+=(e.paidAmount||0);
      if(!paid3dByItem[e.itemId]) paid3dByItem[e.itemId]={itemId:e.itemId,label:e.label||e.itemId,amt:0,idx:-1,dlabel:''};
      paid3dByItem[e.itemId].amt+=(e.paidAmount||0);
      if(di>paid3dByItem[e.itemId].idx){ paid3dByItem[e.itemId].idx=di; paid3dByItem[e.itemId].dlabel=payDateLabel(e); }
    }
  });
  (pp.recent||[]).forEach(function(rec){
    if(!rec||seen[rec.id]) return; seen[rec.id]=true;
    var approved=rec.amount||0, s=paidStatsForItem(store,rec.id), paid=s.total;
    approvedTotal+=approved; paidTotal+=paid;
    var entry={itemId:rec.id,label:rec.label||rec.id,approved:approved,paid:paid,balance:Math.max(0,approved-paid),at:rec.at};
    if(isClosed(store,rec.id)){ closedCount++; closedWriteOff+=Math.max(0,approved-paid); return; }
    if(paid<=0){ fresh.push(entry); outstandingTotal+=entry.balance; }
    else if(paid<approved){ partPaid.push(entry); outstandingTotal+=entry.balance; partBalanceTotal+=entry.balance; }
    else { settledCount++; }
  });
  // s6.7: surface items approved by M+S that were never POSTED to the outflow group (e.g. approved while
  // OUTFLOW_POST_ENABLED was off). Mirrors the s6.3 buildOutflowLog approved-pass so the in-group summary
  // shows the COMPLETE approved universe, not just posted items. Capital-IN raises are excluded narrowly
  // (isCapitalInflowLabel) so a mis-posted "X Contribution of Rs Y" never shows as a payable outflow, while
  // a contribution REPAYMENT (money paid back to a promoter) correctly stays in the list.
  (store.events||[]).forEach(function(e){
    if(e.type!=='approved' || seen[e.itemId]) return;
    if(isCapitalInflowLabel(e.label)) return;
    if(isClosed(store,e.itemId)) return;
    if(paidStatsForItem(store,e.itemId).total>0) return;
    seen[e.itemId]=true;
    var aAmt=e.amount||0; approvedTotal+=aAmt; outstandingTotal+=aAmt;
    fresh.push({ itemId:e.itemId, label:e.label||e.itemId, approved:aAmt, paid:0, balance:aAmt, at:e.at, unposted:true });
  });
  fresh.sort(function(a,b){ return b.approved-a.approved; });                 // biggest amount first
  partPaid.sort(function(a,b){ return (Date.parse(a.at)||0)-(Date.parse(b.at)||0); });
  var n=0, numbered=[];
  fresh.forEach(function(e){ e.n=++n; numbered.push(e); });
  partPaid.forEach(function(e){ e.n=++n; numbered.push(e); });
  var paidRecent=Object.keys(paid3dByItem).map(function(k){return paid3dByItem[k];}).sort(function(a,b){return b.idx-a.idx;});
  var freshTotal=fresh.reduce(function(t,e){return t+e.approved;},0);
  return {
    approved:{count:Object.keys(seen).length,total:approvedTotal}, paid:{total:paidTotal},
    paid3d:{total:paid3dTotal,items:paidRecent,days:PAID_WINDOW_DAYS}, outstanding:{total:outstandingTotal},
    partPaidBalance:{total:partBalanceTotal,count:partPaid.length}, closed:{count:closedCount,writeOff:closedWriteOff},
    settledCount:settledCount, fresh:fresh, freshTotal:freshTotal, partPaid:partPaid, numbered:numbered,
    reconciles:(paidTotal+outstandingTotal+closedWriteOff)===approvedTotal
  };
}
// Render the categorised summary. `now` injectable for deterministic tests.
function formatPaymentsSummary(sum, now){
  now=now||new Date();
  var DIV='\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500';
  if(!sum||sum.approved.count===0) return '\uD83D\uDCCA *PAYMENTS SUMMARY*\n'+DIV+'\nNothing has been posted to the payments group yet.';
  function lbl(s){ s=(s||'').replace(/\n/g,' ').trim(); return s.length>38?s.substring(0,38)+'\u2026':s; }
  var dateStr=now.toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'numeric',timeZone:'Asia/Kolkata'});
  var timeStr=now.toLocaleTimeString('en-IN',{hour:'2-digit',minute:'2-digit',hour12:true,timeZone:'Asia/Kolkata'}).toLowerCase();
  var L=[];
  L.push('\uD83D\uDCCA *PAYMENTS SUMMARY*'); L.push(dateStr+' \u00B7 '+timeStr); L.push(DIV);
  L.push('\u2705 *Approved (due):*  '+sum.approved.count+' \u00B7 \u20B9'+formatINR(sum.approved.total));
  L.push('\uD83D\uDD53 *Outstanding:*  \u20B9'+formatINR(sum.outstanding.total));
  L.push('\uD83D\uDCB8 *Paid to date:*  \u20B9'+formatINR(sum.paid.total)+'  \u00B7  last '+sum.paid3d.days+'d: \u20B9'+formatINR(sum.paid3d.total));
  if(sum.partPaidBalance.count>0) L.push('\uD83D\uDFE1 *Part-paid:*  '+sum.partPaidBalance.count+' \u00B7 \u20B9'+formatINR(sum.partPaidBalance.total)+' balance');
  if(sum.closed.count>0) L.push('\uD83D\uDEAB *Closed:*  '+sum.closed.count+' \u00B7 \u20B9'+formatINR(sum.closed.writeOff)+' written off');
  L.push(DIV);
  L.push('\uD83D\uDD34 *APPROVED \u00B7 NOT PAID*  ('+sum.fresh.length+' \u00B7 \u20B9'+formatINR(sum.freshTotal)+')');
  if(sum.fresh.length){ L.push('_reply the number to pay_'); sum.fresh.forEach(function(it){ L.push(it.n+'. '+lbl(it.label)+'  \u20B9'+formatINR(it.approved)); }); }
  else L.push('_none \u2014 all approved items are paid or closed._');
  if(sum.partPaid.length){ L.push(''); L.push('\uD83D\uDFE1 *PART-PAID*  ('+sum.partPaid.length+')'); L.push('_reply the number to log the next instalment_');
    sum.partPaid.forEach(function(it){ L.push(it.n+'. '+lbl(it.label)); L.push('    paid \u20B9'+formatINR(it.paid)+' of \u20B9'+formatINR(it.approved)+' \u00B7 *bal \u20B9'+formatINR(it.balance)+'*'); }); }
  if(sum.paid3d.items.length){ L.push(''); L.push('\uD83D\uDFE2 *PAID \u00B7 last '+sum.paid3d.days+' days*  ('+sum.paid3d.items.length+' \u00B7 \u20B9'+formatINR(sum.paid3d.total)+')');
    sum.paid3d.items.forEach(function(it){ L.push('\u2713 '+lbl(it.label)+'  \u20B9'+formatINR(it.amt)+'  \u00B7 '+it.dlabel); });
    L.push('_only the last '+sum.paid3d.days+' days of payments are listed_'); }
  L.push(DIV); L.push(sum.closed.count>0?'Paid + Outstanding + Closed = Approved':'Paid + Outstanding = Approved');
  return L.join('\n');
}

// Mark a paid expense back to UNPAID — removes its paid event(s) so it can be re-tested.
function markItemUnpaid(itemId){
  var store=loadEventStore(); var before=store.events.length;
  _eventStoreCache.events = store.events.filter(function(e){ return !(e.type==='paid' && e.itemId===itemId); });
  var removed = before - _eventStoreCache.events.length;
  if(removed>0) _persistEventStore();
  return { itemId:itemId, removed:removed,
    message: removed ? ('Marked unpaid — removed '+removed+' paid event(s); it can be paid again.') : 'No paid event found for that id.' };
}
// v2.10.0-s5.14: reopen a closed/cancelled item (undo a close) — removes its closed event(s).
function reopenItem(itemId){
  var store=loadEventStore(); var before=store.events.length;
  _eventStoreCache.events = store.events.filter(function(e){ return !(e.type==='closed' && e.itemId===itemId); });
  var removed = before - _eventStoreCache.events.length;
  if(removed>0) _persistEventStore();
  return { itemId:itemId, removed:removed,
    message: removed ? ('Reopened — removed '+removed+' close event(s); it is back on the list.') : 'No close event found for that id.' };
}
// v2.11.0-s6.3: dashboard one-click reconcile. Marks an approved/posted item paid for its remaining
// balance by recording a 'paid' event (mode 'Manual', date = IST today). NO ledger write — this is a
// books-reconciliation mark, not a real disbursement. Reversible via markItemUnpaid. amountOpt records
// a partial instead of the full balance (capped at the balance).
function markItemPaid(itemId, amountOpt){
  var pp=loadPaidPosted(), store=loadEventStore();
  var rec=findPostedRec(pp, itemId), appr=findApprovedEvent(store, itemId), s=paidStatsForItem(store, itemId);
  if(isClosed(store, itemId)) return { itemId:itemId, ok:false, message:'Item is closed — reopen it first.' };
  var approved=(rec&&rec.amount)||(appr&&appr.amount)||s.total||0;
  var balance=Math.max(0, approved - s.total);
  if(balance<=0) return { itemId:itemId, ok:false, message:'Already fully paid — nothing left to mark.' };
  var amt=(amountOpt!=null && !isNaN(amountOpt) && +amountOpt>0) ? Math.min(+amountOpt, balance) : balance;
  var label=(rec&&rec.label)||(appr&&appr.label)||(s.last&&s.last.label)||itemId;
  var d=new Date(Date.now()+5.5*3600000), dd=String(d.getUTCDate()).padStart(2,'0'), mm=String(d.getUTCMonth()+1).padStart(2,'0');
  var today=dd+'/'+mm+'/'+d.getUTCFullYear();
  recordPaidEvent(itemId, label, amt, { date:today, mode:'Manual', head:'', tag:'', person:'', entity:(rec&&rec.entity)||'', bankAc:(rec&&rec.bankAc)||'' }, s.count+1);
  var newPaid=s.total+amt;
  return { itemId:itemId, ok:true, marked:amt, approved:approved, paidTotal:newPaid, balance:Math.max(0,approved-newPaid), fullyPaid:newPaid>=approved,
    message:'Marked paid \u20B9'+amt+(newPaid>=approved?' \u2014 now fully paid.':' \u2014 balance \u20B9'+Math.max(0,approved-newPaid)+'.') };
}
// Append a message id to an item's thread (so deleting the item can clear the whole conversation).
function appendThreadMsg(itemId, msgId){
  try{
    if(!itemId || !msgId) return;
    var p=loadPaidPosted(); var pmid=p.byItem[itemId]; if(!pmid) return; var rec=p.items[pmid]; if(!rec) return;
    if(!rec.threadMsgIds) rec.threadMsgIds=[];
    if(rec.threadMsgIds.indexOf(msgId)<0){ rec.threadMsgIds.push(msgId); savePaidPosted(p); }
  }catch(e){}
}
// Remove a pushed item from the dashboard; optionally delete the WHOLE group thread for it
// (the original PAYMENT DUE post + every paid-flow message tracked against it).
async function unpostOutflowItem(itemId, deleteMsg){
  var p=loadPaidPosted(); var msgId=p.byItem[itemId]; var rec=msgId?p.items[msgId]:null;
  var ids=[]; if(msgId) ids.push(msgId);
  if(rec && rec.threadMsgIds) rec.threadMsgIds.forEach(function(id){ if(ids.indexOf(id)<0) ids.push(id); });
  var deleted=0, failed=0, waError=null;
  if(deleteMsg && ids.length){
    if(waReady && waClient){
      for(var i=0;i<ids.length;i++){
        try{ var m=await waClient.getMessageById(ids[i]); if(m){ await m.delete(true); deleted++; } else failed++; }
        catch(e){ failed++; waError=e.message; }
      }
    } else { waError='WhatsApp not connected — removed from dashboard only'; }
  }
  if(msgId){ delete p.items[msgId]; delete p.byItem[itemId]; p.recent=(p.recent||[]).filter(function(r){return r.id!==itemId;}); savePaidPosted(p); }
  return { itemId:itemId, removedFromDashboard:!!msgId, postedMsgId:msgId||null, threadCount:ids.length,
    waDeleted:deleted, waFailed:failed, waError:waError,
    message: msgId
      ? ('Removed from dashboard'+(deleteMsg?('; deleted '+deleted+' of '+ids.length+' group message(s)'+(failed?(' ('+failed+' could not be deleted — likely an accountant reply the bot cannot remove unless it is group admin, or too old)'):'')+'.'):'.'))
      : 'No posted record for that id.' };
}
function alreadyPosted(itemId){ try{ var p=loadPaidPosted(); return !!(p.byItem && p.byItem[itemId]); }catch(e){ return false; } }

// Which approved item does this "paid" message refer to?
async function resolvePostedItem(msg){
  var p=loadPaidPosted();
  if(msg && msg.hasQuotedMsg){
    try{ var q=await msg.getQuotedMessage(); var qid=q&&(q.id._serialized||q.id.id); if(qid && p.items[qid]) return p.items[qid]; }catch(e){}
  }
  if(p.recent && p.recent.length){
    var top=p.recent[0]; var t=top.at?Date.parse(top.at):0;
    if(t && (Date.now()-t) <= PAID_POSTED_FRESH_MS) return top;
  }
  return null;
}
// ── v2.10.0-s5.14: interactive summary — number→item map, posted lookup, session builder ──
// Each time the summary is posted to the group we save its numbering so a bare-number reply
// (or "close <n>") maps back to the right item.
var PAID_SUMMARY_MAP_FILE = './wa_auth/paid_summary_map.json';   // { at, items:[{n,itemId,label}] }
function loadPaidSummaryMap(){ try{ if(fs.existsSync(PAID_SUMMARY_MAP_FILE)) return JSON.parse(fs.readFileSync(PAID_SUMMARY_MAP_FILE,'utf8')); }catch(e){} return null; }
function savePaidSummaryMap(m){ try{ if(!fs.existsSync('./wa_auth')) fs.mkdirSync('./wa_auth',{recursive:true}); fs.writeFileSync(PAID_SUMMARY_MAP_FILE, JSON.stringify(m,null,1)); }catch(e){ console.error('[PaidSummary] map save:',e.message); } }
function summaryItemByNumber(n){ var m=loadPaidSummaryMap(); if(!m||!m.items) return null; for(var i=0;i<m.items.length;i++){ if(m.items[i].n===n) return m.items[i]; } return null; }
// Find the posted record for an item id (the approved/due figure + entity/account live here).
function findPostedRec(pp, itemId){ var r=(pp.recent||[]).filter(function(x){ return x.id===itemId; }); return r.length?r[0]:null; }
// Build a paid-flow session for a posted item, instalment-aware: balance = approved − paidSoFar,
// seq = next instalment number. Used by both the "paid"-quote trigger and the summary-number trigger.
function newPaidSession(rec, store, byName){
  var s = paidStatsForItem(store, rec.id);
  var approved = rec.amount||0;
  var balance = Math.max(0, approved - s.total);
  return { itemId:rec.id, label:rec.label, amount:approved, paidSoFar:s.total, balance:balance, seq:s.count+1,
    entity:rec.entity, bankAc:rec.bankAc, description:rec.description||rec.label,
    step:'amount', answers:{}, startedAt:new Date().toISOString(), lastAt:new Date().toISOString(), by:byName };
}

// Display name + stable session key for an accountant in the payments group.
async function resolveAccountant(msg){
  var author=msg.author||msg.from||'';
  var who=await identifySender(author);
  return { key:author, name:(who&&who.contactName)||'' };
}

function paidModeMenu(){ return paidNumberedMenu(LEDGER_MODES); }
function paidTagMenu(){ return paidNumberedMenu(LEDGER_TAGS); }
function paidHeadMenu(){ return paidNumberedMenu(LEDGER_HEADS); }
function paidNumberedMenu(list){ var L=[]; for(var i=0;i<list.length;i++){ var a=list[i]; L.push((i+1)+'. '+(a==='—'?'— (none / blank)':a)); } return L.join('\n'); }
function paidInflowTagMenu(){ return paidNumberedMenu(LEDGER_INFLOW_TAGS); }
function paidAccountMenu(){ return paidNumberedMenu(LEDGER_ACCOUNTS); }
function paidEntityMenu(){ return paidNumberedMenu(LEDGER_ENTITIES); }
// Resolve a typed value to a canonical list entry. Returns {match} | {ambiguous:[...]} | {none:true}.
function resolveFromList(input, list){
  var low=(input||'').trim().toLowerCase();
  for(var i=0;i<list.length;i++){ if(list[i].toLowerCase()===low) return { match:list[i] }; }
  var hits=list.filter(function(a){ return a!=='—' && a.toLowerCase().indexOf(low)>=0; });
  if(hits.length===1) return { match:hits[0] };
  if(hits.length>1) return { ambiguous:hits };
  return { none:true };
}
function resolveAccount(input){ var r=resolveFromList(input,LEDGER_ACCOUNTS); return r.match?{account:r.match}:r; }
function resolveEntity(input){ var r=resolveFromList(input,LEDGER_ENTITIES); return r.match?{entity:r.match}:r; }
function personForTag(tag){ if(tag==='Directors-SM') return 'SM'; if(tag==='Directors-MM') return 'MM'; return ''; }
// Human-readable label for the step an in-progress session is sitting on (used by the guard).
var PAID_STEP_LABEL = { amount:'the amount paid', date:'the date', mode:'the payment mode', head:'the Head', tag:'the Tag', account:'the paying account', entity:'the entity', confirm:'confirming the row' };
function paidRowPreview(row){
  var a=rowToArray(row);
  var cols=['Date','Entity','Head','Description','Tag','IN/OUT','Amount','Mode','Person','Bank A/C','Transfer To','Notes'];
  var L=['*Row to record:*'];
  for(var i=0;i<cols.length;i++){ var v=a[i]; if(i===6) v='Rs.'+formatINR(parseAmount(v)); L.push(cols[i]+': '+((v===''||v==null)?'—':v)); }
  return L.join('\n');
}

// Core step machine. Mutates the passed session, calls in-scope guess helpers.
// Steps: amount → date → mode → head → tag → confirm. Returns {reply,done,cancelled,recordArgs}.
// ── v2.11.0-s6.2: amount-step branch + over-approval re-approval ───────────────
var REAPPROVAL_REASONS = ['Price revised','Extra work','GST / taxes','Measurement change','Other'];
function reapprovalReasonMenu(){ var L=[]; for(var i=0;i<REAPPROVAL_REASONS.length;i++) L.push((i+1)+'. '+REAPPROVAL_REASONS[i]); return L.join('\n'); }
var REAPPROVAL_FILE = './wa_auth/reapproval_pending.json';   // { items:{ <reapprovalMsgId>:{itemId,code,label,approved,paidSoFar,balance,thisInstalment,attempted,reason,isFinal,at,mm,sm,resolved} } }
function loadReapprovals(){ try{ if(fs.existsSync(REAPPROVAL_FILE)){ var r=JSON.parse(fs.readFileSync(REAPPROVAL_FILE,'utf8')); if(r&&r.items) return r; } }catch(e){} return { items:{} }; }
function saveReapprovals(r){ try{ if(!fs.existsSync('./wa_auth')) fs.mkdirSync('./wa_auth',{recursive:true}); fs.writeFileSync(REAPPROVAL_FILE, JSON.stringify(r,null,1)); }catch(e){ console.error('[Reapproval] save:',e.message); } }
// Register a posted re-approval keyed by its WhatsApp msgId, so an M/S swipe-reply on it can lift the amount.
function registerReapproval(rp, msgId){ if(!msgId) return; var r=loadReapprovals(); r.items[msgId]={ itemId:rp.itemId, code:rp.code, label:rp.label, approved:rp.approved, paidSoFar:rp.paidSoFar, balance:rp.balance, thisInstalment:rp.thisInstalment, attempted:rp.attempted, reason:rp.reason, isFinal:rp.isFinal, at:new Date().toISOString() }; saveReapprovals(r); }
// Lift the live approved (due) figure to the re-approved amount. paid_posted.rec.amount is what
// newPaidSession + the summary read, so this is what unblocks the payment. Records a 'reapproved' audit event.
function liftPayableAmount(itemId, newAmount, code){
  var pp=loadPaidPosted(), changed=false;
  (pp.recent||[]).forEach(function(rec){ if(rec.id===itemId){ rec.amount=newAmount; changed=true; } });
  if(pp.items && pp.items[itemId]) pp.items[itemId].amount=newAmount;
  if(changed) savePaidPosted(pp);
  recordEvent('reapproved', { itemId:itemId, code:code||payableCodeFor(itemId), newAmount:newAmount }, 'reapproved:'+itemId+':'+newAmount);
  return changed;
}
// Build the RE-APPROVAL message for the approval group. Two variants: fresh/mid-stream (no prior
// instalments) and final-instalment (shows the already-paid history). rp carries approved/attempted/etc.
function buildReapprovalMessage(rp){
  var over=rp.attempted-rp.approved;
  var DIV='\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500';
  var L=['\uD83D\uDD01 *RE-APPROVAL NEEDED*'+(rp.isFinal?'  \u00B7  final instalment':''), DIV, rp.label, ''];
  if(rp.isFinal){
    L.push('Originally approved:  \u20B9'+formatINR(rp.approved));
    L.push('Already paid:         \u20B9'+formatINR(rp.paidSoFar));
    L.push('Balance left:         \u20B9'+formatINR(rp.balance));
    L.push('This instalment:      \u20B9'+formatINR(rp.thisInstalment));
    L.push(DIV);
    L.push('Total would become:   \u20B9'+formatINR(rp.attempted)+'   (\u20B9'+formatINR(over)+' over approved)');
  } else {
    L.push('Originally approved:  \u20B9'+formatINR(rp.approved));
    L.push('Now being paid:       \u20B9'+formatINR(rp.attempted));
    L.push('Increase:             +\u20B9'+formatINR(over));
  }
  L.push(''); L.push('Reason: '+rp.reason); L.push(DIV);
  L.push('M & S \u2014 approve the revised \u20B9'+formatINR(rp.attempted)+', or reject.');
  L.push('\u2705 reply \"ok\"   \u274C reply \"no\"');
  return L.join('\n');
}
// Package the paid-flow's over-amount block as a signal the outer handler posts + registers.
function reapprovalSignalFrom(session, reason){
  var paidSoFar=session.paidSoFar||0, thisInst=session.pendingAmount;
  return { done:true, reply:null, reapproval:{
    itemId:session.itemId, code:payableCodeFor(session.itemId), label:session.label,
    approved:session.amount, paidSoFar:paidSoFar, balance:session.balance||0,
    thisInstalment:thisInst, attempted:paidSoFar+thisInst, reason:reason, isFinal:paidSoFar>0
  } };
}
async function paidFlowAdvance(session, inputRaw){
  var input=(inputRaw||'').trim(), low=input.toLowerCase();
  if(/^(cancel|reset|clear|stop)$/i.test(low)){ return { reply:'Paid entry cancelled. Reply "paid" on an approved item to start again.', done:true, cancelled:true }; }

  if(session.step==='amount'){
    var bal = (session.balance!=null ? session.balance : session.amount);
    var amt;
    if(low==='ok'){ amt=bal; }
    else { amt=extractLineAmount(input,false)||parseAmount(input); if(!amt){ return { reply:'Didn\'t catch an amount. Type the actual amount paid, or "ok" to use the balance Rs.'+formatINR(bal)+'.' }; } }
    session.pendingAmount=amt;
    if(amt===bal){ session.answers.amount=amt; session.step='date'; return { reply:'2/7 *Date?* — reply "today" or dd/mm (e.g. 14/06).' }; }
    if(amt<bal){ session.step='partask';
      return { reply:'\u20B9'+formatINR(amt)+' is *less* than the balance of \u20B9'+formatINR(bal)+'. Is this —\n1. *Part payment* (\u20B9'+formatINR(amt)+' now, \u20B9'+formatINR(bal-amt)+' still due)\n2. *Final / reduced* (settles it; \u20B9'+formatINR(bal-amt)+' will not be paid)\n\nReply *1* or *2*.' }; }
    session.step='overreason';   // amt > bal : blocked until M+S re-approve the higher amount
    return { reply:'\u26D4 \u20B9'+formatINR(amt)+' is *more* than the balance of \u20B9'+formatINR(bal)+' (approved \u20B9'+formatINR(session.amount)+(session.paidSoFar>0?', already paid \u20B9'+formatINR(session.paidSoFar):'')+').\nThis can\'t be paid until *M+S re-approve* the higher amount. Why is it over?\n'+reapprovalReasonMenu()+'\n\nReply a number, or *cancel*.' };
  }
  if(session.step==='partask'){
    if(/^1\b|^part/i.test(low)){ session.answers.amount=session.pendingAmount; session.step='date';
      return { reply:'Noted as a *part payment* (balance after: \u20B9'+formatINR((session.balance||0)-session.pendingAmount)+').\n\n2/7 *Date?* — reply "today" or dd/mm.' }; }
    if(/^2\b|^final|^reduc/i.test(low)){ session.answers.amount=session.pendingAmount; session.closeAfter=true; session.step='date';
      return { reply:'Noted as *final / reduced* — \u20B9'+formatINR((session.balance||0)-session.pendingAmount)+' will be written off (not paid); the item closes after this.\n\n2/7 *Date?* — reply "today" or dd/mm.' }; }
    return { reply:'Reply *1* for part payment (rest still due) or *2* for final / reduced (settles it).' };
  }
  if(session.step==='overreason'){
    var _r='';
    if(/^\d+$/.test(low)){ var _ri=parseInt(low,10); if(_ri>=1 && _ri<=REAPPROVAL_REASONS.length) _r=REAPPROVAL_REASONS[_ri-1]; }
    if(!_r){ return { reply:'Pick a reason by number:\n'+reapprovalReasonMenu() }; }
    if(_r==='Other'){ session.step='overnote'; return { reply:'Type a short note explaining the increase:' }; }
    return reapprovalSignalFrom(session, _r);
  }
  if(session.step==='overnote'){
    var _n=input.trim(); if(!_n){ return { reply:'Type a short note explaining the increase:' }; }
    return reapprovalSignalFrom(session, 'Other — '+_n);
  }
  if(session.step==='date'){
    session.answers.date = toLedgerDate(/^today$/i.test(low)?'today':input);
    session.step='mode';
    return { reply:'3/7 *Mode?* — reply the number:\n'+paidModeMenu() };
  }
  if(session.step==='mode'){
    var mi=parseInt(low,10);
    if(!(mi>=1 && mi<=LEDGER_MODES.length)){ return { reply:'Pick the mode by number:\n'+paidModeMenu() }; }
    session.answers.mode=LEDGER_MODES[mi-1];
    var head=await aiGuessHead(session.description, session.entity);
    if(!head) head=guessHeadFallback(session.description);
    session.guessHead=head; session.step='head';
    var hlead = head ? ('my guess: *'+head+'*. Reply "ok" to accept, or pick a number') : 'pick a number';
    return { reply:'4/7 *Head?* — '+hlead+':\n'+paidHeadMenu()+'\n(or type the exact head name)' };
  }
  if(session.step==='head'){
    var headv='';
    if(low==='ok' && session.guessHead){ headv=session.guessHead; }
    else if(/^\d+$/.test(low)){
      var hi=parseInt(low,10);
      if(hi>=1 && hi<=LEDGER_HEADS.length){ headv=LEDGER_HEADS[hi-1]; }
    }
    else { for(var hj=0;hj<LEDGER_HEADS.length;hj++){ if(LEDGER_HEADS[hj].toLowerCase()===low){ headv=LEDGER_HEADS[hj]; break; } } }
    if(!headv){ return { reply:'Pick the Head by number (1\u2013'+LEDGER_HEADS.length+'), "ok" for the guess, or type an exact head name:\n'+paidHeadMenu() }; }
    session.answers.head = headv;
    var aiTag = await aiGuessTag(session.description, session.entity);   // validated to LEDGER_TAGS, or null
    var g = guessTagAndPerson(session.description, session.entity) || null;   // rule fallback (offline)
    var finalTag = aiTag || (g && LEDGER_TAGS.indexOf(g.tag)>=0 ? g.tag : '');  // only offer an in-list tag guess
    session.guessTag = finalTag;
    session.guessPerson = personForTag(finalTag) || (g ? (g.person||'') : '');
    session.step='tag';
    var lead = session.guessTag ? ('my guess: *'+session.guessTag+'*. Reply "ok" to accept, or pick a number') : 'pick a number';
    return { reply:'5/7 *Tag?* — '+lead+':\n'+paidTagMenu()+'\n(or type the tag name)' };
  }
  if(session.step==='tag'){
    var tag='';
    if(low==='ok' && session.guessTag){ tag=session.guessTag; }
    else {
      var ti=parseInt(low,10);
      if(ti>=1 && ti<=LEDGER_TAGS.length){ tag=LEDGER_TAGS[ti-1]; }
      else { for(var i=0;i<LEDGER_TAGS.length;i++){ if(LEDGER_TAGS[i].toLowerCase()===low){ tag=LEDGER_TAGS[i]; break; } } }
    }
    if(!tag){ return { reply:'Pick the Tag by number, "ok" for the guess, or type a full tag:\n'+paidTagMenu() }; }
    session.answers.tag=tag;
    session.answers.person = personForTag(tag) || session.guessPerson || '';
    session.step='entity';
    return { reply:'6/7 *Which entity / company?* — reply the number, or type the name:\n'+paidEntityMenu() };
  }
  if(session.step==='entity'){
    var ent='';
    if(/^\d+$/.test(low)){
      var en=parseInt(low,10);
      if(en>=1 && en<=LEDGER_ENTITIES.length){ ent=LEDGER_ENTITIES[en-1]; }
      else { return { reply:'Pick the entity by number (1\u2013'+LEDGER_ENTITIES.length+') or type the name:\n'+paidEntityMenu() }; }
    } else {
      var re=resolveEntity(input);
      if(re.entity){ ent=re.entity; }
      else if(re.ambiguous){ return { reply:'\u201c'+input+'\u201d matches several: '+re.ambiguous.join(', ')+'. Type the exact one, or reply its number:\n'+paidEntityMenu() }; }
      else { return { reply:'\u26A0\uFE0F \u201c'+input+'\u201d isn\u2019t in the entity list. Reply a number (1\u2013'+LEDGER_ENTITIES.length+') or type an exact name:\n'+paidEntityMenu() }; }
    }
    session.answers.entity=ent;
    session.step='account';
    return { reply:'7/7 *Paid from which account?* — reply the number, or type the account name:\n'+paidAccountMenu() };
  }
  if(session.step==='account'){
    var acct='';
    if(/^\d+$/.test(low)){
      var ac=parseInt(low,10);
      if(ac>=1 && ac<=LEDGER_ACCOUNTS.length){ acct=LEDGER_ACCOUNTS[ac-1]; }
      else { return { reply:'Pick the account by number (1\u2013'+LEDGER_ACCOUNTS.length+') or type the account name:\n'+paidAccountMenu() }; }
    } else {
      var r=resolveAccount(input);
      if(r.account){ acct=r.account; }
      else if(r.ambiguous){ return { reply:'\u201c'+input+'\u201d matches several accounts: '+r.ambiguous.join(', ')+'. Type the exact one, or reply its number:\n'+paidAccountMenu() }; }
      else { return { reply:'\u26A0\uFE0F \u201c'+input+'\u201d isn\u2019t one of the known accounts. Reply a number (1\u2013'+LEDGER_ACCOUNTS.length+') or type an exact account name:\n'+paidAccountMenu() }; }
    }
    session.answers.bankAc=acct;
    var item={ id:session.itemId, seq:session.seq, entity:session.entity, bankAc:session.bankAc, description:session.description, label:session.label };
    session.row = assemblePaymentRow(item, session.answers);
    session.step='confirm';
    return { reply: paidRowPreview(session.row)+'\n\nReply *CONFIRM* to record, or *cancel*.' };
  }
  if(session.step==='confirm'){
    if(/^(confirm|yes|ok|y)$/i.test(low)){
      return { reply:'\u2713 Recorded.', done:true,
        recordArgs:{ itemId:session.itemId, label:session.label, paidAmount:session.answers.amount, seq:session.seq, row:session.row,
          closeAfter:!!session.closeAfter, approved:session.amount, paidTotalAfter:(session.paidSoFar||0)+session.answers.amount,
          fields:{ date:session.row.date, mode:session.row.mode, head:session.row.head, tag:session.row.tag, person:session.row.person,
            transferTo:session.row.transferTo, entity:session.row.entity, bankAc:session.row.bankAc } } };
    }
    return { reply:'Reply *CONFIRM* to record this row, or *cancel* to discard.' };
  }
  return { reply:'Reply "paid" on an approved item to start.', done:true };
}

// WhatsApp adapter: gate to the payments group + authorised accountants, manage
// per-accountant session state, drive paidFlowAdvance, and fire recordPaidEvent.
async function handlePaidFlow(msg){
  try{
    if(!msg || !waReady) return false;
    if(msg.from !== CONFIG.PAYMENT_OUTFLOW_GROUP_JID) return false;
    if(msg.fromMe) return false;
    var who=await resolveAccountant(msg);
    if(!(await isAuthorisedAccountant(msg.author||msg.from, who.name))) return false;
    var body=(msg.body||'').trim();
    if(!body) return false;
    var send=function(t){ return waClient.sendMessage(CONFIG.PAYMENT_OUTFLOW_GROUP_JID, t).then(function(m){ try{ if(ses && m && m.id) appendThreadMsg(ses.itemId, m.id._serialized); }catch(e){} return m; }); };

    // v2.10.0-s5.13: on-demand payments summary. "summary"/"status" replies with the
    // Approved-due / Paid / Yet-to-pay roll-up. Answered without touching any open paid
    // session (return true so the word is never fed into a Q&A step). ses is still
    // undefined here, so send()'s thread-append guard is a no-op for this message.
    if(/^(summary|status)$/i.test(body)){
      try{
        var sm=buildPaymentsSummary();
        savePaidSummaryMap({ at:new Date().toISOString(), items:(sm.numbered||[]).map(function(x){ return { n:x.n, itemId:x.itemId, label:x.label }; }) });
        await send(formatPaymentsSummary(sm));
      }catch(e){ console.error('[PaidFlow] summary:', e.message); }
      return true;
    }

    var st=loadPaidState(); prunePaidState(st);
    var ses=st.sessions[who.key];

    if(!ses){
      // v2.10.0-s5.14: close <n> [yes] — close/cancel an item as-is (anyone in the group, incl. accountants).
      var cm = body.match(/^close\s+(\d+)(\s+yes)?\s*$/i);
      if(cm){
        var cn = parseInt(cm[1],10), cConfirm = !!cm[2];
        var cmi = summaryItemByNumber(cn);
        if(!cmi){ await send('I don\u2019t have item #'+cn+' in the latest summary. Type *summary* to refresh the list, then *close '+cn+'*.'); return true; }
        var cStore = loadEventStore();
        if(isClosed(cStore, cmi.itemId)){ await send('*'+(cmi.label||cmi.itemId)+'* is already closed.'); return true; }
        var cRec = findPostedRec(loadPaidPosted(), cmi.itemId);
        var cApproved = cRec ? (cRec.amount||0) : 0;
        var cPaid = paidStatsForItem(cStore, cmi.itemId).total;
        var cWriteOff = Math.max(0, cApproved - cPaid);
        if(!cConfirm){
          var cLine = cPaid>0
            ? ('Approved \u20B9'+formatINR(cApproved)+' \u00B7 paid \u20B9'+formatINR(cPaid)+' \u2192 \u20B9'+formatINR(cWriteOff)+' written off, item closed.')
            : ('Approved \u20B9'+formatINR(cApproved)+' \u00B7 paid \u20B90 \u2192 cancelled, nothing will be paid.');
          await send('Close *'+(cmi.label||cmi.itemId)+'* as-is?\n'+cLine+'\nReply *close '+cn+' yes* to confirm.');
          return true;
        }
        recordClosedEvent(cmi.itemId, cmi.label, cApproved, cPaid);
        await send('\u2705 Closed *'+(cmi.label||cmi.itemId)+'* as-is \u2014 '+(cWriteOff>0?('\u20B9'+formatINR(cWriteOff)+' written off.'):'nothing outstanding.'));
        return true;
      }
      // v2.10.0-s5.14: a bare number from the latest summary → log the next instalment for that item.
      if(/^\d+$/.test(body)){
        var nn = parseInt(body,10);
        var nmi = summaryItemByNumber(nn);
        if(!nmi){ await send('I don\u2019t have item #'+nn+'. Type *summary* to see the current list and numbers.'); return true; }
        var nStore = loadEventStore();
        if(isClosed(nStore, nmi.itemId)){ await send('*'+(nmi.label||nmi.itemId)+'* is closed. Reopen it from the dashboard if it needs more payments.'); return true; }
        var nRec = findPostedRec(loadPaidPosted(), nmi.itemId);
        if(!nRec){   // s6.7: summary may list approved-but-unposted items; synthesize a posted rec from the approved event so they are payable
          var nAev = findApprovedEvent(nStore, nmi.itemId);
          if(nAev) nRec = registerPostedApproved('synth_'+Date.now()+'_'+Math.floor(Math.random()*1e6), { id:nAev.itemId, label:nAev.label, amount:nAev.amount, entity:(nmi&&nmi.entity)||'', bankAc:(nmi&&nmi.bankAc)||'', description:nAev.label });
        }
        if(!nRec){ await send('I can\u2019t find the posted item for #'+nn+'. Type *summary* to refresh.'); return true; }
        ses = newPaidSession(nRec, nStore, who.name);
        if(ses.balance<=0){ await send('*'+ses.label+'* is already fully paid (\u20B9'+formatINR(ses.amount)+'). Nothing left to log.'); return true; }
        st.sessions[who.key]=ses; savePaidState(st);
        try{ if(msg.id) appendThreadMsg(ses.itemId, msg.id._serialized); }catch(e){}
        var nNth = ses.seq>1 ? (' (instalment #'+ses.seq+')') : '';
        await send((who.name?who.name+' \u2014 ':'')+'logging payment for *'+ses.label+'*'+nNth+'.\nApproved \u20B9'+formatINR(ses.amount)+' \u00B7 paid so far \u20B9'+formatINR(ses.paidSoFar)+' \u00B7 *balance \u20B9'+formatINR(ses.balance)+'*\n\n1/7 *Amount paid?* — reply "ok" for \u20B9'+formatINR(ses.balance)+', or type the actual amount.');
        return true;
      }
      // "paid" on a quoted post — start/continue (instalment-aware).
      if(!/^paid\b/i.test(body)) return false;          // not in a flow, not a trigger
      var item=await resolvePostedItem(msg);
      if(!item){ await send((who.name?who.name+': ':'')+'I don\'t see a posted approved item to mark paid. Reply "paid" on the approved item I post here, or type *summary* and reply with its number.'); return true; }
      var pStore = loadEventStore();
      if(isClosed(pStore, item.id)){ await send('*'+(item.label||item.id)+'* is closed.'); return true; }
      ses = newPaidSession(item, pStore, who.name);
      if(ses.balance<=0){ await send('*'+ses.label+'* is already fully paid (\u20B9'+formatINR(ses.amount)+').'); return true; }
      st.sessions[who.key]=ses; savePaidState(st);
      try{ if(msg.id) appendThreadMsg(ses.itemId, msg.id._serialized); }catch(e){}   // the "paid" trigger msg
      var pNth = ses.seq>1 ? (' (instalment #'+ses.seq+')') : '';
      await send((who.name?who.name+' \u2014 ':'')+'marking *'+ses.label+'*'+pNth+' as paid.\nApproved \u20B9'+formatINR(ses.amount)+(ses.paidSoFar>0?(' \u00B7 paid so far \u20B9'+formatINR(ses.paidSoFar)+' \u00B7 *balance \u20B9'+formatINR(ses.balance)+'*'):'')+'\n\n1/7 *Amount paid?* — reply "ok" for \u20B9'+formatINR(ses.balance)+', or type the actual amount.');
      return true;
    }

    // In-progress guard: a fresh "paid" while a Q&A is already open would otherwise be
    // fed into the current question (e.g. silently parsed as a date). Intercept it so the
    // accountant must consciously cancel or finish, rather than corrupt the open entry.
    try{ if(msg.id) appendThreadMsg(ses.itemId, msg.id._serialized); }catch(e){}   // the accountant's in-flow reply
    if(/^paid\b/i.test(body)){
      var qlabel = PAID_STEP_LABEL[ses.step] || 'the current question';
      await send('\u26A0\uFE0F You\u2019re already part-way through marking *'+ses.label+'* as paid (currently on: '+qlabel+').\nReply *cancel* to scrap that and start over, or just answer the question above to finish it.');
      return true;
    }
    var out=await paidFlowAdvance(ses, body);
    ses.lastAt=new Date().toISOString();
    // s6.2: over-approval -> post a re-approval to the approval group; block the payment until M+S lift it
    if(out.reapproval){
      try{
        var _rmsg = buildReapprovalMessage(out.reapproval);
        var _sent = await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, _rmsg);
        registerReapproval(out.reapproval, _sent && _sent.id && (_sent.id._serialized||_sent.id.id));
        await send('\u26D4 \u20B9'+formatINR(out.reapproval.attempted)+' is over the approved \u20B9'+formatINR(out.reapproval.approved)+'. Sent to *M+S for re-approval* (reason: '+out.reapproval.reason+'). It can\u2019t be paid until both approve.');
      }catch(e){ console.error('[Reapproval] post:', e.message); await send('Couldn\u2019t post the re-approval request \u2014 please try again.'); }
      delete st.sessions[who.key]; savePaidState(st);
      return true;
    }
    var ledgerSuffix='';
    if(out.recordArgs){
      try{ recordPaidEvent(out.recordArgs.itemId, out.recordArgs.label, out.recordArgs.paidAmount, out.recordArgs.fields, out.recordArgs.seq); }
      catch(e){ console.error('[PaidFlow] recordPaidEvent:', e.message); }
      // v2.10.0-s4: attempt the ledger write (no-op while capture-only; logs in dry-run)
      try{
        var w = await writeRowToLedger(out.recordArgs.row);
        if(w.written) ledgerSuffix = '\n\u2705 Written to the Ledger ('+(w.plan&&w.plan.a1Range||'appended')+').';
        else if(w.dryRun) ledgerSuffix = '\n[dry-run] Would write to '+(w.plan&&w.plan.a1Range||'(bottom)')+' \u2014 nothing changed.';
        else if(w.dup) ledgerSuffix = '\n(Ledger row already exists \u2014 not duplicated.)';
        else if(w.skipped && w.plan && w.plan.action==='newday-blocked') ledgerSuffix = '\n(Not written to the Ledger: no '+out.recordArgs.row.date+' block yet.)';
        else ledgerSuffix = '\n(Capture-only \u2014 not written to the Sheet.)';
      }catch(e){ console.error('[PaidFlow] writeRowToLedger:', e.message); ledgerSuffix='\n(Ledger write errored \u2014 captured only.)'; }
      // s6.2: 'final / reduced' path -> close the item (writes off the unpaid remainder), no further instalments
      if(out.recordArgs.closeAfter){
        try{ recordClosedEvent(out.recordArgs.itemId, out.recordArgs.label, out.recordArgs.approved, out.recordArgs.paidTotalAfter);
             ledgerSuffix += '\n(Item closed \u2014 \u20B9'+formatINR(Math.max(0,(out.recordArgs.approved||0)-(out.recordArgs.paidTotalAfter||0)))+' written off.)'; }
        catch(e){ console.error('[PaidFlow] close:', e.message); }
      }
      // s6.6: if this payment is a promoter contribution REPAYMENT, knock it off that promoter's loan account
      try{
        var _crp=detectContributionRepayment(out.recordArgs.label);
        if(_crp){ recordEvent('contrepay', { promoter:_crp.promoter, entity:(out.recordArgs.fields&&out.recordArgs.fields.entity)||'', amount:out.recordArgs.paidAmount, date:(out.recordArgs.fields&&out.recordArgs.fields.date)||'', itemId:out.recordArgs.itemId }, 'contrepay:'+out.recordArgs.itemId+':'+out.recordArgs.seq);
          ledgerSuffix += '\n\u21A9\uFE0F Recorded as a *'+_crp.promoter+' contribution repayment* \u2014 reduced the '+_crp.promoter+' loan account.'; }
      }catch(e){ console.error('[PaidFlow] contrepay:', e.message); }
    }
    if(out.done) delete st.sessions[who.key];
    savePaidState(st);
    if(out.reply) await send(out.reply + ledgerSuffix);
    return true;
  }catch(e){ console.error('[PaidFlow]', e.message); return false; }
}

// ── v2.10.0-s5.17: INFLOW + TRANSFER group ──────────────────────────────────
// One group (INFLOW_GROUP_JID) handles two things, routed by the first word:
//   "received …"  → INFLOW  → writes an IN row (Tag = receivable code → receivables SUMIFS).
//   "transfer …"  → TRANSFER → writes a TRANSFER row (bankAc = from, transferTo = to).
// The opening line is parsed for amount/mode (+ from/to for transfer, payer for inflow) and those
// steps are pre-filled & skipped; only the missing answers are asked. CONFIRM always shows the row.
var IO_STEPS = {
  inflow:   [['amount','amount'],['date','date'],['mode','mode'],['head','head'],['tag','tag'],['entity','entity'],['account','bankAc'],['fromwhom','fromWhom']],
  transfer: [['amount','amount'],['date','date'],['mode','mode'],['fromacct','fromAcct'],['toacct','toAcct'],['entity','entity']],
  contribution: [['amount','amount'],['promoter','promoter'],['date','date'],['mode','mode'],['account','bankAc'],['entity','entity']]
};
function parseRoughAmount(s){
  var m=String(s||'').match(/(?:rs\.?|inr|\u20B9)?\s*([\d,]+(?:\.\d+)?)\s*(?:(lakhs?|lacs?|lac|lk|crores?|cr|k|thousand|l)\b)?/i);
  if(!m) return 0;
  var n=parseFloat(m[1].replace(/,/g,'')); if(isNaN(n)) return 0;
  var u=(m[2]||'').toLowerCase();
  if(/^l$|lakh|lac|lk/.test(u)) n*=100000; else if(/cr/.test(u)) n*=10000000; else if(/k|thousand/.test(u)) n*=1000;
  return Math.round(n);
}
function parseModeKeyword(body){
  if(/\brtgs\b|\bimps\b/i.test(body)) return 'RTGS';
  if(/\bneft\b/i.test(body)) return 'NEFT';
  if(/cheque|\bchq\b|\bcheck\b/i.test(body)) return 'Chq';
  if(/\bcash\b/i.test(body)) return 'Cash';
  if(/\bpdc\b/i.test(body)) return 'PDC';
  return '';
}
// Route + best-effort slot extraction from the opening line. Returns {kind, pre} or null (no trigger).
function parseInflowOpening(body){
  var raw=(body||'').trim(), low=raw.toLowerCase();
  if(!raw) return null;
  // --- money-like guard: kills "ok", "thanks", "got it 100%", "call at 5". A real inflow has an amount. ---
  var amount = parseRoughAmount(raw) || 0;
  var hasUnit = /\d\s*(?:lakhs?|lacs?|lac|lk|crores?|cr|k|thousand|l)\b/i.test(raw);
  var hasRs   = /(?:rs\.?|inr|\u20B9)\s*[\d,]/i.test(raw) || /\b(?:rupees?|rs)\b/i.test(low);
  var moneyLike = amount>0 && (hasUnit || hasRs || amount>=1000);
  if(!moneyLike) return null;
  // --- intent: word order doesn't matter; transfer wins over inflow when it's account->account ---
  var transferVerb = /\b(transfer(red|s)?|trf|tfr|moved?|shift(ed)?)\b/i.test(low);
  var inflowVerb   = /\b(received?|recie?ved|recvd|recd|rcvd|got|credit(ed)?|deposit(ed)?|inflow|collected|collection)\b/i.test(low) || /\bcame\s+in\b/i.test(low);
  var ft=raw.match(/from\s+(.+?)\s+to\s+([^,]+?)(?:\s+(?:rtgs|neft|imps|cheque|chq|cash|pdc)\b|\s*$)/i);
  var ftAccounts=false, rf=null, rt=null;
  if(ft){ rf=resolveAccount(ft[1].trim()); rt=resolveAccount(ft[2].trim()); ftAccounts=!!(rf.account && rt.account); }
  var fromPayer = /\bfrom\s+\S/i.test(low) || /\bby\s+\S/i.test(low);
  // contribution = promoter (MM/SM) putting money in. Triggered by a contribution word, or by the
  // promoter being the named source. A plain "drawing" is never a contribution.
  var contribIntent = /\b(contribution|contrib|capital|infus(ed|ion)|invest(ed|ment)?)\b/i.test(low) || /\b(put(ting)?|brought)\s+in\b/i.test(low);
  var promoter = detectPromoter(raw);
  var leadPromoter = /^\s*(mm|sm)\b/i.test(raw);
  var mfPayer = (raw.match(/\b(?:from|by)\s+(.+?)(?:\s+(?:into|in|rtgs|neft|imps|cheque|chq|cash|pdc)\b|\s*$)/i)||[])[1]||'';
  var fromIsPromoter = !!detectPromoter(mfPayer);
  var isContribution = !/\bdrawing/i.test(low) && (contribIntent || (promoter && (leadPromoter || fromIsPromoter)));
  var kind = (transferVerb || ftAccounts) ? 'transfer'
           : isContribution ? 'contribution'
           : (inflowVerb || fromPayer) ? 'inflow' : null;
  if(!kind) return null;
  // --- best-effort slot pre-fill (order-independent) ---
  var pre={ amount: amount, mode: parseModeKeyword(raw) };
  if(kind==='transfer'){
    if(ft){ if(rf && rf.account)pre.fromAcct=rf.account; if(rt && rt.account)pre.toAcct=rt.account; }
  } else if(kind==='contribution'){
    if(promoter) pre.promoter=promoter;
    var mic=raw.match(/\b(?:into|in)\s+(.+?)(?:\s+from\b|\s*$)/i);
    if(mic){ var rac=resolveAccount(mic[1].trim()); if(rac.account)pre.intoAcct=rac.account; }
  } else {
    var mi=raw.match(/\b(?:into|in)\s+(.+?)(?:\s+from\b|\s*$)/i);
    if(mi){ var ra=resolveAccount(mi[1].trim()); if(ra.account)pre.intoAcct=ra.account; }
    var mf=raw.match(/\bfrom\s+(.+?)(?:\s+(?:into|in|rtgs|neft|imps|cheque|chq|cash|pdc)\b|\s*$)/i);
    if(mf) pre.fromWhom=mf[1].trim();
  }
  return { kind:kind, pre:pre };
}
function ioFirstPendingStep(ses){
  var steps=IO_STEPS[ses.kind];
  for(var i=0;i<steps.length;i++){ var k=steps[i][1]; if(ses.answers[k]===undefined||ses.answers[k]==='') return steps[i][0]; }
  return 'confirm';
}
function newIoSession(kind, pre, byName){
  pre=pre||{};
  var ses={ kind:kind, id:(kind==='transfer'?'trf':kind==='contribution'?'con':'in')+'-'+Date.now()+'-'+Math.random().toString(36).slice(2,7),
    answers:{}, startedAt:new Date().toISOString(), lastAt:new Date().toISOString(), by:byName||'' };
  if(pre.amount) ses.answers.amount=pre.amount;
  if(pre.mode) ses.answers.mode=pre.mode;
  if(kind==='transfer'){ if(pre.fromAcct) ses.answers.fromAcct=pre.fromAcct; if(pre.toAcct) ses.answers.toAcct=pre.toAcct; }
  else if(kind==='contribution'){ if(pre.promoter) ses.answers.promoter=pre.promoter; if(pre.intoAcct) ses.answers.bankAc=pre.intoAcct; }
  else { if(pre.intoAcct) ses.answers.bankAc=pre.intoAcct; if(pre.fromWhom) ses.answers.fromWhom=pre.fromWhom; }
  ses.step=ioFirstPendingStep(ses);
  return ses;
}
function ioIntro(ses){
  var A=ses.answers, b=[];
  if(ses.kind==='transfer'){
    if(A.amount)b.push('\u20B9'+formatINR(A.amount)); if(A.fromAcct)b.push('from '+A.fromAcct); if(A.toAcct)b.push('to '+A.toAcct); if(A.mode)b.push(A.mode);
    return 'logging a *TRANSFER*'+(b.length?' \u2014 '+b.join(' \u00B7 '):'')+'.';
  }
  if(ses.kind==='contribution'){
    if(A.amount)b.push('\u20B9'+formatINR(A.amount)); if(A.promoter)b.push('by '+A.promoter); if(A.bankAc)b.push('into '+A.bankAc); if(A.mode)b.push(A.mode);
    return 'logging a *CONTRIBUTION*'+(b.length?' \u2014 '+b.join(' \u00B7 '):'')+'.';
  }
  if(A.amount)b.push('\u20B9'+formatINR(A.amount)); if(A.fromWhom)b.push('from '+A.fromWhom); if(A.bankAc)b.push('into '+A.bankAc); if(A.mode)b.push(A.mode);
  return 'logging an *INFLOW*'+(b.length?' \u2014 '+b.join(' \u00B7 '):'')+'.';
}
function ioNextPrompt(ses){
  var steps=IO_STEPS[ses.kind], total=steps.length, idx=0;
  for(var i=0;i<steps.length;i++){ if(steps[i][0]===ses.step){ idx=i+1; break; } }
  var n=idx+'/'+total+' ', k=ses.kind;
  switch(ses.step){
    case 'amount':   return n+'*Amount '+(k==='transfer'?'transferred':k==='contribution'?'contributed':'received')+'?* \u2014 type the amount (e.g. 500000 or "5 lakh").';
    case 'promoter': return n+'*Whose contribution \u2014 MM or SM?* \u2014 reply MM or SM.';
    case 'date':     return n+'*Date?* \u2014 reply "today" or dd/mm (e.g. 14/06).';
    case 'mode':     return n+'*Mode?* \u2014 reply the number:\n'+paidModeMenu();
    case 'head':     return n+'*Head?* \u2014 pick a number:\n'+paidHeadMenu()+'\n(or type the exact head name)';
    case 'tag':      return n+'*Tag (receivable)?* \u2014 pick a number:\n'+paidInflowTagMenu()+'\n(or type the exact tag)';
    case 'entity':   return n+'*Which entity / company?* \u2014 reply the number, or type the name:\n'+paidEntityMenu();
    case 'account':  return n+'*Received into which account?* \u2014 reply the number, or type the account name:\n'+paidAccountMenu();
    case 'fromwhom': return n+'*Received from whom?* \u2014 type the payer / source (buyer name, etc.).';
    case 'fromacct': return n+'*From which account?* \u2014 reply the number, or type the account name:\n'+paidAccountMenu();
    case 'toacct':   return n+'*To which account?* \u2014 reply the number, or type the account name:\n'+paidAccountMenu();
  }
  return '';
}
function ioPickFromList(low, input, list){
  if(/^\d+$/.test(low)){ var i=parseInt(low,10); if(i>=1&&i<=list.length) return list[i-1]; return null; }
  for(var j=0;j<list.length;j++){ if(list[j].toLowerCase()===low) return list[j]; }
  return null;
}
function ioPickAccount(low, input){
  if(/^\d+$/.test(low)){ var i=parseInt(low,10); if(i>=1&&i<=LEDGER_ACCOUNTS.length) return {value:LEDGER_ACCOUNTS[i-1]}; return {error:'Pick the account by number (1\u2013'+LEDGER_ACCOUNTS.length+') or type the name:\n'+paidAccountMenu()}; }
  var r=resolveAccount(input);
  if(r.account) return {value:r.account};
  if(r.ambiguous) return {error:'\u201C'+input+'\u201D matches several: '+r.ambiguous.join(', ')+'. Type the exact one or its number:\n'+paidAccountMenu()};
  return {error:'\u26A0\uFE0F \u201C'+input+'\u201D isn\u2019t a known account. Reply a number or type an exact name:\n'+paidAccountMenu()};
}
function ioPickEntity(low, input){
  if(/^\d+$/.test(low)){ var i=parseInt(low,10); if(i>=1&&i<=LEDGER_ENTITIES.length) return {value:LEDGER_ENTITIES[i-1]}; return {error:'Pick the entity by number (1\u2013'+LEDGER_ENTITIES.length+') or type the name:\n'+paidEntityMenu()}; }
  var r=resolveEntity(input);
  if(r.entity) return {value:r.entity};
  if(r.ambiguous) return {error:'\u201C'+input+'\u201D matches several: '+r.ambiguous.join(', ')+'. Type the exact one or its number:\n'+paidEntityMenu()};
  return {error:'\u26A0\uFE0F \u201C'+input+'\u201D isn\u2019t in the entity list. Reply a number or type an exact name:\n'+paidEntityMenu()};
}
function ioConsume(ses, step, input, low){
  var A=ses.answers;
  if(step==='amount'){ var amt=parseRoughAmount(input)||extractLineAmount(input,false)||parseAmount(input); if(!amt) return {error:'Didn\'t catch an amount. Type the amount (e.g. 500000 or "5 lakh").'}; A.amount=amt; return {}; }
  if(step==='date'){ A.date=toLedgerDate(/^today$/i.test(low)?'today':input); return {}; }
  if(step==='mode'){ var mi=parseInt(low,10); if(!(mi>=1&&mi<=LEDGER_MODES.length)) return {error:'Pick the mode by number:\n'+paidModeMenu()}; A.mode=LEDGER_MODES[mi-1]; return {}; }
  if(step==='head'){ var hv=ioPickFromList(low,input,LEDGER_HEADS); if(!hv) return {error:'Pick the Head by number (1\u2013'+LEDGER_HEADS.length+') or type an exact head:\n'+paidHeadMenu()}; A.head=hv; return {}; }
  if(step==='tag'){ var tv=ioPickFromList(low,input,LEDGER_INFLOW_TAGS); if(!tv) return {error:'Pick the Tag by number or type the exact tag:\n'+paidInflowTagMenu()}; A.tag=tv; return {}; }
  if(step==='entity'){ var ev=ioPickEntity(low,input); if(ev.error) return {error:ev.error}; A.entity=ev.value; return {}; }
  if(step==='account'){ var av=ioPickAccount(low,input); if(av.error) return {error:av.error}; A.bankAc=av.value; return {}; }
  if(step==='fromwhom'){ if(!input) return {error:'Type the payer / source name.'}; A.fromWhom=input; return {}; }
  if(step==='promoter'){ var pp=detectPromoter(input); if(!pp) return {error:'Reply *MM* or *SM* \u2014 whose contribution is this?'}; A.promoter=pp; return {}; }
  if(step==='fromacct'){ var fa=ioPickAccount(low,input); if(fa.error) return {error:fa.error}; A.fromAcct=fa.value; return {}; }
  if(step==='toacct'){ var ta=ioPickAccount(low,input); if(ta.error) return {error:ta.error}; A.toAcct=ta.value; return {}; }
  return {};
}
function ioFlowAdvance(ses, inputRaw){
  var input=(inputRaw||'').trim(), low=input.toLowerCase();
  if(/^(cancel|reset|clear|stop)$/i.test(low)) return { reply:(ses.kind==='transfer'?'Transfer':'Inflow')+' entry cancelled.', done:true };
  if(ses.step==='confirm'){
    if(/^(confirm|yes|ok|y)$/i.test(low)){
      var rec={ row:ses.row };
      if(ses.kind==='contribution'){ var Ac=ses.answers; rec.contribution={ promoter:Ac.promoter, entity:Ac.entity||'', amount:Ac.amount, date:Ac.date }; }
      return { reply:'\u2713 Recorded.', done:true, recordArgs:rec };
    }
    return { reply:'Reply *CONFIRM* to record, or *cancel* to discard.' };
  }
  var r=ioConsume(ses, ses.step, input, low);
  if(r && r.error) return { reply:r.error };
  ses.step=ioFirstPendingStep(ses);
  if(ses.step==='confirm'){
    ses.row = (ses.kind==='transfer') ? assembleTransferRow(ses) : (ses.kind==='contribution') ? assembleContributionRow(ses) : assembleInflowRow(ses);
    return { reply: paidRowPreview(ses.row)+'\n\nReply *CONFIRM* to record, or *cancel*.' };
  }
  return { reply: ioNextPrompt(ses) };
}
// WhatsApp adapter for the inflow/transfer group. Same auth + ledger-write gating as the paid flow.
async function handleInflowFlow(msg){
  try{
    if(!msg || !waReady) return false;
    if(msg.from !== CONFIG.INFLOW_GROUP_JID) return false;
    if(msg.fromMe) return false;
    var who=await resolveAccountant(msg);
    if(!(await isAuthorisedAccountant(msg.author||msg.from, who.name))) return false;
    var body=(msg.body||'').trim();
    if(!body) return false;
    var send=function(t){ return waClient.sendMessage(CONFIG.INFLOW_GROUP_JID, t); };

    var st=loadPaidState(); prunePaidState(st);
    var key=who.key+'#io';                 // namespaced so it can't collide with an outflow paid session
    var ses=st.sessions[key];

    // v2.11.0-s6.6: promoter loan-account statement on demand (only when no flow is mid-session)
    if(!ses && /^(contributions?|promoter\s+accounts?|(mm|sm)\s+(account|contributions?))\s*$/i.test(body)){
      var pq = /\bsm\b/i.test(body)?'SM':(/\bmm\b/i.test(body)?'MM':null);
      await send(formatContributionStatement(pq));
      return true;
    }

    if(!ses){
      var opening=parseInflowOpening(body);
      if(!opening) return false;           // not a trigger word → ignore
      ses=newIoSession(opening.kind, opening.pre, who.name);
      st.sessions[key]=ses; savePaidState(st);
      await send((who.name?who.name+' \u2014 ':'')+ioIntro(ses)+'\n\n'+ioNextPrompt(ses));
      return true;
    }

    var out=ioFlowAdvance(ses, body);
    ses.lastAt=new Date().toISOString();
    var ledgerSuffix='';
    if(out.recordArgs){
      try{
        var w=await writeRowToLedger(out.recordArgs.row);
        if(w.written) ledgerSuffix='\n\u2705 Written to the Ledger ('+(w.plan&&w.plan.a1Range||'appended')+').';
        else if(w.dryRun) ledgerSuffix='\n[dry-run] Would write to '+(w.plan&&w.plan.a1Range||'(bottom)')+' \u2014 nothing changed.';
        else if(w.dup) ledgerSuffix='\n(Ledger row already exists \u2014 not duplicated.)';
        else if(w.skipped && w.plan && w.plan.action==='newday-blocked') ledgerSuffix='\n(Not written: no '+out.recordArgs.row.date+' block yet.)';
        else ledgerSuffix='\n(Capture-only \u2014 not written to the Sheet.)';
      }catch(e){ console.error('[Inflow] writeRowToLedger:', e.message); ledgerSuffix='\n(Ledger write errored \u2014 not written.)'; }
      if(out.recordArgs.contribution){
        var c=out.recordArgs.contribution;
        try{ recordEvent('contribution', { promoter:c.promoter, entity:c.entity, amount:c.amount, date:c.date }, 'contribution:'+ses.id);
             ledgerSuffix += '\n\u2705 Logged as *'+c.promoter+' contribution* \u2014 added to the '+c.promoter+' loan account.'; }
        catch(e){ console.error('[Inflow] contribution event:', e.message); }
      }
    }
    if(out.done) delete st.sessions[key];
    savePaidState(st);
    if(out.reply) await send(out.reply + ledgerSuffix);
    return true;
  }catch(e){ console.error('[Inflow]', e.message); return false; }
}

// ── v2.10.0-s3: THE BRIDGE — approved item → post into the outflow group ─────
// Toggle-gated (default OFF) like every other side-effecting stage: deploying is a
// no-op until the toggle is ON, so it can ship before going live.
// Idempotent: the same expense id is posted at most once (byItem index).
// v2.10.0-s3.1: the toggle is now a RUNTIME switch flippable from the locked control
// panel (saved to ./wa_auth/outflow_post.json, read live). The OUTFLOW_POST_ENABLED env
// var is the boot DEFAULT used only until the dashboard toggle has been set at least once.
var OUTFLOW_POST_ENABLED = (process.env.OUTFLOW_POST_ENABLED === 'true');
var OUTFLOW_POST_STATE_FILE = './wa_auth/outflow_post.json';
function loadOutflowPostEnabled(){
  try{ if(fs.existsSync(OUTFLOW_POST_STATE_FILE)){ return JSON.parse(fs.readFileSync(OUTFLOW_POST_STATE_FILE,'utf8')).enabled===true; } }catch(e){}
  return OUTFLOW_POST_ENABLED;   // env-var default until the dashboard toggle overrides it
}
function saveOutflowPostEnabled(on){ try{ if(!fs.existsSync('./wa_auth')) fs.mkdirSync('./wa_auth',{recursive:true}); fs.writeFileSync(OUTFLOW_POST_STATE_FILE, JSON.stringify({enabled:!!on, at:new Date().toISOString()})); }catch(e){ console.error('[Outflow] toggle save:',e.message); } }
async function postApprovedToOutflow(item, amount, force, opts){
  opts = opts || {};
  if(!force && !loadOutflowPostEnabled()) return false;   // auto path respects the live toggle; manual/sweep passes force
  if(!waReady || !waClient || !item || !item.id) return false;
  if(alreadyPosted(item.id)) return false;          // never double-post
  try{
    var entity = item.entity || '';
    var bankAc = item.bankAc || '';
    var desc   = item.description || item.label || '';
    // v2.10.0-s5.13: a dummy/test post (opts.test) is identical to the real bridge post but carries a
    // visible \u27E8TEST\u27E9 tag in the header so it can never be mistaken for a real payment-due item.
    var header = opts.test ? '*PAYMENT DUE*  \u27E8TEST\u27E9 \u2014 approved by M+S' : '*PAYMENT DUE* \u2014 approved by M+S';
    var lines = [header, '', desc, '*Rs.'+formatINR(amount)+'*'];
    if(entity) lines.push('Company: '+entity);
    if(bankAc) lines.push('From: '+bankAc);
    lines.push('', 'Once paid, reply *paid* on this message to log it.');
    var sent = await waClient.sendMessage(CONFIG.PAYMENT_OUTFLOW_GROUP_JID, lines.join('\n'));
    var postedMsgId = (sent && sent.id && (sent.id._serialized || sent.id.id)) || ('post_'+item.id+'_'+Date.now());
    registerPostedApproved(postedMsgId, { id:item.id, label:desc, amount:amount, entity:entity, bankAc:bankAc, description:desc });
    console.log('[Outflow] posted approved item', item.id, 'Rs.'+amount, 'as', postedMsgId);
    return true;
  }catch(e){ console.error('[Outflow] post:', e.message); return false; }
}

// ── v2.10.0-s5.2: backfill — sourced from the approval AUDIT (fast). ──────────
// s5.1 sourced this from buildReconciliation().awaitingPayment, but that runs the
// per-item AI Ledger matcher (tens of seconds) — far too heavy for a page load, so the
// queue page hung. We now use buildApprovalAudit().fullyApproved directly: fast, shows
// the real approved backlog, enriched with Company/From from the request body. Trade-off:
// this does NOT auto-exclude items already paid in the Ledger (that needed the slow
// matcher), so push selectively — per-item buttons let you skip anything already paid.
function mapApprovedToQueue(approved, pp){            // pure: testable offline
  var list = (approved||[]).map(function(e){
    var ef = parseExpenseFields(e.body||'');
    var posted = !!(pp && pp.byItem && pp.byItem[e.id]);
    return {
      itemId: e.id,
      label: ef.description || e.vendor || (e.body||'').replace(/\n/g,' ').substring(0,60) || e.id,
      amount: e.approvedAmount || e.amount,
      approvedAt: e.date ? (e.date.toISOString ? e.date.toISOString() : String(e.date)) : null,
      entity: ef.entity || '',
      bankAc: ef.bankAc || '',
      posted: posted,
      paid: false,
      status: posted ? 'posted' : 'pending'
    };
  });
  list.sort(function(x,y){ return (Date.parse(y.approvedAt)||0)-(Date.parse(x.approvedAt)||0); });
  return list;
}
async function listApprovedForOutflow(days){
  var audit = await buildApprovalAudit(days || 15);
  return mapApprovedToQueue(audit.fullyApproved, loadPaidPosted());
}
// Manually push one approved item (admin action; bypasses the auto toggle, still dedupes).
async function pushOneApprovedToOutflow(itemId){
  var list = await listApprovedForOutflow(), a=null;
  for(var i=0;i<list.length;i++){ if(list[i].itemId===itemId){ a=list[i]; break; } }
  if(!a) return { error:'no awaiting-payment item with id '+itemId+' (it may already be paid in the Ledger)' };
  if(a.posted) return { skipped:true, reason:'already posted', itemId:itemId };
  var ok = await postApprovedToOutflow({ id:a.itemId, label:a.label, amount:a.amount, entity:a.entity, bankAc:a.bankAc, description:a.label }, a.amount, true);
  return ok ? { pushed:true, itemId:itemId, label:a.label, amount:a.amount } : { error:'post failed (WhatsApp not connected?)', itemId:itemId };
}
// One-time catch-up: push every awaiting-payment item not already posted.
// s6.7: a NARROW capital-in test. Only the "<who> Contribution of Rs <amount>" capital-raise phrasing is
// treated as money INTO a company (and hidden from the outflow payments universe). Deliberately does NOT
// match a contribution REPAYMENT like "SM EXCESS contribution old" (a payout back to the promoter = a real
// outflow), nor a drawing. Keep this tight so payments are never silently hidden on a loose keyword.
function isCapitalInflowLabel(label){
  return /contribution\s+of\s+(rs|inr|rupee|\u20B9)/i.test(label||'');
}

// s6.7: the catch-up source, RE-SOURCED from the event store (was the lossy chat-history audit). Every
// approved event that is not a capital-in raise, not already posted, not paid, not closed -> needs posting.
function listApprovedFromEventStore(){
  var store=loadEventStore(), out=[], seen={};
  (store.events||[]).forEach(function(e){
    if(e.type!=='approved' || seen[e.itemId]) return;
    if(isCapitalInflowLabel(e.label)) return;
    if(alreadyPosted(e.itemId)) return;
    if(isClosed(store,e.itemId)) return;
    if(paidStatsForItem(store,e.itemId).total>0) return;
    seen[e.itemId]=true;
    out.push({ itemId:e.itemId, label:e.label||e.itemId, amount:e.amount||0 });
  });
  return out;
}

async function catchUpApprovedToOutflow(){
  var list = listApprovedFromEventStore(), pushed=[], skipped=[];   // s6.7: event-store sourced (was chat-history audit)
  for(var i=0;i<list.length;i++){
    var a=list[i];
    if(a.posted){ skipped.push({ itemId:a.itemId, reason:'posted' }); continue; }
    var ok = await postApprovedToOutflow({ id:a.itemId, label:a.label, amount:a.amount, entity:a.entity, bankAc:a.bankAc, description:a.label }, a.amount, true);
    if(ok) pushed.push({ itemId:a.itemId, label:a.label, amount:a.amount });
    else skipped.push({ itemId:a.itemId, reason:'post failed' });
  }
  return { pushedCount:pushed.length, skippedCount:skipped.length, pushed:pushed, skipped:skipped };
}

// v2.8.11: parse any of the bot's own posts into the item(s) it was about.
// Returns {kind:'digest'|'single', items:[{label,amount}]} or null if not a bot post.
// - digest: '🔔 *PENDING APPROVALS*' with numbered '*N.* label' lines + '*Rs.X*' lines
// - single: '[BOT REMINDER] - ...' or '*⚡ URGENT — approval needed*' (one expense:
//   vendor line after header + 'Amount: Rs.X')
function parseBotPostItems(text){
  if(!text) return null;
  if(/PENDING APPROVALS|🔔/i.test(text)){
    var items=[]; var lines=text.split('\n');
    for(var i=0;i<lines.length;i++){
      var m=lines[i].match(/^\s*\*?(\d+)\.\*?\s+(.+)$/);
      if(m){
        var amt=0;
        var am=(lines[i+1]||'').match(/rs\.?\s*([\d,]+)/i);
        if(am) amt=parseAmount(am[1]);
        items.push({label:m[2].replace(/…\s*$/,'').trim(), amount:amt});
      }
    }
    return items.length ? {kind:'digest', items:items} : {kind:'digest', items:[]};
  }
  if(/^\s*\[BOT REMINDER\]/i.test(text) || /⚡\s*URGENT|URGENT\s*—\s*approval needed/i.test(text)){
    var ls=text.split('\n').map(function(x){return x.trim();}).filter(Boolean);
    var label=ls[1]||'';
    var amt2=0;
    for(var j=0;j<ls.length;j++){
      var am2=ls[j].match(/^Amount:\s*rs\.?\s*([\d,]+)/i);
      if(am2){ amt2=parseAmount(am2[1]); break; }
    }
    return {kind:'single', items:[{label:label.replace(/…\s*$/,'').trim(), amount:amt2}]};
  }
  return null;
}

// Overlay numbered-reply verdicts onto audit expenses + compute state/approvedAmount.
// Rule A: an amend counts as that person's yes at the amended amount; the other
// party's plain yes carries to it. Stricter signal wins (no > hold > question > yes).
// ── v2.8 Module 4: silent re-ask clustering ──────────────────────────────────
// Same amount (±Rs.1) + at least one significant shared word (5+ chars) + within
// 30 days → same clusterId. Cross-sender included (team re-asks). When any member
// is paid, reconciliation retires the whole cluster from awaiting-payment.
var CLUSTER_STOPWORDS = ['kindly','approve','approval','please','payment','amount','request','expense','account','towards','against'];
function clusterWords(e){
  var t = ((e.vendor||'')+' '+(e.body||'')).toLowerCase();
  return t.split(/[^a-z]+/).filter(function(w){ return w.length>=5 && CLUSTER_STOPWORDS.indexOf(w)<0; });
}
function assignClusters(expenses){
  var THIRTY_D = 30*86400000;
  for(var i=0;i<expenses.length;i++){
    var a = expenses[i];
    if(!a.clusterId) a.clusterId = a.id;
    if(!(a.amount>0)) continue;
    var aw = clusterWords(a);
    if(aw.length===0) continue;
    for(var j=i+1;j<expenses.length;j++){
      var b = expenses[j];
      if(!(b.amount>0)) continue;
      if(Math.abs(a.amount-b.amount)>1) continue;
      if(Math.abs(a.date.getTime()-b.date.getTime())>THIRTY_D) continue;
      var bw = clusterWords(b);
      var shared = aw.some(function(w){ return bw.indexOf(w)>=0; });
      if(shared){ b.clusterId = a.clusterId; }
    }
  }
}

function applyVerdictOverrides(expenses){
  var v = loadVerdicts();
  // v2.8.12: build an id->expense map, plus a content reconciler so a verdict stored
  // under a digest-map id that has since drifted still finds its expense by label+amount.
  var byId = {};
  expenses.forEach(function(e){ byId[e.id] = e; });
  function vStop(s){ return ['the','and','for','with','from','this','that','please','approve','kindly','payment','amount','pending','approvals','approval','request','requests','needs','reply','number','yes','hold','later','reject','rs','inr','total','both'].indexOf(s)>=0; }
  function reconcileByContent(label, amount){
    var w = (label||'').toLowerCase().replace(/…\s*$/,'').split(/[^a-z0-9]+/).filter(function(x){ return x.length>=4 && !vStop(x); });
    if(!w.length) return null;
    var cands = expenses.filter(function(e){
      var ev = ((e.vendor||'')+' '+(e.body||'')).toLowerCase();
      return w.some(function(word){ return ev.indexOf(word)>=0; });
    });
    if(cands.length>1 && amount>0){ var nar=cands.filter(function(e){ return e.amount===amount; }); if(nar.length) cands=nar; }
    return cands.length===1 ? cands[0] : null;
  }
  // Resolve each verdict-store key to a live expense (direct id, else content fallback).
  Object.keys(v).forEach(function(key){
    var ov = v[key];
    if(!ov || (!ov.mm && !ov.sm)) return;
    var target = byId[key];
    if(!target && (ov._label || ov._amount!=null)){
      target = reconcileByContent(ov._label, ov._amount);
    }
    if(target && !target._verdictApplied){
      // attach the override onto the resolved expense (so the loop below uses it)
      target._resolvedVerdict = ov;
    }
  });
  expenses.forEach(function(e){
    var ov = v[e.id] || e._resolvedVerdict;
    e.requestedAmount = e.amount;
    e.approvedAmount = e.amount;
    if(ov){
      ['mm','sm'].forEach(function(r){
        if(!ov[r]) return;
        var vd = ov[r].verdict;
        if(vd==='yes') e.status[r]='yes';
        else if(vd==='no') e.status[r]='no';
        else if(vd==='hold') e.status[r]='hold';
        else if(vd==='question') e.status[r]='question';
        else if(vd==='amend'){ e.status[r]='yes'; if(ov[r].amount>0 && ov[r].amount<e.approvedAmount) e.approvedAmount=ov[r].amount; }
      });
      e.verdictLog = ov;
    }
    var st = e.status;
    if(st.mm==='no'||st.sm==='no') e.state='rejected';
    else if(st.mm==='hold'||st.sm==='hold') e.state='held';
    else if(st.mm==='question'||st.sm==='question') e.state='query';
    else if(st.mm==='yes'&&st.sm==='yes') e.state=(e.approvedAmount!==e.requestedAmount)?'amended':'approved';
    else e.state='pending';
  });
}

// ── v2.8 grouped digest: scoreboard + numbered items + held section ──────────
async function buildApprovalReminderDigest() {
  var audit = await buildApprovalAudit(REMINDER_MAX_AGE_DAYS + 1);
  var nowMs = Date.now();
  var cutoffMs = REMINDER_MAX_AGE_DAYS * 86400000;
  var store = loadEventStore();
  // s6.8: an entry is DONE (must NOT show as "needs approval") if it is already fully approved by M+S,
  // already paid, or is the bot's own "Approved (M+S)" confirmation echo wrongly re-read as a request.
  function alreadyDone(e){
    var lbl = ((e.vendor||'')+' '+(e.body||''));
    if(/approved\s*\(m\s*\+\s*s\)/i.test(lbl)) return true;
    if(findApprovedEvent(store, e.id)) return true;
    if(paidStatsForItem(store, e.id).total>0) return true;
    return false;
  }
  var keep = function(e){
    var hasAmt = e.amount > 0 || (e.subItems && e.subItems.length > 0);
    var ageOk = (nowMs - e.date.getTime()) <= cutoffMs;
    var afterStart = e.date.getTime() >= REPORT_START_MS;
    return hasAmt && ageOk && afterStart && !isContributionEntry(e) && !alreadyDone(e);
  };
  var base = audit.partialApproval.concat(audit.noApproval).filter(keep);
  var held = (audit.onHold||[]).filter(keep);
  // partition every still-pending entry by WHO has not yet said yes (held folds in under whoever must act)
  var onM=[], onS=[], onBoth=[], seenId={};
  base.concat(held).forEach(function(e){
    if(seenId[e.id]) return; seenId[e.id]=true;
    if(e.state==='rejected'||e.state==='approved'||e.state==='amended') return;
    var mYes = e.status && e.status.mm==='yes';
    var sYes = e.status && e.status.sm==='yes';
    if(mYes && sYes) return;            // belt-and-braces; alreadyDone already removed both-approved
    if(sYes && !mYes) onM.push(e);
    else if(mYes && !sYes) onS.push(e);
    else onBoth.push(e);
  });
  if(onM.length+onS.length+onBoth.length === 0) return null;
  var byOldest = function(a,b){ return a.date.getTime()-b.date.getTime(); };
  onM.sort(byOldest); onS.sort(byOldest); onBoth.sort(byOldest);
  function sum(arr){ return arr.reduce(function(s,e){return s+(e.approvedAmount||e.amount);},0); }
  function age(e){ var h=Math.floor((nowMs-e.date.getTime())/3600000); return h>=24?Math.floor(h/24)+'d':h+'h'; }
  function label(e){
    var raw = e.vendor || e.body || '';
    var cleaned = (typeof cleanDetails==='function') ? cleanDetails(raw) : raw;
    var l = (cleaned && cleaned.length>1 ? cleaned : raw).replace(/\n/g,' ').trim();
    return l.length>48 ? l.substring(0,48)+'…' : l;
  }
  var d = new Date(nowMs+5.5*3600000);
  var dayLabel = d.getUTCDate()+' '+['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][d.getUTCMonth()];
  // CONTINUOUS global numbering across the three messages, so "5 yes" maps no matter which message it is
  // quoted on; each message shows only its own slice but the numbers never reset.
  var n=0, allItems=[];
  function buildMsg(titleWord, note, arr, mentionJids){
    if(!arr.length) return null;
    var firstN = n+1, segItems=[];
    var lines = ['🔔 *APPROVALS NEEDED — '+titleWord+'*  ·  '+dayLabel];
    lines.push(arr.length+(arr.length===1?' request':' requests')+' · Rs.'+formatINR(sum(arr)));
    if(note) lines.push('_'+note+'_');
    lines.push('');
    arr.forEach(function(e){
      n++;
      var heldTag = (e.state==='held' || (e.status&&(e.status.mm==='hold'||e.status.sm==='hold'))) ? ' _(on hold)_' : '';
      var q = e.state==='query' ? ' _(query open)_' : '';
      lines.push('*'+n+'.* '+label(e));
      lines.push('*Rs.'+formatINR(e.approvedAmount||e.amount)+'* _('+age(e)+' ago)_'+heldTag+q);
      var ef = parseExpenseFields(e.body||'');
      var item = {n:n, id:e.id, label:label(e), amount:e.approvedAmount||e.amount, sender:e.sender, entity:ef.entity, bankAc:ef.bankAc, description:ef.description||label(e)};
      segItems.push(item); allItems.push(item);
    });
    lines.push('');
    lines.push('_Reply with the number:_  *'+firstN+' yes* · *'+firstN+' no* · *'+firstN+' hold*');
    if(arr.length>1) lines.push('_Many at once: '+firstN+' yes '+(firstN+1)+' yes …_');
    return { kind:titleWord, text: lines.join('\n'), mentionJids: mentionJids, items: segItems };
  }
  var MM=CONFIG.MM_PHONE+'@c.us', SM=CONFIG.SM_PHONE+'@c.us';
  var messages=[];
  var mMsg = buildMsg('M', 'S has already approved these — waiting on M', onM, [MM]);
  var sMsg = buildMsg('S', 'M has already approved these — waiting on S', onS, [SM]);
  var bMsg = buildMsg('M & S', 'fresh — neither has approved yet', onBoth, [MM,SM]);
  [mMsg,sMsg,bMsg].forEach(function(x){ if(x) messages.push(x); });
  if(!messages.length) return null;
  return { messages: messages, count: n, items: allItems, text: messages.map(function(m){return m.text;}).join('\n\n———\n\n') };
}
async function sendApprovalReminderDigest() {
  if(!waReady){ console.log('[Digest] WA not connected'); return 0; }
  if(!CONFIG.BOT_ENABLED){ console.log('[Digest] bot disabled'); return 0; }
  try {
    var digest = await buildApprovalReminderDigest();
    if(!digest || !digest.messages || !digest.messages.length){ console.log('[Digest] nothing pending in window'); return 0; }
    var msgIds=[];
    for(var i=0;i<digest.messages.length;i++){
      var seg = digest.messages[i];
      var sentMsg = await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, seg.text, { mentions: seg.mentionJids });
      var mid = sentMsg && sentMsg.id ? (sentMsg.id._serialized || sentMsg.id.id) : null;
      msgIds.push(mid);
      seg.items.forEach(function(it){ it.msgId = mid; });   // tag so a quoted reply resolves to that message's slice
    }
    saveDigestMap({ at: new Date().toISOString(), msgId: msgIds[0], msgIds: msgIds, items: digest.items });
    console.log('[Digest] posted', digest.messages.length, 'message(s),', digest.count, 'pending; map saved with', digest.items.length, 'items');
    return digest.count;
  } catch(e){ console.error('[Digest] error:', e.message); return 0; }
}

// ── v2.8 verdict parsing ──────────────────────────────────────────────────────
// Parses "1 yes 2 yes 3 no 4 hold", "1-4 yes", "all ok", "3 ok 50000", "2 why".
function parseVerdictMessage(body, maxN){
  if(!body) return null;
  var t = body.toLowerCase().replace(/;+/g,' ').replace(/,(?=\s|$)/g,' ').replace(/\s+/g,' ').trim();
  if(!/\d|all/.test(t)) return null;
  if(!/(yes|ok|okay|approve|no|reject|hold|why|reason)/.test(t)) return null;
  var out = [];
  function push(n, vd, amt){ if(n>=1 && n<=maxN) out.push({n:n, verdict:vd, amount:amt||0}); }
  function vmap(w){ if(/^(yes|ok|okay|approved?|approve)$/.test(w)) return 'yes'; if(/^(no|rejected?|reject)$/.test(w)) return 'no'; if(w==='hold') return 'hold'; if(/^(why|reason)$/.test(w)) return 'question'; return null; }
  var rest = t;
  // ranges: "1-4 yes" / "1 to 4 yes"
  rest = rest.replace(/(\d+)\s*(?:-|to)\s*(\d+)\s+(yes|ok|okay|approved?|no|rejected?|hold)/g, function(_,a,b,w){
    var vd=vmap(w); for(var i=parseInt(a);i<=parseInt(b);i++) push(i,vd);
    return ' ';
  });
  // all: "all yes"
  rest = rest.replace(/\ball\s+(yes|ok|okay|approved?|no|rejected?|hold)\b/g, function(_,w){
    var vd=vmap(w); for(var i=1;i<=maxN;i++) push(i,vd);
    return ' ';
  });
  // per-item with optional amount: "3 ok 50000" / "3 yes 50k" / "2 why".
  // Amount must look like an amount (unit, 4+ digits, comma format, or Rs prefix)
  // so a bare following item number ("1 yes 2 yes") is never eaten as an amount.
  var re = /(\d+)\s*[:.\)]?\s*(yes|ok|okay|approved?|approve|no|rejected?|reject|hold|why|reason)\b\s*\??\s*((?:rs\.?\s*)?\d[\d,]*\.?\d*\s*(?:k|lakhs?|lacs?|crores?|cr)\b|rs\.?\s*\d[\d,]*\.?\d*|\d{4,}[\d,]*|\d{1,3}(?:,\d{2,3})+)?/g;
  var m;
  while((m = re.exec(rest)) !== null){
    var vd = vmap(m[2]);
    if(!vd) continue;
    var amt = 0;
    if(m[3]){ amt = extractLineAmount(m[3], false) || parseAmount(m[3]); }
    if(vd==='yes' && amt>0) push(parseInt(m[1]),'amend',amt);
    else push(parseInt(m[1]),vd,amt);
  }
  if(out.length===0) return null;
  // last verdict for an item wins within one message
  var byN = {}; out.forEach(function(o){ byN[o.n]=o; });
  return Object.keys(byN).map(function(k){ return byN[k]; });
}

// Sonnet fallback for messy verdict messages.
async function aiParseVerdicts(body, items){
  if(!CONFIG.CLAUDE_API_KEY) return null;
  var list = items.map(function(it){ return it.n+'. '+it.label+' Rs.'+formatINR(it.amount); }).join('\n');
  var prompt = 'A company promoter replied to this numbered approval list:\n'+list+'\n\nTheir reply: "'+body.substring(0,400)+'"\n\nExtract per-item verdicts. verdict is one of yes/no/hold/question/amend (amend = approved at a different amount; include amount in rupees). Reply ONLY strict JSON: {"verdicts":[{"n":1,"verdict":"yes","amount":0}]} — empty array if the reply is not about these items.';
  try{
    var resp = await fetch('https://api.anthropic.com/v1/messages', { method:'POST', headers:{'Content-Type':'application/json','x-api-key':CONFIG.CLAUDE_API_KEY,'anthropic-version':'2023-06-01'}, body: JSON.stringify({ model:'claude-sonnet-4-6', max_tokens:400, messages:[{role:'user',content:prompt}] }) });
    if(!resp.ok) return null;
    var data = await resp.json(); var text='';
    if(data.content) for(var i=0;i<data.content.length;i++) if(data.content[i].type==='text'){ text=data.content[i].text; break; }
    text = text.replace(/```json|```/g,'').trim();
    var p; try{ p=JSON.parse(text); }catch(e){ var mm=text.match(/\{[\s\S]*\}/); if(!mm) return null; p=JSON.parse(mm[0]); }
    if(!p.verdicts || !p.verdicts.length) return null;
    return p.verdicts.filter(function(v){ return v.n>=1 && v.n<=items.length && /^(yes|no|hold|question|amend)$/.test(v.verdict); }).map(function(v){ return {n:v.n, verdict:v.verdict, amount:parseAmount(v.amount)||0}; });
  }catch(e){ console.error('[AI verdicts]', e.message); return null; }
}

// Handle group messages from M or S as numbered verdicts against the latest digest.
// v2.11.0-s6.4: group-post policy for a SINGLE promoter verdict. A lone YES posts NOTHING (it reads
// like the expense is already approved) — only the combined "Approved (M+S)" line announces a yes, once
// BOTH are in. A no/hold still posts (terminal/explicit, not misleading). Returns null = stay silent.
function perVerdictNotice(v, whoLabel, label, amount){
  var amt = amount ? (' Rs.'+formatINR(amount)) : '';
  if(v==='no')   return '\u274C '+label+amt+' \u2014 rejected by '+whoLabel+'.';
  if(v==='hold') return '\u23F8\uFE0F '+label+amt+' \u2014 put on hold by '+whoLabel+'.';
  return null;
}
async function handlePromoterVerdicts(msg){
  try{
    if(msg.from !== CONFIG.APPROVAL_GROUP_JID) return false;
    var author = (msg.author||'');
    // v2.8.13: WhatsApp now delivers group authors as @lid IDs (e.g. 102469514330302@lid),
    // not phone numbers — so a raw phone-prefix check fails. Resolve role via
    // identifySender (name-based, same as the rest of the bot), with a phone fallback.
    var role = author.indexOf(CONFIG.MM_PHONE)===0 ? 'mm' : (author.indexOf(CONFIG.SM_PHONE)===0 ? 'sm' : null);
    if(!role){
      var who2 = await identifySender(author);
      if(who2 && (who2.role==='mm'||who2.role==='sm')) role = who2.role;
    }
    if(!role) return false;
    var body = (msg.body||'').trim();
    if(!body) return false;
    // v2.10.0-s5.20: M/S approve by swipe-replying a bare "ok/yes/no/hold" DIRECTLY on the
    // EXPENSE REQUEST post (confirmed via /api/debug-replies + screenshot — both promoters do
    // this). The old path only counted replies to the DIGEST, so these were silently dropped
    // and the M+S gate never closed. The quoted post's msgId IS the item id, so record the
    // verdict against it directly — no digest needed — and echo a visible confirmation.
    if(msg.hasQuotedMsg){
      try{
        var _qm = await msg.getQuotedMessage();
        var _qBody = (_qm && _qm.body) || '';
        var _qId = _qm && (_qm.id._serialized || _qm.id.id);
        // s6.2: M/S swipe-reply on a RE-APPROVAL post (matched by the pending registry, not text).
        // Both-yes lifts the live approved amount so the accountant can pay the higher figure.
        var _reaps = loadReapprovals();
        if(_qId && _reaps.items[_qId] && !_reaps.items[_qId].resolved){
          var _rp = _reaps.items[_qId];
          var _bare2 = body.toLowerCase().trim();
          var _v2 = /^(yes|ok|okay|approved?|approve|done|haan|theek|\u{1F44D}|\u2705)$/u.test(_bare2) ? 'yes'
                  : /^(no|reject(ed)?|nahi)$/.test(_bare2) ? 'no' : /^hold$/.test(_bare2) ? 'hold' : null;
          if(_v2){
            _rp[role] = { verdict:_v2, at:new Date().toISOString() };
            var _whoR = role==='mm' ? 'M' : 'S';
            // s6.4: no lone-verdict receipt — only the rejection / both-yes lines below post.
            if(_v2==='no'){
              _rp.resolved='rejected'; saveReapprovals(_reaps);
              try{ await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, '\u274C Re-approval rejected \u2014 '+_rp.label+' stays at \u20B9'+formatINR(_rp.approved)+'.'); }catch(e){}
            } else if(_rp.mm && _rp.mm.verdict==='yes' && _rp.sm && _rp.sm.verdict==='yes'){
              liftPayableAmount(_rp.itemId, _rp.attempted, _rp.code);
              _rp.resolved='approved'; saveReapprovals(_reaps);
              try{ await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, '\u2705 Re-approved (M+S): '+_rp.label+' \u2014 now \u20B9'+formatINR(_rp.attempted)+'. The accountant can pay it.'); }catch(e){}
            } else { saveReapprovals(_reaps); }
            console.log('[Reapproval] verdict', _whoR, _v2, 'on', _qId);
            return true;
          }
        }
        if(_qId && /^\s*\*?EXPENSE REQUEST/i.test(_qBody)){
          var _bare = body.toLowerCase().trim();
          var _v = /^(yes|ok|okay|approved?|approve|done|haan|theek|\u{1F44D}|\u2705)$/u.test(_bare) ? 'yes'
                 : /^(no|reject(ed)?|nahi)$/.test(_bare) ? 'no'
                 : /^hold$/.test(_bare) ? 'hold' : null;
          if(_v){
            var _dm = _qBody.match(/Details:\s*(.+)/i);
            var _lbl = _dm ? _dm[1].trim() : '(expense)';
            var _am = _qBody.match(/Amount:\s*rs\.?\s*([\d,]+)/i);
            var _amt = _am ? parseAmount(_am[1]) : 0;
            var _store = loadVerdicts();
            if(!_store[_qId]) _store[_qId] = {};
            _store[_qId]._label = _lbl; _store[_qId]._amount = _amt;
            _store[_qId][role] = { verdict:_v, amount:0, raw:body.substring(0,200), at:new Date().toISOString() };
            recordVerdictEvent(_qId, _lbl, _amt, role, _v, 0, body);
            var _whoL = role==='mm' ? 'M' : 'S';
            var _pvn = perVerdictNotice(_v, _whoL, _lbl, _amt);   // s6.4: lone YES is silent; no/hold still post
            if(_pvn){ try{ await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, _pvn); }catch(e){} }
            var _mmV=_store[_qId].mm, _smV=_store[_qId].sm;
            var _isYes=function(x){ return x && (x.verdict==='yes'||x.verdict==='amend'); };
            if(_isYes(_mmV) && _isYes(_smV)){
              recordApprovedEvent(_qId, _lbl, _amt);
              try{ await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, '\u2705 Approved (M+S): '+_lbl+(_amt?' Rs.'+formatINR(_amt):'')); }catch(e){}
            }
            saveVerdicts(_store);
            console.log('[Verdicts] req-reply', _whoL, _v, 'on', _qId, '-', _lbl);
            return true;
          }
        }
      }catch(e){ console.error('[Verdicts] req-reply branch:', e.message); }
    }
    var map = loadDigestMap();
    if(!map || !map.items || !map.items.length) return false;
    var verdicts = parseVerdictMessage(body, map.items.length);
    // v2.8.11: M/S swipe-reply ANY bot post (digest, [BOT REMINDER], ⚡ URGENT) with a
    // bare "ok/yes/no/hold". Resolve per item the quoted post actually listed:
    // digest → every listed item; reminder/urgent → exactly that one expense.
    // A single-item post can NEVER blanket-approve the whole list.
    if(!verdicts){
      var quotedId=null, quotedBodyV=null;
      if(msg.hasQuotedMsg){ try{ var q=await msg.getQuotedMessage(); quotedId=q.id._serialized||q.id.id; quotedBodyV=q.body||''; }catch(e){} }
      var botPost = parseBotPostItems(quotedBodyV);
      var repliedToBotPost = quotedId && (botPost || (map.msgId && quotedId===map.msgId));
      var bare = body.toLowerCase().trim();
      var bareVerdict = /^(yes|ok|okay|approved?|approve|done|haan|theek)$/.test(bare) ? 'yes'
                      : /^(no|reject(ed)?|nahi)$/.test(bare) ? 'no'
                      : /^hold$/.test(bare) ? 'hold' : null;
      var allVerdict = /^all\s+(yes|ok|okay|approved?|approve)$/.test(bare) ? 'yes'
                      : /^all\s+(no|reject(ed)?)$/.test(bare) ? 'no' : null;
      if(repliedToBotPost && (allVerdict || bareVerdict)){
        var vd = allVerdict || bareVerdict;
        function normLabel(s){ return (s||'').toLowerCase().replace(/…\s*$/,'').trim(); }
        function resolveQuotedItem(qi){
          // exact label first (digest labels are generated by the same code as the map)
          var exact = map.items.filter(function(it){ return normLabel(it.label)===normLabel(qi.label); });
          var cands = exact.length ? exact : map.items.filter(function(it){
            var w = normLabel(qi.label).split(/[^a-z0-9]+/).filter(function(x){return x.length>=4;});
            var il = normLabel(it.label);
            return w.length && w.some(function(word){ return il.indexOf(word)>=0; });
          });
          if(cands.length>1 && qi.amount>0){
            var nar = cands.filter(function(it){ return it.amount===qi.amount; });
            if(nar.length) cands = nar;
          }
          return cands.length===1 ? cands[0] : null;
        }
        var targets = [];
        if(botPost && botPost.items.length){
          botPost.items.forEach(function(qi){
            var hit = resolveQuotedItem(qi);
            if(hit && targets.indexOf(hit)<0) targets.push(hit);
          });
        } else if(quotedId && ((map.msgIds && map.msgIds.indexOf(quotedId)>=0) || quotedId===map.msgId)){
          var seg = (map.items||[]).filter(function(it){ return it.msgId===quotedId; });  // bare verdict quoting ONE of the 3 messages -> only that message's items
          targets = seg.length ? seg : (map.items||[]).slice();
        }
        if(targets.length){
          verdicts = targets.map(function(it){ return {n:it.n, verdict:vd, amount:0}; });
        } else {
          // Quoted a bot post but nothing safely resolved — never guess, never blanket-apply.
          await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, "Couldn't match that "+(botPost&&botPost.kind==='single'?'reminder':'digest')+" to the current pending list. Please reply on the latest digest with the number, e.g. \"1 "+vd+"\".");
          return true;
        }
      }
    }
    if(!verdicts && /\d/.test(body) && /(yes|ok|no|hold|why|reason|approve|reject)/i.test(body)){
      verdicts = await aiParseVerdicts(body, map.items);
    }
    if(!verdicts || !verdicts.length) return false;
    var store = loadVerdicts();
    var who = role==='mm' ? 'M' : 'S';
    // v2.8.17: no per-reply echo. Record verdicts silently; only announce an item
    // once BOTH M and S have approved it. Partial/hold/reject/query states are
    // reported by the 10 AM / 7 PM digests, so they need no real-time echo.
    var nowApproved = [];   // items that reached full M+S approval on THIS reply
    verdicts.forEach(function(v){
      var item = map.items[v.n-1];
      if(!item || item.n!==v.n){ item = null; map.items.forEach(function(it){ if(it.n===v.n) item=it; }); }
      if(!item){ return; }
      if(!store[item.id]) store[item.id] = {};
      store[item.id]._label = item.label;   // v2.8.12: enable id-drift-safe reconciliation
      store[item.id]._amount = item.amount;
      store[item.id][role] = { verdict:v.verdict, amount:v.amount||0, raw:body.substring(0,200), at:new Date().toISOString() };
      recordVerdictEvent(item.id, item.label, item.amount, role, v.verdict, v.amount||0, body);
      // Is this item now approved by BOTH parties (per the verdict store)?
      var mmV = store[item.id].mm, smV = store[item.id].sm;
      var isYes = function(x){ return x && (x.verdict==='yes' || x.verdict==='amend'); };
      if(isYes(mmV) && isYes(smV)){
        var amt = item.amount;
        if(mmV.verdict==='amend' && mmV.amount>0) amt = mmV.amount;
        if(smV.verdict==='amend' && smV.amount>0 && (!amt || smV.amount<amt)) amt = smV.amount;
        nowApproved.push({ label:item.label, amount:amt, item:item });
        recordApprovedEvent(item.id, item.label, amt);
      }
    });
    saveVerdicts(store);
    if(nowApproved.length){
      var conf = nowApproved.map(function(a){ return '✓ Approved (M+S): '+a.label+' Rs.'+formatINR(a.amount); });
      await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, conf.join('\n'));
      // v2.10.0-s3: bridge each newly-approved item into the outflow group (toggle-gated, idempotent)
      for(var qi=0; qi<nowApproved.length; qi++){
        try{ await postApprovedToOutflow(nowApproved[qi].item, nowApproved[qi].amount); }
        catch(e){ console.error('[Outflow] bridge:', e.message); }
      }
    }
    console.log('[Verdicts]', who, 'recorded', verdicts.length, 'item(s);', nowApproved.length, 'now fully approved');
    return true;
  }catch(e){ console.error('[Verdicts]', e.message); return false; }
}
// ── DM Relay state ───────────────────────────────────────────────────────────
var DM_STATE_FILE = './wa_auth/dm_state.json';
function loadDMState() {
  try {
    if(fs.existsSync(DM_STATE_FILE)){
      return JSON.parse(fs.readFileSync(DM_STATE_FILE, 'utf8'));
    }
  } catch(e) { console.error('[DM] state load failed:', e.message); }
  return { pending: {} };
}
function saveDMState(state) {
  try {
    if(!fs.existsSync('./wa_auth')) fs.mkdirSync('./wa_auth', { recursive: true });
    fs.writeFileSync(DM_STATE_FILE, JSON.stringify(state, null, 2));
  } catch(e) { console.error('[DM] state save failed:', e.message); }
}
function pruneStaleDMState(state) {
  var now = Date.now();
  Object.keys(state.pending).forEach(function(jid){
    var entry = state.pending[jid];
    if(now - new Date(entry.lastUpdate).getTime() > 30*60*1000){
      delete state.pending[jid];
    }
  });
}
async function isAuthorisedAccountant(rawJid, contactName) {
  if(!rawJid) return false;
  if(rawJid.indexOf('@g.us') >= 0) return false;
  var phoneOnly = null;
  if(rawJid.indexOf('@c.us') >= 0){
    phoneOnly = rawJid.split('@')[0].replace(/[^0-9]/g, '');
  } else if(rawJid.indexOf('@lid') >= 0){
    var resolvedPhone = null;
    try {
      var contact = await waClient.getContactById(rawJid);
      if(contact){
        if(contact.number){
          var n = String(contact.number).replace(/[^0-9]/g, '');
          if(n.length >= 10 && n.length <= 13) resolvedPhone = n;
        }
        if(!resolvedPhone){
          var candidates = [contact.pushname, contact.name, contact.shortName, contact.verifiedName];
          for(var ci=0; ci<candidates.length; ci++){
            var cn = String(candidates[ci] || '');
            var phoneMatch = cn.match(/\+?(91)?[\s\-]?(\d{5})[\s\-]?(\d{5})/);
            if(phoneMatch){
              var maybe = '91' + phoneMatch[2] + phoneMatch[3];
              if(maybe.length === 12){ resolvedPhone = maybe; break; }
            }
          }
        }
      }
      if(!resolvedPhone){
        try {
          var chat = await waClient.getChatById(rawJid);
          if(chat && chat.name){
            var phoneMatch2 = String(chat.name).match(/\+?(91)?[\s\-]?(\d{5})[\s\-]?(\d{5})/);
            if(phoneMatch2){
              var maybe2 = '91' + phoneMatch2[2] + phoneMatch2[3];
              if(maybe2.length === 12) resolvedPhone = maybe2;
            }
          }
        } catch(e) {}
      }
    } catch(e) {
      console.log('[Auth] LID resolve failed for', rawJid, ':', e.message);
    }
    if(!resolvedPhone){
      if(CONFIG.LID_WHITELIST && CONFIG.LID_WHITELIST.indexOf(rawJid) >= 0){
        console.log('[Auth] LID', rawJid, 'allowed via LID_WHITELIST');
        return true;
      }
      console.log('[Auth] LID could not be resolved to a phone:', rawJid, '(name:', contactName, ')');
      return false;
    }
    phoneOnly = resolvedPhone;
    console.log('[Auth] LID', rawJid, 'resolved to phone', phoneOnly);
  } else {
    return false;
  }
  if(!phoneOnly) return false;
  var whitelist = CONFIG.ACCOUNTANT_PHONES.concat([CONFIG.MM_PHONE, CONFIG.SM_PHONE]).concat(CONFIG.TEST_PHONES || []);
  if(whitelist.indexOf(phoneOnly) >= 0){
    console.log('[Auth] allow:', phoneOnly, '(', contactName, ')');
    return true;
  }
  console.log('[Auth] reject:', phoneOnly, '(', contactName, ') - not in whitelist');
  return false;
}
function buildGroupPostFromDM(entry, posterName) {
  var lines = ['*EXPENSE REQUEST*', ''];
  if(entry.details) lines.push('Details: ' + entry.details);
  if(entry.subItems && entry.subItems.length > 1){
    var total = entry.subItems.reduce(function(s,it){return s+it.amount;},0);
    lines.push('Amount: Rs.' + formatINR(total) + ' total');
    entry.subItems.forEach(function(si){ lines.push('  - ' + si.vendor + ' Rs.' + formatINR(si.amount)); });
  } else if(entry.amount > 0) {
    lines.push('Amount: Rs.' + formatINR(entry.amount));
  }
  if(entry.company) lines.push('Company: ' + entry.company);
  if(entry.fromAC) lines.push('From: ' + entry.fromAC);
  lines.push('Posted by: ' + posterName);
  lines.push('');
  lines.push('M/S please review.');
  return lines.join('\n');
}

// ── v2.7 NEW: smart free-form parser helpers ─────────────────────────────────
// Extract company + account references from anywhere in a free-form DM message
// by matching against the live Fund Position list. Returns { companyMatches, acMatches }.
// Used to avoid asking the accountant for fields they already mentioned.
async function smartExtractCompanyAccount(text) {
  if(!text) return { companyMatches: [], acMatches: [] };
  var lower = text.toLowerCase();
  var fp;
  try { fp = await getFundPosition(); } catch(e) { return { companyMatches: [], acMatches: [] }; }
  var companies = [];
  var accounts = [];
  fp.forEach(function(a){
    if(a.company && companies.indexOf(a.company) < 0) companies.push(a.company);
    if(a.bankAC && accounts.indexOf(a.bankAC) < 0) accounts.push(a.bankAC);
  });
  // Find companies whose full name appears in the text (case-insensitive)
  var companyMatches = companies.filter(function(c){
    return c.length >= 4 && lower.indexOf(c.toLowerCase()) >= 0;
  });
  // Find bank accounts whose full name appears in the text
  var acMatches = accounts.filter(function(a){
    return a.length >= 4 && lower.indexOf(a.toLowerCase()) >= 0;
  });
  // Also try partial matches: any unique 2+ word token from a company/account name found in text
  // This catches "hansaflon" matching "Hansaflon Buildcon"
  if(companyMatches.length === 0){
    companies.forEach(function(c){
      var firstWord = c.split(/\s+/)[0];
      if(firstWord && firstWord.length >= 5 && lower.indexOf(firstWord.toLowerCase()) >= 0){
        companyMatches.push(c);
      }
    });
  }
  if(acMatches.length === 0){
    accounts.forEach(function(a){
      var tokens = a.split(/\s+/).filter(function(t){ return t.length >= 4; });
      // Match if ALL tokens of the account name appear in the text
      var allFound = tokens.length > 0 && tokens.every(function(t){ return lower.indexOf(t.toLowerCase()) >= 0; });
      if(allFound) acMatches.push(a);
    });
  }
  // Dedupe
  companyMatches = Array.from(new Set(companyMatches));
  acMatches = Array.from(new Set(acMatches));
  return { companyMatches: companyMatches, acMatches: acMatches };
}

// Count how many distinct amount patterns are in a free-form message body.
// Used to enforce "one expense at a time" — refuses multi-amount requests.
function countAmountPatterns(body) {
  if(!body) return 0;
  var found = [];
  // Pattern A: <number> lakh/lac/cr/crore/thousand/hundred/hazaar/l (with currency unit)
  var unitMatches = body.match(/\d[\d,]*\.?\d*\s*(?:crores?|cr|lakhs?|lacs?|thousands?|hundreds?|hazaar|hazar|l\b)/gi) || [];
  unitMatches.forEach(function(m){ found.push(m); });
  // Pattern B: <number> k (thousand)
  var kMatches = body.match(/\d[\d,]*\.?\d*\s*k\b/gi) || [];
  kMatches.forEach(function(m){ found.push(m); });
  // Pattern C: Rs./INR/₹ prefix
  var rsMatches = body.match(/(?:rs\.?\s*|inr\s*|\u20B9\s*)\d[\d,]*\.?\d*/gi) || [];
  rsMatches.forEach(function(m){ found.push(m); });
  // Pattern D: Indian-format commas (e.g. 7,08,708) — only if not already covered by Rs prefix
  var commaMatches = body.match(/\b\d{1,3}(?:,\d{2,3}){1,3}\b/g) || [];
  commaMatches.forEach(function(m){
    // Skip if this match is part of a Rs-prefixed match we already counted
    var alreadyCounted = found.some(function(f){ return f.indexOf(m) >= 0; });
    if(!alreadyCounted) found.push(m);
  });
  return found.length;
}

// ── v2.7.2 NEW: AI arbiter for multi-amount messages ─────────────────────────
// Distinguishes a single approval request that happens to mention several numbers
// (cost breakdown / payment plan — e.g. Umesh's granite message) from a genuine
// multi-payment request (e.g. "one cheque for Ajit Singh Rs.X and one for Suresh Rs.Y").
// Returns one of:
//   { kind:'single', approvalAmount:<n>, details:'<clean context summary>' }
//   { kind:'multiple', count:<n> }
//   { kind:'unclear' }                      (fall back to the blunt block)
// Best-effort: any error → 'unclear'.
async function aiParseExpenseIntent(body) {
  if(!CONFIG.CLAUDE_API_KEY || !body) return { kind: 'unclear' };
  var prompt = 'An accountant sent this expense message in a real-estate firm. It may contain several numbers (totals, amounts already paid, balances, instalment splits) but usually only ONE amount is actually being requested for approval right now.\n\n' +
    'MESSAGE:\n"""\n' + body.substring(0, 1500) + '\n"""\n\n' +
    'Decide:\n' +
    '- If this is ONE approval request (even if it lists cost breakdown / payment plan / past payments as context), return the single amount being requested for approval NOW, plus a clean one-line context summary preserving the useful details (vendor, total, paid, balance, instalment plan).\n' +
    '- If this is genuinely MULTIPLE separate payment requests (e.g. distinct cheques to different parties each needing approval), return kind "multiple" with the count.\n\n' +
    'The amount being requested is usually the one attached to words like "approve", "pay", "release", "kindly approve". Totals, amounts already paid, and balances are context, NOT the ask.\n\n' +
    'Reply ONLY with strict JSON on one line:\n' +
    '{"kind":"single","approvalAmount":<number>,"details":"<clean summary>"}  OR  {"kind":"multiple","count":<number>}';
  try {
    var resp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': CONFIG.CLAUDE_API_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ model: 'claude-sonnet-4-6', max_tokens: 400, messages: [{ role: 'user', content: prompt }] })
    });
    if(!resp.ok){ console.error('[AI parse] HTTP', resp.status); return { kind: 'unclear' }; }
    var data = await resp.json();
    var text = '';
    if(data.content){ for(var i=0;i<data.content.length;i++){ if(data.content[i].type==='text'){ text=data.content[i].text; break; } } }
    if(!text) return { kind: 'unclear' };
    text = text.replace(/```json|```/g,'').trim();
    var parsed;
    try { parsed = JSON.parse(text); } catch(e){ var m=text.match(/\{[\s\S]*\}/); if(m){ try{ parsed=JSON.parse(m[0]); }catch(e2){ return { kind:'unclear' }; } } else return { kind:'unclear' }; }
    if(parsed.kind === 'single'){
      var amt = parseAmount(parsed.approvalAmount);
      if(amt > 0) return { kind:'single', approvalAmount: amt, details: (parsed.details||'').toString().substring(0,250) };
      return { kind:'unclear' };
    }
    if(parsed.kind === 'multiple'){
      return { kind:'multiple', count: parseInt(parsed.count) || 2 };
    }
    return { kind:'unclear' };
  } catch(e){ console.error('[AI parse] exception:', e.message); return { kind:'unclear' }; }
}

// ── Handle accountant DM ─────────────────────────────────────────────────────
async function handleAccountantDM(msg) {
  if(!msg || !waReady) return false;
  var rawFrom = msg.from || '';
  if(rawFrom.indexOf('@g.us') >= 0) return false;
  if(rawFrom.indexOf('@c.us') < 0 && rawFrom.indexOf('@lid') < 0) return false;
  if(msg.fromMe) return false;
  var senderInfo = await identifySender(rawFrom);
  if(!(await isAuthorisedAccountant(rawFrom, senderInfo.contactName))){
    console.log('[DM] unauthorised sender (silent):', rawFrom, senderInfo.contactName);
    if(process.env.REPLY_TO_UNAUTHORISED === 'true'){
      if(!global._unauthorisedNotified) global._unauthorisedNotified = {};
      var dayKey = rawFrom + '_' + new Date().toISOString().split('T')[0];
      if(!global._unauthorisedNotified[dayKey]){
        global._unauthorisedNotified[dayKey] = true;
        try {
          await waClient.sendMessage(rawFrom, 'This is an automated assistant. Your number is not authorised. Please contact admin if you need access.');
        } catch(e) { /* ignore */ }
      }
    }
    return false;
  }
  var body = (msg.body || '').trim();
  var hasMedia = msg.hasMedia || false;
  var thisMsgId = msg.id._serialized || msg.id.id;
  console.log('[DM] from', senderInfo.contactName || rawFrom, ':', body.substring(0,80), hasMedia?'[+media]':'');
  var state = loadDMState();
  pruneStaleDMState(state);
  if(/^\s*(cancel|reset|clear)\s*$/i.test(body)){
    delete state.pending[rawFrom];
    saveDMState(state);
    await waClient.sendMessage(rawFrom, 'Pending request cleared. Send a new expense to start over.');
    return true;
  }
  var phoneOfSender = rawFrom.indexOf('@c.us')>=0 ? rawFrom.split('@')[0] : null;
  if(!phoneOfSender){
    try {
      var c = await waClient.getContactById(rawFrom);
      var nameStr = String((c && (c.pushname||c.name||c.shortName)) || '');
      var pm = nameStr.match(/\+?(91)?[\s\-]?(\d{5})[\s\-]?(\d{5})/);
      if(pm) phoneOfSender = '91' + pm[2] + pm[3];
    } catch(e){}
  }
  if(/^\s*silent\s+on\s*$/i.test(body)){
    if(phoneOfSender !== SILENT_OBSERVER){
      await waClient.sendMessage(rawFrom, 'Only the silent-mode observer ('+SILENT_OBSERVER+') can toggle this.');
      return true;
    }
    saveSilentMode(true);
    await waClient.sendMessage(rawFrom, 'Silent mode ON. I will stop posting reminders to the approval group. Daily summary at 7 PM will come to you privately.');
    return true;
  }
  if(/^\s*silent\s+off\s*$/i.test(body)){
    if(phoneOfSender !== SILENT_OBSERVER){
      await waClient.sendMessage(rawFrom, 'Only the silent-mode observer ('+SILENT_OBSERVER+') can toggle this.');
      return true;
    }
    saveSilentMode(false);
    await waClient.sendMessage(rawFrom, 'Silent mode OFF. Resuming group reminders and evening report to Day Book group.');
    return true;
  }
  if(/^\s*silent\s+status\s*$/i.test(body)){
    var s = loadSilentMode();
    await waClient.sendMessage(rawFrom, 'Silent mode is currently '+(s?'ON':'OFF')+'.');
    return true;
  }
  // ── v2.8 Module 7: unapproved-payment alert toggle (Day Book group) ───────
  if(/^\s*unapproved\s+(on|off|status)\s*$/i.test(body)){
    if(phoneOfSender !== SILENT_OBSERVER){
      await waClient.sendMessage(rawFrom, 'Only the observer can toggle this.');
      return true;
    }
    var uw = body.match(/^\s*unapproved\s+(on|off|status)\s*$/i)[1].toLowerCase();
    if(uw==='status'){
      await waClient.sendMessage(rawFrom, 'Unapproved-payment alert to Day Book group is '+(loadUnapprovedAlert()?'ON':'OFF')+'. It always appears in your private summary regardless.');
    } else {
      saveUnapprovedAlert(uw==='on');
      await waClient.sendMessage(rawFrom, 'Unapproved-payment Day Book alert '+(uw==='on'?'ON — flagged payments will post to the Day Book group with the 7 PM cycle.':'OFF — findings stay in your private summary only.'));
    }
    return true;
  }
  // ── Manual intervention commands (only from SILENT_OBSERVER) ─────────────
  if(phoneOfSender === SILENT_OBSERVER){
    var cmdMatch = body.match(/^\s*(\d+)\s+(ok|confirm|reject|flag|ignore|chase|paid)(?:\s+(.+))?\s*$/i);
    if(cmdMatch){
      var idx = parseInt(cmdMatch[1]);
      var verb = cmdMatch[2].toLowerCase();
      var arg = cmdMatch[3] ? cmdMatch[3].trim() : '';
      var dmState = loadDMState();
      var lastOutliers = (dmState.lastOutliers && dmState.lastOutliers.items) || [];
      var target = lastOutliers.find(function(o){ return o.id === idx; });
      if(!target){
        await waClient.sendMessage(rawFrom, 'No outlier #'+idx+' in the most recent report. Run /api/outliers to see current list.');
        return true;
      }
      var cache = loadMatchCache();
      if(!cache.matches) cache.matches = {};
      if(!cache.rejected) cache.rejected = {};
      if(!cache.manualPaid) cache.manualPaid = {};
      if(verb === 'ok' || verb === 'confirm'){
        if(target.ledgerHash){
          cache.matches[target.expenseId] = Object.assign(cache.matches[target.expenseId] || {}, {
            ledgerHash: target.ledgerHash, stage: 'manual', confidence: 'manual', manuallyConfirmed: true, ts: new Date().toISOString()
          });
        }
        saveMatchCache(cache);
        await waClient.sendMessage(rawFrom, '✓ Outlier #'+idx+' confirmed and learned. Future similar matches will auto-pass.');
        return true;
      }
      if(verb === 'reject' || verb === 'flag'){
        if(target.ledgerHash){
          if(!cache.rejected[target.expenseId]) cache.rejected[target.expenseId] = [];
          if(cache.rejected[target.expenseId].indexOf(target.ledgerHash) < 0) cache.rejected[target.expenseId].push(target.ledgerHash);
          if(cache.matches[target.expenseId] && cache.matches[target.expenseId].ledgerHash === target.ledgerHash){
            cache.matches[target.expenseId].manuallyRejected = true;
          }
        }
        saveMatchCache(cache);
        await waClient.sendMessage(rawFrom, '✗ Outlier #'+idx+' rejected. The matcher will not suggest this pairing again.');
        return true;
      }
      if(verb === 'ignore'){
        if(!cache.ignored) cache.ignored = {};
        cache.ignored[target.expenseId] = true;
        saveMatchCache(cache);
        await waClient.sendMessage(rawFrom, '⏭ Outlier #'+idx+' will be hidden from future reports.');
        return true;
      }
      if(verb === 'paid'){
        cache.manualPaid[target.expenseId] = { paidDate: arg || new Date().toISOString().split('T')[0], ts: new Date().toISOString() };
        saveMatchCache(cache);
        await waClient.sendMessage(rawFrom, '✓ Outlier #'+idx+' marked as paid'+(arg?' on '+arg:'')+'. The matcher will reflect this in the next report.');
        return true;
      }
      if(verb === 'chase'){
        try {
          await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, '[BOT NUDGE] Reminder — please log Ledger entry for: ' + (target.expenseId || ''));
          await waClient.sendMessage(rawFrom, '📣 Nudge posted in approval group for outlier #'+idx+'.');
        } catch(e){
          await waClient.sendMessage(rawFrom, 'Could not post nudge: '+e.message);
        }
        return true;
      }
    }
  }
  // ── v2.6 NEW: MORE X drill-down commands (only from SILENT_OBSERVER) ─────
  // MORE STALE | MORE PAID | MORE TOLERANCE | MORE AWAITING | MORE MISMATCH
  // MORE UNMATCHED | MORE RECURRING — each dumps the full bucket as chunked text.
  if(phoneOfSender === SILENT_OBSERVER){
    var moreMatch = body.match(/^\s*more\s+(stale|paid|tolerance|awaiting|mismatch|unmatched|recurring)\s*$/i);
    if(moreMatch){
      var which = moreMatch[1].toLowerCase();
      try {
        var sendChunked = async function(jid, text){
          var maxLen = 3500;
          if(text.length <= maxLen){ await waClient.sendMessage(jid, text); return; }
          var chunks = [];
          var lines = text.split('\n');
          var cur = '';
          for(var i=0; i<lines.length; i++){
            if((cur + lines[i] + '\n').length > maxLen){ chunks.push(cur); cur = ''; }
            cur += lines[i] + '\n';
          }
          if(cur) chunks.push(cur);
          for(var ci=0; ci<chunks.length; ci++){
            await waClient.sendMessage(jid, '(' + (ci+1) + '/' + chunks.length + ')\n' + chunks[ci]);
            if(ci < chunks.length-1) await new Promise(function(r){setTimeout(r,1200);});
          }
        };
        if(which === 'stale'){
          var audit = await buildApprovalAudit(15);
          var allStale = audit.partialApproval.concat(
            audit.noApproval.filter(function(e){ return e.amount > 0; })
          );
          allStale.sort(function(a,b){ return b.date.getTime() - a.date.getTime(); });
          if(allStale.length === 0){
            await waClient.sendMessage(rawFrom, 'No stale pending approvals.');
          } else {
            var total = 0;
            var ls = ['FULL STALE PENDING LIST (' + allStale.length + '):', ''];
            allStale.forEach(function(e){
              var status = e.status.mm === 'yes' ? 'S pending' : e.status.sm === 'yes' ? 'M pending' : e.status.mm === 'question' ? 'Query open' : e.status.sm === 'question' ? 'Query open' : 'Both pending';
              var age = Math.floor((Date.now() - e.date.getTime()) / (60*60*1000));
              ls.push('- ' + (e.vendor || e.body.substring(0,40)) + ' Rs.' + formatINR(e.amount) + ' [' + status + ', ' + age + 'h]');
              total += e.amount;
            });
            ls.push('');
            ls.push('Total: Rs.' + formatINR(total));
            await sendChunked(rawFrom, ls.join('\n'));
          }
          return true;
        }
        var rec = await buildReconciliation(30);
        if(which === 'paid'){
          if(rec.paid.length === 0){ await waClient.sendMessage(rawFrom, 'No fully-matched paid items.'); return true; }
          var sorted = rec.paid.slice().sort(function(a,b){
            var aD = (a.matchResult && a.matchResult.match) ? a.matchResult.match.date.getTime() : 0;
            var bD = (b.matchResult && b.matchResult.match) ? b.matchResult.match.date.getTime() : 0;
            return bD - aD;
          });
          var lp = ['FULL PAID LIST (' + sorted.length + '):', ''];
          sorted.forEach(function(e){
            var m = e.matchResult ? e.matchResult.match : null;
            var dt = m ? m.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'}) : '';
            lp.push('  ✓ ' + (e.vendor || e.body.substring(0,40)) + ' Rs.' + formatINR(e.amount) + (m ? ' → ' + dt + ', ' + (m.bankAC||'-') : ''));
          });
          lp.push('');
          lp.push('Total paid: Rs.' + formatINR(rec.summary.totalPaid));
          await sendChunked(rawFrom, lp.join('\n'));
          return true;
        }
        if(which === 'tolerance'){
          if(rec.paidWithTolerance.length === 0){ await waClient.sendMessage(rawFrom, 'No items in the tolerance bucket.'); return true; }
          var lt = ['FULL TOLERANCE LIST (' + rec.paidWithTolerance.length + '):', ''];
          rec.paidWithTolerance.forEach(function(e){
            var m = e.matchResult ? e.matchResult.match : null;
            lt.push('  ~ ' + (e.vendor || e.body.substring(0,40)) + ' Rs.' + formatINR(e.amount) + ' (paid Rs.' + formatINR(m ? m.amount : 0) + ')');
          });
          await sendChunked(rawFrom, lt.join('\n'));
          return true;
        }
        if(which === 'awaiting'){
          if(rec.awaitingPayment.length === 0){ await waClient.sendMessage(rawFrom, 'Nothing currently awaiting payment.'); return true; }
          var sortedA = rec.awaitingPayment.slice().sort(function(a,b){ return b.date.getTime() - a.date.getTime(); });
          var la = ['FULL AWAITING LIST (' + sortedA.length + '):', ''];
          sortedA.forEach(function(e){
            var d = e.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'});
            la.push('  ⏳ ' + (e.vendor || e.body.substring(0,40)) + ' Rs.' + formatINR(e.amount) + ' (approved ' + d + ')');
          });
          la.push('');
          la.push('Total awaiting: Rs.' + formatINR(rec.summary.totalAwaiting));
          await sendChunked(rawFrom, la.join('\n'));
          return true;
        }
        if(which === 'mismatch'){
          if(rec.possibleMatch.length === 0){ await waClient.sendMessage(rawFrom, 'No possible mismatches.'); return true; }
          var lm2 = ['FULL MISMATCH LIST (' + rec.possibleMatch.length + '):', ''];
          rec.possibleMatch.forEach(function(e){
            var m = e.matchResult ? e.matchResult.match : null;
            lm2.push('  ⚠ ' + (e.vendor || e.body.substring(0,40)) + ' approved Rs.' + formatINR(e.amount) + ' vs Ledger Rs.' + formatINR(m ? m.amount : 0));
          });
          await sendChunked(rawFrom, lm2.join('\n'));
          return true;
        }
        if(which === 'unmatched'){
          if(!rec.ledgerWithoutApproval || rec.ledgerWithoutApproval.length === 0){
            await waClient.sendMessage(rawFrom, 'No unmatched Ledger payments — every recent payment has an approval.');
            return true;
          }
          var lu = ['LEDGER PAYMENTS WITHOUT APPROVAL (' + rec.ledgerWithoutApproval.length + '):', '', '(Last ' + REVERSE_SCAN_WINDOW_DAYS + ' days, floor Rs.' + formatINR(REVERSE_SCAN_MIN_AMOUNT) + ')', ''];
          rec.ledgerWithoutApproval.forEach(function(le){
            var dt = le.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'});
            var desc = (le.description||le.entity||'').substring(0,50);
            lu.push('  ❗ ' + desc + ' Rs.' + formatINR(le.amount) + ' (' + (le.bankAC||'-') + ', ' + dt + ')');
          });
          lu.push('');
          lu.push('Total: Rs.' + formatINR(rec.summary.totalUnmatchedLedger || 0));
          await sendChunked(rawFrom, lu.join('\n'));
          return true;
        }
        if(which === 'recurring'){
          if(!rec.ledgerRecurring || rec.ledgerRecurring.length === 0){
            await waClient.sendMessage(rawFrom, 'No recurring/auto Ledger items in the recent window.');
            return true;
          }
          var lr = ['RECURRING/AUTO LEDGER ITEMS (' + rec.ledgerRecurring.length + '):', '', '(Suppressed from unmatched list)', ''];
          rec.ledgerRecurring.forEach(function(le){
            var dt = le.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'});
            var desc = (le.description||le.entity||'').substring(0,50);
            lr.push('  ℹ ' + desc + ' Rs.' + formatINR(le.amount) + ' (' + (le.bankAC||'-') + ', ' + dt + ')');
          });
          await sendChunked(rawFrom, lr.join('\n'));
          return true;
        }
      } catch(e) {
        console.error('[MORE cmd]', e.message);
        await waClient.sendMessage(rawFrom, 'Failed to fetch list: ' + e.message);
        return true;
      }
    }
  }
  if(/^\s*help\s*$/i.test(body)){
    var helpLines = [
      'Send me an expense request and I will post it in the approval group.',
      '',
      'I need 4 things:',
      '1. Details (what the payment is for)',
      '2. Amount',
      '3. Company',
      '4. Bank A/C to pay from',
      '',
      'You can send them all at once like:',
      'Details: TMT bar payment slab 110-118',
      'Amount: 7,08,708',
      'Company: Hansaflon Buildcon',
      'From: Hansaflon JKB',
      '',
      'Or just write naturally — I will pick out company/account names if you mention them, and ask for whatever is missing.',
      '',
      'IMPORTANT: One expense per message. If you have multiple amounts, send them one at a time.',
      '',
      'Attachments (PDF/image): I will read them and confirm the amount with you BEFORE posting — so filename/OCR errors do not slip through.',
      '',
      'Commands:',
      '  show              see your current draft',
      '  edit <field>: x   change a field (details/amount/company/from)',
      '  edit from: 7      pick bank A/C #7 from the last list shown',
      '  cancel            clear and start over',
      '  yes               post the draft to the group',
      '  show              see your current draft',
      '  urgent            list your pending requests',
      '  urgent <n>        push request #n to the group urgently (tags M/S)'
    ];
    if(phoneOfSender === SILENT_OBSERVER){
      helpLines.push('');
      helpLines.push('— Observer commands —');
      helpLines.push('MORE STALE       full pending list');
      helpLines.push('MORE PAID        full matched/paid list');
      helpLines.push('MORE AWAITING    full not-yet-paid list');
      helpLines.push('MORE MISMATCH    full possible-mismatch list');
      helpLines.push('MORE UNMATCHED   Ledger payments without approval');
      helpLines.push('MORE RECURRING   suppressed auto/recurring items');
      helpLines.push('MORE TOLERANCE   paid within 5% tolerance');
      helpLines.push('silent on/off/status');
      helpLines.push('<n> ok | reject | flag | chase | paid <date>  (outlier action)');
    }
    await waClient.sendMessage(rawFrom, helpLines.join('\n'));
    return true;
  }

  // ── v2.7 NEW: SHOW command — display current draft ────────────────────────
  if(/^\s*show\s*$/i.test(body)){
    var draft = state.pending[rawFrom];
    if(!draft){
      await waClient.sendMessage(rawFrom, 'No pending draft. Send an expense request to start one.');
      return true;
    }
    var showLines = ['Your current draft:', ''];
    showLines.push('Details: ' + (draft.details || '(not set)'));
    showLines.push('Amount:  ' + (draft.amount > 0 ? 'Rs.' + formatINR(draft.amount) : '(not set)'));
    showLines.push('Company: ' + (draft.company || '(not set)'));
    showLines.push('From:    ' + (draft.fromAC || '(not set)'));
    if(draft.mediaFiles && draft.mediaFiles.length) showLines.push('Attachments: ' + draft.mediaFiles.length);
    if(draft.askedFor) showLines.push('\nWaiting for: ' + draft.askedFor);
    showLines.push('\nReply yes to post, edit <field>: <value> to change, or cancel to clear.');
    await waClient.sendMessage(rawFrom, showLines.join('\n'));
    return true;
  }

  // ── v2.7.2 NEW: URGENT trigger — re-notify a pending request before scheduled digests ──
  // `urgent`        → list this accountant's pending requests (0–14 days), numbered
  // `urgent <n>`    → immediately post that item to the approval group as URGENT,
  //                   tagging whoever hasn't approved. 2-hour cooldown per expense.
  var urgentMatch = body.match(/^\s*urgent\s*(\d+)?\s*$/i);
  if(urgentMatch){
    var myName = (senderInfo.contactName || '').toLowerCase();
    var uAudit = await buildApprovalAudit(REMINDER_MAX_AGE_DAYS + 1);
    var nowU = Date.now();
    var cutoffU = REMINDER_MAX_AGE_DAYS * 86400000;
    // This accountant's own pending requests, within window, real amount
    var myPending = uAudit.partialApproval.concat(uAudit.noApproval).filter(function(e){
      var mine = (e.sender||'').toLowerCase() === myName;
      var hasAmt = e.amount > 0 || (e.subItems && e.subItems.length > 0);
      var ageOk = (nowU - e.date.getTime()) <= cutoffU;
      var afterStartU = e.date.getTime() >= REPORT_START_MS;
      return mine && hasAmt && ageOk && afterStartU;
    });
    myPending.sort(function(a,b){ return b.date.getTime() - a.date.getTime(); });
    if(myPending.length === 0){
      await waClient.sendMessage(rawFrom, 'You have no pending requests in the last '+REMINDER_MAX_AGE_DAYS+' days. (If a request is older, please post it again as a fresh request.)');
      return true;
    }
    var pickNum = urgentMatch[1] ? parseInt(urgentMatch[1]) : null;
    if(!pickNum){
      // List them
      var ulines = ['Your pending requests — reply "urgent <number>" to push one now:', ''];
      myPending.forEach(function(e, i){
        var ageHrs = Math.floor((nowU - e.date.getTime())/(60*60*1000));
        var ageStr = ageHrs >= 24 ? Math.floor(ageHrs/24)+'d' : ageHrs+'h';
        var who = e.status.mm==='yes' ? 'S pending' : e.status.sm==='yes' ? 'M pending' : 'both pending';
        ulines.push((i+1)+'. '+(e.vendor||(e.body||'').substring(0,40))+' — Rs.'+formatINR(e.amount)+' ('+who+', '+ageStr+')');
      });
      await waClient.sendMessage(rawFrom, ulines.join('\n'));
      return true;
    }
    // urgent <n> → push that item
    if(pickNum < 1 || pickNum > myPending.length){
      await waClient.sendMessage(rawFrom, 'No item #'+pickNum+'. Reply "urgent" to see your list.');
      return true;
    }
    var target = myPending[pickNum - 1];
    // 2-hour cooldown per expense
    var urgState = loadDMState();
    if(!urgState.urgentCooldown) urgState.urgentCooldown = {};
    var lastUrgent = urgState.urgentCooldown[target.id];
    if(lastUrgent && (nowU - new Date(lastUrgent).getTime()) < 2*60*60*1000){
      var mins = Math.ceil((2*60*60*1000 - (nowU - new Date(lastUrgent).getTime()))/60000);
      await waClient.sendMessage(rawFrom, 'I already pushed this one recently. Please wait '+mins+' more minutes before pushing it again.');
      return true;
    }
    // Build the urgent post
    var needMu = target.status.mm !== 'yes';
    var needSu = target.status.sm !== 'yes';
    var uTags = [], uMentions = [];
    if(needMu){ uTags.push('@'+CONFIG.MM_PHONE); uMentions.push(CONFIG.MM_PHONE+'@c.us'); }
    if(needSu){ uTags.push('@'+CONFIG.SM_PHONE); uMentions.push(CONFIG.SM_PHONE+'@c.us'); }
    var ageHrsU = Math.floor((nowU - target.date.getTime())/(60*60*1000));
    var ageStrU = ageHrsU >= 24 ? Math.floor(ageHrsU/24)+'d' : ageHrsU+'h';
    var upost = ['*⚡ URGENT — approval needed*', ''];
    upost.push((target.vendor || (target.body||'').substring(0,60)));
    upost.push('Amount: Rs.'+formatINR(target.amount));
    upost.push('Pending '+ageStrU+' · requested by '+target.sender);
    upost.push('');
    upost.push(uTags.join(' ')+' please review urgently.');
    try {
      await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, upost.join('\n'), { mentions: uMentions });
      urgState.urgentCooldown[target.id] = new Date().toISOString();
      saveDMState(urgState);
      await waClient.sendMessage(rawFrom, 'Pushed as urgent to the approval group, tagging '+(needMu&&needSu?'M and S':needMu?'M':'S')+'.');
    } catch(e){
      await waClient.sendMessage(rawFrom, 'Could not post: '+e.message);
    }
    return true;
  }

  // 2+ amount patterns no longer auto-reject. AI decides: single ask with context
  // (Umesh's payment-plan style) vs genuine multiple requests (Ajit Singh style).
  var existingDraft = state.pending[rawFrom];
  var isAnsweringQuestion = existingDraft && existingDraft.askedFor;
  var isVisionConfirm = existingDraft && existingDraft.awaitingVisionConfirm;
  var isYesPost = /^\s*(yes|post|send|y)\s*$/i.test(body);
  var isEditCmd = /^\s*edit\s+/i.test(body);
  var isAiAmountConfirm = existingDraft && existingDraft.awaitingAiAmountConfirm;
  if(!isAnsweringQuestion && !isVisionConfirm && !isYesPost && !isEditCmd && !isAiAmountConfirm && body){
    var amtCount = countAmountPatterns(body);
    if(amtCount >= 2){
      var intent = await aiParseExpenseIntent(body);
      if(intent.kind === 'multiple'){
        await waClient.sendMessage(rawFrom, [
          'This looks like ' + intent.count + ' separate payment requests.',
          '',
          'Please send one expense request at a time. M and S review each separately, and Ledger matching stays clean.',
          '',
          'Send the first one by itself, then the next after it is posted.',
          '',
          'Reply cancel to clear.'
        ].join('\n'));
        return true;
      } else if(intent.kind === 'single'){
        // One ask + context. Pre-fill the draft and confirm the amount before posting.
        if(!state.pending[rawFrom]) state.pending[rawFrom] = { details: '', amount: 0, company: '', fromAC: '', mediaIds: [], lastUpdate: new Date().toISOString(), askedFor: null, posterName: senderInfo.contactName || rawFrom, subItems: null, companyOptions: [], fromACOptions: [] };
        var d0 = state.pending[rawFrom];
        d0.lastUpdate = new Date().toISOString();
        d0.posterName = senderInfo.contactName || d0.posterName;
        if(intent.details && !d0.details) d0.details = intent.details;
        d0.amount = intent.approvalAmount;
        d0.awaitingAiAmountConfirm = true;
        // Pull company/account out of the text if mentioned
        try {
          var sm0 = await smartExtractCompanyAccount(body);
          if(sm0.companyMatches.length === 1 && !d0.company) d0.company = sm0.companyMatches[0];
          if(sm0.acMatches.length === 1 && !d0.fromAC) d0.fromAC = sm0.acMatches[0];
        } catch(e){}
        saveDMState(state);
        var aiLines = ['I read this as ONE request for approval:'];
        aiLines.push('  Amount: Rs.' + formatINR(intent.approvalAmount));
        if(intent.details) aiLines.push('  Details: ' + intent.details);
        aiLines.push('');
        aiLines.push('The full breakdown will be included for M/S.');
        aiLines.push('');
        aiLines.push('Confirm: reply "yes" if Rs.' + formatINR(intent.approvalAmount) + ' is the amount to approve, or send the correct amount.');
        await waClient.sendMessage(rawFrom, aiLines.join('\n'));
        return true;
      } else {
        // unclear → safe fallback: block, ask to simplify
        await waClient.sendMessage(rawFrom, [
          'I see ' + amtCount + ' amounts here and I am not sure which one needs approval.',
          '',
          'Please send just the amount to approve, like: "approve 46,200 for granite balance freight".',
          '',
          'Reply cancel to clear.'
        ].join('\n'));
        return true;
      }
    }
  }

  // ── v2.7.2: handle reply to AI amount confirmation ──────────────────────
  if(existingDraft && existingDraft.awaitingAiAmountConfirm){
    var d1 = existingDraft;
    if(/^\s*(yes|y|ok|correct|sahi)\s*$/i.test(body)){
      d1.awaitingAiAmountConfirm = false;
      saveDMState(state);
      // fall through to normal validate-required-fields flow below
    } else {
      // Try to parse a corrected amount from their reply
      var corr = parseExpenseMessage(body);
      if(corr[0].amount > 0){
        d1.amount = corr[0].amount;
        d1.awaitingAiAmountConfirm = false;
        saveDMState(state);
        await waClient.sendMessage(rawFrom, 'Updated to Rs.' + formatINR(d1.amount) + '.');
        // fall through
      } else {
        await waClient.sendMessage(rawFrom, 'Reply "yes" to confirm Rs.' + formatINR(d1.amount) + ', or send the correct amount (e.g. "46,200" or "46200").');
        return true;
      }
    }
  }

  if(!state.pending[rawFrom]) state.pending[rawFrom] = { details: '', amount: 0, company: '', fromAC: '', mediaIds: [], lastUpdate: new Date().toISOString(), askedFor: null, posterName: senderInfo.contactName || rawFrom, subItems: null, companyOptions: [], fromACOptions: [] };
  var entry = state.pending[rawFrom];
  entry.lastUpdate = new Date().toISOString();
  entry.posterName = senderInfo.contactName || entry.posterName;
  function parseStructuredFields(text) {
    var result = {};
    if(!text) return result;
    var lines = text.split('\n');
    lines.forEach(function(line) {
      var m = line.match(/^\s*(details?|amount|company|from|account|reason|vendor|to)\s*[:\-]\s*(.+)$/i);
      if(m){
        var key = m[1].toLowerCase();
        var val = m[2].trim();
        if(key === 'detail' || key === 'details' || key === 'reason' || key === 'vendor' || key === 'to') result.details = val;
        else if(key === 'amount'){
          var p = parseExpenseMessage(val);
          if(p[0].amount > 0) result.amount = p[0].amount;
        }
        else if(key === 'company') result.company = val;
        else if(key === 'from' || key === 'account') result.fromAC = val;
      }
    });
    return result;
  }
  var structured = parseStructuredFields(body);
  if(structured.details && !entry.details) entry.details = cleanDetails(structured.details) || structured.details;
  if(structured.amount && entry.amount === 0) entry.amount = structured.amount;
  if(structured.company && !entry.company) entry.company = structured.company;
  if(structured.fromAC && !entry.fromAC) entry.fromAC = structured.fromAC;
  var hadAnyStructured = structured.details || structured.amount || structured.company || structured.fromAC;
  if(entry.askedFor && body && !hadAnyStructured){
    if(entry.askedFor === 'details'){
      entry.details = body;
      entry.askedFor = null;
    } else if(entry.askedFor === 'amount'){
      var pa = parseExpenseMessage(body);
      if(pa.length > 1){
        entry.subItems = pa;
        entry.amount = pa.reduce(function(s,p){return s+p.amount;},0);
        entry.askedFor = null;
      } else if(pa[0].amount > 0){
        entry.amount = pa[0].amount;
        entry.askedFor = null;
      } else {
        await waClient.sendMessage(rawFrom, 'I could not read an amount in "' + body.substring(0,50) + '". Try one of these formats:\n  • 3 lac\n  • 3,00,000\n  • 300000\n  • 10k\n  • 1.5 cr');
        saveDMState(state);
        return true;
      }
    } else if(entry.askedFor === 'company'){
      var trimmed = body.trim();
      if(/^\d+$/.test(trimmed) && entry.companyOptions && entry.companyOptions.length){
        var idx = parseInt(trimmed) - 1;
        if(idx >= 0 && idx < entry.companyOptions.length) entry.company = entry.companyOptions[idx];
        else entry.company = trimmed;
      } else {
        entry.company = trimmed;
      }
      entry.askedFor = null;
    } else if(entry.askedFor === 'fromAC'){
      var trimmed2 = body.trim();
      if(/^\d+$/.test(trimmed2) && entry.fromACOptions && entry.fromACOptions.length){
        var idx2 = parseInt(trimmed2) - 1;
        if(idx2 >= 0 && idx2 < entry.fromACOptions.length) entry.fromAC = entry.fromACOptions[idx2];
        else entry.fromAC = trimmed2;
      } else {
        entry.fromAC = trimmed2;
      }
      entry.askedFor = null;
    }
  } else if(body && !hadAnyStructured && !entry.askedFor){
    // v2.7: If there's a pendingConfirm (we asked "did you mean X?") and the user just replied with
    // yes/no/numeric, resolve it here BEFORE doing fresh free-form parse.
    if(entry.pendingConfirm){
      var pc = entry.pendingConfirm;
      var trimmedPC = body.trim().toLowerCase();
      if(trimmedPC === 'yes' || trimmedPC === 'y' || trimmedPC === 'confirm'){
        if(pc.type === 'company') entry.company = pc.value;
        else if(pc.type === 'fromAC') entry.fromAC = pc.value;
        entry.pendingConfirm = null;
        saveDMState(state);
        // Fall through — re-evaluate what's still missing in the next pass
      } else if(/^\d+$/.test(trimmedPC)){
        // User typed a number — defer to the standard numbered-pick flow below.
        // Clear pendingConfirm and let the askedFor logic re-prompt with full list.
        entry.pendingConfirm = null;
        // Continue — askedFor logic will kick in once we reach the validate-required-fields stage
      } else if(trimmedPC === 'no' || trimmedPC === 'n'){
        entry.pendingConfirm = null;
        saveDMState(state);
        // Fall through — will re-prompt below
      }
    }
    var parsed = parseExpenseMessage(body);
    if(parsed.length > 1){
      entry.subItems = parsed;
      if(entry.amount === 0) entry.amount = parsed.reduce(function(s,p){return s+p.amount;},0);
      if(!entry.details) entry.details = parsed.map(function(p){return p.vendor+' '+formatINR(p.amount);}).join(', ');
    } else {
      var p = parsed[0];
      if(p.amount > 0 && entry.amount === 0) entry.amount = p.amount;
      if(!entry.details && body.length > 0){
        var cleaned = cleanDetails(body);
        entry.details = cleaned.length > 1 ? cleaned : body.substring(0, 250);
      }
    }
    // v2.7 NEW: smart free-form extraction of company + account from anywhere in the text.
    // Only pre-fill if the field is currently empty (don't overwrite explicit user input).
    try {
      var smart = await smartExtractCompanyAccount(body);
      if(!entry.company && smart.companyMatches.length === 1){
        // Single confident match — confirm before locking in
        if(!entry.pendingConfirm){
          entry.pendingConfirm = { type: 'company', value: smart.companyMatches[0] };
        }
      } else if(!entry.company && smart.companyMatches.length > 1){
        // Multiple matches — let the standard askedFor flow ask via numbered list
      }
      if(!entry.fromAC && smart.acMatches.length === 1){
        if(!entry.pendingConfirm){
          entry.pendingConfirm = { type: 'fromAC', value: smart.acMatches[0] };
        }
      }
    } catch(e) { /* smart extract is best-effort */ }
  }
  if(hasMedia){
    try {
      var media = await msg.downloadMedia();
      if(media && media.data){
        var visionResult = await extractFromImage(media, thisMsgId);
        if(visionResult){
          // v2.7.2 NEW: multi-amount image guard — mirror the text rule.
          // If the image shows 2+ distinct payable amounts, refuse and ask for one at a time.
          // Cheques are exempt (they carry no expense amount).
          if(visionResult.imageType !== 'cheque' && visionResult.amountCount >= 2){
            await waClient.sendMessage(rawFrom, [
              'This image looks like it has ' + visionResult.amountCount + ' separate payments in it.',
              '',
              'Please send one expense per image — one bill/invoice at a time. Send the first one on its own, then the next after it is posted.',
              '',
              'Reply cancel to clear.'
            ].join('\n'));
            return true;
          }
          entry.mediaIds.push(thisMsgId);
          if(!entry.mediaFiles) entry.mediaFiles = [];
          entry.mediaFiles.push({ msgId: thisMsgId, filename: body || ('attachment_'+entry.mediaFiles.length+'.'+(media.mimetype||'').split('/')[1]), mimetype: media.mimetype, dataB64: media.data });
          // Cheques: no expense info to extract; just attach the media
          if(visionResult.imageType === 'cheque'){
            // No-op for amount/details from cheques
          } else {
            // v2.7 NEW: Vision confirmation step — never auto-fill amount from vision.
            // Always DM the parsed values back to the accountant and require explicit confirmation.
            entry.visionParsed = {
              vendor: visionResult.vendor || '',
              amount: visionResult.amount || 0,
              purpose: visionResult.purpose || '',
              confidence: visionResult.confidence || 'low'
            };
            // Purpose/vendor still feed into details (as before) since they're descriptive context.
            // Amount is the only thing we hold back for human confirmation.
            if(!entry.details && visionResult.confidence !== 'low'){
              var dParts = [];
              if(visionResult.vendor) dParts.push(visionResult.vendor);
              if(visionResult.purpose) dParts.push(visionResult.purpose);
              if(dParts.length > 0) entry.details = dParts.join(' - ').substring(0,250);
            }
            // If we haven't yet captured an amount AND vision extracted one, hold it for confirmation.
            if(entry.amount === 0 && visionResult.amount > 0){
              entry.awaitingVisionConfirm = true;
              saveDMState(state);
              var confLines = ['I read this attachment as:'];
              if(visionResult.vendor) confLines.push('  Vendor: ' + visionResult.vendor);
              confLines.push('  Amount: Rs.' + formatINR(visionResult.amount));
              if(visionResult.purpose) confLines.push('  Purpose: ' + visionResult.purpose);
              if(visionResult.confidence === 'low') confLines.push('  (vision confidence: LOW)');
              confLines.push('');
              confLines.push('Confirm the amount before I post:');
              confLines.push('  • Reply "yes" if Rs.' + formatINR(visionResult.amount) + ' is correct');
              confLines.push('  • Or reply with the correct amount (e.g. "1.5 lac" or "150000")');
              await waClient.sendMessage(rawFrom, confLines.join('\n'));
              return true;
            }
          }
        }
      }
    } catch(e) { console.error('[DM] media download failed:', e.message); }
  }
  // v2.7 NEW: handle response to vision confirmation
  if(entry.awaitingVisionConfirm && body){
    var trimmedVC = body.trim().toLowerCase();
    if(trimmedVC === 'yes' || trimmedVC === 'y' || trimmedVC === 'confirm' || trimmedVC === 'ok'){
      // Accept vision's amount as-is
      if(entry.visionParsed && entry.visionParsed.amount > 0){
        entry.amount = entry.visionParsed.amount;
      }
      entry.awaitingVisionConfirm = false;
      saveDMState(state);
      await waClient.sendMessage(rawFrom, 'Confirmed Rs.' + formatINR(entry.amount) + '. Continuing...');
      // Fall through — required-fields validation will pick up next
    } else {
      // Try parsing a corrected amount
      var pcVC = parseExpenseMessage(body);
      if(pcVC[0].amount > 0){
        entry.amount = pcVC[0].amount;
        entry.awaitingVisionConfirm = false;
        saveDMState(state);
        await waClient.sendMessage(rawFrom, 'Updated amount to Rs.' + formatINR(entry.amount) + '. Continuing...');
        // Fall through
      } else {
        await waClient.sendMessage(rawFrom, 'I could not read an amount in "' + body.substring(0,50) + '". Reply "yes" to accept Rs.' + formatINR(entry.visionParsed.amount) + ' or send the correct amount (e.g. "1.5 lac").');
        saveDMState(state);
        return true;
      }
    }
  }
  // v2.7 NEW: if we have a pendingConfirm (smart-detected company/account), ask before list-ask
  if(entry.pendingConfirm && !entry.askedFor){
    var pcEntry = entry.pendingConfirm;
    saveDMState(state);
    var label = pcEntry.type === 'company' ? 'company' : 'bank A/C';
    await waClient.sendMessage(rawFrom, 'Did you mean ' + label + ': *' + pcEntry.value + '*?\n\nReply "yes" to confirm, "no" to pick from the full list, or just type the correct name.');
    return true;
  }
  if(!entry.details){
    entry.askedFor = 'details';
    saveDMState(state);
    await waClient.sendMessage(rawFrom, 'What is this payment for? Reply with the details (vendor name + reason).');
    return true;
  }
  if(entry.amount <= 0){
    entry.askedFor = 'amount';
    saveDMState(state);
    await waClient.sendMessage(rawFrom, 'How much is this expense for? Reply with amount (e.g. "3 lac", "300000", or "10k").');
    return true;
  }
  if(!entry.company){
    entry.askedFor = 'company';
    var companies = '';
    try {
      var fp = await getFundPosition();
      var uniqueCompanies = [];
      fp.forEach(function(a){ if(a.company && uniqueCompanies.indexOf(a.company) < 0) uniqueCompanies.push(a.company); });
      entry.companyOptions = uniqueCompanies;
      if(uniqueCompanies.length > 0) companies = '\n\nReply with a number or the name:\n' + uniqueCompanies.map(function(c,i){return (i+1)+'. '+c;}).join('\n');
    } catch(e) { entry.companyOptions = []; }
    saveDMState(state);
    await waClient.sendMessage(rawFrom, 'Which company is this expense for?' + companies);
    return true;
  }
  if(!entry.fromAC){
    entry.askedFor = 'fromAC';
    var accounts = '';
    try {
      var fp2 = await getFundPosition();
      var allAccounts = [];
      fp2.forEach(function(a){ if(a.bankAC && allAccounts.indexOf(a.bankAC) < 0) allAccounts.push(a.bankAC); });
      entry.fromACOptions = allAccounts;
      if(allAccounts.length > 0) accounts = '\n\nReply with a number or the name:\n' + allAccounts.map(function(c,i){return (i+1)+'. '+c;}).join('\n');
    } catch(e) { entry.fromACOptions = []; }
    saveDMState(state);
    await waClient.sendMessage(rawFrom, 'Which bank A/C should we pay from?' + accounts);
    return true;
  }
  var preview = buildGroupPostFromDM(entry, entry.posterName);
  var confirmText = 'Ready to post in the approval group:\n\n' + preview + '\n\nReply:\n  yes                          to post\n  edit <field>: <new value>    to change details/amount/company/from\n  show                         to see this draft again\n  cancel                       to clear and start over';
  if(body.toLowerCase() === 'yes' || body.toLowerCase() === 'post' || body.toLowerCase() === 'send'){
    try {
      var groupText = buildGroupPostFromDM(entry, entry.posterName);
      var postedMsg;
      if(entry.mediaFiles && entry.mediaFiles.length > 0){
        var firstMedia = entry.mediaFiles[0];
        var mm = new MessageMedia(firstMedia.mimetype, firstMedia.dataB64, firstMedia.filename);
        postedMsg = await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, mm, { caption: groupText });
        for(var mi=1; mi<entry.mediaFiles.length; mi++){
          var nm = new MessageMedia(entry.mediaFiles[mi].mimetype, entry.mediaFiles[mi].dataB64, entry.mediaFiles[mi].filename);
          await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, nm);
          await new Promise(function(r){setTimeout(r,1500);});
        }
      } else {
        postedMsg = await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, groupText);
      }
      delete state.pending[rawFrom];
      saveDMState(state);
      await waClient.sendMessage(rawFrom, 'Posted to approval group. M/S will review and respond.');
      console.log('[DM] posted to group from', entry.posterName);
    } catch(e) {
      console.error('[DM] post failed:', e.message);
      await waClient.sendMessage(rawFrom, 'Failed to post: ' + e.message + '. Reply "yes" to retry or "cancel" to clear.');
    }
    return true;
  }
  var editMatch = body.match(/^\s*edit\s+(details|amount|company|from|account)\s*:\s*(.+)$/i);
  if(editMatch){
    var fld = editMatch[1].toLowerCase();
    var val = editMatch[2].trim();
    if(fld === 'details') entry.details = val;
    else if(fld === 'amount'){
      var pe = parseExpenseMessage(val);
      if(pe[0].amount > 0) entry.amount = pe[0].amount;
      else {
        await waClient.sendMessage(rawFrom, 'I could not read an amount in "' + val.substring(0,50) + '". Try "3 lac", "3,00,000", or "300000".');
        saveDMState(state);
        return true;
      }
    }
    else if(fld === 'company'){
      // v2.7 NEW: support serial-number pick (e.g. "edit company: 3")
      if(/^\d+$/.test(val) && entry.companyOptions && entry.companyOptions.length){
        var ci = parseInt(val) - 1;
        if(ci >= 0 && ci < entry.companyOptions.length) entry.company = entry.companyOptions[ci];
        else entry.company = val;
      } else {
        entry.company = val;
      }
    }
    else if(fld === 'from' || fld === 'account'){
      // v2.7 NEW: support serial-number pick (e.g. "edit from: 7")
      if(/^\d+$/.test(val) && entry.fromACOptions && entry.fromACOptions.length){
        var fi = parseInt(val) - 1;
        if(fi >= 0 && fi < entry.fromACOptions.length) entry.fromAC = entry.fromACOptions[fi];
        else entry.fromAC = val;
      } else {
        entry.fromAC = val;
      }
    }
    saveDMState(state);
    await waClient.sendMessage(rawFrom, 'Updated. Here is the current draft:\n\n' + buildGroupPostFromDM(entry, entry.posterName) + '\n\nReply "yes" to post, or "edit <field>: <value>" to keep editing.');
    return true;
  }
  saveDMState(state);
  await waClient.sendMessage(rawFrom, confirmText);
  return true;
}
// ── Stale-pending scanner ────────────────────────────────────────────────────
var STALE_STATE_FILE = './wa_auth/reminder_state.json';
function loadStaleState() {
  try {
    if(fs.existsSync(STALE_STATE_FILE)){
      return JSON.parse(fs.readFileSync(STALE_STATE_FILE, 'utf8'));
    }
  } catch(e) { console.error('[Stale] state load failed:', e.message); }
  return { reminded: {} };
}
function saveStaleState(state) {
  try {
    if(!fs.existsSync('./wa_auth')) fs.mkdirSync('./wa_auth', { recursive: true });
    fs.writeFileSync(STALE_STATE_FILE, JSON.stringify(state, null, 2));
  } catch(e) { console.error('[Stale] state save failed:', e.message); }
}
// ── Silent mode toggle ───────────────────────────────────────────────────────
var SILENT_STATE_FILE = './wa_auth/silent_mode.json';
var SILENT_OBSERVER = '917838537000';
// ── v2.8 Module 7: unapproved-payment Day Book alert (toggle, default OFF) ───
var UNAPPROVED_ALERT_FILE = './wa_auth/unapproved_alert.json';
function loadUnapprovedAlert(){ try{ if(fs.existsSync(UNAPPROVED_ALERT_FILE)){ return JSON.parse(fs.readFileSync(UNAPPROVED_ALERT_FILE,'utf8')).enabled===true; } }catch(e){} return false; }
function saveUnapprovedAlert(on){ try{ if(!fs.existsSync('./wa_auth')) fs.mkdirSync('./wa_auth',{recursive:true}); fs.writeFileSync(UNAPPROVED_ALERT_FILE, JSON.stringify({enabled:!!on})); }catch(e){} }
// Posts flagged unapproved payments to the Day Book group when the toggle is ON.
// The same findings ALWAYS appear in the private EOD summary regardless.
async function postUnapprovedAlertIfEnabled(rec){
  try{
    if(!loadUnapprovedAlert()) return 0;
    if(!waReady || !rec || !rec.ledgerWithoutApproval || rec.ledgerWithoutApproval.length===0) return 0;
    var items = rec.ledgerWithoutApproval.slice(0,10);
    var lines = ['*PAYMENTS WITHOUT M/S APPROVAL*',''];
    var tot=0;
    items.forEach(function(le,i){
      var dt = le.date ? le.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'}) : '';
      lines.push((i+1)+'. '+(le.description||'(no description)').substring(0,60)+' — Rs.'+formatINR(le.amount)+' · '+(le.bankAC||'-')+' · '+dt);
      tot+=le.amount;
    });
    if(rec.ledgerWithoutApproval.length>10) lines.push('+'+(rec.ledgerWithoutApproval.length-10)+' more');
    lines.push('');
    lines.push(rec.ledgerWithoutApproval.length+' payment(s) · Rs.'+formatINR(tot)+' — please verify these were authorised.');
    await waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID, lines.join('\n'));
    console.log('[UnapprovedAlert] posted', items.length, 'to Day Book');
    return items.length;
  }catch(e){ console.error('[UnapprovedAlert]', e.message); return 0; }
}
function loadSilentMode() {
  try {
    if(fs.existsSync(SILENT_STATE_FILE)){
      var s = JSON.parse(fs.readFileSync(SILENT_STATE_FILE, 'utf8'));
      return s.enabled === true;
    }
  } catch(e) { console.error('[Silent] load failed:', e.message); }
  return true;
}
function saveSilentMode(enabled) {
  try {
    if(!fs.existsSync('./wa_auth')) fs.mkdirSync('./wa_auth', { recursive: true });
    fs.writeFileSync(SILENT_STATE_FILE, JSON.stringify({ enabled: enabled, updatedAt: new Date().toISOString() }, null, 2));
    console.log('[Silent] mode set to:', enabled ? 'ON (silent)' : 'OFF (group reminders active)');
  } catch(e) { console.error('[Silent] save failed:', e.message); }
}
function getSilentObserverJid() {
  return SILENT_OBSERVER + '@c.us';
}
function buildStaleReminderText(expense, now) {
  var mm = expense.status.mm, sm = expense.status.sm;
  var bothPending = mm === 'pending' && sm === 'pending';
  var mmOnly = mm === 'pending' && sm === 'yes';
  var smOnly = sm === 'pending' && mm === 'yes';
  var queryMM = mm === 'question', querySM = sm === 'question';
  var queryAnswered = expense.queryAnswer ? true : false;
  var mentionJids = [];
  var mentionTags = [];
  if(mm === 'pending' || (queryMM && queryAnswered)) {
    mentionJids.push(CONFIG.MM_PHONE + '@c.us');
    mentionTags.push('@' + CONFIG.MM_PHONE);
  }
  if(sm === 'pending' || (querySM && queryAnswered)) {
    mentionJids.push(CONFIG.SM_PHONE + '@c.us');
    mentionTags.push('@' + CONFIG.SM_PHONE);
  }
  var queryUnanswered = (queryMM || querySM) && !queryAnswered;
  if(queryUnanswered) {
    mentionJids = []; mentionTags = [];
    CONFIG.ACCOUNTANT_PHONES.forEach(function(p){
      mentionJids.push(p + '@c.us');
      mentionTags.push('@' + p);
    });
  }
  if(mentionJids.length === 0) return null;
  var minutesPending = Math.floor((now - expense.date.getTime()) / (60*1000));
  var lines = [];
  var header;
  if(queryUnanswered) header = '[BOT REMINDER] Query unanswered - pending ' + minutesPending + ' min';
  else if(queryMM || querySM) header = '[BOT REMINDER] Query answered - awaiting M/S - pending ' + minutesPending + ' min';
  else if(bothPending) header = '[BOT REMINDER] Approval needed - pending ' + minutesPending + ' min';
  else if(mmOnly) header = '[BOT REMINDER] M approval needed - pending ' + minutesPending + ' min';
  else if(smOnly) header = '[BOT REMINDER] S approval needed - pending ' + minutesPending + ' min';
  else return null;
  lines.push(header);
  lines.push('');
  var vendor = expense.vendor || expense.body.substring(0, 60);
  lines.push(vendor);
  if(expense.subItems && expense.subItems.length > 1){
    var total = expense.subItems.reduce(function(s, it){ return s + it.amount; }, 0);
    lines.push('Amount: Rs.' + formatINR(total) + ' total');
    expense.subItems.forEach(function(si){ lines.push('  - ' + si.vendor + ' Rs.' + formatINR(si.amount)); });
  } else if(expense.amount > 0){
    lines.push('Amount: Rs.' + formatINR(expense.amount));
  }
  var d = expense.date.toLocaleDateString('en-IN', {day:'numeric', month:'short', timeZone:'Asia/Kolkata'});
  var t = expense.date.toLocaleTimeString('en-IN', {hour:'2-digit', minute:'2-digit', timeZone:'Asia/Kolkata'});
  lines.push('Requested by: ' + expense.sender + ' - ' + d + ', ' + t);
  if(expense.supportingDocs && expense.supportingDocs.length > 0){
    var docNames = expense.supportingDocs.map(function(dd){ return dd.filename; }).join(', ');
    lines.push('Supporting docs: ' + docNames);
  }
  lines.push('');
  var mmLabel = mm==='yes'?'M: Ok':mm==='question'?'M: query raised':'M: pending';
  var smLabel = sm==='yes'?'S: Ok':sm==='question'?'S: query raised':'S: pending';
  lines.push(mmLabel + ' | ' + smLabel);
  if((queryMM||querySM) && queryAnswered){
    var ans = expense.queryAnswer;
    var who = ans.role === 'mm' ? 'M' : 'S';
    var answerLabel = ans.answerByRole && ans.answerByRole !== 'accountant' ? (ans.answerByRole + ' (' + ans.answerBy + ')') : ans.answerBy;
    lines.push('');
    lines.push(who + ' asked:');
    lines.push('"' + ans.question + '"');
    lines.push('');
    lines.push(answerLabel + ' answered:');
    lines.push('"' + ans.answer + '"');
  }
  lines.push('');
  lines.push(mentionTags.join(' ') + ' please reply: Yes / No / Hold / or reason');
  return { text: lines.join('\n'), mentionJids: mentionJids };
}
async function scanStalePendings() {
  if(!waReady || !CONFIG.BOT_ENABLED) return 0;
  if(loadSilentMode()){console.log('[Stale] silent mode ON — skipping group scan');return 0;}
  try {
    var state = loadStaleState();
    var audit = await buildApprovalAudit(2);
    var now = Date.now();
    var THIRTY_MIN = 30 * 60 * 1000;
    var nowIST = new Date(now);
    var hourIST = parseInt(nowIST.toLocaleString('en-IN', { hour: '2-digit', hour12: false, timeZone: 'Asia/Kolkata' }));
    if(hourIST >= 21 || hourIST < 9) {
      console.log('[Stale] quiet hours (IST '+hourIST+'h), skipping scan');
      return 0;
    }
    var candidates = audit.partialApproval.concat(
      audit.noApproval.filter(function(e){ return e.amount > 0 || (e.subItems && e.subItems.length > 0); })
    );
    var sentCount = 0;
    var delay = function(ms){ return new Promise(function(r){ setTimeout(r, ms); }); };
    for(var i=0; i<candidates.length; i++) {
      var expense = candidates[i];
      var expenseAge = now - expense.date.getTime();
      if(expenseAge < THIRTY_MIN) continue;
      if(state.reminded[expense.id]) continue;
      var built = buildStaleReminderText(expense, now);
      if(!built) continue;
      try {
        await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, built.text, { mentions: built.mentionJids });
        state.reminded[expense.id] = { sentAt: new Date().toISOString(), missing: [] };
        if(expense.status.mm === 'pending') state.reminded[expense.id].missing.push('mm');
        if(expense.status.sm === 'pending') state.reminded[expense.id].missing.push('sm');
        sentCount++;
        console.log('[Stale] reminded', expense.id, '(', expense.amount, ')');
        await delay(2000);
      } catch(e) {
        console.error('[Stale] send failed for', expense.id, e.message);
      }
    }
    if(sentCount > 0) saveStaleState(state);
    return sentCount;
  } catch(e) {
    console.error('[Stale] scan failed:', e.message);
    return 0;
  }
}
// ── v2.6 REWRITE: buildStalePendingSection — top-N recent + older rollup ──
async function buildStalePendingSection() {
  try {
    var audit = await buildApprovalAudit(7);
    var stillPending = audit.partialApproval.concat(
      audit.noApproval.filter(function(e){ return e.amount > 0; })
    ).filter(function(e){ return e.date.getTime() >= REPORT_START_MS; }); // v2.8 cutoff
    if(stillPending.length === 0) return '';
    var nowMs = Date.now();
    stillPending.sort(function(a,b){ return b.date.getTime() - a.date.getTime(); });
    var recent = [];
    var older = [];
    stillPending.forEach(function(e){
      var ageHrs = (nowMs - e.date.getTime()) / (60*60*1000);
      if(ageHrs <= STALE_RECENT_HOURS) recent.push(e);
      else older.push(e);
    });
    if(recent.length > STALE_TOP_N){
      older = recent.slice(STALE_TOP_N).concat(older);
      recent = recent.slice(0, STALE_TOP_N);
    }
    var lines = [''];
    lines.push('--- STALE PENDING (recent) ---');
    lines.push('');
    var totalRecent = 0;
    recent.forEach(function(e){
      var status = e.status.mm === 'yes' ? 'S pending' : e.status.sm === 'yes' ? 'M pending' : e.status.mm === 'question' ? 'Query open' : e.status.sm === 'question' ? 'Query open' : 'Both pending';
      var age = Math.floor((nowMs - e.date.getTime()) / (60*60*1000));
      lines.push('- ' + (e.vendor || e.body.substring(0,40)) + ' Rs.' + formatINR(e.amount) + ' [' + status + ', ' + age + 'h]');
      totalRecent += e.amount;
    });
    lines.push('');
    lines.push('Total recent: Rs.' + formatINR(totalRecent));
    if(older.length > 0){
      var totalOlder = older.reduce(function(s,e){return s + e.amount;}, 0);
      lines.push('+ ' + older.length + ' older items totalling Rs.' + formatINR(totalOlder) + ' — reply MORE STALE to see all');
    }
    return lines.join('\n');
  } catch(e) {
    console.error('[StaleSection] failed:', e.message);
    return '';
  }
}
// ── Ledger + Fund Position ────────────────────────────────────────────────────
async function getLedgerData(dateStr) {
  var rows=await readSheet('Ledger!A:L');
  var target=dateStr||new Date().toISOString().split('T')[0], entries=[];
  for(var i=0;i<rows.length;i++){
    var row=rows[i]; if(!row[0]||!row[5])continue;
    var cd=parseSheetDate(row[0]); if(!cd)continue;
    if(cd.toISOString().split('T')[0]===target){
      entries.push({date:cd,entity:row[1]||'',head:row[2]||'',description:row[3]||'',tag:row[4]||'',inOut:row[5]||'',amount:parseAmount(row[6]),mode:row[7]||'',person:row[8]||'',bankAC:row[9]||'',transferTo:row[10]||'',notes:row[11]||''});
    }
  }
  return entries;
}
async function getFundPosition() {
  // v2.10.0-s5.18: range widened A4:J40 (was A4:J27 — the s5.x Fund Position inserts
  // for Fervor/Tremendous/RMS pushed SM PDC + PDC past row 27 and they were being dropped
  // from the daily report). Stop at the TOTAL row so the Less/Net label rows below it are
  // never read as accounts, regardless of how many account rows exist above.
  var rows=await readSheet('Fund Position!A4:J40'), accounts=[];
  for(var i=1;i<rows.length;i++){
    var r=rows[i];
    var label=(r[1]||'').toString().trim();
    if(/^TOTAL/i.test(label)||/^Less/i.test(label)||/^Net\b/i.test(label))break; // totals/footer → end of accounts
    if(!label)continue;                                                            // skip stray blank rows
    accounts.push({num:r[0]||'',company:r[1]||'',bankAC:r[2]||'',opening:parseAmount(r[3]),todayIn:parseAmount(r[4]),todayOut:parseAmount(r[5]),closing:parseAmount(r[6]),cheques:parseAmount(r[7]),netBal:parseAmount(r[8]),status:r[9]||'Usable'});
  }
  return accounts;
}
async function getLedgerRange(startDate, endDate) {
  var rows = await readSheet('Ledger!A:L');
  var entries = [];
  var startMs = startDate ? startDate.getTime() : 0;
  var endMs = endDate ? endDate.getTime() : Date.now() + 86400000;
  for(var i=0; i<rows.length; i++){
    var row = rows[i]; if(!row[0] || !row[5]) continue;
    var cd = parseSheetDate(row[0]); if(!cd) continue;
    var t = cd.getTime();
    if(t < startMs || t > endMs) continue;
    entries.push({
      date: cd,
      entity: row[1]||'', head: row[2]||'', description: row[3]||'',
      tag: row[4]||'', inOut: row[5]||'', amount: parseAmount(row[6]),
      mode: row[7]||'', person: row[8]||'', bankAC: row[9]||'',
      transferTo: row[10]||'', notes: row[11]||''
    });
  }
  return entries;
}
// ── Stage 2: Fuzzy matching helpers ──────────────────────────────────────────
function levenshtein(a, b) {
  if(!a) return b ? b.length : 0;
  if(!b) return a.length;
  a = a.toLowerCase(); b = b.toLowerCase();
  if(a === b) return 0;
  var m = a.length, n = b.length;
  var prev = new Array(n+1), curr = new Array(n+1);
  for(var i=0; i<=n; i++) prev[i] = i;
  for(var i2=1; i2<=m; i2++){
    curr[0] = i2;
    for(var j=1; j<=n; j++){
      var cost = a[i2-1] === b[j-1] ? 0 : 1;
      curr[j] = Math.min(curr[j-1]+1, prev[j]+1, prev[j-1]+cost);
    }
    var tmp = prev; prev = curr; curr = tmp;
  }
  return prev[n];
}
function soundex(s) {
  if(!s) return '';
  s = s.toUpperCase().replace(/[^A-Z]/g, '');
  if(!s) return '';
  var first = s[0];
  var map = { B:1,F:1,P:1,V:1, C:2,G:2,J:2,K:2,Q:2,S:2,X:2,Z:2, D:3,T:3, L:4, M:5,N:5, R:6 };
  var code = '';
  var prev = map[first] || '';
  for(var i=1; i<s.length && code.length<3; i++){
    var c = map[s[i]];
    if(c && c !== prev) code += c;
    if(s[i] !== 'H' && s[i] !== 'W') prev = c || '';
  }
  return (first + code + '000').substring(0,4);
}
function fuzzyMatch(a, b) {
  if(!a || !b) return false;
  a = a.toLowerCase(); b = b.toLowerCase();
  if(a === b) return true;
  if(a.indexOf(b) >= 0 || b.indexOf(a) >= 0) return true;
  var maxLen = Math.max(a.length, b.length);
  var threshold = maxLen <= 8 ? 3 : 4;
  var dist = levenshtein(a, b);
  if(dist <= threshold) return true;
  if(/^[a-z]+$/.test(a) && /^[a-z]+$/.test(b)){
    if(soundex(a) === soundex(b)) return true;
  }
  return false;
}
function ledgerFuzzyWordOverlap(ledgerEntry, expense) {
  if(!ledgerEntry || !expense) return null;
  var ledgerText = ((ledgerEntry.description||'') + ' ' + (ledgerEntry.entity||'') + ' ' + (ledgerEntry.person||'')).toLowerCase();
  var expenseTextRaw = (expense.vendor||'') + ' ' + (expense.body||'');
  if(expense.supportingDocs && expense.supportingDocs.length){
    expense.supportingDocs.forEach(function(d){ expenseTextRaw += ' ' + (d.vendor||'') + ' ' + (d.purpose||''); });
  }
  var expenseText = expenseTextRaw.toLowerCase();
  var stopWords = ['the','and','for','with','from','this','that','please','approve','kindly','payment','amount','rs','inr','total','expense','expenses','final'];
  var expWords = expenseText.split(/[^a-z0-9]+/).filter(function(w){
    return w.length >= 4 && stopWords.indexOf(w) < 0;
  });
  var ledgerWords = ledgerText.split(/[^a-z0-9]+/).filter(function(w){ return w.length >= 4; });
  for(var i=0; i<expWords.length; i++){
    for(var j=0; j<ledgerWords.length; j++){
      if(fuzzyMatch(expWords[i], ledgerWords[j])){
        return { matchedExpWord: expWords[i], matchedLedgerWord: ledgerWords[j] };
      }
    }
  }
  return null;
}
function ledgerWordOverlap(ledgerEntry, expense) {
  if(!ledgerEntry || !expense) return false;
  var ledgerText = ((ledgerEntry.description||'') + ' ' + (ledgerEntry.entity||'') + ' ' + (ledgerEntry.person||'')).toLowerCase();
  var expenseTextRaw = (expense.vendor||'') + ' ' + (expense.body||'');
  if(expense.supportingDocs && expense.supportingDocs.length){
    expense.supportingDocs.forEach(function(d){ expenseTextRaw += ' ' + (d.vendor||'') + ' ' + (d.purpose||''); });
  }
  var expenseText = expenseTextRaw.toLowerCase();
  var stopWords = ['the','and','for','with','from','this','that','please','approve','kindly','payment','amount','rs','inr','total','expense','expenses','final'];
  var expWords = expenseText.split(/[^a-z0-9]+/).filter(function(w){
    return w.length >= 3 && stopWords.indexOf(w) < 0;
  });
  var ledgerWords = ledgerText.split(/[^a-z0-9]+/);
  for(var w=0; w<expWords.length; w++){
    if(ledgerWords.indexOf(expWords[w]) >= 0) return true;
  }
  return false;
}
// ── Match Cache ──────────────────────────────────────────────────────────────
var MATCH_CACHE_FILE = './wa_auth/match_cache.json';
function loadMatchCache() {
  try {
    if(fs.existsSync(MATCH_CACHE_FILE)){
      return JSON.parse(fs.readFileSync(MATCH_CACHE_FILE, 'utf8'));
    }
  } catch(e) { console.error('[MatchCache] load failed:', e.message); }
  return { matches: {}, rejected: {}, manualPaid: {} };
}
function saveMatchCache(cache) {
  try {
    if(!fs.existsSync('./wa_auth')) fs.mkdirSync('./wa_auth', { recursive: true });
    fs.writeFileSync(MATCH_CACHE_FILE, JSON.stringify(cache, null, 2));
  } catch(e) { console.error('[MatchCache] save failed:', e.message); }
}
function ledgerEntryHash(le) {
  if(!le) return null;
  var d = le.date ? le.date.toISOString().split('T')[0] : '';
  return d + '|' + le.amount + '|' + (le.bankAC||'') + '|' + (le.description||'').substring(0,50);
}
// ── Stage 3: Haiku semantic matcher ──────────────────────────────────────────
async function haikuSemanticMatch(expense, candidates) {
  if(!CONFIG.CLAUDE_API_KEY) return null;
  if(!candidates || candidates.length === 0) return null;
  var top5 = candidates.slice(0,5);
  var expenseSummary = (expense.vendor || '') + (expense.body && expense.body !== expense.vendor ? ' · ' + expense.body : '');
  if(expense.supportingDocs && expense.supportingDocs.length){
    expense.supportingDocs.forEach(function(d){
      if(d.vendor) expenseSummary += ' · doc vendor: ' + d.vendor;
      if(d.purpose) expenseSummary += ' · doc purpose: ' + d.purpose;
    });
  }
  var candidateLines = top5.map(function(c, i){
    var dt = c.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',year:'numeric'});
    return (i) + '. ' + dt + ' · Rs.' + c.amount + ' · ' + (c.bankAC||'-') + ' · ' + (c.description||'') + (c.entity?' · '+c.entity:'') + (c.person?' · person:'+c.person:'');
  }).join('\n');
  var prompt = 'You are matching an approved expense request to its actual Ledger payment.\n\n' +
    'APPROVED EXPENSE:\n' +
    'Date: ' + expense.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',year:'numeric'}) + '\n' +
    'Amount: Rs.' + expense.amount + '\n' +
    'Description: ' + expenseSummary + '\n\n' +
    'CANDIDATE LEDGER ENTRIES:\n' + candidateLines + '\n\n' +
    'Which candidate (if any) is the actual payment for this approval? ' +
    'Consider synonyms (e.g. TMT bar = Steel rods), informal vs formal names, Hindi/English mix, abbreviations. ' +
    'Reply ONLY with strict JSON: {"match": <index 0-' + (top5.length-1) + '|null>, "confidence": <0.0-1.0>, "reasoning": "<one sentence>"}';
  try {
    var resp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': CONFIG.CLAUDE_API_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-6',
        max_tokens: 200,
        messages: [{ role: 'user', content: prompt }]
      })
    });
    if(!resp.ok){ console.error('[Haiku] HTTP', resp.status); return null; }
    var data = await resp.json();
    var text = data.content && data.content[0] && data.content[0].text || '';
    var jsonMatch = text.match(/\{[\s\S]*\}/);
    if(!jsonMatch) return null;
    var parsed = JSON.parse(jsonMatch[0]);
    if(parsed.match === null || parsed.match === undefined) return { match: null, confidence: 0, reasoning: parsed.reasoning };
    var idx = parseInt(parsed.match);
    if(isNaN(idx) || idx < 0 || idx >= top5.length) return null;
    return {
      match: top5[idx],
      confidence: parseFloat(parsed.confidence) || 0,
      reasoning: parsed.reasoning || '',
      candidatesShown: top5.length
    };
  } catch(e) {
    console.error('[Haiku] semantic match failed:', e.message);
    return null;
  }
}
function matchLedgerEntry(expense, ledgerEntries) {
  // v2.8: reconcile against the APPROVED amount (Rule A amendments), not the requested one.
  var effAmt = (expense && (expense.approvedAmount || expense.amount)) || 0;
  if(!expense || effAmt <= 0) return null;
  var approvalDate = expense.date.getTime();
  var fourteenDaysMs = 14 * 24 * 60 * 60 * 1000;
  var sevenDaysMs = 7 * 24 * 60 * 60 * 1000;
  var strictMatches = [];
  var tolMatches = [];
  var amountOnlyCandidates = [];
  for(var i=0; i<ledgerEntries.length; i++){
    var le = ledgerEntries[i];
    if(le.inOut !== 'OUT') continue;
    var ledgerMs = le.date.getTime();
    var dateDiff = ledgerMs - approvalDate;
    if(dateDiff < -86400000) continue;
    if(dateDiff > fourteenDaysMs) continue;
    var amtDiff = Math.abs(le.amount - effAmt);
    var pctDiff = amtDiff / effAmt;
    var strictWords = ledgerWordOverlap(le, expense);
    var fuzzyWordsRes = strictWords ? null : ledgerFuzzyWordOverlap(le, expense);
    var fuzzyWords = fuzzyWordsRes ? true : false;
    if(amtDiff <= 1 && strictWords){
      strictMatches.push({ entry: le, dateDiffDays: Math.round(dateDiff/86400000) });
      continue;
    }
    if(pctDiff <= 0.05 && strictWords){
      tolMatches.push({ entry: le, dateDiffDays: Math.round(dateDiff/86400000), pctDiff: pctDiff });
      continue;
    }
    // v2.8: fuzzy and date-proximity cases no longer auto-match (they produced the
    // BSES/YEIDA false pairings). They become candidates for the Sonnet matcher.
    if(pctDiff <= 0.10 || ((strictWords || fuzzyWords) && Math.abs(dateDiff) <= sevenDaysMs)){
      amountOnlyCandidates.push(le);
    }
  }
  if(strictMatches.length > 0){
    return { status: 'paid', confidence: 'high', stage: 'exact', match: strictMatches[0].entry, dateDiffDays: strictMatches[0].dateDiffDays };
  }
  if(tolMatches.length > 0){
    return { status: 'paid_with_tolerance', confidence: 'medium', stage: 'exact_tolerance', match: tolMatches[0].entry, dateDiffDays: tolMatches[0].dateDiffDays, pctDiff: tolMatches[0].pctDiff };
  }
  if(amountOnlyCandidates.length > 0){
    return { status: 'awaiting_payment', confidence: null, stage: 'needs_ai', match: null, candidates: amountOnlyCandidates };
  }
  return { status: 'awaiting_payment', confidence: null, stage: 'no_match', match: null };
}
async function matchLedgerEntryWithAI(expense, ledgerEntries, cache) {
  if(!cache) cache = loadMatchCache();
  var cached = cache.matches[expense.id];
  if(cached){
    var matchedEntry = ledgerEntries.find(function(le){ return ledgerEntryHash(le) === cached.ledgerHash; });
    if(matchedEntry){
      return {
        status: cached.manuallyRejected ? 'awaiting_payment' : 'paid',
        confidence: cached.manuallyConfirmed ? 'manual' : (cached.confidence || 'cached'),
        stage: 'cached',
        match: matchedEntry,
        cachedFromStage: cached.stage,
        manuallyConfirmed: cached.manuallyConfirmed || false
      };
    }
  }
  if(cache.manualPaid && cache.manualPaid[expense.id]){
    return { status: 'paid', confidence: 'manual', stage: 'manual_paid', match: null, manualPaidDate: cache.manualPaid[expense.id].paidDate };
  }
  var result = matchLedgerEntry(expense, ledgerEntries);
  if(!result) return null;
  if(result.status === 'paid' || result.status === 'paid_with_tolerance'){
    if(result.match){
      cache.matches[expense.id] = {
        ledgerHash: ledgerEntryHash(result.match),
        stage: result.stage,
        confidence: result.confidence,
        approvedAt: expense.date.toISOString(),
        paidAt: result.match.date ? result.match.date.toISOString() : null,
        gapDays: typeof result.dateDiffDays==='number' ? result.dateDiffDays : null,
        ts: new Date().toISOString()
      };
      saveMatchCache(cache);
    }
    return result;
  }
  if(result.stage === 'needs_ai' && result.candidates && result.candidates.length > 0 && CONFIG.CLAUDE_API_KEY){
    var rejectedHashes = (cache.rejected[expense.id]) || [];
    var validCandidates = result.candidates.filter(function(c){ return rejectedHashes.indexOf(ledgerEntryHash(c)) < 0; });
    if(validCandidates.length === 0){
      return { status: 'awaiting_payment', confidence: null, stage: 'no_match', match: null };
    }
    var aiResult = await haikuSemanticMatch(expense, validCandidates);
    if(aiResult && aiResult.match){
      var aiMatch = aiResult.match;
      var status, confidence;
      if(aiResult.confidence >= 0.85){ status = 'paid'; confidence = 'ai_high'; }
      else if(aiResult.confidence >= 0.55){ status = 'possible_match'; confidence = 'ai_medium'; }
      else { return { status: 'awaiting_payment', confidence: null, stage: 'ai_rejected', aiReasoning: aiResult.reasoning }; }
      if(confidence === 'ai_high'){
        cache.matches[expense.id] = {
          ledgerHash: ledgerEntryHash(aiMatch),
          stage: 'ai',
          confidence: confidence,
          aiConfidence: aiResult.confidence,
          aiReasoning: aiResult.reasoning,
          approvedAt: expense.date.toISOString(),
          paidAt: aiMatch.date ? aiMatch.date.toISOString() : null,
          gapDays: aiMatch.date ? Math.round((aiMatch.date.getTime()-expense.date.getTime())/86400000) : null,
          ts: new Date().toISOString()
        };
        saveMatchCache(cache);
      }
      return {
        status: status,
        confidence: confidence,
        stage: 'ai',
        match: aiMatch,
        aiConfidence: aiResult.confidence,
        aiReasoning: aiResult.reasoning
      };
    }
  }
  return result;
}
// ── v2.6 EXTENDED: buildReconciliation with reverse-direction scan ───────────
async function buildReconciliation(days) {
  var audit = await buildApprovalAudit(days || 30);
  var approved = audit.fullyApproved;
  // Even if no approvals, we still want to run the reverse pass so a Ledger
  // payment with no approval at all gets flagged. So we always continue.
  var earliest = approved.length > 0
    ? approved.reduce(function(min, e){ return e.date.getTime() < min ? e.date.getTime() : min; }, Date.now())
    : Date.now() - (REVERSE_SCAN_WINDOW_DAYS * 86400000);
  var startDate = new Date(earliest - 86400000);
  var endDate = new Date(Date.now() + 86400000);
  var ledgerEntries = await getLedgerRange(startDate, endDate);
  var paid = [], paidWithTolerance = [], possibleMatch = [], awaitingPayment = [];
  var totalApproved = 0, totalPaid = 0, totalAwaiting = 0;
  var cache = loadMatchCache();
  var matcherStats = { exact: 0, exact_tolerance: 0, fuzzy: 0, ai: 0, cached: 0, manual: 0, awaiting: 0, possible: 0 };
  // FORWARD pass: each approved expense -> find Ledger match
  for(var ei=0; ei<approved.length; ei++){
    var expense = approved[ei];
    totalApproved += expense.amount;
    if(expense.subItems && expense.subItems.length > 1){
      var allMatched = true;
      var subResults = [];
      for(var si_i=0; si_i<expense.subItems.length; si_i++){
        var si = expense.subItems[si_i];
        var subExpense = { id: expense.id+':sub'+si_i, amount: si.amount, vendor: si.vendor, body: si.vendor, date: expense.date, supportingDocs: expense.supportingDocs };
        var subMatch = await matchLedgerEntryWithAI(subExpense, ledgerEntries, cache);
        subResults.push({ subItem: si, match: subMatch });
        if(!subMatch || subMatch.status === 'awaiting_payment') allMatched = false;
        if(subMatch && subMatch.stage) matcherStats[subMatch.stage] = (matcherStats[subMatch.stage]||0) + 1;
      }
      var combined = Object.assign({}, expense, { subItemResults: subResults, allSubItemsMatched: allMatched });
      if(allMatched){ paid.push(combined); totalPaid += expense.amount; }
      else { awaitingPayment.push(combined); totalAwaiting += expense.amount; }
      continue;
    }
    var result = await matchLedgerEntryWithAI(expense, ledgerEntries, cache);
    var withMatch = Object.assign({}, expense, { matchResult: result });
    if(result && result.stage) matcherStats[result.stage] = (matcherStats[result.stage]||0) + 1;
    if(!result){
      awaitingPayment.push(withMatch); totalAwaiting += expense.amount;
    } else if(result.status === 'paid'){
      paid.push(withMatch); totalPaid += expense.amount;
    } else if(result.status === 'paid_with_tolerance'){
      paidWithTolerance.push(withMatch); totalPaid += expense.amount;
    } else if(result.status === 'possible_match'){
      possibleMatch.push(withMatch);
    } else {
      awaitingPayment.push(withMatch); totalAwaiting += expense.amount;
    }
  }
  // v2.8 Module 4: cluster retirement — when any member of a re-ask cluster is
  // paid, its siblings are the same logical expense and leave awaiting-payment.
  var paidClusters = {};
  paid.concat(paidWithTolerance).forEach(function(it){ if(it.clusterId) paidClusters[it.clusterId]=true; });
  var clusterResolved = [];
  awaitingPayment = awaitingPayment.filter(function(it){
    if(it.clusterId && paidClusters[it.clusterId]){
      clusterResolved.push(it);
      totalAwaiting -= it.amount;
      return false;
    }
    return true;
  });
  // v2.6 REVERSE pass: Ledger entries with no matching approval
  // Collect every Ledger entry that was matched in the forward pass
  var matchedHashes = {};
  function _addMatched(items){
    items.forEach(function(item){
      if(item.matchResult && item.matchResult.match){
        matchedHashes[ledgerEntryHash(item.matchResult.match)] = true;
      }
      if(item.subItemResults){
        item.subItemResults.forEach(function(sr){
          if(sr.match && sr.match.match){
            matchedHashes[ledgerEntryHash(sr.match.match)] = true;
          }
        });
      }
    });
  }
  _addMatched(paid);
  _addMatched(paidWithTolerance);
  _addMatched(possibleMatch);
  // Walk Ledger OUT entries within the reverse-scan window
  var nowMs = Date.now();
  var windowStartMs = nowMs - (REVERSE_SCAN_WINDOW_DAYS * 86400000);
  var ledgerWithoutApproval = [];
  var ledgerRecurring = [];
  for(var li=0; li<ledgerEntries.length; li++){
    var le = ledgerEntries[li];
    if(le.inOut !== 'OUT') continue;
    if(le.amount < REVERSE_SCAN_MIN_AMOUNT) continue;
    if(le.date.getTime() < windowStartMs) continue;
    if(matchedHashes[ledgerEntryHash(le)]) continue;
    if(isRecurringPattern(le)){
      le.autoDebit = isAutoDebitEntry(le); // v2.8: highlight auto-debited EMIs informationally
      ledgerRecurring.push(le);
    } else {
      ledgerWithoutApproval.push(le);
    }
  }
  ledgerWithoutApproval.sort(function(a,b){
    var dDiff = b.date.getTime() - a.date.getTime();
    if(dDiff !== 0) return dDiff;
    return b.amount - a.amount;
  });
  ledgerRecurring.sort(function(a,b){ return b.date.getTime() - a.date.getTime(); });
  return {
    paid: paid,
    paidWithTolerance: paidWithTolerance,
    possibleMatch: possibleMatch,
    awaitingPayment: awaitingPayment,
    clusterResolved: clusterResolved,
    ledgerWithoutApproval: ledgerWithoutApproval,
    ledgerRecurring: ledgerRecurring,
    matcherStats: matcherStats,
    cacheSize: Object.keys(cache.matches).length,
    summary: {
      totalApproved: totalApproved,
      totalPaid: totalPaid,
      totalAwaiting: totalAwaiting,
      totalPossible: possibleMatch.reduce(function(s,e){return s+e.amount;},0),
      totalUnmatchedLedger: ledgerWithoutApproval.reduce(function(s,e){return s+e.amount;},0),
      totalRecurringLedger: ledgerRecurring.reduce(function(s,e){return s+e.amount;},0),
      countApproved: approved.length,
      countPaid: paid.length,
      countPaidTolerance: paidWithTolerance.length,
      countPossible: possibleMatch.length,
      countAwaiting: awaitingPayment.length,
      countUnmatchedLedger: ledgerWithoutApproval.length,
      countRecurringLedger: ledgerRecurring.length
    }
  };
}
// ── Outlier Detection ────────────────────────────────────────────────────────
async function buildOutliers(rec) {
  var outliers = [];
  var SEVEN_DAYS_MS = 7 * 86400000;
  var now = Date.now();
  rec.paidWithTolerance.forEach(function(e){
    var m = e.matchResult ? e.matchResult.match : null;
    if(!m) return;
    var diff = m.amount - e.amount;
    if(Math.abs(diff) >= 500){
      outliers.push({
        type: 'amount_mismatch',
        expense: e,
        ledger: m,
        approved: e.amount,
        actualPaid: m.amount,
        diff: diff,
        pctDiff: e.matchResult.pctDiff,
        likelyTDS: diff < 0 && Math.abs(diff/e.amount) < 0.05
      });
    }
  });
  rec.possibleMatch.forEach(function(e){
    if(!e.matchResult) return;
    if(e.matchResult.stage === 'ai' || e.matchResult.confidence === 'ai_medium'){
      outliers.push({
        type: 'ai_low_confidence',
        expense: e,
        ledger: e.matchResult.match,
        aiConfidence: e.matchResult.aiConfidence,
        aiReasoning: e.matchResult.aiReasoning
      });
    } else {
      outliers.push({
        type: 'possible_match',
        expense: e,
        ledger: e.matchResult.match,
        approved: e.amount,
        actualPaid: e.matchResult.match ? e.matchResult.match.amount : 0
      });
    }
  });
  rec.awaitingPayment.forEach(function(e){
    var ageMs = now - e.date.getTime();
    if(ageMs >= SEVEN_DAYS_MS){
      outliers.push({
        type: 'stale_awaiting',
        expense: e,
        ageDays: Math.floor(ageMs / 86400000)
      });
    }
  });
  rec.paid.forEach(function(e){
    if(!e.matchResult || !e.matchResult.dateDiffDays) return;
    if(e.matchResult.dateDiffDays >= 10){
      outliers.push({
        type: 'date_drift',
        expense: e,
        ledger: e.matchResult.match,
        daysAfterApproval: e.matchResult.dateDiffDays
      });
    }
  });
  outliers.forEach(function(o, i){ o.id = i + 1; });
  return outliers;
}
// ── v2.6 REWRITE: buildReconciliationSection — top-N per section + reverse-scan ──
async function buildReconciliationSection() {
  try {
    var rec = await buildReconciliation(30);
    if(rec.summary.countApproved === 0 && (!rec.ledgerWithoutApproval || rec.ledgerWithoutApproval.length === 0)) return '';
    function sortPaidByLedgerDate(arr){
      var copy = arr.slice();
      copy.sort(function(a,b){
        var aD = (a.matchResult && a.matchResult.match) ? a.matchResult.match.date.getTime() : 0;
        var bD = (b.matchResult && b.matchResult.match) ? b.matchResult.match.date.getTime() : 0;
        return bD - aD;
      });
      return copy;
    }
    function sortByApprovalDateDesc(arr){
      var copy = arr.slice();
      copy.sort(function(a,b){ return b.date.getTime() - a.date.getTime(); });
      return copy;
    }
    var lines = [''];
    lines.push('--- APPROVED EXPENSES STATUS ---');
    lines.push('');
    if(rec.paid.length > 0){
      var sortedPaid = sortPaidByLedgerDate(rec.paid);
      var shown = sortedPaid.slice(0, REPORT_TOP_N);
      lines.push('Recent paid (top ' + shown.length + ' of ' + rec.paid.length + '):');
      shown.forEach(function(e){
        var m = e.matchResult ? e.matchResult.match : null;
        var bankAC = m ? m.bankAC : '';
        var dt = m ? m.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'}) : '';
        lines.push('  ✓ ' + (e.vendor || e.body.substring(0,30)) + ' Rs.' + formatINR(e.amount) + (m ? ' → ' + dt + ', ' + bankAC : ''));
      });
      if(rec.paid.length > REPORT_TOP_N){
        lines.push('  + ' + (rec.paid.length - REPORT_TOP_N) + ' more paid items — reply MORE PAID to see all');
      }
      lines.push('');
    }
    if(rec.paidWithTolerance.length > 0){
      var sortedTol = sortPaidByLedgerDate(rec.paidWithTolerance);
      var shownTol = sortedTol.slice(0, REPORT_TOP_N);
      lines.push('Paid (within 5% tolerance) (top ' + shownTol.length + ' of ' + rec.paidWithTolerance.length + '):');
      shownTol.forEach(function(e){
        var m = e.matchResult ? e.matchResult.match : null;
        lines.push('  ~ ' + (e.vendor || e.body.substring(0,30)) + ' Rs.' + formatINR(e.amount) + ' (paid Rs.' + formatINR(m ? m.amount : 0) + ')');
      });
      if(rec.paidWithTolerance.length > REPORT_TOP_N){
        lines.push('  + ' + (rec.paidWithTolerance.length - REPORT_TOP_N) + ' more — reply MORE TOLERANCE to see all');
      }
      lines.push('');
    }
    if(rec.awaitingPayment.length > 0){
      var sortedAwait = sortByApprovalDateDesc(rec.awaitingPayment);
      var shownAwait = sortedAwait.slice(0, REPORT_TOP_N);
      lines.push('Approved but not yet paid (top ' + shownAwait.length + ' of ' + rec.awaitingPayment.length + '):');
      shownAwait.forEach(function(e){
        var d = e.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'});
        lines.push('  ⏳ ' + (e.vendor || e.body.substring(0,30)) + ' Rs.' + formatINR(e.amount) + ' (approved ' + d + ')');
      });
      if(rec.awaitingPayment.length > REPORT_TOP_N){
        lines.push('  + ' + (rec.awaitingPayment.length - REPORT_TOP_N) + ' more — reply MORE AWAITING to see all');
      }
      lines.push('');
    }
    if(rec.possibleMatch.length > 0){
      var sortedPoss = sortByApprovalDateDesc(rec.possibleMatch);
      var shownPoss = sortedPoss.slice(0, REPORT_TOP_N);
      lines.push('Possible mismatch — needs review (top ' + shownPoss.length + ' of ' + rec.possibleMatch.length + '):');
      shownPoss.forEach(function(e){
        var m = e.matchResult ? e.matchResult.match : null;
        lines.push('  ⚠ ' + (e.vendor || e.body.substring(0,30)) + ' approved Rs.' + formatINR(e.amount) + ' vs Ledger Rs.' + formatINR(m ? m.amount : 0));
      });
      if(rec.possibleMatch.length > REPORT_TOP_N){
        lines.push('  + ' + (rec.possibleMatch.length - REPORT_TOP_N) + ' more — reply MORE MISMATCH to see all');
      }
      lines.push('');
    }
    // v2.6 NEW: reverse-scan — Ledger entries without WhatsApp approval
    if(rec.ledgerWithoutApproval && rec.ledgerWithoutApproval.length > 0){
      var shownRev = rec.ledgerWithoutApproval.slice(0, REPORT_TOP_N);
      lines.push('⚠ Ledger payments WITHOUT approval (top ' + shownRev.length + ' of ' + rec.ledgerWithoutApproval.length + '):');
      shownRev.forEach(function(le){
        var dt = le.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'});
        var desc = (le.description||le.entity||'').substring(0,40);
        lines.push('  ❗ ' + desc + ' Rs.' + formatINR(le.amount) + ' (' + (le.bankAC||'-') + ', ' + dt + ')');
      });
      if(rec.ledgerWithoutApproval.length > REPORT_TOP_N){
        lines.push('  + ' + (rec.ledgerWithoutApproval.length - REPORT_TOP_N) + ' more — reply MORE UNMATCHED to see all');
      }
      lines.push('');
    }
    if(rec.ledgerRecurring && rec.ledgerRecurring.length > 0){
      lines.push('ℹ ' + rec.ledgerRecurring.length + ' recurring/auto Ledger items suppressed (EMI, salary, bank charges, etc.) — reply MORE RECURRING to see');
      lines.push('');
    }
    lines.push('Total approved: Rs.' + formatINR(rec.summary.totalApproved));
    lines.push('Total paid: Rs.' + formatINR(rec.summary.totalPaid));
    lines.push('Awaiting payment: Rs.' + formatINR(rec.summary.totalAwaiting));
    if(rec.summary.totalUnmatchedLedger > 0){
      lines.push('Ledger unmatched: Rs.' + formatINR(rec.summary.totalUnmatchedLedger));
    }
    return lines.join('\n');
  } catch(e) {
    console.error('[Reconciliation section]', e.message);
    return '';
  }
}
// ── EOD Daily Report Builder (HTML for the JPEG sent to SILENT_OBSERVER) ─────
function escapeHtml(s) {
  if(s === null || s === undefined) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
function buildEODReportHTML(opts) {
  var d = opts.date;
  var audit = opts.audit || { fullyApproved:[], partialApproval:[], noApproval:[], allExpenses:[] };
  var rec = opts.rec || { paid:[], paidWithTolerance:[], possibleMatch:[], awaitingPayment:[], summary:{} };
  var outliers = opts.outliers || [];
  var isFriday = opts.isFriday || false;
  var weekStats = opts.weekStats || null;
  var todayDate = new Date(d);
  var todayStr = todayDate.toLocaleDateString('en-IN',{day:'numeric',month:'long',year:'numeric',timeZone:'Asia/Kolkata'});
  var weekday = todayDate.toLocaleDateString('en-IN',{weekday:'long',timeZone:'Asia/Kolkata'});
  var todayKey = d;
  var todaysApprovals = audit.allExpenses ? audit.allExpenses.filter(function(e){
    return e.date.toISOString().split('T')[0] === todayKey;
  }) : [];
  var todayBoth = todaysApprovals.filter(function(e){ return e.status.mm==='yes' && e.status.sm==='yes'; });
  var todayOne  = todaysApprovals.filter(function(e){ return (e.status.mm==='yes')!==(e.status.sm==='yes'); });
  var todayNone = todaysApprovals.filter(function(e){ return e.status.mm!=='yes' && e.status.sm!=='yes'; });
  var todayBothAmt = todayBoth.reduce(function(s,e){return s+e.amount;},0);
  var todayOneAmt = todayOne.reduce(function(s,e){return s+e.amount;},0);
  var todayNoneAmt = todayNone.reduce(function(s,e){return s+e.amount;},0);
  var todayTotalAmt = todayBothAmt + todayOneAmt + todayNoneAmt;
  function bar(pct){ return Math.max(0, Math.min(100, Math.round(pct))); }
  function pctOf(part, whole){ return whole > 0 ? (part/whole)*100 : 0; }
  // v2.6: also show top-N in the JPEG (mirror text format)
  var paidShown = rec.paid.slice().sort(function(a,b){
    var aD = (a.matchResult && a.matchResult.match) ? a.matchResult.match.date.getTime() : 0;
    var bD = (b.matchResult && b.matchResult.match) ? b.matchResult.match.date.getTime() : 0;
    return bD - aD;
  }).slice(0, REPORT_TOP_N);
  var paidExtra = rec.paid.length - paidShown.length;
  var paidHTML = paidShown.length ? paidShown.map(function(e){
    var m = e.matchResult ? e.matchResult.match : null;
    var subDocs = e.supportingDocs && e.supportingDocs.length ? '<div class="item-doc">📎 ' + escapeHtml(e.supportingDocs.map(function(d){return d.filename;}).join(', ')) + '</div>' : '';
    var dt = m ? m.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'}) : '';
    var stageTag = e.matchResult && e.matchResult.stage ? '<span class="stage-tag stage-'+escapeHtml(e.matchResult.stage)+'">'+escapeHtml(e.matchResult.stage)+'</span>' : '';
    return '<div class="item">'+
      '<div class="item-row"><span class="item-name">'+escapeHtml(e.vendor || (e.body||'').substring(0,40))+' '+stageTag+'</span><span class="item-amount">Rs.'+formatINR(e.amount)+'</span></div>'+
      (m ? '<div class="item-meta">Ledger '+escapeHtml(dt)+' · '+escapeHtml(m.bankAC||'-')+'</div>' : '')+
      subDocs+'</div>';
  }).join('') : '<div class="empty">None today</div>';
  if(paidExtra > 0) paidHTML += '<div class="empty">+ ' + paidExtra + ' more — reply MORE PAID</div>';
  var awaitShown = rec.awaitingPayment.slice().sort(function(a,b){ return b.date.getTime() - a.date.getTime(); }).slice(0, REPORT_TOP_N);
  var awaitExtra = rec.awaitingPayment.length - awaitShown.length;
  var awaitingHTML = awaitShown.length ? awaitShown.map(function(e){
    var d2 = e.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'});
    var ageDays = Math.floor((Date.now() - e.date.getTime()) / 86400000);
    return '<div class="item amber">'+
      '<div class="item-row"><span class="item-name">'+escapeHtml(e.vendor || (e.body||'').substring(0,40))+'</span><span class="item-amount">Rs.'+formatINR(e.amount)+'</span></div>'+
      '<div class="item-meta">approved '+escapeHtml(d2)+' · '+ageDays+'d ago, no Ledger entry yet</div></div>';
  }).join('') : '<div class="empty">None — all approved expenses paid</div>';
  if(awaitExtra > 0) awaitingHTML += '<div class="empty">+ ' + awaitExtra + ' more — reply MORE AWAITING</div>';
  // v2.6: reverse-scan section in JPEG
  var revShown = (rec.ledgerWithoutApproval || []).slice(0, REPORT_TOP_N);
  var revExtra = (rec.ledgerWithoutApproval || []).length - revShown.length;
  var revHTML = revShown.length ? revShown.map(function(le){
    var dt = le.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'});
    return '<div class="item red">'+
      '<div class="item-row"><span class="item-name">❗ '+escapeHtml((le.description||le.entity||'').substring(0,40))+'</span><span class="item-amount">Rs.'+formatINR(le.amount)+'</span></div>'+
      '<div class="item-meta">'+escapeHtml(le.bankAC||'-')+' · '+escapeHtml(dt)+' · no approval found</div></div>';
  }).join('') : '';
  if(revExtra > 0) revHTML += '<div class="empty">+ ' + revExtra + ' more — reply MORE UNMATCHED</div>';
  var partialFresh = (audit.partialApproval || []).filter(function(e){
    return e.date.getTime() >= REPORT_START_MS && (Date.now()-e.date.getTime()) <= REMINDER_MAX_AGE_DAYS*86400000;
  });
  var partialHTML = partialFresh.length ? partialFresh.slice(0, REPORT_TOP_N).map(function(e){
    var who = e.status.mm==='yes' ? 'M ✓ · S pending' : (e.status.sm==='yes' ? 'S ✓ · M pending' : 'Both pending');
    var hours = Math.floor((Date.now() - e.date.getTime()) / (60*60*1000));
    var ageStr = hours >= 24 ? Math.floor(hours/24)+'d' : hours+'h';
    var subTag = e.subItems && e.subItems.length>1 ? ' · '+e.subItems.length+' sub-items' : '';
    return '<div class="item gray">'+
      '<div class="item-row"><span class="item-name">'+escapeHtml(e.vendor || (e.body||'').substring(0,40))+'</span><span class="item-amount">Rs.'+formatINR(e.amount)+'</span></div>'+
      '<div class="item-meta">'+escapeHtml(who)+' · '+ageStr+escapeHtml(subTag)+'</div></div>';
  }).join('') : '';
  var partialExtra = (audit.partialApproval || []).length - REPORT_TOP_N;
  if(partialExtra > 0) partialHTML += '<div class="empty">+ ' + partialExtra + ' more — reply MORE STALE</div>';
  var queryHTML = (audit.noApproval || []).filter(function(e){return e.queryAnswer;}).slice(0, REPORT_TOP_N).map(function(e){
    return '<div class="item gray">'+
      '<div class="item-row"><span class="item-name">'+escapeHtml(e.vendor || (e.body||'').substring(0,40))+'</span><span class="item-amount">Rs.'+formatINR(e.amount)+'</span></div>'+
      '<div class="item-meta">Query answered · awaiting M + S</div></div>';
  }).join('');
  var stuckOnMM = (audit.partialApproval || []).filter(function(e){ return e.status.mm==='pending' && e.status.sm==='yes'; }).reduce(function(s,e){return s+e.amount;},0);
  var outliersHTML = outliers.length ? outliers.map(function(o){
    var n = o.id;
    if(o.type === 'amount_mismatch'){
      var sign = o.diff < 0 ? '−' : '+';
      var pct = (Math.abs(o.diff)/o.approved*100).toFixed(1);
      return '<div class="item red">'+
        '<div class="item-row"><span class="item-name"><b>'+n+'.</b> Amount mismatch — '+escapeHtml(o.expense.vendor||'')+'</span></div>'+
        '<div class="item-meta">Approved Rs.'+formatINR(o.approved)+' · Ledger Rs.'+formatINR(o.actualPaid)+'<br>Diff '+sign+'Rs.'+formatINR(Math.abs(o.diff))+' ('+sign+pct+'%)'+(o.likelyTDS?' — likely TDS':'')+'</div>'+
        '<div class="action-box"><span class="cmd">'+n+' ok</span>confirm <span class="cmd">'+n+' flag</span>investigate <span class="cmd">'+n+' ignore</span>skip</div></div>';
    }
    if(o.type === 'stale_awaiting'){
      return '<div class="item red">'+
        '<div class="item-row"><span class="item-name"><b>'+n+'.</b> Stale — '+escapeHtml(o.expense.vendor||'')+'</span></div>'+
        '<div class="item-meta">Rs.'+formatINR(o.expense.amount)+' · approved '+o.ageDays+'d ago, no Ledger entry yet</div>'+
        '<div class="action-box"><span class="cmd">'+n+' chase</span>nudge accountant <span class="cmd">'+n+' paid &lt;date&gt;</span>mark paid</div></div>';
    }
    if(o.type === 'ai_low_confidence'){
      return '<div class="item red">'+
        '<div class="item-row"><span class="item-name"><b>'+n+'.</b> AI match (low conf '+(o.aiConfidence||0).toFixed(2)+') — '+escapeHtml(o.expense.vendor||'')+'</span></div>'+
        '<div class="item-meta">Possible: '+escapeHtml(o.ledger ? o.ledger.description+' Rs.'+formatINR(o.ledger.amount) : '')+'<br><i>'+escapeHtml(o.aiReasoning||'')+'</i></div>'+
        '<div class="action-box"><span class="cmd">'+n+' confirm</span>accept match <span class="cmd">'+n+' reject</span>reject match</div></div>';
    }
    if(o.type === 'possible_match'){
      return '<div class="item red">'+
        '<div class="item-row"><span class="item-name"><b>'+n+'.</b> Possible match — '+escapeHtml(o.expense.vendor||'')+'</span></div>'+
        '<div class="item-meta">Approved Rs.'+formatINR(o.approved)+' · Candidate Rs.'+formatINR(o.actualPaid)+'</div>'+
        '<div class="action-box"><span class="cmd">'+n+' confirm</span>accept <span class="cmd">'+n+' reject</span>reject</div></div>';
    }
    if(o.type === 'date_drift'){
      return '<div class="item red">'+
        '<div class="item-row"><span class="item-name"><b>'+n+'.</b> Date drift — '+escapeHtml(o.expense.vendor||'')+'</span></div>'+
        '<div class="item-meta">Paid '+o.daysAfterApproval+'d after approval — review</div>'+
        '<div class="action-box"><span class="cmd">'+n+' ok</span>confirm <span class="cmd">'+n+' flag</span>investigate</div></div>';
    }
    return '';
  }).join('') : '<div class="empty">No outliers — everything matches</div>';
  var weeklyHTML = '';
  if(isFriday && weekStats){
    var total = weekStats.totalMatches || 1;
    var pctExact = pctOf(weekStats.exact||0, total);
    var pctFuzzy = pctOf(weekStats.fuzzy||0, total);
    var pctAI    = pctOf(weekStats.ai||0, total);
    var pctCached= pctOf(weekStats.cached||0, total);
    weeklyHTML =
      '<div class="section weekly">'+
      '<div class="section-header">📈 MATCHER LEARNING (this week)</div>'+
      '<div class="row"><span class="row-label">Total matches</span><span class="row-value">'+total+'</span></div>'+
      '<div style="margin-top:10px">'+
        '<div class="bar-row"><span class="bar-icon">🎯</span><span class="bar-label">Exact</span><span class="bar-track"><span class="bar-fill green" style="width:'+bar(pctExact)+'%"></span></span><span class="bar-num">'+(weekStats.exact||0)+' · '+pctExact.toFixed(0)+'%</span></div>'+
        '<div class="bar-row"><span class="bar-icon">🔍</span><span class="bar-label">Fuzzy</span><span class="bar-track"><span class="bar-fill amber" style="width:'+bar(pctFuzzy)+'%"></span></span><span class="bar-num">'+(weekStats.fuzzy||0)+' · '+pctFuzzy.toFixed(0)+'%</span></div>'+
        '<div class="bar-row"><span class="bar-icon">🤖</span><span class="bar-label">Haiku AI</span><span class="bar-track"><span class="bar-fill ai" style="width:'+bar(pctAI)+'%"></span></span><span class="bar-num">'+(weekStats.ai||0)+' · '+pctAI.toFixed(0)+'%</span></div>'+
        '<div class="bar-row"><span class="bar-icon">💾</span><span class="bar-label">Cached</span><span class="bar-track"><span class="bar-fill cyan" style="width:'+bar(pctCached)+'%"></span></span><span class="bar-num">'+(weekStats.cached||0)+' · '+pctCached.toFixed(0)+'%</span></div>'+
      '</div>'+
      '<div class="total-row"><span class="label">AI cost this week</span><span class="val">~Rs.'+(weekStats.aiCost||'0').toString()+'</span></div>'+
      '<div class="total-row"><span class="label">Manual confirmations</span><span class="val green">'+(weekStats.manualConfirm||0)+'</span></div>'+
      '<div class="total-row"><span class="label">Manual rejections</span><span class="val amber">'+(weekStats.manualReject||0)+'</span></div>'+
      '</div>';
  }
  var html = '<!DOCTYPE html><html><head><meta charset="UTF-8"><style>'+
    '*{box-sizing:border-box;margin:0;padding:0}'+
    'body{background:#ffffff;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;padding:30px 20px;color:#000000}'+
    '.phone-frame{max-width:420px;margin:0 auto;background:#ffffff;border-radius:24px;overflow:hidden;border:1px solid #000}'+
    '.header{background:#ffffff;padding:14px 16px;display:flex;align-items:center;gap:12px;border-bottom:1px solid #cccccc}'+
    '.avatar{width:40px;height:40px;border-radius:50%;background:linear-gradient(135deg,#000000,#333333);display:flex;align-items:center;justify-content:center;color:white;font-weight:600;font-size:16px}'+
    '.header-name{color:#000000;font-size:16px;font-weight:500}'+
    '.header-status{color:#555555;font-size:12px;margin-top:2px}'+
    '.chat-area{background:#ffffff;padding:16px 12px;}'+
    '.timestamp{text-align:center;color:#555555;font-size:12px;margin:8px 0 16px}'+
    '.message{background:#ffffff;border-radius:8px;padding:14px 16px;margin-bottom:10px;max-width:95%}'+
    '.report-title{font-size:14px;font-weight:600;color:#000000;margin-bottom:4px;letter-spacing:0.3px}'+
    '.report-subtitle{font-size:11px;color:#555555;margin-bottom:12px}'+
    '.section{margin-top:14px;padding-top:12px;border-top:1px solid #cccccc}'+
    '.section:first-of-type{border-top:none;margin-top:8px;padding-top:0}'+
    '.section-header{font-size:11px;font-weight:700;color:#000000;text-transform:uppercase;letter-spacing:0.6px;margin-bottom:8px}'+
    '.row{display:flex;justify-content:space-between;align-items:center;font-size:13px;color:#000000;margin-bottom:5px;line-height:1.4}'+
    '.row-label{color:#555555;font-size:12px}.row-value{font-weight:500}'+
    '.bar-row{display:flex;align-items:center;gap:8px;margin-bottom:6px;font-size:12px}'+
    '.bar-icon{width:18px;text-align:center;font-size:13px}.bar-label{width:72px;color:#222222;font-size:11px}'+
    '.bar-track{flex:1;height:8px;background:#cccccc;border-radius:4px;overflow:hidden}'+
    '.bar-fill{height:100%;border-radius:4px}'+
    '.bar-fill.green{background:linear-gradient(90deg,#000000,#000000)}'+
    '.bar-fill.amber{background:linear-gradient(90deg,#888888,#aaaaaa)}'+
    '.bar-fill.red{background:linear-gradient(90deg,#000000,#444444)}'+
    '.bar-fill.ai{background:linear-gradient(90deg,#777777,#999999)}'+
    '.bar-fill.cyan{background:linear-gradient(90deg,#4abdc4,#7adde2)}'+
    '.bar-num{color:#222222;font-size:11px;min-width:75px;text-align:right}'+
    '.item{background:#111b21;border-left:3px solid #000000;padding:8px 10px;margin-bottom:6px;border-radius:4px}'+
    '.item.amber{border-left-color:#888888}.item.red{border-left-color:#000000}.item.gray{border-left-color:#54656f}'+
    '.item-row{display:flex;justify-content:space-between;font-size:12px}'+
    '.item-name{color:#000000;flex:1}.item-amount{color:#000000;font-weight:600;margin-left:8px;white-space:nowrap}'+
    '.item-meta{color:#555555;font-size:11px;margin-top:3px}'+
    '.item-doc{color:#6db4ff;font-size:11px;margin-top:3px;font-style:italic}'+
    '.total-row{display:flex;justify-content:space-between;margin-top:8px;padding-top:8px;border-top:1px dashed #cccccc;font-size:12px}'+
    '.total-row .label{color:#555555}.total-row .val{color:#000000;font-weight:600}'+
    '.total-row .val.green{color:#000000}.total-row .val.amber{color:#aaaaaa}'+
    '.action-box{background:#1f2c33;border:1px solid #cccccc;border-radius:6px;padding:8px 10px;margin-top:6px;font-size:11px;color:#555555;line-height:1.5}'+
    '.action-box .cmd{color:#000000;font-family:monospace;background:#ffffff;padding:1px 5px;border-radius:3px;margin-right:4px}'+
    '.empty{color:#555555;font-size:12px;font-style:italic;padding:6px 0}'+
    '.footer-note{margin-top:12px;padding-top:10px;border-top:1px solid #cccccc;color:#555555;font-size:11px;text-align:center;font-style:italic}'+
    '.stage-tag{display:inline-block;background:#cccccc;color:#555555;font-size:9px;padding:1px 5px;border-radius:3px;margin-left:4px;font-weight:400;text-transform:uppercase;letter-spacing:0.3px}'+
    '.stage-tag.stage-ai{background:#3a2a55;color:#999999}'+
    '.stage-tag.stage-cached{background:#2a3a4a;color:#7adde2}'+
    '.stage-tag.stage-fuzzy{background:#3a3525;color:#aaaaaa}'+
    '.delivered{color:#53bdeb;font-size:11px;margin-left:4px}'+
  '</style></head><body>'+
    '<div class="phone-frame">'+
      '<div class="header"><div class="avatar">F</div><div class="header-info"><div class="header-name">Fidato MIS Bot</div><div class="header-status">+91 98701 11582 · online</div></div></div>'+
      '<div class="chat-area">'+
        '<div class="timestamp">Today, 7:00 PM</div>'+
        '<div class="message">'+
          '<div class="report-title">FIDATO MIS — DAILY REPORT'+(opts.part===2?' · Part 2':(opts.part===1?' · Part 1':''))+'</div>'+
          '<div class="report-subtitle">'+escapeHtml(todayStr)+' · '+escapeHtml(weekday)+'</div>'+
          (opts.part===2 ? '' :
          '<div class="section">'+
            '<div class="section-header">Today\'s Activity</div>'+
            '<div class="row"><span class="row-label">Requests posted</span><span class="row-value">'+todaysApprovals.length+'</span></div>'+
            '<div class="row"><span class="row-label">Total requested</span><span class="row-value">Rs.'+formatINR(todayTotalAmt)+'</span></div>'+
            '<div style="margin-top:10px">'+
              '<div class="bar-row"><span class="bar-icon">✓</span><span class="bar-label">Both ✓</span><span class="bar-track"><span class="bar-fill green" style="width:'+bar(pctOf(todayBoth.length,todaysApprovals.length||1))+'%"></span></span><span class="bar-num">'+todayBoth.length+' · Rs.'+formatINR(todayBothAmt)+'</span></div>'+
              '<div class="bar-row"><span class="bar-icon">◐</span><span class="bar-label">One only</span><span class="bar-track"><span class="bar-fill amber" style="width:'+bar(pctOf(todayOne.length,todaysApprovals.length||1))+'%"></span></span><span class="bar-num">'+todayOne.length+' · Rs.'+formatINR(todayOneAmt)+'</span></div>'+
              '<div class="bar-row"><span class="bar-icon">○</span><span class="bar-label">Neither</span><span class="bar-track"><span class="bar-fill red" style="width:'+bar(pctOf(todayNone.length,todaysApprovals.length||1))+'%"></span></span><span class="bar-num">'+todayNone.length+' · Rs.'+formatINR(todayNoneAmt)+'</span></div>'+
            '</div>'+
          '</div>'+
          '<div class="section">'+
            '<div class="section-header">✓ Recent Paid (top '+REPORT_TOP_N+')</div>'+
            paidHTML+
          '</div>'+
          '<div class="section">'+
            '<div class="section-header">⏳ Approved, Awaiting Payment</div>'+
            awaitingHTML+
            '<div class="total-row"><span class="label">Total paid</span><span class="val green">Rs.'+formatINR(rec.summary.totalPaid||0)+'</span></div>'+
            '<div class="total-row"><span class="label">Awaiting payment</span><span class="val amber">Rs.'+formatINR(rec.summary.totalAwaiting||0)+'</span></div>'+
          '</div>')+
          (opts.part===1 ? '' :
          (revHTML ?
            '<div class="section">'+
              '<div class="section-header">⚠ Ledger Payments WITHOUT Approval</div>'+
              revHTML+
              (rec.summary.totalUnmatchedLedger ? '<div class="total-row"><span class="label">Total unmatched</span><span class="val amber">Rs.'+formatINR(rec.summary.totalUnmatchedLedger)+'</span></div>' : '')+
            '</div>' : ''
          )+
          (partialHTML || queryHTML ?
            '<div class="section">'+
              '<div class="section-header">◐ Partial — Needs M/S (top '+REPORT_TOP_N+')</div>'+
              partialHTML + queryHTML +
              (stuckOnMM > 0 ? '<div class="total-row"><span class="label">Stuck on M</span><span class="val amber">Rs.'+formatINR(stuckOnMM)+'</span></div>' : '')+
            '</div>' : ''
          )+
          '<div class="section">'+
            '<div class="section-header">⚠ Manual Intervention</div>'+
            outliersHTML+
          '</div>'+
          weeklyHTML+
          '<div class="footer-note">'+
            'Reply: MORE STALE | PAID | AWAITING | MISMATCH | UNMATCHED · ' +
            (isFriday ? 'Weekly model report included above.' : 'Next weekly — Friday 7 PM') +
          '</div>')+
        '</div>'+
      '</div>'+
    '</div>'+
  '</body></html>';
  return html;
}
function computeWeeklyMatcherStats(cache) {
  if(!cache) cache = loadMatchCache();
  var oneWeekAgo = Date.now() - 7*86400000;
  var stats = { exact:0, exact_tolerance:0, fuzzy:0, ai:0, cached:0, manual:0, totalMatches:0, manualConfirm:0, manualReject:0, aiCost:0 };
  Object.keys(cache.matches).forEach(function(k){
    var m = cache.matches[k];
    var ts = m.ts ? new Date(m.ts).getTime() : 0;
    if(ts < oneWeekAgo) return;
    stats.totalMatches++;
    if(m.stage === 'exact') stats.exact++;
    else if(m.stage === 'exact_tolerance') stats.exact_tolerance++;
    else if(m.stage === 'fuzzy') stats.fuzzy++;
    else if(m.stage === 'ai') stats.ai++;
    if(m.manuallyConfirmed) stats.manualConfirm++;
    if(m.manuallyRejected) stats.manualReject++;
  });
  stats.cached = Object.keys(cache.matches).length;
  stats.aiCost = (stats.ai * 0.07).toFixed(2);
  return stats;
}
async function sendEODReport(dateStr) {
  if(!waReady){ console.log('[EOD] WA not ready'); return; }
  try {
    var d = dateStr || new Date().toISOString().split('T')[0];
    var audit = await buildApprovalAudit(30);
    var rec = await buildReconciliation(30);
    var outliers = await buildOutliers(rec);
    var dayOfWeek = new Date(d).getDay();
    var isFriday = dayOfWeek === 5;
    var weekStats = isFriday ? computeWeeklyMatcherStats() : null;
    // v2.8 Module 8: black-on-white, posted in two parts.
    var html1 = buildEODReportHTML({ date: d, audit: audit, rec: rec, outliers: outliers, isFriday: isFriday, weekStats: weekStats, part: 1 });
    var html2 = buildEODReportHTML({ date: d, audit: audit, rec: rec, outliers: outliers, isFriday: isFriday, weekStats: weekStats, part: 2 });
    var img1 = await htmlToImage(html1, 460, 1200);
    var img2 = await htmlToImage(html2, 460, 1400);
    var buf = Buffer.isBuffer(img1) ? img1 : Buffer.from(img1);
    var buf2 = Buffer.isBuffer(img2) ? img2 : Buffer.from(img2);
    var captionLines = ['📊 Daily Report — '+ new Date(d).toLocaleDateString('en-IN',{day:'numeric',month:'short',year:'numeric',timeZone:'Asia/Kolkata'})];
    var todaysCount = (audit.allExpenses || []).filter(function(e){ return e.date.toISOString().split('T')[0] === d; }).length;
    captionLines.push(todaysCount+' requests today · '+(rec.paid.length)+' paid · Rs.'+formatINR(rec.summary.totalAwaiting||0)+' awaiting');
    if(rec.ledgerWithoutApproval && rec.ledgerWithoutApproval.length > 0){
      captionLines.push('⚠ '+rec.ledgerWithoutApproval.length+' Ledger payment(s) without approval — reply MORE UNMATCHED');
    }
    if(outliers.length > 0) captionLines.push(outliers.length+' outlier(s) need your input — reply with command shown in image');
    if(isFriday) captionLines.push('📈 Weekly matcher learning included.');
    var jid = getSilentObserverJid();
    await waClient.sendMessage(jid, new MessageMedia('image/png', buf.toString('base64'), 'EOD_'+d+'_part1.png'), { caption: captionLines.join('\n') });
    await waClient.sendMessage(jid, new MessageMedia('image/png', buf2.toString('base64'), 'EOD_'+d+'_part2.png'), { caption: 'Part 2 — pending, held and manual intervention' });
    console.log('[EOD] sent 2 parts to', jid);
    saveDMState(Object.assign(loadDMState(), { lastOutliers: { date: d, items: outliers.map(function(o){
      return { id: o.id, type: o.type, expenseId: o.expense.id, ledgerHash: o.ledger ? ledgerEntryHash(o.ledger) : null };
    })}}));
    return { success: true, outlierCount: outliers.length, unmatchedLedgerCount: (rec.ledgerWithoutApproval||[]).length, rec: rec };
  } catch(e) {
    console.error('[EOD] failed:', e.message);
    return { error: e.message };
  }
}
// ── Report HTML (legacy daily MIS report) ─────────────────────────────────────
async function generateDailyReport(dateStr){
  var entries=await getLedgerData(dateStr);var fp=await getFundPosition();
  var tIn=0,tOut=0,inflows=[],outflows=[];
  entries.forEach(function(e){if(e.inOut==='IN'){tIn+=e.amount;inflows.push(e);}if(e.inOut==='OUT'){tOut+=e.amount;outflows.push(e);}});
  var byTag={};outflows.forEach(function(e){var t=e.tag||'Other';if(!byTag[t])byTag[t]={total:0,items:[]};byTag[t].total+=e.amount;byTag[t].items.push(e);});
  return{date:dateStr,totalIn:tIn,totalOut:tOut,net:tIn-tOut,inflows:inflows,outflows:outflows,byTag:byTag,fundPosition:fp,entryCount:entries.length};
}
function buildReportHTML(data){
  var h='<!DOCTYPE html><html><head><meta charset="utf-8"><style>body{font-family:Arial,sans-serif;background:#fff;padding:20px;max-width:800px;margin:0 auto;color:#222}.hdr{text-align:center;border-bottom:2px solid #333;padding-bottom:10px;margin-bottom:15px}.hdr h1{font-size:22px;margin:0}.hdr p{color:#666;margin:4px 0 0}.metrics{display:flex;gap:10px;margin:15px 0}.mc{flex:1;background:#f5f5f5;border-radius:8px;padding:12px;text-align:center}.mc .lbl{font-size:11px;color:#888}.mc .val{font-size:20px;font-weight:bold;margin:4px 0 0}.gn{color:#0a7}.rd{color:#c33}.bl{color:#36a}.sec{font-size:14px;font-weight:bold;color:#555;border-bottom:1px solid #ddd;padding:8px 0 4px;margin:15px 0 8px}table{width:100%;border-collapse:collapse;font-size:12px}th{text-align:left;padding:5px;background:#f0f0f0;font-size:11px;color:#666}td{padding:5px;border-top:1px solid #eee}.amt{text-align:right;font-family:monospace}tr.tot td{border-top:2px solid #999;font-weight:bold;background:#fafafa}.usb{color:#0a7}.unusb{color:#c33}</style></head><body>';
  var _wd=['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
  var _pd=parseSheetDate(data.date)||new Date(data.date);
  var _dd=isNaN(_pd)?data.date:(_wd[_pd.getDay()]+', '+('0'+_pd.getDate()).slice(-2)+'/'+('0'+(_pd.getMonth()+1)).slice(-2)+'/'+_pd.getFullYear());
  h+='<div class="hdr"><h1>Daily MIS Report</h1><p>'+_dd+' | '+data.entryCount+' transactions</p></div>';
  h+='<div class="metrics"><div class="mc"><div class="lbl">Total Inflows</div><div class="val gn">'+formatINR(data.totalIn)+'</div></div><div class="mc"><div class="lbl">Total Outflows</div><div class="val rd">'+formatINR(data.totalOut)+'</div></div><div class="mc"><div class="lbl">Net</div><div class="val '+(data.net>=0?'bl':'rd')+'">'+formatINR(data.net)+'</div></div></div>';
  if(data.inflows.length>0){h+='<div class="sec">INFLOWS</div><table><tr><th>Description</th><th>Entity</th><th>Tag</th><th>Bank A/C</th><th style="text-align:right">Amount</th></tr>';data.inflows.forEach(function(e){h+='<tr><td>'+e.description+'</td><td>'+e.entity+'</td><td>'+e.tag+'</td><td>'+e.bankAC+'</td><td class="amt gn">'+formatINR(e.amount)+'</td></tr>';});h+='</table>';}
  h+='<div class="sec">OUTFLOWS</div><table><tr><th>Description</th><th>Head</th><th>Entity</th><th>Tag</th><th>Bank A/C</th><th>Mode</th><th style="text-align:right">Amount</th></tr>';
  data.outflows.forEach(function(e){h+='<tr><td>'+e.description+'</td><td>'+e.head+'</td><td>'+e.entity+'</td><td>'+e.tag+'</td><td>'+e.bankAC+'</td><td>'+e.mode+'</td><td class="amt rd">'+formatINR(e.amount)+'</td></tr>';});h+='</table>';
  h+='<div class="sec">FUND POSITION</div><table><tr><th>Account</th><th style="text-align:right">Opening</th><th style="text-align:right">IN</th><th style="text-align:right">OUT</th><th style="text-align:right">Closing</th><th>Status</th></tr>';
  var _usable=0,_unusable=0;
  data.fundPosition.forEach(function(a){
    var _isUsable=/^usable$/i.test((a.status||'').toString().trim());
    if(_isUsable)_usable+=(a.closing||0);else _unusable+=(a.closing||0);
    h+='<tr><td>'+a.bankAC+'</td><td class="amt">'+formatINR(a.opening)+'</td><td class="amt gn">'+formatINR(a.todayIn)+'</td><td class="amt rd">'+formatINR(a.todayOut)+'</td><td class="amt">'+formatINR(a.closing)+'</td><td>'+(a.status||'')+'</td></tr>';
  });
  h+='<tr class="tot"><td colspan="4">TOTAL USABLE (closing)</td><td class="amt usb">'+formatINR(_usable)+'</td><td>Usable</td></tr>';
  h+='<tr class="tot"><td colspan="4">TOTAL UNUSABLE (closing)</td><td class="amt unusb">'+formatINR(_unusable)+'</td><td>Blocked</td></tr>';
  h+='</table></body></html>';
  return h;
}
// ── v2.10.0-s5.2: approval catch-up (record-only) ─────────────────────────────
// One-off reconciliation for the dropped-verdict era: scans approval-group history,
// finds every bare ok/yes/no/hold that M or S swiped DIRECTLY onto an EXPENSE REQUEST
// (the taps the pre-s5.20 code threw away), and records them. Dry-run by default —
// writes ONLY when commit===true. Record-only: writes verdicts + approved events, NEVER
// posts to the outflow group and NEVER touches the Ledger. Idempotent (event store
// dedupes by key; verdict-store writes are last-write-wins for the same verdict).
async function buildVerdictBackfill(days, commit){
  var messages = await fetchApprovalMessages(days||30);
  messages.sort(function(a,b){ return a.timestamp - b.timestamp; }); // oldest first
  var byItem = {};
  for(var i=0;i<messages.length;i++){
    var m = messages[i];
    if(!m.hasQuotedMsg) continue;
    var author = m.author||m.from||'';
    var role = author.indexOf(CONFIG.MM_PHONE)===0 ? 'mm' : (author.indexOf(CONFIG.SM_PHONE)===0 ? 'sm' : null);
    if(!role){ try{ var who=await identifySender(author); if(who&&(who.role==='mm'||who.role==='sm')) role=who.role; }catch(e){} }
    if(!role) continue;
    var body=(m.body||'').trim(); if(!body) continue;
    var bare=body.toLowerCase().trim();
    var v = /^(yes|ok|okay|approved?|approve|done|haan|theek|\u{1F44D}|\u2705)$/u.test(bare) ? 'yes'
          : /^(no|reject(ed)?|nahi)$/.test(bare) ? 'no'
          : /^hold$/.test(bare) ? 'hold' : null;
    if(!v) continue;
    var qBody='', qId=null;
    try{ var q=await m.getQuotedMessage(); qBody=(q&&q.body)||''; qId=q&&(q.id._serialized||q.id.id); }catch(e){}
    if(!qId || !/^\s*\*?EXPENSE REQUEST/i.test(qBody)) continue;
    var dm=qBody.match(/Details:\s*(.+)/i); var lbl=dm?dm[1].trim():'(expense)';
    var am=qBody.match(/Amount:\s*rs\.?\s*([\d,]+)/i); var amt=am?parseAmount(am[1]):0;
    if(!byItem[qId]) byItem[qId]={label:lbl, amount:amt, mm:null, sm:null};
    byItem[qId].label=lbl; byItem[qId].amount=amt;
    byItem[qId][role]={verdict:v, at:new Date(m.timestamp*1000).toISOString()}; // latest wins
  }
  var store = loadVerdicts();
  var es = loadEventStore();
  var approvedSet = {}; es.events.forEach(function(e){ if(e.type==='approved'&&e.itemId) approvedSet[e.itemId]=true; });
  var isYes=function(x){ return x==='yes'||x==='amend'; };
  var preview={ scannedMessages:messages.length, days:(days||30), itemsWithVerdicts:Object.keys(byItem).length,
                wouldRecordVerdicts:0, alreadyRecorded:0, items:[], wouldApprove:[] };
  Object.keys(byItem).forEach(function(id){
    var b=byItem[id], ex=store[id]||{};
    ['mm','sm'].forEach(function(r){
      if(!b[r]) return;
      if(ex[r] && ex[r].verdict===b[r].verdict) preview.alreadyRecorded++; else preview.wouldRecordVerdicts++;
    });
    var mmV=(b.mm&&b.mm.verdict)||(ex.mm&&ex.mm.verdict)||null;
    var smV=(b.sm&&b.sm.verdict)||(ex.sm&&ex.sm.verdict)||null;
    var both=isYes(mmV)&&isYes(smV);
    var newly=both && !approvedSet[id];
    preview.items.push({ itemId:id, label:b.label, amount:b.amount, mm:mmV, sm:smV, bothYes:both, newlyApproved:newly });
    if(newly) preview.wouldApprove.push({ label:b.label, amount:b.amount });
  });
  preview.wouldApproveCount = preview.wouldApprove.length;
  if(commit){
    var committed={ verdicts:0, approved:0 };
    Object.keys(byItem).forEach(function(id){
      var b=byItem[id];
      if(!store[id]) store[id]={};
      store[id]._label=b.label; store[id]._amount=b.amount;
      ['mm','sm'].forEach(function(r){
        if(!b[r]) return;
        store[id][r]={ verdict:b[r].verdict, amount:0, raw:'[backfill]', at:b[r].at };
        recordVerdictEvent(id, b.label, b.amount, r, b[r].verdict, 0, '[backfill]');
        committed.verdicts++;
      });
      var mmV2=store[id].mm&&store[id].mm.verdict, smV2=store[id].sm&&store[id].sm.verdict;
      if(isYes(mmV2)&&isYes(smV2) && !approvedSet[id]){ recordApprovedEvent(id, b.label, b.amount); approvedSet[id]=true; committed.approved++; }
    });
    saveVerdicts(store);
    preview.committed=committed;
    preview.note='Recorded only — no outflow posting, no Ledger write. Re-running is safe (idempotent).';
  } else {
    preview.note='DRY-RUN — nothing written. Add &commit=1 (via the Commit button) to record.';
  }
  return preview;
}
// ── v2.11.0-s6.1: one-time Payable-code back-fill for existing approved events ──
// Assigns P-YYMMDD-NNN to every approved event that has no code yet, in seq order, using
// each event's recorded approval date (`at`). Dry-run by default; commit mutates + persists.
// Idempotent: already-coded events are skipped, so re-running adds nothing.
// NOTE: the 30 historical approvals were recorded by the s5.20 catch-up on 20 Jun, so their
// `at` is 20 Jun — they will batch as P-260620-NNN. New approvals from now carry their true date.
function buildPayableCodeBackfill(commit){
  var store=loadEventStore();
  var approved=(store.events||[]).filter(function(e){ return e.type==='approved'; }).sort(function(a,b){ return a.seq-b.seq; });
  var toCode=approved.filter(function(e){ return !e.code; });
  var assigned=[];
  if(commit){
    toCode.forEach(function(e){
      var code=mintPayableCode(Date.parse(e.at)||Date.now(), store);   // reads codes already set this pass
      e.code=code; assigned.push({ itemId:e.itemId, label:e.label, code:code });
    });
    if(toCode.length) _persistEventStore();
  } else {
    var sim={};
    toCode.forEach(function(e){
      var d=new Date((Date.parse(e.at)||Date.now())+5.5*3600000);
      var pfx='P-'+String(d.getUTCFullYear()).slice(-2)+('0'+(d.getUTCMonth()+1)).slice(-2)+('0'+d.getUTCDate()).slice(-2)+'-';
      if(sim[pfx]==null){ var mx=0; store.events.forEach(function(x){ if(x.type==='approved'&&x.code&&x.code.indexOf(pfx)===0){ var n=parseInt(x.code.slice(pfx.length),10); if(!isNaN(n)&&n>mx)mx=n; } }); sim[pfx]=mx; }
      sim[pfx]++; assigned.push({ itemId:e.itemId, label:e.label, code:pfx+('00'+sim[pfx]).slice(-3) });
    });
  }
  return { totalApproved:approved.length, alreadyCoded:approved.length-toCode.length, toAssign:toCode.length, committed:!!commit, assigned:assigned };
}
// ── Endpoints ─────────────────────────────────────────────────────────────────
app.get('/health',function(req,res){res.json({status:'ok',version:'2.11.0-s6.8',whatsapp:waReady?'connected':'disconnected',sheets:sheetsApi?'initialized':'not configured',botEnabled:CONFIG.BOT_ENABLED,visionEnabled:CONFIG.CLAUDE_API_KEY?true:false,visionCacheSize:visionCache.size,reverseScanWindowDays:REVERSE_SCAN_WINDOW_DAYS,reverseScanMinAmount:REVERSE_SCAN_MIN_AMOUNT});});
// ── v2.8.18 endpoint lock: Basic Auth on all /api/* (/health stays open) ──────
var _crypto = require('crypto');
var PANEL_USER = process.env.PANEL_USER || '';
var PANEL_PASSWORD = process.env.PANEL_PASSWORD || '';
if (!PANEL_USER || !PANEL_PASSWORD) {
  console.warn('[AUTH] WARNING: PANEL_USER/PANEL_PASSWORD not set — all /api/* endpoints are LOCKED (fail-closed). Set them in Railway env vars.');
}
// per-IP failed-attempt tracker (in-memory; resets on restart)
var _authFails = {};
var AUTH_MAX_FAILS = 5;
var AUTH_LOCK_MS = 5 * 60 * 1000; // 5 min lockout after MAX_FAILS
function _safeEq(a, b) {
  var ba = Buffer.from(String(a)); var bb = Buffer.from(String(b));
  if (ba.length !== bb.length) return false;
  try { return _crypto.timingSafeEqual(ba, bb); } catch (e) { return false; }
}
function authGate(req, res, next) {
  var ip = (req.headers['x-forwarded-for'] || req.ip || req.connection.remoteAddress || 'unknown').split(',')[0].trim();
  var now = Date.now();
  var rec = _authFails[ip];
  if (rec && rec.lockUntil && now < rec.lockUntil) {
    var secs = Math.ceil((rec.lockUntil - now) / 1000);
    res.set('Retry-After', String(secs));
    return res.status(429).json({ error: 'Too many failed attempts. Locked for ' + secs + 's.' });
  }
  // read live from process.env at request time (matches /health which works)
  var EU = process.env.PANEL_USER || '';
  var EP = process.env.PANEL_PASSWORD || '';
  // fail closed if not configured
  if (!EU || !EP) {
    res.set('WWW-Authenticate', 'Basic realm="Fidato MIS", charset="UTF-8"');
    return res.status(503).json({ error: 'Auth not configured on server.' });
  }
  var hdr = req.headers.authorization || '';
  var m = /^Basic\s+(.+)$/i.exec(hdr);
  if (m) {
    var decoded = '';
    try { decoded = Buffer.from(m[1], 'base64').toString('utf8'); } catch (e) { decoded = ''; }
    var idx = decoded.indexOf(':');
    var u = idx >= 0 ? decoded.slice(0, idx) : '';
    var p = idx >= 0 ? decoded.slice(idx + 1) : '';
    var okU = _safeEq(u, EU);
    var okP = _safeEq(p, EP);
    if (okU && okP) {
      if (_authFails[ip]) delete _authFails[ip]; // clear on success
      return next();
    }
    // failed credentials
    rec = _authFails[ip] || { count: 0, lockUntil: 0 };
    rec.count += 1;
    if (rec.count >= AUTH_MAX_FAILS) { rec.lockUntil = now + AUTH_LOCK_MS; rec.count = 0; }
    _authFails[ip] = rec;
  }
  res.set('WWW-Authenticate', 'Basic realm="Fidato MIS", charset="UTF-8"');
  return res.status(401).json({ error: 'Authentication required.' });
}
// gate everything below this line (all /api/*). /health is registered ABOVE and stays open.
app.use('/api', authGate);

// ── v2.8.19 master control panel (served at /api/panel, behind the lock) ──────
var PANEL_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Fidato MIS — Control Panel</title>
<style>
  :root{
    --navy:#191C3C; --terra:#9F5355; --bg:#f4f5f7; --card:#ffffff;
    --line:#e3e5ea; --ink:#1d2030; --muted:#6b7080; --ok:#1f9d55; --okbg:#e8f6ee;
    --warn:#9a6b00; --warnbg:#fbf3e0; --danger:#b3261e; --dangerbg:#fbeae9; --info:#2456b3; --infobg:#e9f0fb;
  }
  *{box-sizing:border-box}
  body{margin:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;background:var(--bg);color:var(--ink);}
  header{background:var(--navy);color:#fff;padding:14px 20px;display:flex;align-items:center;justify-content:space-between;}
  header .l{display:flex;align-items:center;gap:10px;}
  header .dot{width:9px;height:9px;border-radius:50%;background:#7fdca4;display:inline-block;}
  header h1{font-size:15px;font-weight:600;margin:0;}
  header .sub{font-size:12px;opacity:.7;margin:0;}
  .wrap{max-width:880px;margin:0 auto;padding:18px;}
  .tabs{display:flex;gap:6px;flex-wrap:wrap;border-bottom:1px solid var(--line);padding-bottom:12px;margin-bottom:16px;}
  .tab{font-size:13px;padding:7px 13px;border-radius:8px;cursor:pointer;color:var(--muted);border:1px solid transparent;user-select:none;background:none;}
  .tab:hover{background:#eceef2;}
  .tab.active{background:var(--infobg);color:var(--info);font-weight:600;}
  .tab.dz.active{background:var(--dangerbg);color:var(--danger);}
  .cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:16px;}
  .card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px;}
  .card .k{font-size:12px;color:var(--muted);margin:0 0 5px;}
  .card .v{font-size:20px;font-weight:600;margin:0;}
  .btn{display:flex;align-items:center;gap:10px;width:100%;text-align:left;padding:12px 14px;font-size:14px;background:var(--card);border:1px solid var(--line);border-radius:9px;cursor:pointer;color:var(--ink);margin-bottom:9px;transition:background .12s;}
  .btn:hover{background:#f0f2f6;}
  .btn .ico{font-size:16px;width:20px;text-align:center;}
  .btn.dz{color:var(--danger);border-color:#ecc7c5;}
  .btn.act{color:var(--info);}
  .note{font-size:12px;color:var(--muted);margin:8px 2px 18px;line-height:1.6;}
  .out{background:#0f1117;color:#d7dbe6;border-radius:9px;padding:14px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;white-space:pre-wrap;word-break:break-word;max-height:340px;overflow:auto;margin-top:6px;display:none;}
  .out.show{display:block;}
  .out .lbl{color:#7fa7ff;display:block;margin-bottom:6px;}
  .modal{position:fixed;inset:0;background:rgba(10,12,20,.45);display:none;align-items:center;justify-content:center;padding:20px;z-index:50;}
  .modal.show{display:flex;}
  .modal .box{background:#fff;border-radius:12px;max-width:380px;width:100%;padding:20px;}
  .modal h3{margin:0 0 8px;font-size:16px;}
  .modal p{margin:0 0 16px;font-size:13px;color:var(--muted);line-height:1.6;}
  .modal .row{display:flex;gap:8px;justify-content:flex-end;}
  .modal button{font-size:13px;padding:8px 16px;border-radius:8px;border:1px solid var(--line);background:#fff;cursor:pointer;}
  .modal button.go{background:var(--danger);color:#fff;border-color:var(--danger);}
  .modal button.goact{background:var(--info);color:#fff;border-color:var(--info);}
  .spin{display:inline-block;width:13px;height:13px;border:2px solid #ccd;border-top-color:var(--info);border-radius:50%;animation:sp .7s linear infinite;vertical-align:middle;}
  @keyframes sp{to{transform:rotate(360deg)}}
</style>
</head>
<body>
<header>
  <div class="l">
    <span class="dot" id="hdot"></span>
    <div><h1>Fidato MIS — Control Panel</h1><p class="sub" id="hsub">loading…</p></div>
  </div>
  <div style="font-size:12px;opacity:.8;">v<span id="hver">—</span></div>
</header>
<div class="wrap">
  <div class="tabs" id="tabs"></div>
  <div class="cards" id="cards"></div>
  <div id="buttons"></div>
  <div class="out" id="out"></div>
</div>

<div class="modal" id="modal">
  <div class="box">
    <h3 id="mTitle">Are you sure?</h3>
    <p id="mBody"></p>
    <div class="row">
      <button onclick="closeModal()">Cancel</button>
      <button id="mGo" class="goact" onclick="confirmModal()">Confirm</button>
    </div>
  </div>
</div>

<script>
// ---- endpoint catalogue ----
var TABS = [
  {id:'approvals', label:'Approvals', items:[
    {ep:'/api/approval-audit?days=3', ico:'\\uD83D\\uDC41', label:'View pending & recent approvals'},
    {ep:'/api/reminder-digest-preview', ico:'\\uD83D\\uDD14', label:'Preview the digest (no send)'},
    {ep:'/api/reminder-digest-send', ico:'\\uD83D\\uDCE4', label:'Send digest to approval group', act:true, confirm:'This posts the approval digest to your WhatsApp approval group.'},
    {ep:'/api/send-reminders', ico:'\\u23F0', label:'Send reminders now', act:true, confirm:'This sends reminder messages on WhatsApp.'},
    {ep:'/api/approval-backfill?days=30', ico:'🔍', label:'Catch-up — preview dropped approvals (dry-run)'},
    {ep:'/api/approval-backfill?days=30&commit=1', ico:'🔁', label:'Catch-up — record approvals (record-only)', act:true, confirm:'Record all M/S approvals found on past EXPENSE REQUESTs? Writes verdicts + approved events only — no outflow posting, no Ledger write. Safe to re-run.'}
  ]},
  {id:'reports', label:'Reports', items:[
    {ep:'/api/eod-preview', ico:'\\uD83D\\uDCC4', label:'Preview tonight\\u2019s EOD report'},
    {ep:'/api/eod-image', ico:'\\uD83D\\uDDBC', label:'EOD as an image'},
    {ep:'/api/eod-send', ico:'\\uD83D\\uDCE4', label:'Send EOD report now', act:true, confirm:'This sends the EOD report on WhatsApp.'},
    {ep:'/api/daily-report', ico:'\\uD83D\\uDCCA', label:'Daily report', act:true, confirm:'This may post the daily report to WhatsApp.'},
    {ep:'/api/report-status', ico:'\\u2139', label:'Report status'}
  ]},
  {id:'recon', label:'Reconciliation', items:[
    {ep:'/api/reconciliation', ico:'\\u2696', label:'Run reconciliation'},
    {ep:'/api/fund-position', ico:'\\uD83D\\uDCB0', label:'Fund position'},
    {ep:'/api/ledger', ico:'\\uD83D\\uDCD2', label:'Ledger view'},
    {ep:'/api/outliers', ico:'\\u26A0', label:'Outliers'},
    {ep:'/api/matcher-stats', ico:'\\uD83D\\uDCC8', label:'Matcher stats'}
  ]},
  {id:'controls', label:'Controls', items:[
    {ep:'/api/bot/on', ico:'\\u2705', label:'Turn bot ON', act:true, confirm:'Enable the bot?'},
    {ep:'/api/bot/off', ico:'\\u23F8', label:'Turn bot OFF', act:true, confirm:'Disable the bot? It will stop posting and processing.'},
    {ep:'/api/silent-status', ico:'\\u2139', label:'Silent mode status'},
    {ep:'/api/silent-on', ico:'\\uD83D\\uDD07', label:'Silent mode ON', act:true, confirm:'Turn on silent mode (bot stops scanning/posting)?'},
    {ep:'/api/silent-off', ico:'\\uD83D\\uDD0A', label:'Silent mode OFF', act:true, confirm:'Turn off silent mode?'},
    {ep:'/api/unapproved-alert-toggle', ico:'\\uD83D\\uDD14', label:'Toggle unapproved alert', act:true, confirm:'Toggle the unapproved-alert setting?'},
    {ep:'/api/outflow-post-status', ico:'\\u2139', label:'Outflow posting status'},
    {ep:'/api/outflow-post-on', ico:'\\uD83D\\uDE80', label:'Outflow posting ON (go live)', act:true, confirm:'Start auto-posting approved items into the payments group for accountants to mark paid? (Still capture-only — no Sheet write.)'},
    {ep:'/api/outflow-post-off', ico:'\\u23F9', label:'Outflow posting OFF', act:true, confirm:'Stop posting approved items to the payments group?'},
    {ep:'/api/outflow-post-dummy?label=Test%20payment%20due&amount=111190', ico:'\\uD83E\\uDDEA', label:'Post a ⟨TEST⟩ payment-due (dummy)', act:true, confirm:'Post a ⟨TEST⟩-tagged dummy PAYMENT DUE item into the payments group? It is clearly marked test and safe to use freely (it posts even when outflow posting is OFF). Reply paid on it, or type summary in the group, to exercise the flow.'},
    {ep:'/api/ledger-write-status', ico:'\\uD83D\\uDCD2', label:'Ledger write status'},
    {ep:'/api/ledger-dryrun-on', ico:'\\uD83E\\uDDEA', label:'Ledger dry-run ON (rehearsal)', act:true, confirm:'Turn ledger dry-run ON? The bot will log the row it would write but write nothing — this also pauses any real writes.'},
    {ep:'/api/ledger-dryrun-off', ico:'\\u270D', label:'Ledger dry-run OFF', act:true, confirm:'Turn ledger dry-run OFF? If LEDGER_WRITE_ENABLED is on, confirmed rows will then be written to the actual Sheet.'},
    {ep:'/api/outflow-queue', ico:'\\uD83D\\uDCE4', label:'Approved → payments queue', link:true},
    {ep:'/api/outflow-log', ico:'\\uD83D\\uDCCB', label:'Payments log (posted + paid)'}
  ]},
  {id:'debug', label:'Debug', items:[
    {ep:'/api/wa-status', ico:'\\uD83D\\uDCF1', label:'WhatsApp status'},
    {ep:'/api/groups', ico:'\\uD83D\\uDC65', label:'Groups & JIDs'},
    {ep:'/api/debug-messages', ico:'\\uD83D\\uDCAC', label:'Recent messages'},
    {ep:'/api/debug-replies', ico:'\\u21A9', label:'Recent replies'},
    {ep:'/api/debug-verdict', ico:'\\u2696', label:'Verdict store'},
    {ep:'/api/dm-state', ico:'\\u2709', label:'DM state'},
    {ep:'/api/whoami', ico:'\\uD83D\\uDC64', label:'Who am I'},
    {ep:'/api/auth-list', ico:'\\uD83D\\uDD11', label:'Auth list'},
    {ep:'/api/stale-state', ico:'\\uD83D\\uDD52', label:'Stale state'},
    {ep:'/api/match-cache', ico:'\\uD83D\\uDDC2', label:'Match cache'}
  ]},
  {id:'dz', label:'Danger zone', dz:true, items:[
    {ep:'/api/wa-reset', ico:'\\uD83D\\uDD04', label:'Reset WhatsApp session (re-pair)', dz:true, confirm:'This LOGS THE BOT OUT of WhatsApp. You will have to re-scan the QR to reconnect. This cannot be undone.'},
    {ep:'/api/dm-clear', ico:'\\uD83D\\uDDD1', label:'Clear DM state', dz:true, confirm:'This wipes the bot\\u2019s DM state. This cannot be undone.'},
    {ep:'/api/match-cache-clear', ico:'\\uD83E\\uDDF9', label:'Clear match cache', dz:true, confirm:'This clears the reconciliation match cache. This cannot be undone.'},
    {ep:'/api/stale-reset', ico:'\\u267B', label:'Reset stale scan', dz:true, confirm:'This resets the stale-scan state. This cannot be undone.'}
  ]}
];

var active = 'approvals';
var pendingEp = null, pendingDz = false;

function el(id){return document.getElementById(id);}
function renderTabs(){
  var h='';
  TABS.forEach(function(t){
    h += '<button class="tab'+(t.dz?' dz':'')+(t.id===active?' active':'')+'" onclick="switchTab(\\''+t.id+'\\')">'+t.label+'</button>';
  });
  el('tabs').innerHTML=h;
}
function switchTab(id){ active=id; el('out').classList.remove('show'); renderTabs(); renderButtons(); }
function renderButtons(){
  var t = TABS.filter(function(x){return x.id===active;})[0];
  var h='';
  t.items.forEach(function(it,i){
    var cls='btn'+(it.dz?' dz':(it.act?' act':''));
    if(it.link){ h += '<button class="'+cls+'" onclick="window.open(\\''+it.ep+'\\',\\'_blank\\')"><span class="ico">'+it.ico+'</span>'+it.label+'</button>'; }
    else { h += '<button class="'+cls+'" onclick="hit(\\''+active+'\\','+i+')"><span class="ico">'+it.ico+'</span>'+it.label+'</button>'; }
  });
  if(t.dz){ h += '<p class="note">Every action here is destructive and asks you to confirm twice.</p>'; }
  else if(t.items.some(function(x){return x.act;})){ h += '<p class="note">Actions marked in blue post to WhatsApp or change settings — they ask you to confirm first.</p>'; }
  el('buttons').innerHTML=h;
}
var confirmsLeft = 0;
function hit(tabId,i){
  var t = TABS.filter(function(x){return x.id===tabId;})[0];
  var it = t.items[i];
  if(it.confirm){
    pendingEp = it.ep; pendingDz = !!it.dz;
    confirmsLeft = it.dz ? 2 : 1;   // danger zone needs two confirms
    el('mTitle').textContent = it.dz ? 'Confirm — destructive' : 'Are you sure?';
    el('mBody').textContent = it.confirm;
    var go = el('mGo');
    go.className = it.dz ? 'go' : 'goact';
    go.textContent = it.dz ? 'Yes, do it' : 'Confirm';
    el('modal').classList.add('show');
  } else {
    run(it.ep);
  }
}
function closeModal(){ el('modal').classList.remove('show'); pendingEp=null; confirmsLeft=0; }
function confirmModal(){
  confirmsLeft--;
  if(confirmsLeft > 0){
    // show the final confirmation step (danger zone)
    el('mTitle').textContent = 'Final confirmation';
    el('mBody').textContent = 'Last chance — are you absolutely sure? This cannot be undone.';
    var go = el('mGo'); go.className='go'; go.textContent='Yes, I am sure';
    return; // modal stays open for the second click
  }
  var ep = pendingEp;
  el('modal').classList.remove('show');
  pendingEp=null;
  if(ep) run(ep);
}

var lastRaw = null;
function run(ep){
  var out = el('out');
  out.classList.add('show');
  out.innerHTML = '<span class="lbl">'+ep+'</span><span class="spin"></span> running…';
  fetch(ep, {credentials:'same-origin'})
    .then(function(r){ return r.text().then(function(tx){ return {ok:r.ok, status:r.status, tx:tx, ct:r.headers.get('content-type')||''}; }); })
    .then(function(res){
      var data = null;
      try { data = JSON.parse(res.tx); } catch(e){}
      lastRaw = res.tx;
      var head = '<span class="lbl">'+ep+'  \\u2192  HTTP '+res.status+'  <a href="#" onclick="showRaw(event)" style="color:#9ab;float:right;">raw</a></span>';
      if(data && typeof data === 'object'){
        out.innerHTML = head + '<div id="pretty">'+formatResult(ep, data)+'</div>';
      } else {
        // HTML or non-JSON (e.g. report previews) — offer to open in a tab
        out.innerHTML = head + '<div style="color:#bcc;">This endpoint returned a page, not data. <a href="'+ep+'" target="_blank" style="color:#7fa7ff;">Open it in a new tab</a>.</div>';
      }
    })
    .catch(function(err){
      out.innerHTML = '<span class="lbl">'+ep+'</span>Error: '+escapeHtml(String(err));
    });
}
function showRaw(e){ e.preventDefault(); var p=document.getElementById('pretty'); if(p){ p.outerHTML='<pre id="pretty" style="margin:0;white-space:pre-wrap;">'+escapeHtml(JSON.stringify(JSON.parse(lastRaw),null,2))+'</pre>'; } }

function fmtMoney(n){ if(n==null||isNaN(n)) return ''; return '\\u20B9'+Number(n).toLocaleString('en-IN'); }
function row(cells, isHead){ var tag=isHead?'th':'td'; var st=isHead?'text-align:left;color:#9ab;font-weight:600;border-bottom:1px solid #2a2f3a;':'border-bottom:1px solid #1c2129;'; return '<tr>'+cells.map(function(c){return '<'+tag+' style="padding:5px 10px 5px 0;'+st+'">'+c+'</'+tag+'>';}).join('')+'</tr>'; }
function table(rows){ return '<table style="width:100%;border-collapse:collapse;font-size:12px;">'+rows.join('')+'</table>'; }

function formatResult(ep, d){
  if(d.error){ return '<div style="color:#ff9b9b;">Error: '+escapeHtml(String(d.error))+'</div>'; }

  // approval audit
  if(ep.indexOf('/api/approval-audit')===0 && d.summary){
    var s=d.summary, h='';
    h += '<div style="margin-bottom:10px;">'+
         pill('Approved', s.fullyApproved, '#1f9d55') + pill('Partial', s.partialApproval, '#9a6b00') +
         pill('Pending', s.noApproval, '#b3261e') + pill('On hold', s.onHold, '#6b7080') +
         pill('Rejected', s.rejected, '#6b7080') + ' <span style="color:#8893a5;">of '+s.totalExpenseRequests+' total · '+s.period+'</span></div>';
    function section(title, arr, color){
      if(!arr || !arr.length) return '';
      var rows=[row(['Date','Vendor / details','Amount','M','S'],true)];
      arr.forEach(function(e){
        var who=e.message? escapeHtml(e.vendor||e.message.slice(0,48)) : '';
        rows.push(row([e.date||'', who, e.amountFormatted||fmtMoney(e.amount), tick(e.mm), tick(e.sm)]));
      });
      return '<div style="margin:10px 0 4px;color:'+color+';font-weight:600;">'+title+' ('+arr.length+')</div>'+table(rows);
    }
    h += section('Pending', d.noApproval, '#ff9b9b');
    h += section('Partial', d.partialApproval, '#e0b057');
    h += section('Fully approved', d.fullyApproved, '#7fdca4');
    if(d.onHold&&d.onHold.length) h += section('On hold', d.onHold, '#9aa4b5');
    if(d.rejected&&d.rejected.length) h += section('Rejected', d.rejected, '#9aa4b5');
    return h;
  }

  // reconciliation
  if(ep.indexOf('/api/reconciliation')===0){
    var rows=[];
    Object.keys(d).forEach(function(k){
      var v=d[k];
      if(Array.isArray(v)){ rows.push(row([escapeHtml(k), v.length+' items']));}
      else if(typeof v==='object' && v){ rows.push(row([escapeHtml(k), escapeHtml(JSON.stringify(v))]));}
      else { rows.push(row([escapeHtml(k), escapeHtml(String(v))])); }
    });
    return table([row(['Field','Value'],true)].concat(rows));
  }

  // fund position
  if(ep.indexOf('/api/fund-position')===0 && d.accounts){
    var rows=[row(['Account','Balance'],true)];
    (Array.isArray(d.accounts)?d.accounts:Object.keys(d.accounts).map(function(k){return{name:k,balance:d.accounts[k]};})).forEach(function(a){
      rows.push(row([escapeHtml(a.name||a.account||''), fmtMoney(a.balance!=null?a.balance:a.amount)]));
    });
    return table(rows);
  }

  // ledger
  if(ep.indexOf('/api/ledger')===0 && d.entries){
    var hdr = '<div style="margin-bottom:8px;">'+pill('In', fmtMoney(d.totalIn), '#1f9d55')+pill('Out', fmtMoney(d.totalOut), '#b3261e')+pill('Net', fmtMoney(d.net), '#2456b3')+' <span style="color:#8893a5;">'+(d.count||0)+' entries · '+(d.date||'')+'</span></div>';
    var rows=[row(['Entity','Description','In/Out','Amount'],true)];
    d.entries.slice(0,100).forEach(function(e){
      rows.push(row([escapeHtml(e.entity||''), escapeHtml((e.description||'').slice(0,50)), e.inOut||'', fmtMoney(e.amount)]));
    });
    return hdr+table(rows);
  }

  // generic object → key/value table
  var rows=[row(['Field','Value'],true)];
  Object.keys(d).forEach(function(k){
    var v=d[k];
    var disp = (v==null)?'' : (typeof v==='object' ? (Array.isArray(v)? v.length+' items' : escapeHtml(JSON.stringify(v).slice(0,120))) : escapeHtml(String(v)));
    rows.push(row([escapeHtml(k), disp]));
  });
  return table(rows);
}
function pill(label, val, color){ return '<span style="display:inline-block;background:'+color+'22;color:'+color+';border-radius:6px;padding:2px 9px;margin-right:6px;font-size:12px;">'+label+': <b>'+(val!=null?val:'\\u2014')+'</b></span>'; }
function tick(v){ if(v===true||v==='approved'||v==='yes') return '<span style="color:#7fdca4;">\\u2713</span>'; if(v===false||v==null||v==='') return '<span style="color:#6b7080;">\\u2014</span>'; return '<span style="color:#e0b057;">'+escapeHtml(String(v))+'</span>'; }
function escapeHtml(s){ return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

// load header status + stat cards from /health (open) and approval-audit
function loadStatus(){
  fetch('/health',{credentials:'same-origin'}).then(function(r){return r.json();}).then(function(h){
    el('hver').textContent = h.version || '—';
    el('hsub').textContent = 'WhatsApp '+(h.whatsapp||'?')+' · sheets '+(h.sheets||'?')+' · bot '+(h.botEnabled?'on':'off');
    el('hdot').style.background = (h.whatsapp==='connected') ? '#7fdca4' : '#e0a96d';
  }).catch(function(){});
  fetch('/api/approval-audit?days=3',{credentials:'same-origin'}).then(function(r){return r.ok?r.json():null;}).then(function(a){
    if(!a) return;
    var s = a.summary || {};
    var pending = s.noApproval!=null ? s.noApproval : 0;
    var approved = s.fullyApproved!=null ? s.fullyApproved : 0;
    var total = s.totalExpenseRequests!=null ? s.totalExpenseRequests : 0;
    el('cards').innerHTML =
      card('Pending approval', pending) +
      card('Fully approved', approved) +
      card('Total requests', total);
  }).catch(function(){});
}
function card(k,v){ return '<div class="card"><p class="k">'+k+'</p><p class="v">'+v+'</p></div>'; }

renderTabs(); renderButtons(); loadStatus();
</script>
</body>
</html>
`;
app.get('/api/panel', function(req, res){ res.type('html').send(PANEL_HTML); });
var OUTFLOW_QUEUE_HTML = `<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Approved → Payments Queue</title>
<style>
  body{background:#0f1115;color:#e6e9ef;font-family:-apple-system,Segoe UI,Roboto,sans-serif;margin:0;padding:16px;}
  h1{font-size:17px;margin:0 0 4px;} .sub{color:#8b93a3;font-size:12px;margin-bottom:14px;}
  .bar{display:flex;gap:8px;align-items:center;margin-bottom:14px;flex-wrap:wrap;}
  button{background:#1c2230;color:#e6e9ef;border:1px solid #2a3242;border-radius:8px;padding:8px 12px;font-size:13px;cursor:pointer;}
  button:hover{background:#242c3d;} button:disabled{opacity:.4;cursor:default;}
  .all{background:#1f3a5f;border-color:#34507a;}
  table{width:100%;border-collapse:collapse;font-size:13px;}
  th,td{text-align:left;padding:8px 8px;border-bottom:1px solid #1c2129;vertical-align:top;}
  th{color:#8b93a3;font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:.04em;}
  .amt{text-align:right;white-space:nowrap;} .st{font-size:11px;padding:2px 7px;border-radius:20px;}
  .pending{background:#3a2f12;color:#e5c97a;} .posted{background:#16324a;color:#7fb0e0;} .paid{background:#16402a;color:#76c596;}
  .part-paid{background:#3a2f12;color:#f0c66b;} .closed{background:#3a1f1f;color:#d98a8a;} .approved{background:#2a2342;color:#b9a6e6;}
  .push{background:#1f3a5f;border-color:#34507a;color:#cfe3ff;padding:5px 10px;font-size:12px;}
  .msg{margin:10px 0;padding:8px 10px;border-radius:8px;font-size:12px;display:none;}
  .ok{background:#16321f;color:#8fd6a6;} .err{background:#3a1c1c;color:#e09a9a;}
  .empty{color:#8b93a3;padding:20px 0;}
</style></head>
<body>
  <h1>Approved → Payments queue</h1>
  <div class="sub">Items approved by M+S that have no matching Ledger payment yet (sourced from your approval history, not the event store). Push one into the payments group, or run a one-time catch-up for everything not yet posted. Anything already paid in the Ledger is excluded automatically.</div>
  <div class="bar">
    <button class="all" id="allBtn" onclick="catchUp()">Push all pending (catch-up)</button>
    <button onclick="load()">Refresh</button>
    <span id="count" class="sub" style="margin:0;"></span>
  </div>
  <div id="msg" class="msg"></div>
  <div id="tbl"><div class="empty">Loading…</div></div>

  <h1 style="margin-top:26px;">Payments log — pushed to the group</h1>
  <div class="sub">Everything posted to the payments group, with paid status. While testing you can mark a paid item back to <b>unpaid</b>, or <b>delete</b> a pushed item (which also deletes its message in the group).</div>
  <div class="bar"><button onclick="loadLog()">Refresh log</button><span id="logcount" class="sub" style="margin:0;"></span></div>
  <div id="logtbl"><div class="empty">Loading…</div></div>
<script>
function esc(s){ return String(s==null?'':s).replace(/[&<>"]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c];}); }
function inr(n){ if(n==null||isNaN(n))return ''; return '\\u20B9'+Number(n).toLocaleString('en-IN'); }
function flash(t,ok){ var m=document.getElementById('msg'); m.textContent=t; m.className='msg '+(ok?'ok':'err'); m.style.display='block'; }
function load(){
  var tbl=document.getElementById('tbl');
  tbl.innerHTML='<div class="empty">Reading approval history… this can take a few seconds.</div>';
  var ctrl=new AbortController(); var to=setTimeout(function(){ctrl.abort();},90000);
  fetch('/api/outflow-pending',{credentials:'same-origin',signal:ctrl.signal}).then(function(r){return r.json();}).then(function(d){
    clearTimeout(to);
    if(d&&d.error){ tbl.innerHTML='<div class="empty">Couldn\\'t load: '+esc(d.error)+'</div>'; flash('Error: '+d.error,false); return; }
    var items=(d&&d.items)||[]; var pend=items.filter(function(x){return x.status==='pending';}).length;
    document.getElementById('count').textContent=items.length+' approved \\u00b7 '+pend+' pending';
    document.getElementById('allBtn').disabled = pend===0;
    if(!items.length){ tbl.innerHTML='<div class="empty">No approved items found in the last 15 days. (Add ?days=30 to the URL to look further back.)</div>'; return; }
    var h='<table><tr><th>Approved</th><th>Description</th><th class="amt">Amount</th><th>Status</th><th></th></tr>';
    items.forEach(function(it){
      var when=it.approvedAt?new Date(it.approvedAt).toLocaleDateString('en-IN',{day:'2-digit',month:'short'}):'';
      var act = it.status==='pending' ? '<button class="push" onclick="push(\\''+esc(it.itemId)+'\\',this)">Push</button>' : '';
      h+='<tr><td class="sub" style="margin:0;">'+esc(when)+'</td><td>'+esc(it.label)+'</td><td class="amt">'+inr(it.amount)+'</td><td><span class="st '+it.status+'">'+it.status+'</span></td><td>'+act+'</td></tr>';
    });
    h+='</table>'; tbl.innerHTML=h;
  }).catch(function(e){
    clearTimeout(to);
    var why = (e&&e.name==='AbortError') ? 'timed out (the approval history is large — try ?days=7)' : String(e);
    tbl.innerHTML='<div class="empty">Load failed: '+esc(why)+'</div>'; flash('Load failed: '+why,false);
  });
}
function push(id,btn){ if(btn){btn.disabled=true;btn.textContent='…';}
  fetch('/api/outflow-push?id='+encodeURIComponent(id),{credentials:'same-origin'}).then(function(r){return r.json();}).then(function(d){
    if(d.pushed){ flash('Pushed: '+(d.label||id)+' '+inr(d.amount),true); }
    else if(d.skipped){ flash('Skipped ('+d.reason+').',true); }
    else { flash('Error: '+(d.error||'unknown'),false); }
    load();
  }).catch(function(e){ flash('Push failed: '+e,false); if(btn){btn.disabled=false;btn.textContent='Push';} });
}
function catchUp(){ var b=document.getElementById('allBtn'); b.disabled=true; b.textContent='Pushing…';
  fetch('/api/outflow-catchup',{credentials:'same-origin'}).then(function(r){return r.json();}).then(function(d){
    if(d.error){ flash('Error: '+d.error,false); } else { flash('Catch-up done: pushed '+d.pushedCount+', skipped '+d.skippedCount+'.',true); }
    b.textContent='Push all pending (catch-up)'; load();
  }).catch(function(e){ flash('Catch-up failed: '+e,false); b.disabled=false; b.textContent='Push all pending (catch-up)'; });
}
function loadLog(){
  var t=document.getElementById('logtbl'); t.innerHTML='<div class="empty">Loading…</div>';
  fetch('/api/outflow-log',{credentials:'same-origin'}).then(function(r){return r.json();}).then(function(d){
    if(d&&d.error){ t.innerHTML='<div class="empty">Couldn\\'t load: '+esc(d.error)+'</div>'; return; }
    var items=(d&&d.items)||[];
    document.getElementById('logcount').textContent=items.length+' posted \\u00b7 '+(d.paidCount||0)+' paid';
    if(!items.length){ t.innerHTML='<div class="empty">Nothing pushed to the group yet.</div>'; return; }
    var h='<table><tr><th>When</th><th>Description</th><th class="amt">Amount</th><th>Mode</th><th>Account</th><th>Status</th><th></th></tr>';
    items.forEach(function(it){
      var when=(it.paidDetails&&it.paidDetails.at)||it.postedAt;
      when=when?new Date(when).toLocaleDateString('en-IN',{day:'2-digit',month:'short'}):'';
      var mode=(it.paidDetails&&it.paidDetails.mode)||''; var acct=(it.paidDetails&&it.paidDetails.bankAc)||it.bankAc||'';
      var amt=(it.paidDetails&&it.paidDetails.amount)||it.amount;
      var acts='';
      if(it.status==='approved'||it.status==='posted'||it.status==='part-paid'){ acts+='<button class="push" style="background:#16402a;border-color:#1f7a45;color:#76e0a0;" onclick="markPaid(\\''+esc(it.itemId)+'\\',this)">Mark paid</button> '; }
      if(it.paid){ acts+='<button class="push" style="background:#3a2f12;border-color:#5a4a1a;color:#e5c97a;" onclick="markUnpaid(\\''+esc(it.itemId)+'\\',this)">Mark unpaid</button> '; }
      if(it.postedMsgId){ acts+='<button class="push" style="background:#3a1c1c;border-color:#5a2a2a;color:#e09a9a;" onclick="unpost(\\''+esc(it.itemId)+'\\',this)">Delete</button>'; }
      h+='<tr><td class="sub" style="margin:0;">'+esc(when)+'</td><td>'+esc(it.label)+'</td><td class="amt">'+inr(amt)+'</td><td>'+esc(mode)+'</td><td>'+esc(acct)+'</td><td><span class="st '+it.status+'">'+it.status+'</span></td><td style="white-space:nowrap;">'+acts+'</td></tr>';
    });
    h+='</table>'; t.innerHTML=h;
  }).catch(function(e){ t.innerHTML='<div class="empty">Load failed: '+esc(String(e))+'</div>'; });
}
function markPaid(id,btn){ if(btn){btn.disabled=true;btn.textContent='…';}
  fetch('/api/paid-mark?id='+encodeURIComponent(id),{credentials:'same-origin'}).then(function(r){return r.json();}).then(function(d){
    flash(d.message||(d.error?('Error: '+d.error):'Done'), !d.error); loadLog(); load();
  }).catch(function(e){ flash('Failed: '+e,false); if(btn){btn.disabled=false;btn.textContent='Mark paid';} });
}
function markUnpaid(id,btn){ if(btn){btn.disabled=true;btn.textContent='…';}
  fetch('/api/paid-undo?id='+encodeURIComponent(id),{credentials:'same-origin'}).then(function(r){return r.json();}).then(function(d){
    flash(d.message||(d.error?('Error: '+d.error):'Done'), !d.error); loadLog(); load();
  }).catch(function(e){ flash('Failed: '+e,false); if(btn){btn.disabled=false;btn.textContent='Mark unpaid';} });
}
function unpost(id,btn){
  if(!confirm('Delete this pushed item from the dashboard AND delete its message in the payments group?')) return;
  if(btn){btn.disabled=true;btn.textContent='…';}
  fetch('/api/outflow-unpost?id='+encodeURIComponent(id)+'&deleteMsg=1',{credentials:'same-origin'}).then(function(r){return r.json();}).then(function(d){
    flash(d.message||(d.error?('Error: '+d.error):'Done'), !d.error); loadLog(); load();
  }).catch(function(e){ flash('Failed: '+e,false); if(btn){btn.disabled=false;btn.textContent='Delete';} });
}
load();
loadLog();
</script>
</body></html>`;
app.get('/api/pair',function(req,res){
  if(waReady)return res.send('<html><body style="display:flex;justify-content:center;align-items:center;min-height:100vh;background:#111"><h1 style="color:#0f0">WhatsApp Connected</h1></body></html>');
  if(!latestQRDataUrl)return res.send('<html><body style="display:flex;justify-content:center;align-items:center;min-height:100vh;background:#111"><h1 style="color:white">Waiting for QR...</h1></body></html>');
  res.send('<html><body style="display:flex;justify-content:center;align-items:center;min-height:100vh;background:#111"><div style="text-align:center"><h1 style="color:white">Scan QR with WhatsApp</h1><img src="'+latestQRDataUrl+'" style="width:300px"/></div></body></html>');
});
app.get('/api/wa-status',function(req,res){res.json({connected:waReady});});
app.get('/api/approval-backfill',async function(req,res){try{if(!waReady)return res.json({error:'WhatsApp not connected'});var days=parseInt(req.query.days)||30;var commit=req.query.commit==='1'||req.query.commit==='true';var result=await buildVerdictBackfill(days,commit);res.json(result);}catch(e){res.json({error:e.message});}});
app.get('/api/event-store',function(req,res){try{var s=loadEventStore();var limit=parseInt(req.query.limit)||200;var evs=s.events.slice(-limit).reverse();var counts={verdict:0,approved:0,paid:0};s.events.forEach(function(e){if(counts[e.type]!=null)counts[e.type]++;});res.json({version:s.version,createdAt:s.createdAt,totalEvents:s.events.length,counts:counts,showing:evs.length,events:evs});}catch(e){res.json({error:e.message});}});
app.get('/api/payable-code-backfill',function(req,res){try{res.json(buildPayableCodeBackfill(req.query.commit==='1'||req.query.commit==='true'));}catch(e){res.json({error:e.message});}});
app.get('/api/event-store-diff',async function(req,res){
  try{
    var days=parseInt(req.query.days)||15;
    var audit=await buildApprovalAudit(days);
    var store=loadEventStore();
    var storeCreated=store.createdAt||null;
    // index the store's approved events by itemId
    var storeApproved={};
    store.events.forEach(function(ev){ if(ev.type==='approved' && ev.itemId){ storeApproved[ev.itemId]=ev; } });
    // audit's fully-approved items
    var auditApproved=(audit.fullyApproved||[]).map(function(e){
      return { id:e.id, label:(e.vendor||e.label||(e.body?e.body.substring(0,60):'')||'').toString().slice(0,80), amount:e.amount, date:e.date?e.date.toISOString():null };
    });
    var matched=[], auditOnly=[], storeOnly=[];
    var seen={};
    auditApproved.forEach(function(a){
      seen[a.id]=true;
      if(storeApproved[a.id]){
        matched.push({ id:a.id, label:a.label, amount:a.amount });
      }else{
        // approval the chat-history audit sees but the event store missed.
        // pre-existing = its approval likely happened before the store was created.
        var pre = storeCreated && a.date && (new Date(a.date) < new Date(storeCreated));
        auditOnly.push({ id:a.id, label:a.label, amount:a.amount, date:a.date, preExisting: !!pre });
      }
    });
    // events the store has but the audit doesn't currently classify as approved (would be an anomaly)
    Object.keys(storeApproved).forEach(function(id){
      if(!seen[id]){ var ev=storeApproved[id]; storeOnly.push({ id:id, label:ev.label, amount:ev.amount, at:ev.at }); }
    });
    var preCount=auditOnly.filter(function(x){return x.preExisting;}).length;
    var newMismatch=auditOnly.filter(function(x){return !x.preExisting;});
    res.json({
      storeCreatedAt: storeCreated,
      auditWindowDays: days,
      summary: {
        auditFullyApproved: auditApproved.length,
        storeApprovedEvents: Object.keys(storeApproved).length,
        matched: matched.length,
        auditOnly_total: auditOnly.length,
        auditOnly_preExistingBacklog: preCount,
        auditOnly_newMismatch: newMismatch.length,
        storeOnly_anomalies: storeOnly.length
      },
      note: "matched = in both (good). auditOnly_preExistingBacklog = approved before the store existed (expected, ignore). auditOnly_newMismatch = NEW approvals the store should have caught but didn't (investigate). storeOnly = store has it, audit doesn't (investigate).",
      matched: matched,
      newMismatch: newMismatch,
      preExistingBacklog: auditOnly.filter(function(x){return x.preExisting;}),
      storeOnlyAnomalies: storeOnly
    });
  }catch(e){ res.json({error:e.message}); }
});
app.get('/api/groups',async function(req,res){if(!waReady)return res.json({error:'Not connected'});try{var chats=await waClient.getChats();res.json({groups:chats.filter(function(c){return c.isGroup;}).map(function(c){return{name:c.name,jid:c.id._serialized};})});}catch(e){res.json({error:e.message});}});
app.get('/api/bot/on',function(req,res){CONFIG.BOT_ENABLED=true;res.json({botEnabled:true});});
app.get('/api/bot/off',function(req,res){CONFIG.BOT_ENABLED=false;res.json({botEnabled:false});});
app.get('/api/ledger',async function(req,res){try{var date=req.query.date||new Date().toISOString().split('T')[0];var entries=await getLedgerData(date);var tIn=0,tOut=0;entries.forEach(function(e){if(e.inOut==='IN')tIn+=e.amount;if(e.inOut==='OUT')tOut+=e.amount;});res.json({date:date,entries:entries,totalIn:tIn,totalOut:tOut,net:tIn-tOut,count:entries.length});}catch(e){res.json({error:e.message});}});
app.get('/api/fund-position',async function(req,res){try{res.json({accounts:await getFundPosition()});}catch(e){res.json({error:e.message});}});
app.get('/api/approval-audit',async function(req,res){
  try{
    var days=parseInt(req.query.days)||15;
    var audit=await buildApprovalAudit(days);
    var fmt=function(e){return{date:e.date.toISOString().split('T')[0],time:e.date.toLocaleTimeString('en-IN',{timeZone:'Asia/Kolkata',hour:'2-digit',minute:'2-digit'}),message:e.body.substring(0,300),sender:e.sender,vendor:e.vendor,amount:e.amount,amountFormatted:e.amount>0?formatINR(e.amount):'',purpose:e.purpose||'',subItems:e.subItems||null,hasMedia:e.hasMedia,visionParsed:e.visionParsed||false,mm:e.status.mm,sm:e.status.sm,mmReply:e.mmApproval?e.mmApproval.raw:null,smReply:e.smApproval?e.smApproval.raw:null,mmName:e.mmApproval?e.mmApproval.name:null,smName:e.smApproval?e.smApproval.name:null,queryAnswer:e.queryAnswer||null,supportingDocs:e.supportingDocs||null};};
    res.json({summary:{period:days+' days',totalMessages:audit.totalMessages,totalExpenseRequests:audit.totalExpenses,fullyApproved:audit.fullyApproved.length,partialApproval:audit.partialApproval.length,noApproval:audit.noApproval.length,onHold:audit.onHold.length,rejected:audit.rejected.length,dedupedCount:audit.dedupedCount||0,visionCacheSize:audit.visionCacheSize},fullyApproved:audit.fullyApproved.map(fmt),partialApproval:audit.partialApproval.map(fmt),noApproval:audit.noApproval.map(fmt),onHold:audit.onHold.map(fmt),rejected:audit.rejected.map(fmt)});
  }catch(e){res.json({error:e.message});}
});
app.get('/api/send-reminders',async function(req,res){try{if(!waReady)return res.json({error:'WhatsApp not connected'});var count=await sendPendingReminders();res.json({success:true,remindersSent:count});}catch(e){res.json({error:e.message});}});
app.get('/api/unapproved-alert-toggle',function(req,res){var st=(req.query.state||'').toLowerCase();if(st==='on'||st==='off'){saveUnapprovedAlert(st==='on');}res.json({enabled:loadUnapprovedAlert(),note:'Day Book group alert; findings always appear in private summary.'});});
app.get('/api/reminder-digest-send',async function(req,res){try{if(!waReady)return res.json({error:'WhatsApp not connected'});var count=await sendApprovalReminderDigest();res.json({success:true,pendingPosted:count});}catch(e){res.json({error:e.message});}});
app.get('/api/reminder-digest-preview',async function(req,res){try{var d=await buildApprovalReminderDigest();res.json(d||{empty:true,message:'No pending approvals in the '+REMINDER_MAX_AGE_DAYS+'-day window'});}catch(e){res.json({error:e.message});}});
// v2.8.14 diagnostic: inspect the saved digest map, the verdict store, and how a test
// reply would parse — to debug why a verdict isn't registering.
app.get('/api/debug-verdict',async function(req,res){try{
  var map = loadDigestMap();
  var store = loadVerdicts();
  var testBody = req.query.body || '1 Yes';
  var parsed = map && map.items ? parseVerdictMessage(testBody, map.items.length) : null;
  res.json({
    digestMap: map ? { at: map.at, msgId: map.msgId, items: map.items } : null,
    verdictStoreKeys: Object.keys(store),
    verdictStore: store,
    testBody: testBody,
    parsedVerdicts: parsed
  });
}catch(e){res.json({error:e.message, stack:(e.stack||'').substring(0,400)});}});
app.get('/api/send-reminder-test',async function(req,res){
  try{
    if(!waReady)return res.json({error:'WhatsApp not connected'});
    var to=req.query.to; if(!to)return res.json({error:'pass ?to=917838537000'});
    var jid=to.replace(/[^0-9]/g,'')+'@c.us';
    var delay=function(ms){return new Promise(function(r){setTimeout(r,ms);});};
    var samples=[
      {id:'s1',date:new Date(),body:'SM Drawing',sender:'sushant',vendor:'SM Drawing',amount:400000,subItems:null,status:{mm:'pending',sm:'yes'},mmApproval:null,smApproval:{raw:'Ok',date:new Date(),name:'sumit'}},
      {id:'s2',date:new Date(),body:'Kackar + Innocept RMC',sender:'umesh katyal',vendor:'Kackar + Innocept RMC',amount:300000,subItems:[{vendor:'Kackar',amount:300000},{vendor:'Innocept RMC',amount:300000}],status:{mm:'pending',sm:'yes'},mmApproval:null,smApproval:{raw:'Ok',date:new Date(),name:'sumit'}},
      {id:'s3',date:new Date(),body:'Trinity Gurugram advance',sender:'sushant',vendor:'Trinity Gurugram advance',amount:100000,subItems:null,status:{mm:'question',sm:'pending'},mmApproval:{raw:'Advance to whom??',date:new Date(),name:'madhur mittal'},smApproval:null}
    ];
    var count=0;
    for(var i=0;i<samples.length;i++){var text=buildReminderText(samples[i]);if(!text)continue;await waClient.sendMessage(jid,text);await delay(1500);count++;}
    res.json({success:true,sentTo:jid,messages:count});
  }catch(e){res.json({error:e.message});}
});
app.get('/api/debug-messages',async function(req,res){try{if(!waReady)return res.json({error:'Not connected'});var chat=await waClient.getChatById(CONFIG.APPROVAL_GROUP_JID);var msgs=await chat.fetchMessages({limit:50});var result=[];for(var i=0;i<msgs.length;i++){var m=msgs[i];var info=await identifySender(m.author||m.from||'');result.push({rawSender:m.author||m.from||'',contactName:info.contactName,role:info.role,isReply:m.hasQuotedMsg,hasMedia:m.hasMedia,body:(m.body||'').substring(0,100),time:new Date(m.timestamp*1000).toLocaleString('en-IN',{timeZone:'Asia/Kolkata'})});}res.json({totalMessages:result.length,mmNames:CONFIG.MM_NAMES,smNames:CONFIG.SM_NAMES,messages:result});}catch(e){res.json({error:e.message});}});
// v2.8.5 diagnostic: show M/S replies WITH the body of the message each one quoted,
// so a past swipe-"ok" can be matched to the expense it was actually replying to.
app.get('/api/debug-replies',async function(req,res){try{if(!waReady)return res.json({error:'Not connected'});
  var limit=parseInt(req.query.limit)||80;
  var chat=await waClient.getChatById(CONFIG.APPROVAL_GROUP_JID);
  var msgs=await chat.fetchMessages({limit:limit});
  var out=[];
  for(var i=0;i<msgs.length;i++){
    var m=msgs[i];
    var info=await identifySender(m.author||m.from||'');
    if(info.role!=='mm'&&info.role!=='sm') continue;
    var rec={role:info.role,who:info.contactName,body:(m.body||'').trim(),msgId:(m.id&&(m.id._serialized||m.id.id))||null,isReply:!!m.hasQuotedMsg,time:new Date(m.timestamp*1000).toLocaleString('en-IN',{timeZone:'Asia/Kolkata'}),quoted:null};
    if(m.hasQuotedMsg){try{var q=await m.getQuotedMessage();rec.quoted={msgId:(q.id&&(q.id._serialized||q.id.id))||null,body:(q.body||'').substring(0,160),fromBot:/PENDING APPROVALS|\[BOT REMINDER\]|EXPENSE REQUEST|🔔/i.test(q.body||'')};}catch(e){rec.quoted={error:e.message};}}
    out.push(rec);
  }
  res.json({count:out.length,replies:out});
}catch(e){res.json({error:e.message});}});
app.get('/api/preview',async function(req,res){try{res.send(buildReportHTML(await generateDailyReport(req.query.date||new Date().toISOString().split('T')[0])));}catch(e){res.status(500).json({error:e.message});}});
app.get('/api/preview-image',async function(req,res){try{var img=await htmlToImage(buildReportHTML(await generateDailyReport(req.query.date||new Date().toISOString().split('T')[0])),800,1200);var buf=Buffer.isBuffer(img)?img:Buffer.from(img);res.set('Content-Type','image/png');res.set('Content-Length',String(buf.length));res.set('Cache-Control','no-store');res.end(buf);}catch(e){res.status(500).json({error:e.message});}});
app.get('/api/daily-report',async function(req,res){try{if(!waReady)return res.json({error:'Not connected'});if(!CONFIG.BOT_ENABLED)return res.json({error:'Bot paused'});var d=req.query.date||new Date().toISOString().split('T')[0];var data=await generateDailyReport(d);var img=await htmlToImage(buildReportHTML(data),800,1200);var buf=Buffer.isBuffer(img)?img:Buffer.from(img);await waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID,new MessageMedia('image/png',buf.toString('base64'),'MIS_'+d+'.png'),{caption:'MIS Report - '+d+'\nIN: '+formatINR(data.totalIn)+' | OUT: '+formatINR(data.totalOut)+' | NET: '+formatINR(data.net)});res.json({success:true,date:d});}catch(e){res.status(500).json({error:e.message});}});
app.get('/api/test-send',async function(req,res){try{if(!waReady)return res.json({error:'Not connected'});await waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID,'MIS Bot test - '+new Date().toLocaleString('en-IN',{timeZone:'Asia/Kolkata'}));res.json({success:true});}catch(e){res.json({error:e.message});}});
app.get('/api/report-status',function(req,res){res.json({botEnabled:CONFIG.BOT_ENABLED,whatsapp:waReady,version:'2.8.17',visionEnabled:CONFIG.CLAUDE_API_KEY?true:false,reverseScanWindowDays:REVERSE_SCAN_WINDOW_DAYS,reverseScanMinAmount:REVERSE_SCAN_MIN_AMOUNT});});
app.get('/api/vision-test',async function(req,res){try{if(!waReady)return res.json({error:'Not connected'});var msgId=req.query.msgId;if(!msgId)return res.json({error:'pass ?msgId=...'});var chat=await waClient.getChatById(CONFIG.APPROVAL_GROUP_JID);var msgs=await chat.fetchMessages({limit:200});var target=null;for(var i=0;i<msgs.length;i++){var sid=msgs[i].id._serialized||msgs[i].id.id;if(sid===msgId){target=msgs[i];break;}}if(!target)return res.json({error:'message not found in last 200'});if(!target.hasMedia)return res.json({error:'no media'});var media=await target.downloadMedia();if(!media)return res.json({error:'failed to download'});visionCache.delete(msgId);var result=await extractFromImage(media,msgId);res.json({msgId:msgId,mimetype:media.mimetype,dataSize:media.data?media.data.length:0,parsed:result});}catch(e){res.json({error:e.message});}});
app.get('/api/whoami',async function(req,res){
  try {
    if(!waReady) return res.json({error:'not connected'});
    var chats = await waClient.getChats();
    var dms = chats.filter(function(c){ return !c.isGroup; }).slice(0, 30);
    var out = [];
    for(var i=0;i<dms.length;i++){
      var c = dms[i];
      var contact = null;
      try { contact = await waClient.getContactById(c.id._serialized); } catch(e){}
      out.push({
        jid: c.id._serialized,
        name: c.name || (contact ? (contact.pushname || contact.name) : ''),
        number: contact ? contact.number : null,
        lastMsg: c.lastMessage ? (c.lastMessage.body || '').substring(0,80) : ''
      });
    }
    res.json({recentDMs: out});
  } catch(e) { res.json({error: e.message}); }
});
app.get('/api/auth-list',function(req,res){res.json({accountants:CONFIG.ACCOUNTANT_PHONES,testNumbers:CONFIG.TEST_PHONES||[],mm:CONFIG.MM_PHONE,sm:CONFIG.SM_PHONE,note:'Only these phone numbers can DM the bot for expense relay. @lid (anonymous) JIDs are always rejected.'});});
app.get('/api/dm-state',function(req,res){try{res.json(loadDMState());}catch(e){res.json({error:e.message});}});
app.get('/api/dm-clear',function(req,res){try{saveDMState({pending:{}});res.json({success:true});}catch(e){res.json({error:e.message});}});
app.get('/api/reconciliation',async function(req,res){
  try {
    var days = parseInt(req.query.days) || 30;
    var rec = await buildReconciliation(days);
    var fmt = function(e){
      var m = e.matchResult ? e.matchResult.match : null;
      return {
        date: e.date.toISOString().split('T')[0],
        vendor: e.vendor,
        amount: e.amount,
        amountFormatted: formatINR(e.amount),
        body: e.body ? e.body.substring(0,200) : '',
        sender: e.sender,
        subItems: e.subItems || null,
        subItemResults: e.subItemResults || null,
        supportingDocs: e.supportingDocs || null,
        matchStatus: e.matchResult ? e.matchResult.status : null,
        matchConfidence: e.matchResult ? e.matchResult.confidence : null,
        ledgerMatch: m ? {
          date: m.date.toISOString().split('T')[0],
          entity: m.entity,
          description: m.description,
          amount: m.amount,
          mode: m.mode,
          bankAC: m.bankAC,
          person: m.person
        } : null,
        dateDiffDays: e.matchResult ? e.matchResult.dateDiffDays : null,
        pctDiff: e.matchResult ? e.matchResult.pctDiff : null
      };
    };
    var fmtLedger = function(le){
      return {
        date: le.date.toISOString().split('T')[0],
        entity: le.entity,
        description: le.description,
        head: le.head,
        tag: le.tag,
        amount: le.amount,
        amountFormatted: formatINR(le.amount),
        bankAC: le.bankAC,
        person: le.person,
        notes: le.notes
      };
    };
    res.json({
      summary: rec.summary,
      paid: rec.paid.map(fmt),
      paidWithTolerance: rec.paidWithTolerance.map(fmt),
      possibleMatch: rec.possibleMatch.map(fmt),
      awaitingPayment: rec.awaitingPayment.map(fmt),
      ledgerWithoutApproval: (rec.ledgerWithoutApproval||[]).map(fmtLedger),
      ledgerRecurring: (rec.ledgerRecurring||[]).map(fmtLedger),
      reverseScanConfig: {
        windowDays: REVERSE_SCAN_WINDOW_DAYS,
        minAmount: REVERSE_SCAN_MIN_AMOUNT
      }
    });
  } catch(e) { res.json({error: e.message}); }
});
app.get('/api/silent-on',function(req,res){try{saveSilentMode(true);res.json({success:true,silentMode:true,message:'Silent mode ON. Bot will stop group reminders and DM the observer ('+SILENT_OBSERVER+') with daily summaries.'});}catch(e){res.json({error:e.message});}});
app.get('/api/silent-off',function(req,res){try{saveSilentMode(false);res.json({success:true,silentMode:false,message:'Silent mode OFF. Bot will resume group reminders and post evening report to Day Book group.'});}catch(e){res.json({error:e.message});}});
app.get('/api/silent-status',function(req,res){try{res.json({silentMode:loadSilentMode(),observer:SILENT_OBSERVER});}catch(e){res.json({error:e.message});}});
app.get('/api/outflow-post-on',function(req,res){try{saveOutflowPostEnabled(true);res.json({success:true,outflowPosting:true,message:'Outflow posting ON. Newly approved items will auto-post to the payments group for the accountants to mark paid. (Still capture-only — no Sheet write.)'});}catch(e){res.json({error:e.message});}});
app.get('/api/outflow-post-off',function(req,res){try{saveOutflowPostEnabled(false);res.json({success:true,outflowPosting:false,message:'Outflow posting OFF. Approved items will no longer be posted to the payments group.'});}catch(e){res.json({error:e.message});}});
app.get('/api/outflow-post-status',function(req,res){try{res.json({outflowPosting:loadOutflowPostEnabled(),envDefault:OUTFLOW_POST_ENABLED,note:'Panel toggle overrides the OUTFLOW_POST_ENABLED env default once set.'});}catch(e){res.json({error:e.message});}});
// v2.10.0-s5.13: lock-protected dummy payment-due. Posts a \u27E8TEST\u27E9-tagged PAYMENT DUE item into the
// outflow group via the SAME postApprovedToOutflow path as the real bridge (force-bypasses the toggle, so
// it works even when outflow posting is OFF), registered so a "paid" reply matches back and it appears in
// the summary. Item id is test-<ts> => any ledger row it produces is tagged [bot:test-...] for cleanup.
// Params: label (required-ish; defaults), amount (required), optional entity & account.
app.get('/api/outflow-post-dummy',async function(req,res){
  try{
    if(!waReady) return res.json({error:'WhatsApp not connected'});
    var label = (req.query.label||'').toString().trim() || 'TEST payment-due item';
    var amount = parseAmount(String(req.query.amount||'')) || parseFloat(req.query.amount);
    if(!amount) return res.json({error:'missing/invalid amount \u2014 pass &amount=12345'});
    var id = 'test-'+Date.now()+'-'+Math.random().toString(36).slice(2,6);
    var item = { id:id, label:label, description:label, entity:(req.query.entity||'PDC'), bankAc:(req.query.account||'') };
    var ok = await postApprovedToOutflow(item, amount, true, { test:true });
    res.json(ok ? { posted:true, itemId:id, label:label, amount:amount, note:'\u27E8TEST\u27E9 payment-due posted to the outflow group. Reply "paid" on it, or type "summary" in the group, to exercise the flow.' }
                : { error:'post failed (WhatsApp down, or duplicate id)' });
  }catch(e){ res.json({error:e.message}); }
});
app.get('/api/ledger-dryrun-on',function(req,res){try{saveLedgerDryrun(true);res.json({success:true,dryRun:true,message:'Ledger dry-run ON. The bot computes the exact row+target it would write and logs it, but writes nothing — even if LEDGER_WRITE_ENABLED is on. Safe rehearsal / runtime pause.'});}catch(e){res.json({error:e.message});}});
app.get('/api/ledger-dryrun-off',function(req,res){try{saveLedgerDryrun(false);res.json({success:true,dryRun:false,message:'Ledger dry-run OFF. If LEDGER_WRITE_ENABLED is on, confirmed rows will now actually be written to the Sheet; if it is off, the bot stays capture-only.'});}catch(e){res.json({error:e.message});}});
app.get('/api/ledger-write-status',function(req,res){try{var dry=loadLedgerDryrun();var live=LEDGER_WRITE_ENABLED;var posture=dry?'DRY-RUN (rehearsal — nothing written)':(live?'LIVE (rows are written to the Sheet)':'CAPTURE-ONLY (no write)');res.json({posture:posture,dryRun:dry,writeEnabled:live,sheetScope:(live?'read/write':'read-only'),writeTab:LEDGER_WRITE_TAB,newDayBlocks:NEWDAY_BLOCK_CREATE_ENABLED,note:'Dry-run wins over write-enabled. LEDGER_WRITE_ENABLED is env+redeploy (it sets the OAuth scope); dry-run is a live panel toggle.'});}catch(e){res.json({error:e.message});}});
// v2.10.0-s5.12: rehearsal test-write. Builds a row via the SAME assemblePaymentRow and fires the SAME
// gated writeRowToLedger (respects dry-run + LEDGER_WRITE_ENABLED + LEDGER_WRITE_TAB + NEWDAY_BLOCK_CREATE_ENABLED).
// Rows are tagged [bot:test-<ts>] in col L for easy cleanup. Params: date (dd/mm or dd/mm/yyyy or 'today'),
// amount, and optional desc/entity/account/head/tag/mode/person.
app.get('/api/ledger-test-write',async function(req,res){
  try{
    var q=req.query;
    if(!q.date) return res.json({error:'missing date — pass ?date=dd/mm or dd/mm/yyyy'});
    var amt=parseAmount(String(q.amount||''))||parseFloat(q.amount);
    if(!amt) return res.json({error:'missing/invalid amount — pass &amount=12345'});
    var entity=q.entity||'PDC', account=q.account||'PDC';
    var item={ id:'test-'+Date.now(), description:(q.desc||'TEST ENTRY (rehearsal)'), label:(q.desc||'TEST ENTRY (rehearsal)'), entity:entity, bankAc:account };
    var answers={ date: toLedgerDate(/^today$/i.test(String(q.date))?'today':q.date), amount:amt,
      head:(q.head||'Other'), tag:(q.tag||'Other'), mode:(q.mode||'Cash'), person:(q.person||''), entity:entity, bankAc:account };
    var row=assemblePaymentRow(item,answers);
    var result=await writeRowToLedger(row);
    res.json({ note:'rehearsal test write — obeys dry-run + LEDGER_WRITE_ENABLED + LEDGER_WRITE_TAB + NEWDAY_BLOCK_CREATE_ENABLED', writeTab:LEDGER_WRITE_TAB, row:row, result:result });
  }catch(e){ res.json({error:e.message}); }
});
app.get('/api/outflow-pending',async function(req,res){try{var days=parseInt(req.query.days)||15;res.json({items:await listApprovedForOutflow(days)});}catch(e){res.json({error:e.message});}});
app.get('/api/outflow-push',async function(req,res){try{if(!waReady)return res.json({error:'WhatsApp not connected'});var id=req.query.id;if(!id)return res.json({error:'missing id'});res.json(await pushOneApprovedToOutflow(id));}catch(e){res.json({error:e.message});}});
app.get('/api/outflow-catchup',async function(req,res){try{if(!waReady)return res.json({error:'WhatsApp not connected'});res.json(await catchUpApprovedToOutflow());}catch(e){res.json({error:e.message});}});
app.get('/api/outflow-queue',function(req,res){res.type('html').send(OUTFLOW_QUEUE_HTML);});
app.get('/api/outflow-log',function(req,res){try{var items=buildOutflowLog();var paidCount=items.filter(function(x){return x.paid;}).length;res.json({count:items.length,paidCount:paidCount,postedCount:items.length-paidCount,items:items});}catch(e){res.json({error:e.message});}});
app.get('/api/paid-undo',function(req,res){try{var id=req.query.id;if(!id)return res.json({error:'missing id'});res.json(markItemUnpaid(id));}catch(e){res.json({error:e.message});}});
app.get('/api/paid-mark',function(req,res){try{var id=req.query.id;if(!id)return res.json({error:'missing id'});var amt=req.query.amount!=null?parseFloat(req.query.amount):null;res.json(markItemPaid(id, amt));}catch(e){res.json({error:e.message});}});
app.get('/api/contributions',function(req,res){try{res.json(buildContributionStatement());}catch(e){res.json({error:e.message});}});
app.get('/api/payment-reopen',function(req,res){try{var id=req.query.id;if(!id)return res.json({error:'missing id'});res.json(reopenItem(id));}catch(e){res.json({error:e.message});}});
app.get('/api/outflow-unpost',async function(req,res){try{var id=req.query.id;if(!id)return res.json({error:'missing id'});var del=req.query.deleteMsg==='1'||req.query.deleteMsg==='true';res.json(await unpostOutflowItem(id,del));}catch(e){res.json({error:e.message});}});
app.get('/api/stale-scan',async function(req,res){try{if(!waReady)return res.json({error:'WhatsApp not connected'});var count=await scanStalePendings();res.json({success:true,remindersSent:count});}catch(e){res.json({error:e.message});}});
app.get('/api/stale-state',function(req,res){try{res.json(loadStaleState());}catch(e){res.json({error:e.message});}});
app.get('/api/stale-reset',function(req,res){try{saveStaleState({reminded:{}});res.json({success:true,message:'Stale reminder state cleared - next scan will re-send any 30+ min pending items'});}catch(e){res.json({error:e.message});}});
app.get('/api/eod-send',async function(req,res){try{var r=await sendEODReport(req.query.date);res.json(r);}catch(e){res.json({error:e.message});}});
app.get('/api/eod-preview',async function(req,res){try{var d=req.query.date||new Date().toISOString().split('T')[0];var audit=await buildApprovalAudit(30);var rec=await buildReconciliation(30);var outliers=await buildOutliers(rec);var dayOfWeek=new Date(d).getDay();var isFriday=req.query.friday==='1'||dayOfWeek===5;var weekStats=isFriday?computeWeeklyMatcherStats():null;res.send(buildEODReportHTML({date:d,audit:audit,rec:rec,outliers:outliers,isFriday:isFriday,weekStats:weekStats}));}catch(e){res.status(500).json({error:e.message});}});
app.get('/api/eod-image',async function(req,res){try{var d=req.query.date||new Date().toISOString().split('T')[0];var audit=await buildApprovalAudit(30);var rec=await buildReconciliation(30);var outliers=await buildOutliers(rec);var dayOfWeek=new Date(d).getDay();var isFriday=req.query.friday==='1'||dayOfWeek===5;var weekStats=isFriday?computeWeeklyMatcherStats():null;var html=buildEODReportHTML({date:d,audit:audit,rec:rec,outliers:outliers,isFriday:isFriday,weekStats:weekStats});var img=await htmlToImage(html,460,2000);var buf=Buffer.isBuffer(img)?img:Buffer.from(img);res.set('Content-Type','image/png');res.set('Cache-Control','no-store');res.end(buf);}catch(e){res.status(500).json({error:e.message});}});
app.get('/api/match-cache',function(req,res){try{res.json(loadMatchCache());}catch(e){res.json({error:e.message});}});
app.get('/api/match-cache-clear',function(req,res){try{saveMatchCache({matches:{},rejected:{},manualPaid:{}});res.json({success:true});}catch(e){res.json({error:e.message});}});
app.get('/api/matcher-stats',function(req,res){try{res.json(computeWeeklyMatcherStats());}catch(e){res.json({error:e.message});}});
app.get('/api/outliers',async function(req,res){try{var rec=await buildReconciliation(30);var outliers=await buildOutliers(rec);res.json({count:outliers.length,outliers:outliers});}catch(e){res.json({error:e.message});}});
app.get('/api/wa-reset',function(req,res){
  try {
    console.log('[WA] Manual reset requested via /api/wa-reset');
    waReady = false;
    latestQR = null; latestQRDataUrl = null;
    try { if (waClient) { waClient.destroy().catch(function(e){}); } } catch(e) {}
    try { if (fs.existsSync('./wa_auth')) { fs.rmSync('./wa_auth', { recursive: true, force: true }); console.log('[WA] wa_auth folder cleared'); } } catch(e) { console.error('[WA] Clear error:', e.message); }
    setTimeout(function() { createWhatsAppClient(); }, 3000);
    res.json({ok:true, message:'WhatsApp reset triggered. Wait 30-60s, then check /api/pair for new QR.'});
  } catch(e) { res.json({error:e.message}); }
});
// ── Crons ─────────────────────────────────────────────────────────────────────
// 7 PM IST — evening report (with v2.6 top-N + reverse-scan sections appended)
cron.schedule('30 13 * * *',async function(){
  if(!CONFIG.BOT_ENABLED||!waReady)return;
  var d=new Date().toISOString().split('T')[0];
  var silent = loadSilentMode();
  var targetJid = silent ? getSilentObserverJid() : CONFIG.WHATSAPP_GROUP_JID;
  var prefix = silent ? '[SILENT MODE] ' : '';
  try {
    var data = await generateDailyReport(d);
    var staleSection = await buildStalePendingSection();
    var recSection = await buildReconciliationSection();
    if(data.entryCount > 0){
      var img = await htmlToImage(buildReportHTML(data),800,1200);
      var buf = Buffer.isBuffer(img)?img:Buffer.from(img);
      var caption = prefix + 'Evening Report - '+d+'\nIN: '+formatINR(data.totalIn)+' | OUT: '+formatINR(data.totalOut) + staleSection + recSection;
      await waClient.sendMessage(targetJid, new MessageMedia('image/png',buf.toString('base64'),'MIS.png'), {caption:caption});
    } else if(staleSection || recSection) {
      await waClient.sendMessage(targetJid, prefix + 'Evening Report - '+d+'\nNo Ledger entries today.'+staleSection+recSection);
    } else if(silent) {
      await waClient.sendMessage(targetJid, prefix + 'Evening Report - '+d+'\nNo Ledger entries and no stale approvals.');
    }
  } catch(e) { console.error('Cron evening:',e.message); }
},{timezone:'Asia/Kolkata'});
// 9 AM IST — morning summary + pending reminders
cron.schedule('30 3 * * *',function(){
  if(!CONFIG.BOT_ENABLED||!waReady)return;
  var y=new Date();y.setDate(y.getDate()-1);var d=y.toISOString().split('T')[0];
  var silent = loadSilentMode();
  var targetJid = silent ? getSilentObserverJid() : CONFIG.WHATSAPP_GROUP_JID;
  var prefix = silent ? '[SILENT MODE] ' : '';
  generateDailyReport(d).then(function(data){
    if(data.entryCount>0){htmlToImage(buildReportHTML(data),800,1200).then(function(img){var buf=Buffer.isBuffer(img)?img:Buffer.from(img);waClient.sendMessage(targetJid,new MessageMedia('image/png',buf.toString('base64'),'MIS.png'),{caption:prefix+'Morning Summary - '+d+'\nIN: '+formatINR(data.totalIn)+' | OUT: '+formatINR(data.totalOut)});});}
  }).catch(function(e){console.error('Cron morning:',e.message);});
  // v2.7.2: pending-approval reminders moved to dedicated 10 AM + 7 PM digests (below).
},{timezone:'Asia/Kolkata'});
// v2.7.2: Approval reminder digests — 10:00 AM and 7:00 PM IST.
// Consolidated single message to the approval group, 14-day window, bypasses silent mode.
cron.schedule('0 10 * * *',function(){
  if(!CONFIG.BOT_ENABLED||!waReady)return;
  sendApprovalReminderDigest().catch(function(e){console.error('[Digest 10AM]',e.message);});
},{timezone:'Asia/Kolkata'});
cron.schedule('0 19 * * *',function(){
  if(!CONFIG.BOT_ENABLED||!waReady)return;
  sendApprovalReminderDigest().catch(function(e){console.error('[Digest 7PM]',e.message);});
},{timezone:'Asia/Kolkata'});
// Every 10 minutes — scan for pending expenses >= 30 min old
cron.schedule('*/10 * * * *',function(){
  if(!CONFIG.BOT_ENABLED||!waReady)return;
  scanStalePendings().catch(function(e){console.error('[Cron stale]',e.message);});
},{timezone:'Asia/Kolkata'});
// 7 PM IST daily — EOD JPEG report privately to SILENT_OBSERVER (with v2.6 reverse-scan section)
cron.schedule('0 19 * * *',function(){
  if(!CONFIG.BOT_ENABLED||!waReady)return;
  sendEODReport().then(function(r){
    if(r && r.rec) return postUnapprovedAlertIfEnabled(r.rec);
    return buildReconciliation(REVERSE_SCAN_WINDOW_DAYS+14).then(postUnapprovedAlertIfEnabled);
  }).catch(function(e){console.error('[EOD cron]',e.message);});
},{timezone:'Asia/Kolkata'});
// ── Startup ──────────────────────────────────────────────────────────────────
initGoogleSheets();
createWhatsAppClient();
app.listen(CONFIG.PORT,function(){
  console.log('\nFidato MIS Server v2.10.0-s5.17 | Port:',CONFIG.PORT,'| Vision:',CONFIG.CLAUDE_API_KEY?'enabled':'disabled');
  console.log('  ReverseScan: window='+REVERSE_SCAN_WINDOW_DAYS+'d, floor=Rs.'+REVERSE_SCAN_MIN_AMOUNT);
  console.log('  Report top-N: stale='+STALE_TOP_N+' (recent='+STALE_RECENT_HOURS+'h), reconciliation='+REPORT_TOP_N);
  console.log('  Smart DM parsing: enabled (free-form vendor/amount/company/account extraction)');
  console.log('  Endpoint lock: '+((PANEL_USER&&PANEL_PASSWORD)?'ON (Basic Auth on /api/*)':'FAIL-CLOSED (PANEL_USER/PANEL_PASSWORD unset)'));
});
