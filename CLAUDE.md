# CLAUDE.md — Project context for AI agents

This file is auto-loaded by Claude Code sessions in this repo. Read it first before touching the frontend. If you change a load-bearing decision (palette, layout primitives, file structure), update this file in the same commit.

---

## Architecture at a glance

```
/opt/video_elicitation_annotation_tool/
├── index.html              ← SPA entry, single page with tabs (Elicit / Segment / Projects)
├── css/styles.css          ← Single stylesheet; ~4100 lines, monolithic by design
├── js/app.js               ← Single SPA controller; ~5200 lines; no framework
├── backend/                ← FastAPI app on port 8005 (systemd: videoelicit-backend)
└── mockup-studio.html      ← Untracked working reference for the Studio theme
```

The Moodle plugin at `/var/www/html/public/local/videoelicit/` embeds `index.html` in an iframe with a JWT in `?token=`. The viewer dashboard at `/var/www/html/public/annotation_viewer.html` is a separate, standalone page (also themed light/dark).

Apache proxies `https://aimove.minesparis.psl.eu/videoelicit-ui/` → `http://127.0.0.1:8005/`. The FastAPI backend mounts the project root at `/static`, so `mockup-studio.html` is reachable at `/videoelicit-ui/static/mockup-studio.html`. `NoCacheStaticFiles` means CSS/HTML edits are picked up on hard refresh — no restart needed.

**Knowledge silos:** the project modal's "Visibility" dropdown
(`renderCohortSelector` / `handleProjectFormSubmit` in `js/app.js`,
`/api/cohorts/managed` + project update in `backend/main.py`) assigns a
project's annotations to a Moodle cohort, siloing them from other
companies' users in CraftPilot's RAG. This is only the frontend half —
the full setup workflow (Moodle admin steps, capability requirements,
verification) is documented in the `craftpilot_backend` repo:
`docs/KNOWLEDGE_SILOS_WORKFLOW.md`.

---

## Frontend design system — "Studio" theme

Calm, professional-workbench aesthetic. White panels floating on a soft sky canvas, with deep blue reserved as the single point of authority. Built to be used 8 hours a day without eye fatigue.

### Palette (use these tokens, never hard-code colors)

| Token | Hex | Use |
|---|---|---|
| `--sky` / `--sky-200` | `#CBECFF` | Wash, decorative tints |
| `--sky-50` | `#F2FAFF` | Hover surfaces, soft panels |
| `--sky-100` | `#E4F4FF` | Active-state pills, badges |
| `--sky-300` | `#A8DCFB` | Hover borders |
| `--blue` | `#0066CC` | **The single accent.** Primary buttons, active tab, focus rings, timecodes, annotation rails |
| `--blue-hover` | `#0052A8` | Hover state of `--blue` |
| `--slate-900` | `#1F2A37` | Primary text |
| `--slate-700` | `#475569` | Secondary text |
| `--slate` | `#64748B` | Tertiary / labels |
| `--canvas` | `#F8FBFE` | Page background |
| `--line` | `#E6EEF6` | Hairline borders (always 1px) |
| `--ink` | `#0B1220` | Video frame interior only |

Semantic accents (used sparingly): `--accent-color` (success `#1F9D6B`), `--danger-color` (`#E03A5C`), `--warning-color` (`#D78A2A`).

### Typography

- **`--font-sans: 'Geist'`** — UI and body. Weights 500 / 600 / 700 do hierarchy work. Letter-spacing `-0.015em` on headings, normal on body. **Never use Inter** (rejected during theme review as too generic).
- **`--font-mono: 'JetBrains Mono'`** — timecodes, durations, technical labels only.
- No serif. An earlier mockup used Instrument Serif italic for headings; the user rejected it as "too elegant." Don't bring it back.

### Shape & motion

- Radii: `--radius-sm 8px`, `--radius-md 12px`, `--radius-lg 16px`. Default to `--radius-md`.
- Shadows: soft and layered. Prefer `--shadow-xs` for resting cards and `--shadow-md` on hover. Never use the heavy default browser shadow.
- Motion: `--ease cubic-bezier(0.22, 1, 0.36, 1)` with `--t-fast 140ms` / `--t-base 240ms` / `--t-slow 420ms`. Hover lifts are `translateY(-1px)`, never more.
- Focus rings: use `--shadow-focus` (a 4px sky halo).

---

## File layout of `css/styles.css`

The stylesheet is monolithic but layered. **Two override blocks at the end win source-order battles with the older rules:**

1. **Lines 1–~3650** — original "Professional Academic" rules (legacy). Tokens are remapped at `:root` so these inherit the new palette automatically. Don't edit these in place unless you have to; prefer adding overrides below.
2. **`░░░░ STUDIO THEME OVERRIDES ░░░░`** — restyles header, tabs, buttons, Elicit-view layout, video stage, control bar, coverage banner, annotations.
3. **`░░░░ STUDIO THEME — FULL SCRUB ░░░░`** — forms, modals, toasts, dropdowns, segment-tab, projects grid, session summary card, coverage actions, empty states, scrollbars, tutorial body.

When you need to style a new component, **append** to (or just below) the FULL SCRUB block — don't scatter rules through the file.

---

## Key layout primitives

### The Elicit view fits in `100dvh` without scrolling

- `.app-container { height: 100dvh; overflow: hidden; }`
- `.main-content { flex: 1; min-height: 0; }` — required for flex children to shrink
- `.video-stage` uses **`container-type: size`**; the video inside uses `width: min(100cqw, 100cqh * 16/9)` so it always fits the stage at 16:9, whichever dimension is the constraint. **Do not** put `aspect-ratio` + `max-height` on the video without that container-query setup — it collapses to content size.
- `.annotations-list { flex: 1; min-height: 0; overflow-y: auto; }` — the right panel scrolls internally while the coverage banner and panel header stay pinned.

### Recording controls are a horizontal bar, not a sidebar

`.recording-controls-sidebar` is the legacy class name kept for `app.js` compatibility, but it's now styled as a horizontal control bar that sits **between** the video stage and the timeline. Three-column grid: status pill | record cluster | timer. If you change the markup, preserve these IDs — `app.js` references them: `recordBtn`, `skipBackBtn`, `skipForwardBtn`, `recordingPulse`, `statusIndicator`, `statusText`, `recordingTimer`, `timerDisplay`.

### Header pattern

Brand block (title + subtitle) sits immediately to the right of the back-to-Moodle link, separated by a `border-left: 1px solid var(--line)` divider, with `margin-right: auto` pushing actions to the far right. **There is no brand icon / logo mark.** It was tried and explicitly removed.

---

## Component vocabulary cheat-sheet

| Pattern | Class / Style |
|---|---|
| Primary action button | `.btn .btn-primary` — solid `--blue`, shadow `0 4px 12px -3px rgba(0,102,204,0.45)` |
| Secondary action | `.btn .btn-secondary` — white surface, hairline border, hover to sky |
| Soft/info action | `.btn .btn-info` — sky-100 background, blue text |
| Icon-only button | `.btn .btn-icon` — circular, hairline, hover lifts |
| Pill / badge | `padding: 2px 8px; background: var(--sky-100); color: var(--blue); border-radius: 999px;` |
| Timecode chip | `font-family: var(--font-mono); font-variant-numeric: tabular-nums; color: var(--blue); background: var(--sky-100);` |
| Card | `background: var(--surface); border: 1px solid var(--line); border-radius: var(--radius-md); box-shadow: var(--shadow-xs);` |
| Card hover | `border-color: var(--sky-300); box-shadow: var(--shadow-md); transform: translateY(-1px);` |
| Annotation card accent | `::before` left rail, 3px wide, `background: var(--blue); opacity: 0.85;` |
| Section heading (small caps) | `font-size: 11px; text-transform: uppercase; letter-spacing: 0.08em; color: var(--slate); font-weight: 600;` |

---

## Environment constraints

- **No `sudo` access.** Any command requiring elevated privileges (e.g. `systemctl restart`, `apt install`) must be run by the user. Suggest the command using `! <command>` so they can run it directly in the session.
- **No access to `.env` files or secrets.** Environment variables (API keys, tokens, DB passwords) are managed by the system outside this repo. Never read, print, or suggest committing `.env` files. Infer configuration from the code (e.g. `os.getenv(...)` calls) without accessing the actual values.

---

## Deploy topology & git hazards

- This repo directory (`/opt/video_elicitation_annotation_tool`) is **both** a git working copy **and** the live source the backend runs from (`videoelicit-backend` execs `uvicorn` directly out of `backend/` here — no separate deploy/copy step for Python code). A `git checkout`/`pull`/merge here changes files the running system actually reads, not just history.
- The Moodle plugin (`local_videoelicit/` in the repo) has a **separate, non-symlinked** copy at `/var/www/html/public/local/videoelicit/` that must be manually `sudo cp`'d over after editing plugin files — there is no sync script or cron job for this.
- **Hazard, confirmed 2026-08-24:** while manually deploying the plugin files, PR #20 got merged into `main` on GitHub, and this directory's checkout moved `main` → then got fast-forwarded via `git pull` — briefly reverting `local_videoelicit/version.php` to an older value on disk. Re-copying from the repo at that moment pushed the stale `version.php` into the Moodle webroot, and Moodle's upgrade page threw a "cannot downgrade" error (its DB already had the newer version recorded from an earlier, successful upgrade).
- **Lesson:** before `sudo cp`-ing repo files into `/var/www/html/public/local/videoelicit/`, run `git status`/`git log -1`/`git branch --show-current` first to confirm what's actually on disk right now — don't assume the working tree still matches what you last edited, especially if any time has passed or other collaborators could have merged/pushed in the meantime.

---

## Workflow rules (learned during this build)

- **Always branch from `main`** before frontend work: `git checkout main && git pull && git checkout -b feat/<name>`. Don't pile changes onto an existing feature branch.
- **Mockup first, port second.** Big visual changes start as a self-contained `mockup-*.html` at the repo root so the user can review on the real server (`/videoelicit-ui/static/mockup-*.html`) before any production CSS is touched.
- **Preserve IDs.** `app.js` reaches into the DOM by ID heavily. Renaming an element ID = silently breaking the app. If you restructure markup, keep IDs intact.
- **No emojis** in code, commits, or generated text unless the user explicitly asks.
- **No new inline styles** — the user has called these out as theme leaks. If you find an old one (`style="background:#xxx..."`), move it to CSS and remove the attribute.
- **Annotation viewer** (`/var/www/html/public/annotation_viewer.html`) lives outside the repo. It has its own dark/light toggle driven by `:root[data-theme="..."]` with the light theme using the same Studio palette.

---

## Design choices already settled (don't relitigate)

These were debated and decided during the Studio theme build. Re-opening them costs trust and time.

- **Theme direction:** "Studio" (calm professional workbench). Alternatives offered and rejected: "Atelier" (editorial serif-heavy) and "Field Notes" (playful sticky-notes).
- **Single accent color.** `--blue` carries all signal. Don't introduce purple/teal/orange accents for "variety."
- **Geist over Inter.** Inter is the AI-slop default; Geist gives a calmer, more deliberate UI tone.
- **No serif headings.** Tried and rejected.
- **No brand mark / logo.** Tried and rejected.
- **Horizontal control bar, not a vertical sidebar.** The sidebar made buttons feel orphaned on tall video frames.
- **Video fits the viewport via container queries**, not a magic-number `max-height: 54vh`.
- **App is `100dvh` with `overflow: hidden`.** The main view never scrolls; only the right-panel annotation list does. Do not reintroduce body scroll.

---

## Verification before claiming done

When you touch the frontend:

1. Hard-refresh in browser (`Ctrl+Shift+R`) — `NoCacheStaticFiles` should serve fresh, but browser cache can lie.
2. Walk every tab (Elicit / Segment / Projects-disabled) and every modal (Select Video, Project, Assign Videos, Local Folder, Tutorial).
3. Check the recording control bar at narrow widths (≤1100px) — the grid should reflow without overlap.
4. Resize the browser window. Video should re-fit at 16:9 with no crops or overflow.
5. 401 errors on `/api/videos` and `/api/tutorial-status` are **expected** when opening the page without a Moodle JWT — not a regression.

---

## Recent history of frontend work on this branch

`feat/studio-theme` (off `main`):

1. `3dc56cd` — applied Studio theme tokens to `:root`, switched Inter→Geist, restructured Elicit view (horizontal control bar, `.video-stage` with container queries).
2. `cd34de3` — aligned header brand block left with hairline divider; stripped inline styles from `#tutorialFab`; removed `max-width: 1800px` cap on `.main-content`.
3. `0cb15e3` — "full scrub": forms, modals, toasts, sort dropdown, segment tab, projects grid, session summary, coverage actions, empty states, scrollbars, tutorial body.

Also touched outside this repo: `/var/www/html/public/annotation_viewer.html` gained a `[data-theme]`-driven light/dark toggle with the light variant using the Studio palette.
