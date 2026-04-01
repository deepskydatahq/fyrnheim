# Fyrnheim Website Design

## Decision

Build a docs + landing page site using Astro Starlight, hosted on Cloudflare Pages, co-located in the main repo under `website/`.

## Tech Stack

- **Framework:** Astro + Starlight
- **Content:** Markdown/MDX
- **Hosting:** Cloudflare Pages (auto-deploy on push to main)
- **Location:** `website/` directory in the fyrnheim repo

## Site Structure

```
/                              Landing page (custom Astro page)
  Hero                         Tagline + pip install + "Get Started" CTA
  Features                     3-4 cards (typed entities, multi-backend, local dev, no SQL)
  Code example                 The 4-step quick start flow
  Comparison table             vs dbt

/docs/                         Starlight docs site
  Getting Started              Install, init, generate, run
  Core Concepts/
    Entities
    Layers                     Prep, Dimension, Snapshot, Activity, Analytics
    Sources                    TableSource, UnionSource, DerivedSource, AggregationSource, EventAggregationSource
    Source Mapping
    Primitives
    Components
    Quality Checks
  Configuration                fyrnheim.yaml, CLI flags, backend config
  API Reference                Public API surface
```

## File Layout

```
website/
  astro.config.mjs
  package.json
  src/
    content/
      docs/
        getting-started.md
        concepts/
          entities.md
          layers.md
          sources.md
          source-mapping.md
          primitives.md
          components.md
          quality.md
        configuration.md
        api-reference.md
    pages/
      index.astro                Custom landing page
  public/
    favicon.svg
```

## Content Strategy

Seed docs from existing README content:

- Getting Started <- README "Install" + "Quick Start"
- Entities <- README "Entities"
- Layers <- README "Layers"
- Sources <- README "Source Types"
- Configuration <- README "Project Configuration" + "Production Deployment"
- Comparison table on landing page <- README "Why Fyrnheim?"

## Style

### Color Palette

- **Background:** Near-black (`#0a0a0a` to `#1a1a1a`)
- **Primary accent:** Amber/lava orange (`#f59e0b` to `#ef4444` gradient)
- **Text:** Off-white (`#f5f5f5`) with muted gray (`#a3a3a3`) for secondary
- **Code blocks:** Slightly lighter dark (`#1e1e1e`) with amber syntax highlights

### Typography

- **Headlines:** Bold sans-serif (Inter or similar), clean and modern
- **Body/docs:** Clean readable sans-serif
- **Code:** JetBrains Mono or Fira Code

### Landing Page Layout

1. **Hero** — fiery landscape as background (image 1), headline "Create data entities from raw sources" in white, `pip install` snippet, CTA button in amber
2. **Features** — dark bg, 3-4 cards with subtle amber glow borders
3. **Code walkthrough** — 4-step quick start, terminal-style code blocks
4. **Comparison table** — vs dbt, amber accents on Fyrnheim column
5. **Footer CTA** — "Get started" + GitHub link

### Messaging

- Headlines: straightforward and technical ("Create data entities from raw sources")
- Explanatory text: Norse/forge flavor ("Forge typed entities from raw data", "Where raw sources are smelted into business objects")
- Subtle, not overdone — the imagery does the heavy lifting

### Image Assets

Source: `~/Downloads/Download_1772794973234/`

- Image 1 (fiery dark landscape) -> hero background
- Image 3 (colored illustration) -> logo/brand mark or docs header
- Images 2 & 4 (monochrome ink) -> section illustrations in docs

## Deployment

- Cloudflare Pages connected to GitHub repo
- Build command: `cd website && npm run build`
- Output directory: `website/dist`
- Auto-deploys on push to main
