# Fyrnheim Website

Public website and documentation for Fyrnheim, built with [Astro](https://astro.build/) and [Starlight](https://starlight.astro.build/).

## Local Development

```bash
npm install
npm run dev       # Start dev server at http://localhost:4321
npm run build     # Production build to dist/
npm run preview   # Preview the production build locally
```

## Project Structure

```
website/
  public/            # Static assets (favicon, images)
  src/
    content/docs/    # Documentation pages (Markdown/MDX)
    pages/           # Custom pages (landing page)
    styles/          # Custom CSS (Starlight theme overrides)
  astro.config.mjs   # Astro + Starlight configuration
```

## Deployment — Cloudflare Pages

### Configuration

| Setting              | Value          |
|----------------------|----------------|
| **Build command**    | `npm run build` |
| **Output directory** | `dist`         |
| **Root directory**   | `website`      |
| **Node version**     | `20`           |

### Environment Variables

| Variable         | Value |
|------------------|-------|
| `NODE_VERSION`   | `20`  |

### Setup Steps

1. Connect your GitHub repository to Cloudflare Pages.
2. Set the **root directory** to `website`.
3. Set the **build command** to `npm run build`.
4. Set the **output directory** to `dist`.
5. Under **Environment variables**, set `NODE_VERSION` to `20`.
6. Deploy.

### Custom Domain

To use a custom domain (e.g., `fyrnheim.dev`):

1. Go to your Cloudflare Pages project settings.
2. Under **Custom domains**, add your domain.
3. Configure DNS as instructed by Cloudflare.
