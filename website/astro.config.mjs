import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  site: 'https://fyrnheim.dev',
  integrations: [
    starlight({
      title: 'fyrnheim',
      expressiveCode: {
        themes: ['github-dark'],
      },
      head: [
        { tag: 'link', attrs: { rel: 'preconnect', href: 'https://fonts.googleapis.com' } },
        { tag: 'link', attrs: { rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: true } },
        { tag: 'link', attrs: { href: 'https://fonts.googleapis.com/css2?family=Skranji:wght@400;700&display=swap', rel: 'stylesheet' } },
      ],
      favicon: '/favicon.svg',
      customCss: ['./src/styles/custom.css'],
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/deepskydatahq/fyrnheim' },
      ],
      sidebar: [
        { label: 'Getting Started', slug: 'getting-started' },
        {
          label: 'Core Concepts',
          collapsed: true,
          items: [
            { label: 'Entities', slug: 'concepts/entities' },
            { label: 'Layers', slug: 'concepts/layers' },
            { label: 'Sources', slug: 'concepts/sources' },
            { label: 'Source Mapping', slug: 'concepts/source-mapping' },
            { label: 'Primitives', slug: 'concepts/primitives' },
            { label: 'Components', slug: 'concepts/components' },
            { label: 'Quality Checks', slug: 'concepts/quality' },
          ],
        },
        { label: 'Configuration', slug: 'configuration' },
        { label: 'API Reference', slug: 'api-reference' },
      ],
    }),
  ],
});
