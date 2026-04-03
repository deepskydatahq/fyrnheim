import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  site: 'https://fyrnheim.dev',
  integrations: [
    starlight({
      title: 'Fyrnheim',
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
            { label: 'Analytics Entities', slug: 'concepts/analytics-entities' },
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
