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
        {
          label: 'Getting Started',
          items: [
            { label: 'Introduction', slug: 'getting-started/introduction' },
          ],
        },
        {
          label: 'Core Concepts',
          collapsed: true,
          items: [
            { label: 'Sources', slug: 'concepts/sources' },
            { label: 'Activities', slug: 'concepts/activities' },
            { label: 'Identity', slug: 'concepts/identity' },
            { label: 'Entity Models', slug: 'concepts/entity-models' },
            { label: 'Analytics', slug: 'concepts/analytics' },
            { label: 'Primitives', slug: 'concepts/primitives' },
          ],
        },
        { label: 'Configuration', slug: 'configuration' },
        { label: 'API Reference', slug: 'api-reference' },
      ],
    }),
  ],
});
