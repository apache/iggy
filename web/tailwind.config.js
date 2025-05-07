/** @type {import('tailwindcss').Config} */
export default {
  content: ['./src/**/*.{html,js,svelte,ts}'],
  darkMode: 'class',
  theme: {
    extend: {
      transitionTimingFunction: {
        'in-expo': 'cubic-bezier(0.95, 0.05, 0.795, 0.035)'
      },
      backdropBlur: {
        xs: '2px',
        xxs: '1px'
      },
      colors: {
        shadeD100: 'var(--shadeD100)',
        shadeD150: 'var(--shadeD150)',
        shadeD170: 'var(--shadeD170)',
        shadeD200: 'var(--shadeD200)',
        shadeD300: 'var(--shadeD300)',
        shadeD400: 'var(--shadeD400)',
        shadeD500: 'var(--shadeD500)',
        shadeD600: 'var(--shadeD600)',
        shadeD700: 'var(--shadeD700)',
        shadeD800: 'var(--shadeD800)',
        shadeD900: 'var(--shadeD900)',
        shadeD1000: 'var(--shadeD1000)',
        shadeD1100: 'var(--shadeD1100)',

        shadeL100: 'var(--shadeL100)',
        shadeL200: 'var(--shadeL200)',
        shadeL300: 'var(--shadeL300)',
        shadeL400: 'var(--shadeL400)',
        shadeL500: 'var(--shadeL500)',
        shadeL600: 'var(--shadeL600)',
        shadeL700: 'var(--shadeL700)',
        shadeL800: 'var(--shadeL800)',
        shadeL900: 'var(--shadeL900)',
        shadeL1000: 'var(--shadeL1000)',

        green100: 'var(--green100)',
        green200: 'var(--green200)',
        green300: 'var(--green300)',
        green400: 'var(--green400)',
        green500: 'var(--green500)'
      }
    }
  },
  plugins: []
};
