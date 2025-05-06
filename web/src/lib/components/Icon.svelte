<script module lang="ts">
  const icons = {
    menu: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><line x1="4" y1="6" x2="20" y2="6"></line><line x1="4" y1="12" x2="20" y2="12"></line><line x1="4" y1="18" x2="20" y2="18"></line>`,
    home: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path> <polyline points="5 12 3 12 12 3 21 12 19 12"></polyline> <path d="M5 12v7a2 2 0 0 0 2 2h10a2 2 0 0 0 2 -2v-7"></path> <path d="M9 21v-6a2 2 0 0 1 2 -2h2a2 2 0 0 1 2 2v6"></path>`,
    stream: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M21 12h-2c-.894 0 -1.662 -.857 -1.761 -2c-.296 -3.45 -.749 -6 -2.749 -6s-2.5 3.582 -2.5 8s-.5 8 -2.5 8s-2.452 -2.547 -2.749 -6c-.1 -1.147 -.867 -2 -1.763 -2h-2"></path>`,
    clients: ` <path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M10 13a2 2 0 1 0 4 0a2 2 0 0 0 -4 0"></path><path d="M8 21v-1a2 2 0 0 1 2 -2h4a2 2 0 0 1 2 2v1"></path><path d="M15 5a2 2 0 1 0 4 0a2 2 0 0 0 -4 0"></path><path d="M17 10h2a2 2 0 0 1 2 2v1"></path><path d="M5 5a2 2 0 1 0 4 0a2 2 0 0 0 -4 0"></path><path d="M3 13v-1a2 2 0 0 1 2 -2h2"></path>`,
    stats: ` <path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M3 4m0 1a1 1 0 0 1 1 -1h16a1 1 0 0 1 1 1v10a1 1 0 0 1 -1 1h-16a1 1 0 0 1 -1 -1z"></path><path d="M7 20h10"></path><path d="M9 16v4"></path><path d="M15 16v4"></path><path d="M9 12v-4"></path><path d="M12 12v-1"></path><path d="M15 12v-2"></path><path d="M12 12v-1"></path>`,
    // calendar: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path> <rect x="4" y="5" width="16" height="16" rx="2"></rect> <line x1="16" y1="3" x2="16" y2="7"></line> <line x1="8" y1="3" x2="8" y2="7"></line> <line x1="4" y1="11" x2="20" y2="11"></line> <rect x="8" y="15" width="2" height="2"></rect>`,
    timeline: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M4 16l6 -7l5 5l5 -6"></path><circle cx="15" cy="14" r="1"></circle><circle cx="10" cy="9" r="1"></circle><circle cx="4" cy="16" r="1"></circle><circle cx="20" cy="8" r="1"></circle>`,
    logs: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M14 3v4a1 1 0 0 0 1 1h4"></path><path d="M17 21h-10a2 2 0 0 1 -2 -2v-14a2 2 0 0 1 2 -2h7l5 5v11a2 2 0 0 1 -2 2z"></path><path d="M9 9l1 0"></path><path d="M9 13l6 0"></path><path d="M9 17l6 0"></path>`,
    plus: ` <path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M12 5l0 14"></path><path d="M5 12l14 0"></path>`,
    close: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M18 6l-12 12"></path><path d="M6 6l12 12"></path>`,
    refresh: `<path stroke="none" d="M0 0h24v24H0z" fill="none" /><path d="M20 11a8.1 8.1 0 0 0 -15.5 -2m-.5 -4v4h4" /><path d="M4 13a8.1 8.1 0 0 0 15.5 2m.5 4v-4h-4" />`,
    settings: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M10.325 4.317c.426 -1.756 2.924 -1.756 3.35 0a1.724 1.724 0 0 0 2.573 1.066c1.543 -.94 3.31 .826 2.37 2.37a1.724 1.724 0 0 0 1.065 2.572c1.756 .426 1.756 2.924 0 3.35a1.724 1.724 0 0 0 -1.066 2.573c.94 1.543 -.826 3.31 -2.37 2.37a1.724 1.724 0 0 0 -2.572 1.065c-.426 1.756 -2.924 1.756 -3.35 0a1.724 1.724 0 0 0 -2.573 -1.066c-1.543 .94 -3.31 -.826 -2.37 -2.37a1.724 1.724 0 0 0 -1.065 -2.572c-1.756 -.426 -1.756 -2.924 0 -3.35a1.724 1.724 0 0 0 1.066 -2.573c-.94 -1.543 .826 -3.31 2.37 -2.37c1 .608 2.296 .07 2.572 -1.065z"></path><circle cx="12" cy="12" r="3"></circle>`,
    trash: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M4 7l16 0"></path><path d="M10 11l0 6"></path><path d="M14 11l0 6"></path><path d="M5 7l1 12a2 2 0 0 0 2 2h8a2 2 0 0 0 2 -2l1 -12"></path><path d="M9 7v-3a1 1 0 0 1 1 -1h4a1 1 0 0 1 1 1v3"></path>`,
    // logout: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M14 8v-2a2 2 0 0 0 -2 -2h-7a2 2 0 0 0 -2 2v12a2 2 0 0 0 2 2h7a2 2 0 0 0 2 -2v-2"></path><path d="M7 12h14l-3 -3m0 6l3 -3"></path>`,
    // group: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><rect x="4" y="4" width="6" height="5" rx="2"></rect><rect x="4" y="13" width="6" height="7" rx="2"></rect><rect x="14" y="4" width="6" height="7" rx="2"></rect><rect x="14" y="15" width="6" height="5" rx="2"></rect>`,
    chevronRight: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><polyline points="9 6 15 12 9 18"></polyline>`,
    narrowArrowLeft: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M5 12l14 0"></path><path d="M5 12l4 4"></path><path d="M5 12l4 -4"></path>`,
    narrowArrowRight: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M5 12l14 0"></path><path d="M15 16l4 -4"></path><path d="M15 8l4 4"></path>`,
    arrowLeft: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M5 12l14 0"></path><path d="M5 12l6 6"></path><path d="M5 12l6 -6"></path>`,
    // chevronLeft: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><polyline points="15 6 9 12 15 18"></polyline>`,
    chevronDown: `  <path stroke="none" d="M0 0h24v24H0z" fill="none" /><path d="M6 9l6 6l6 -6" />`,
    arrowRight: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M5 12l14 0"></path><path d="M13 18l6 -6"></path><path d="M13 6l6 6"></path>`,
    arrowUp: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M12 5l0 14"></path><path d="M18 11l-6 -6"></path><path d="M6 11l6 -6"></path>`,
    arrowDown: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M12 5l0 14"></path><path d="M18 13l-6 6"></path><path d="M6 13l6 6"></path>`,
    moon: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M12 3c.132 0 .263 0 .393 0a7.5 7.5 0 0 0 7.92 12.446a9 9 0 1 1 -8.313 -12.454z"></path>`,
    sun: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M12 12m-4 0a4 4 0 1 0 8 0a4 4 0 1 0 -8 0"></path><path d="M3 12h1m8 -9v1m8 8h1m-9 8v1m-6.4 -15.4l.7 .7m12.1 -.7l-.7 .7m0 11.4l.7 .7m-12.1 -.7l-.7 .7"></path>`,
    login: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M9 8v-2a2 2 0 0 1 2 -2h7a2 2 0 0 1 2 2v12a2 2 0 0 1 -2 2h-7a2 2 0 0 1 -2 -2v-2"></path><path d="M3 12h13l-3 -3"></path><path d="M13 15l3 -3"></path>`,
    logout: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M10 8v-2a2 2 0 0 1 2 -2h7a2 2 0 0 1 2 2v12a2 2 0 0 1 -2 2h-7a2 2 0 0 1 -2 -2v-2"></path><path d="M15 12h-12l3 -3"></path><path d="M6 15l-3 -3"></path>`,
    userCircle: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M12 12m-9 0a9 9 0 1 0 18 0a9 9 0 1 0 -18 0"></path><path d="M12 10m-3 0a3 3 0 1 0 6 0a3 3 0 1 0 -6 0"></path><path d="M6.168 18.849a4 4 0 0 1 3.832 -2.849h4a4 4 0 0 1 3.834 2.855">`,
    user: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M8 7a4 4 0 1 0 8 0a4 4 0 0 0 -8 0"></path><path d="M6 21v-2a4 4 0 0 1 4 -4h4a4 4 0 0 1 4 4v2"></path>`,
    adjustments: ` <path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M14 6m-2 0a2 2 0 1 0 4 0a2 2 0 1 0 -4 0"></path><path d="M4 6l8 0"></path><path d="M16 6l4 0"></path><path d="M8 12m-2 0a2 2 0 1 0 4 0a2 2 0 1 0 -4 0"></path><path d="M4 12l2 0"></path><path d="M10 12l10 0"></path><path d="M17 18m-2 0a2 2 0 1 0 4 0a2 2 0 1 0 -4 0"></path><path d="M4 18l11 0"></path><path d="M19 18l1 0"></path>`,
    usersGroup: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M10 13a2 2 0 1 0 4 0a2 2 0 0 0 -4 0"></path><path d="M8 21v-1a2 2 0 0 1 2 -2h4a2 2 0 0 1 2 2v1"></path><path d="M15 5a2 2 0 1 0 4 0a2 2 0 0 0 -4 0"></path><path d="M17 10h2a2 2 0 0 1 2 2v1"></path><path d="M5 5a2 2 0 1 0 4 0a2 2 0 0 0 -4 0"></path><path d="M3 13v-1a2 2 0 0 1 2 -2h2"></path>`,
    shieldCode: ` <path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M12 21a12 12 0 0 1 -8.5 -15a12 12 0 0 0 8.5 -3a12 12 0 0 0 8.5 3a12 12 0 0 1 -.078 7.024"></path><path d="M20 21l2 -2l-2 -2"></path><path d="M17 17l-2 2l2 2"></path>`,
    search: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M10 10m-7 0a7 7 0 1 0 14 0a7 7 0 1 0 -14 0"></path><path d="M21 21l-6 -6"></path>`,
    verticalDots: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><circle cx="12" cy="12" r="1"></circle><circle cx="12" cy="19" r="1"></circle><circle cx="12" cy="5" r="1"></circle>`,
    editPen: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M4 20h4l10.5 -10.5a2.828 2.828 0 1 0 -4 -4l-10.5 10.5v4"></path><path d="M13.5 6.5l4 4"></path>`,
    shieldLock: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M12 3a12 12 0 0 0 8.5 3a12 12 0 0 1 -8.5 15a12 12 0 0 1 -8.5 -15a12 12 0 0 0 8.5 -3"></path><path d="M12 11m-1 0a1 1 0 1 0 2 0a1 1 0 1 0 -2 0"></path><path d="M12 12l0 2.5"></path>`,
    analytics: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M10 9m0 1.105a1.105 1.105 0 0 1 1.105 -1.105h1.79a1.105 1.105 0 0 1 1.105 1.105v9.79a1.105 1.105 0 0 1 -1.105 1.105h-1.79a1.105 1.105 0 0 1 -1.105 -1.105z"></path><path d="M17 3m0 1.105a1.105 1.105 0 0 1 1.105 -1.105h1.79a1.105 1.105 0 0 1 1.105 1.105v15.79a1.105 1.105 0 0 1 -1.105 1.105h-1.79a1.105 1.105 0 0 1 -1.105 -1.105z"></path><path d="M5 19m-2 0a2 2 0 1 0 4 0a2 2 0 1 0 -4 0"></path>`,
    terminal: ` <path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M8 9l3 3l-3 3"></path><path d="M13 15l3 0"></path><path d="M3 4m0 2a2 2 0 0 1 2 -2h14a2 2 0 0 1 2 2v12a2 2 0 0 1 -2 2h-14a2 2 0 0 1 -2 -2z"></path>`,
    caretDown: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M6 10l6 6l6 -6h-12"></path>`,
    caretUp: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M18 14l-6 -6l-6 6h12"></path>`,
    checkCircle: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M12 12m-9 0a9 9 0 1 0 18 0a9 9 0 1 0 -18 0"></path><path d="M9 12l2 2l4 -4"></path>`,
    alertCircle: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M3 12a9 9 0 1 0 18 0a9 9 0 0 0 -18 0"></path><path d="M12 8v4"></path><path d="M12 16h.01"></path>`,
    infoCircle: ` <path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M3 12a9 9 0 1 0 18 0a9 9 0 0 0 -18 0"></path><path d="M12 9h.01"></path><path d="M11 12h1v4h1"></path>`,
    eyeOff: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M10.585 10.587a2 2 0 0 0 2.829 2.828"></path><path d="M16.681 16.673a8.717 8.717 0 0 1 -4.681 1.327c-3.6 0 -6.6 -2 -9 -6c1.272 -2.12 2.712 -3.678 4.32 -4.674m2.86 -1.146a9.055 9.055 0 0 1 1.82 -.18c3.6 0 6.6 2 9 6c-.666 1.11 -1.379 2.067 -2.138 2.87"></path><path d="M3 3l18 18"></path>`,
    eye: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M10 12a2 2 0 1 0 4 0a2 2 0 0 0 -4 0"></path><path d="M21 12c-2.4 4 -5.4 6 -9 6c-3.6 0 -6.6 -2 -9 -6c2.4 -4 5.4 -6 9 -6c3.6 0 6.6 2 9 6"></path>`,
    selector: ` <path stroke="none" d="M0 0h24v24H0z" fill="none" /><path d="M8 9l4 -4l4 4" /><path d="M16 15l-4 4l-4 -4" />`,
    deviceDesktop: ` <path stroke="none" d="M0 0h24v24H0z" fill="none" /><path d="M3 5a1 1 0 0 1 1 -1h16a1 1 0 0 1 1 1v10a1 1 0 0 1 -1 1h-16a1 1 0 0 1 -1 -1v-10z" /><path d="M7 20h10" /><path d="M9 16v4" /><path d="M15 16v4" />`

    // trash: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><line x1="4" y1="7" x2="20" y2="7"></line><line x1="10" y1="11" x2="10" y2="17"></line><line x1="14" y1="11" x2="14" y2="17"></line><path d="M5 7l1 12a2 2 0 0 0 2 2h8a2 2 0 0 0 2 -2l1 -12"></path><path d="M9 7v-3a1 1 0 0 1 1 -1h4a1 1 0 0 1 1 1v3"></path>`,
    // circle: '<circle cx="12" cy="12" r="4"/>',
    // check: '<path d=M5 12l4 4l19 7/>',
    // user: `<path stroke="none" d="M0 0h24v24H0z" fill="none"></path><circle cx="12" cy="12" r="9"></circle><circle cx="12" cy="10" r="3"></circle><path d="M6.168 18.849a4 4 0 0 1 3.832 -2.849h4a4 4 0 0 1 3.834 2.855"></path>`
  };

  export type iconType = keyof typeof icons;
</script>

<script lang="ts">
  import { twMerge } from 'tailwind-merge';

  interface Props {
    strokeWidth?: number;
    name: iconType;
    class?: string;
  }

  let { strokeWidth = 1.7, name, class: className = '' }: Props = $props();
  
</script>

<svg
  viewBox="0 0 24 24"
  stroke-width={strokeWidth}
  class={twMerge('w-[24px] h-[24px] fill-none stroke-current shrink-0', className)}
  stroke-linecap="round"
  stroke-linejoin="round"
>
  {@html icons[name]}
</svg>
