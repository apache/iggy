import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
	base: "/",
	preview: {
		port: 3050,
		strictPort: true,
		host: true,
		allowedHosts: true
	},
	build: {
		outDir: '../dist',
		emptyOutDir: true,
	},
	plugins: [sveltekit()],
	define: {
		SUPERFORMS_LEGACY: true
	}
});
