import { defineConfig } from 'vite';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  root: __dirname,
  base: '/static/dist/',
  resolve: {
    alias: {
      '@': path.resolve(__dirname, 'src'),
    },
  },
  build: {
    outDir: path.resolve(__dirname, 'static', 'dist'),
    emptyOutDir: true,
    manifest: true,
    rollupOptions: {
      input: {
        main: path.resolve(__dirname, 'src', 'main.js'),
      },
      output: {
        manualChunks(id) {
          if (id.includes('node_modules/echarts')) return 'vendor-charts';
          if (id.includes('node_modules/drawflow')) return 'vendor-dag';
          if (id.includes('node_modules/codemirror')) return 'vendor-editors';
          if (id.includes('node_modules/marked') || id.includes('node_modules/dompurify')) return 'vendor-utils';
        },
      },
    },
  },
  server: {
    port: 5173,
    proxy: {
      '/api': { target: process.env.VITE_API_TARGET || 'http://localhost:8000', changeOrigin: true },
      '/ws': { target: (process.env.VITE_API_TARGET || 'http://localhost:8000').replace(/^http/, 'ws'), ws: true },
    },
  },
});
