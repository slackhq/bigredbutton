import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "../static",
    emptyOutDir: true,
    rollupOptions: {
      output: {
        entryFileNames: "big-red-button.js",
        assetFileNames: "big-red-button.[ext]",
      },
    },
  },
  server: {
    proxy: {
      "/big-red-button/api": {
        target: "http://localhost:8080",
        changeOrigin: true,
      },
    },
  },
});
