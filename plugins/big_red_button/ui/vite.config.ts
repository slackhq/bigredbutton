import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "../static",
    emptyOutDir: true,
    cssCodeSplit: false,
    lib: {
      entry: "src/main.tsx",
      formats: ["es"],
      fileName: () => "big-red-button.js",
    },
    rollupOptions: {
      output: {
        assetFileNames: "big-red-button[extname]",
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
