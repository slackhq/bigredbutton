import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import cssInjectedByJsPlugin from "vite-plugin-css-injected-by-js";

export default defineConfig({
  define: {
    "process.env.NODE_ENV": JSON.stringify("production"),
  },
  plugins: [react(), cssInjectedByJsPlugin()],
  build: {
    outDir: "../static",
    emptyOutDir: true,
    lib: {
      entry: "src/main.tsx",
      formats: ["umd"],
      name: "AirflowPlugin",
    },
    rollupOptions: {
      external: ["react", "react-dom", "react/jsx-runtime"],
      output: {
        entryFileNames: "big-red-button.js",
        globals: {
          react: "React",
          "react-dom": "ReactDOM",
          "react/jsx-runtime": "ReactJSXRuntime",
        },
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
