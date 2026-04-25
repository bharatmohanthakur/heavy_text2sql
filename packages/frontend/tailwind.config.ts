import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{ts,tsx}",
    "./components/**/*.{ts,tsx}",
    "./lib/**/*.{ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        bg: "#0b0d12",
        panel: "#11151c",
        border: "#1c2129",
        accent: "#5dd2c2",
        muted: "#8a91a0",
      },
      fontFamily: {
        sans: ["ui-sans-serif", "system-ui", "-apple-system", "Inter"],
        mono: ["ui-monospace", "SFMono-Regular", "Menlo"],
      },
    },
  },
  plugins: [],
};

export default config;
