/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  async rewrites() {
    return [
      // Proxy API + WS to the FastAPI backend in dev so the browser can use
      // a same-origin URL.
      { source: "/api/:path*", destination: "http://127.0.0.1:8011/:path*" },
    ];
  },
};
module.exports = nextConfig;
