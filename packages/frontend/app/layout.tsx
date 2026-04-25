import "./globals.css";
import type { Metadata } from "next";
import Link from "next/link";

export const metadata: Metadata = {
  title: "Ed-Fi Text-to-SQL",
  description: "NL → SQL → results for Ed-Fi ODS",
};

const NAV = [
  { href: "/", label: "Query" },
  { href: "/chat", label: "Chat" },
  { href: "/tables", label: "Tables" },
  { href: "/domains", label: "Domains" },
  { href: "/gold", label: "Gold SQL" },
];

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen flex flex-col">
        <header className="border-b border-border bg-panel">
          <div className="max-w-6xl mx-auto px-6 py-3 flex items-center gap-6">
            <Link href="/" className="font-semibold text-accent">
              Ed-Fi Text-to-SQL
            </Link>
            <nav className="flex gap-4 text-sm text-muted">
              {NAV.map((n) => (
                <Link key={n.href} href={n.href} className="hover:text-accent">
                  {n.label}
                </Link>
              ))}
            </nav>
          </div>
        </header>
        <main className="flex-1 max-w-6xl mx-auto px-6 py-6 w-full">
          {children}
        </main>
      </body>
    </html>
  );
}
