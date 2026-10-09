import type { Metadata } from "next";

import { Providers } from "@/components/providers";
import { Shell } from "@/components/shell";

import "./globals.css";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "Consistency Engine",
  description: "Synthetic probability-consistency dashboard. Not an official Kalshi product.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <Providers>
          <Shell>{children}</Shell>
        </Providers>
      </body>
    </html>
  );
}
