import { Activity, BookOpen, GitBranch, Info, Radar, Scale } from "lucide-react";
import Link from "next/link";

const links = [
  { href: "/", label: "Overview", icon: Activity },
  { href: "/relationships", label: "Relationships", icon: GitBranch },
  { href: "/detections", label: "Detections", icon: Radar },
  { href: "/markets", label: "Markets", icon: Scale },
  { href: "/methodology", label: "Methodology", icon: BookOpen },
  { href: "/about", label: "About", icon: Info },
];

export function Shell({ children }: { children: React.ReactNode }) {
  return (
    <div className="min-h-screen bg-[#141618] text-[#e8eaed]">
      <div className="mx-auto flex min-h-screen max-w-[1400px]">
        <aside className="hidden w-52 shrink-0 border-r border-[#2c3136] p-4 md:block">
          <p className="text-xs uppercase tracking-[0.18em] text-[#9aa0a6]">Consistency</p>
          <p className="mt-1 text-sm text-[#c5cad0]">Engine</p>
          <nav className="mt-6 flex flex-col gap-1">
            {links.map((link) => (
              <Link
                key={link.href}
                href={link.href}
                className="flex items-center gap-2 rounded px-2 py-1.5 text-sm text-[#c5cad0] hover:bg-[#23282d] focus-visible:outline focus-visible:outline-2 focus-visible:outline-[#d0d4d8]"
              >
                <link.icon className="h-4 w-4" aria-hidden />
                {link.label}
              </Link>
            ))}
          </nav>
        </aside>
        <div className="min-w-0 flex-1">
          <header className="flex gap-3 overflow-x-auto border-b border-[#2c3136] px-4 py-2 md:hidden">
            {links.map((link) => (
              <Link key={link.href} href={link.href} className="shrink-0 text-sm text-[#c5cad0]">
                {link.label}
              </Link>
            ))}
          </header>
          <main className="px-4 py-5 md:px-6">{children}</main>
        </div>
      </div>
    </div>
  );
}
