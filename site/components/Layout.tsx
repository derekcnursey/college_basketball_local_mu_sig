import Link from "next/link";
import { ReactNode } from "react";

export default function Layout({ children }: { children: ReactNode }) {
  return (
    <div className="page">
      <header className="site-header">
        <div className="brand">
          <span className="brand-title">College Hoops Edge</span>
          <span className="brand-subtitle">Predictions tracker</span>
        </div>
        <nav className="nav">
          <Link href="/">Today</Link>
          <Link href="/history">History</Link>
          <Link href="/metrics">Metrics</Link>
        </nav>
      </header>
      <main className="content">{children}</main>
    </div>
  );
}
