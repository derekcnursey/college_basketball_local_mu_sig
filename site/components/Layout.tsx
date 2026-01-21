import Link from "next/link";
import { useRouter } from "next/router";
import { ReactNode } from "react";

export default function Layout({ children }: { children: ReactNode }) {
  const router = useRouter();
  return (
    <div className="page">
      <header className="site-header">
        <div className="brand">
          <span className="brand-title">College Hoops Edge</span>
          <span className="brand-subtitle">Predictions tracker</span>
        </div>
        <nav className="nav">
          <Link href="/" className={router.pathname === "/" ? "active" : ""}>
            Today
          </Link>
          <Link
            href="/history"
            className={router.pathname === "/history" ? "active" : ""}
          >
            History
          </Link>
          <Link
            href="/metrics"
            className={router.pathname === "/metrics" ? "active" : ""}
          >
            Metrics
          </Link>
        </nav>
      </header>
      <main className="content">{children}</main>
    </div>
  );
}
