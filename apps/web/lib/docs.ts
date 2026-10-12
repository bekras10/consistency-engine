import { readFile } from "node:fs/promises";
import path from "node:path";

/**
 * Read a repository markdown file.
 *
 * `next dev` runs with cwd `apps/web`, so the repo root is two directories up.
 * The Compose image sets `REPO_CONTENT_ROOT` and copies the same files there.
 * A missing file throws. The page does not invent the document.
 */
export async function readRepoDoc(relativePath: string): Promise<string> {
  const configured = process.env.REPO_CONTENT_ROOT;
  const root = configured
    ? path.resolve(configured)
    : path.resolve(process.cwd(), "..", "..");
  return readFile(path.join(root, relativePath), "utf8");
}
