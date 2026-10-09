import { readFile } from "node:fs/promises";
import path from "node:path";

export async function readRepoDoc(relativePath: string): Promise<string> {
  const root = path.resolve(process.cwd(), "..", "..");
  return readFile(path.join(root, relativePath), "utf8");
}
