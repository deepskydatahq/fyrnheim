import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import * as fs from "node:fs/promises";
import * as path from "node:path";

const VALID_STATUSES = new Set(["draft", "ready", "in_progress", "blocked", "complete", "failed"]);

function firstMatch(text: string, regex: RegExp): string | undefined {
  return text.match(regex)?.[1];
}

function parseList(text: string, key: string): string[] {
  const match = text.match(new RegExp(`${key}\\s*=\\s*\\[([^\\]]*)\\]`, "m"));
  if (!match) return [];
  return [...match[1].matchAll(/"([^"]+)"/g)].map((m) => m[1]);
}

async function listToml(dir: string, prefix = ""): Promise<string[]> {
  try {
    const entries = await fs.readdir(dir);
    return entries
      .filter((entry) => entry.endsWith(".toml") && entry.startsWith(prefix))
      .sort()
      .map((entry) => path.join(dir, entry));
  } catch {
    return [];
  }
}

async function readArtifact(file: string) {
  const text = await fs.readFile(file, "utf8");
  return {
    file,
    text,
    id: firstMatch(text, /^id\s*=\s*"([^"]+)"/m) ?? path.basename(file, ".toml"),
    parent: firstMatch(text, /^parent\s*=\s*"([^"]+)"/m),
    title: firstMatch(text, /^title\s*=\s*"([^"]+)"/m) ?? "(untitled)",
    status: firstMatch(text, /^status\s*=\s*"([^"]+)"/m) ?? "unknown",
    triage: firstMatch(text, /^triage\s*=\s*"([^"]+)"/m),
    dependsOn: parseList(text, "depends_on"),
  };
}

async function findArtifact(cwd: string, kind: "missions" | "epics" | "stories", id: string): Promise<string | undefined> {
  const files = await listToml(path.join(cwd, "product", kind), `${id}-`);
  if (files.length > 0) return files[0];
  const exact = path.join(cwd, "product", kind, `${id}.toml`);
  try {
    await fs.access(exact);
    return exact;
  } catch {
    return undefined;
  }
}

async function setTomlStringField(file: string, key: string, value: string): Promise<void> {
  const text = await fs.readFile(file, "utf8");
  const line = new RegExp(`^${key}\\s*=\\s*"[^"]*"`, "m");
  const next = line.test(text) ? text.replace(line, `${key} = "${value}"`) : `${key} = "${value}"\n${text}`;
  await fs.writeFile(file, next);
}

function table(rows: string[][]): string {
  if (rows.length === 0) return "";
  const widths = rows[0].map((_, col) => Math.max(...rows.map((row) => row[col]?.length ?? 0)));
  return rows.map((row) => row.map((cell, col) => (cell ?? "").padEnd(widths[col])).join("  ")).join("\n");
}

export default function fyrnheimWorkflow(pi: ExtensionAPI) {
  pi.on("before_agent_start", async (event) => {
    return {
      systemPrompt:
        event.systemPrompt +
        "\n\nFyrnheim workflow note: product TOML files are the canonical task system. Do not use Beads/bd unless the user explicitly asks for legacy Beads operations. Stories in product/stories are implementation tasks.",
    };
  });

  pi.registerCommand("fyrnheim-status", {
    description: "Show git and product-story status for Fyrnheim",
    handler: async (_args, ctx) => {
      const git = await pi.exec("git", ["status", "--short", "--branch"], { timeout: 10_000 });
      const stories = await Promise.all((await listToml(path.join(ctx.cwd, "product", "stories"))).map(readArtifact));
      const counts = new Map<string, number>();
      for (const story of stories) counts.set(story.status, (counts.get(story.status) ?? 0) + 1);
      const summary = [...counts.entries()].sort().map(([status, count]) => `${status}: ${count}`).join(", ") || "no stories";
      ctx.ui.notify(`Git:\n${git.stdout || git.stderr || "(clean)"}\nStory status: ${summary}`, "info");
    },
  });

  pi.registerCommand("mission-status", {
    description: "Summarize a mission's epics and stories from product TOML files",
    handler: async (args, ctx) => {
      const missionId = args.trim();
      if (!missionId) {
        ctx.ui.notify("Usage: /mission-status M001", "error");
        return;
      }

      const missionFile = await findArtifact(ctx.cwd, "missions", missionId);
      if (!missionFile) {
        ctx.ui.notify(`Mission not found: ${missionId}`, "error");
        return;
      }

      const mission = await readArtifact(missionFile);
      const epics = await Promise.all((await listToml(path.join(ctx.cwd, "product", "epics"), `${missionId}-E`)).map(readArtifact));
      const stories = await Promise.all((await listToml(path.join(ctx.cwd, "product", "stories"), `${missionId}-E`)).map(readArtifact));

      const lines = [
        `Mission ${mission.id}: ${mission.title}`,
        `Status: ${mission.status}`,
        "",
        "Epics:",
        table([["ID", "Status", "Title"], ...epics.map((e) => [e.id, e.status, e.title])]),
        "",
        "Stories:",
        table([["ID", "Status", "Triage", "Title"], ...stories.map((s) => [s.id, s.status, s.triage ?? "-", s.title])]),
      ];
      ctx.ui.notify(lines.join("\n"), "info");
    },
  });

  pi.registerCommand("story-list", {
    description: "List product stories, optionally filtered by status",
    handler: async (args, ctx) => {
      const status = args.trim();
      const stories = await Promise.all((await listToml(path.join(ctx.cwd, "product", "stories"))).map(readArtifact));
      const filtered = status ? stories.filter((story) => story.status === status) : stories;
      const output = table([["ID", "Status", "Triage", "Title"], ...filtered.map((s) => [s.id, s.status, s.triage ?? "-", s.title])]);
      ctx.ui.notify(output || "No matching stories.", "info");
    },
  });

  pi.registerCommand("story-set-status", {
    description: "Set a story status: /story-set-status <story-id> <draft|ready|in_progress|blocked|complete|failed>",
    handler: async (args, ctx) => {
      const [storyId, status] = args.trim().split(/\s+/);
      if (!storyId || !status || !VALID_STATUSES.has(status)) {
        ctx.ui.notify("Usage: /story-set-status <story-id> <draft|ready|in_progress|blocked|complete|failed>", "error");
        return;
      }
      const file = await findArtifact(ctx.cwd, "stories", storyId);
      if (!file) {
        ctx.ui.notify(`Story not found: ${storyId}`, "error");
        return;
      }
      await setTomlStringField(file, "status", status);
      ctx.ui.notify(`Updated ${path.relative(ctx.cwd, file)}: status = ${status}`, "success");
    },
  });
}
