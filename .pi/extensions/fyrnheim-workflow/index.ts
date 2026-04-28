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

async function resolvePrNumber(pi: ExtensionAPI, args: string): Promise<string> {
  const explicit = args.trim().split(/\s+/)[0];
  if (explicit) return explicit.replace(/^#/, "");
  const result = await pi.exec("gh", ["pr", "view", "--json", "number", "--jq", ".number"], { timeout: 10_000 });
  const number = result.stdout.trim();
  if (!number) throw new Error("Could not infer PR number from current branch; pass one explicitly.");
  return number;
}

function checkState(check: any): string {
  if (check.__typename === "CheckRun") return `${check.name}: ${check.status}${check.conclusion ? `/${check.conclusion}` : ""}`;
  if (check.__typename === "StatusContext") return `${check.context}: ${check.state}`;
  return JSON.stringify(check);
}

function checkIsPassing(check: any): boolean {
  if (check.__typename === "CheckRun") return check.status === "COMPLETED" && check.conclusion === "SUCCESS";
  if (check.__typename === "StatusContext") return check.state === "SUCCESS";
  return false;
}

function checkIsPending(check: any): boolean {
  if (check.__typename === "CheckRun") return check.status !== "COMPLETED";
  if (check.__typename === "StatusContext") return ["PENDING", "EXPECTED"].includes(check.state);
  return false;
}

function isUnresolvedActionableComment(comment: any): boolean {
  const author = comment.user?.login ?? comment.author?.login ?? "";
  const body = comment.body ?? "";
  if (!author.toLowerCase().includes("coderabbit")) return false;
  if (body.includes("✅ Addressed")) return false;
  return body.includes("Potential issue") || body.includes("Actionable comments posted");
}

async function getPrSummary(pi: ExtensionAPI, prNumber: string) {
  const view = await pi.exec(
    "gh",
    [
      "pr",
      "view",
      prNumber,
      "--json",
      "number,title,state,isDraft,mergeable,reviewDecision,statusCheckRollup,headRefName,baseRefName,url,comments,reviews",
    ],
    { timeout: 20_000 },
  );
  if (view.code !== 0) throw new Error(view.stderr || view.stdout || `gh pr view ${prNumber} failed`);
  const pr = JSON.parse(view.stdout);

  const inline = await pi.exec(
    "gh",
    ["api", `repos/{owner}/{repo}/pulls/${prNumber}/comments`, "--paginate", "--slurp"],
    { timeout: 20_000 },
  );
  const inlinePages = inline.code === 0 && inline.stdout.trim() ? JSON.parse(inline.stdout) : [];
  const inlineComments = Array.isArray(inlinePages?.[0]) ? inlinePages.flat() : inlinePages;
  const issueComments = pr.comments ?? [];
  const actionable = [...inlineComments, ...issueComments].filter(isUnresolvedActionableComment);
  const checks = pr.statusCheckRollup ?? [];
  const pendingChecks = checks.filter(checkIsPending);
  const failingChecks = checks.filter((check: any) => !checkIsPending(check) && !checkIsPassing(check));

  return { pr, inlineComments, issueComments, actionable, checks, pendingChecks, failingChecks };
}

function renderPrSummary(summary: any): string {
  const { pr, checks, pendingChecks, failingChecks, actionable } = summary;
  const checkRows = checks.map((check: any) => [check.__typename === "CheckRun" ? check.name : check.context, checkState(check)]);
  return [
    `PR #${pr.number}: ${pr.title}`,
    pr.url,
    `State: ${pr.state}${pr.isDraft ? " (draft)" : ""}`,
    `Branch: ${pr.headRefName} -> ${pr.baseRefName}`,
    `Mergeable: ${pr.mergeable}`,
    `Review decision: ${pr.reviewDecision || "none"}`,
    `Pending checks: ${pendingChecks.length}`,
    `Failing checks: ${failingChecks.length}`,
    `Unresolved actionable comments: ${actionable.length}`,
    "",
    "Checks:",
    table([["Name", "State"], ...checkRows]),
  ].join("\n");
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

  pi.registerCommand("quality-gates", {
    description: "Run Fyrnheim quality gates with uv/.venv/plain fallback",
    handler: async (_args, ctx) => {
      const result = await pi.exec("bash", ["scripts/quality-gates.sh"], { timeout: 120_000 });
      const output = [result.stdout, result.stderr].filter(Boolean).join("\n");
      if (result.code === 0) ctx.ui.notify(`Quality gates passed.\n${output}`, "success");
      else ctx.ui.notify(`Quality gates failed (exit ${result.code}).\n${output}`, "error");
    },
  });

  pi.registerCommand("pr-status", {
    description: "Show GitHub PR checks, CodeRabbit status, mergeability, and actionable comments",
    handler: async (args, ctx) => {
      try {
        const prNumber = await resolvePrNumber(pi, args);
        const summary = await getPrSummary(pi, prNumber);
        ctx.ui.notify(renderPrSummary(summary), summary.actionable.length ? "warning" : "info");
      } catch (error) {
        ctx.ui.notify(error instanceof Error ? error.message : String(error), "error");
      }
    },
  });

  pi.registerCommand("pr-merge-if-ready", {
    description: "Squash-merge a PR only when CI/CodeRabbit pass and no actionable comments remain",
    handler: async (args, ctx) => {
      try {
        const prNumber = await resolvePrNumber(pi, args);
        const summary = await getPrSummary(pi, prNumber);
        if (summary.pr.isDraft) {
          ctx.ui.notify(`PR #${prNumber} is a draft; refusing to merge.`, "error");
          return;
        }
        if (summary.pr.mergeable !== "MERGEABLE") {
          ctx.ui.notify(`PR #${prNumber} mergeable state is ${summary.pr.mergeable}; refusing to merge.`, "error");
          return;
        }
        if (summary.pendingChecks.length || summary.failingChecks.length) {
          ctx.ui.notify(`PR #${prNumber} is not ready.\n${renderPrSummary(summary)}`, "error");
          return;
        }
        if (summary.actionable.length) {
          ctx.ui.notify(`PR #${prNumber} has unresolved actionable comments; refusing to merge.\n${renderPrSummary(summary)}`, "error");
          return;
        }
        const merge = await pi.exec("gh", ["pr", "merge", prNumber, "--squash", "--delete-branch"], { timeout: 120_000 });
        const output = [merge.stdout, merge.stderr].filter(Boolean).join("\n");
        if (merge.code === 0) ctx.ui.notify(`Merged PR #${prNumber}.\n${output}`, "success");
        else ctx.ui.notify(`Merge failed for PR #${prNumber}.\n${output}`, "error");
      } catch (error) {
        ctx.ui.notify(error instanceof Error ? error.message : String(error), "error");
      }
    },
  });
}
