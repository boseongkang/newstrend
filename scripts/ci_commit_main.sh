#!/usr/bin/env bash
# ci_commit_main.sh — conflict-safe commit-to-main for CI (2026-08-06).
#
# Why a temp worktree: the old in-place pattern (stash --keep-index →
# commit → pull --rebase -X ours → stash pop) silently poisoned the job
# when the stash pop conflicted: `|| true` swallowed the failure, the
# index was left with unmerged entries, every later step ran on
# conflict-markered files, and the next commit step died with exit 128
# (2026-08-06 run 31091759391 — queued workflow_run built on a stale
# event-time SHA while the 07:25 schedule run pushed the same files).
# Committing from a detached worktree never mutates the build tree, so
# a conflict can never leak into later steps.
#
# Usage:
#   REGEN_PATHS=$'a.json\nsomedir/' APPEND_PATHS=$'hist.json\nlogdir/' \
#     bash scripts/ci_commit_main.sh "commit message"
#
# REGEN_PATHS + APPEND_PATHS together must cover every path this step
# commits. Directories end with "/". Conflict policy (only reachable if
# origin/main moves mid-run, e.g. a manual push — trend-site runs are
# serialized by the `pages` concurrency group):
#   REGEN_PATHS  → ours   (this run's regenerated file wins)
#   APPEND_PATHS → theirs (remote history wins — never clobber it; our
#                          entry is re-added by the next run)
#   unclassified → hard fail, no silent resolution
set -euo pipefail

MSG="${1:?usage: ci_commit_main.sh <commit message>}"
: "${REGEN_PATHS:?REGEN_PATHS is required}"
: "${APPEND_PATHS:?APPEND_PATHS is required}"

GIT_NAME="github-actions[bot]"
GIT_EMAIL="github-actions[bot]@users.noreply.github.com"

ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"

WT=$(mktemp -d)/push-main
cleanup() { git worktree remove --force "$WT" 2>/dev/null || true; git worktree prune 2>/dev/null || true; }
trap cleanup EXIT

# Base the commit on the SHA the outputs were generated FROM (the build
# tree's checkout), NOT on current origin/main: if the build tree is
# stale, committing straight onto the new tip would silently overwrite
# remote history in append-only files without git ever seeing a
# conflict. Building on BASE and merging origin/main recreates the
# proper 3-way comparison, so divergence surfaces as a mergeable/
# conflict case handled by the policy below.
BASE=$(git rev-parse HEAD)
git worktree add --detach "$WT" "$BASE"

ALL_PATHS=$(printf '%s\n%s\n' "$REGEN_PATHS" "$APPEND_PATHS" | sed '/^[[:space:]]*$/d')

# 1) Copy listed outputs from the build tree into the worktree.
while IFS= read -r p; do
  p="${p%/}"
  if [ -d "$ROOT/$p" ]; then
    mkdir -p "$WT/$p"
    # --ignore-times: regenerated JSON often keeps the same byte length
    # (only timestamp digits change), which defeats rsync's size+mtime
    # quick-check and silently drops files from the commit.
    rsync -a --delete --ignore-times "$ROOT/$p/" "$WT/$p/"
  elif [ -f "$ROOT/$p" ]; then
    mkdir -p "$WT/$(dirname "$p")"
    cp -f "$ROOT/$p" "$WT/$p"
  else
    echo "::warning::ci_commit_main: path missing in build tree, skipped: $p"
    continue
  fi
  git -C "$WT" add -A -- "$p"
done <<< "$ALL_PATHS"

if git -C "$WT" diff --cached --quiet; then
  echo "No changes to commit"
  exit 0
fi

gcommit() { git -C "$WT" -c user.name="$GIT_NAME" -c user.email="$GIT_EMAIL" "$@"; }
gcommit commit -m "$MSG"

# Longest matching list entry wins (so reports/history/ can be append
# inside an otherwise-regenerated reports/). Files match exactly, dirs
# match by prefix. Prints "ours", "theirs", or nothing.
classify() {
  local f="$1" best_len=0 best=""
  local e
  while IFS= read -r e; do
    e="${e%/}"; [ -z "$e" ] && continue
    if [ "$f" = "$e" ] || [[ "$f" == "$e"/* ]]; then
      [ "${#e}" -gt "$best_len" ] && { best_len=${#e}; best="ours"; }
    fi
  done <<< "$REGEN_PATHS"
  while IFS= read -r e; do
    e="${e%/}"; [ -z "$e" ] && continue
    if [ "$f" = "$e" ] || [[ "$f" == "$e"/* ]]; then
      [ "${#e}" -gt "$best_len" ] && { best_len=${#e}; best="theirs"; }
    fi
  done <<< "$APPEND_PATHS"
  printf '%s' "$best"
}

# Merge origin/main under the file policy (no-op when not diverged),
# then push; on a rejected push (remote moved mid-attempt) re-fetch,
# re-merge and retry.
merge_policy() {
  git -C "$WT" fetch origin main
  git -C "$WT" merge-base --is-ancestor origin/main HEAD && return 0
  if ! gcommit merge --no-edit origin/main; then
    local conflicted f side
    conflicted=$(git -C "$WT" diff --name-only --diff-filter=U)
    while IFS= read -r f; do
      [ -z "$f" ] && continue
      side=$(classify "$f")
      case "$side" in
        ours)   echo "conflict: $f → ours (regenerated)"
                git -C "$WT" checkout --ours   -- "$f" ;;
        theirs) echo "conflict: $f → theirs (append-only)"
                git -C "$WT" checkout --theirs -- "$f" ;;
        *)      echo "::error::ci_commit_main: conflicted file not classified — add it to REGEN_PATHS or APPEND_PATHS: $f"
                git -C "$WT" merge --abort 2>/dev/null || true
                exit 1 ;;
      esac
      git -C "$WT" add -- "$f"
    done <<< "$conflicted"
    gcommit commit --no-edit
  fi
}

pushed=""
for i in 1 2 3 4 5; do
  merge_policy
  if git -C "$WT" push origin HEAD:main; then
    pushed=yes; break
  fi
  echo "push attempt $i rejected — remote moved, retrying"
  sleep $((RANDOM % 5 + 1))
done
if [ -z "$pushed" ]; then
  echo "::error::ci_commit_main: push failed after 5 attempts"
  exit 1
fi

# Soft-sync the build tree's HEAD (no file changes) so the freshness
# gate's `git diff HEAD -- <path>` sees these files as committed, same
# as the old in-place commit did.
git fetch origin main
git update-ref HEAD "$(git -C "$WT" rev-parse HEAD)"
echo "Pushed $(git -C "$WT" rev-parse --short HEAD) to main"
