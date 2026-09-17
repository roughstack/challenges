#!/bin/sh
set -eu

tracked_paths=$(git ls-files)
forbidden_tracked=$(printf '%s\n' "$tracked_paths" | grep -E '(^|/)(__pycache__|\.pytest_cache|\.mypy_cache|\.ruff_cache|node_modules|coverage|tmp|work)(/|$)|\.(pyc|pyo|class|o|so|dylib|dll|exe|log|tmp|swp)$|(^|/)(\.env($|\.)|coverage\.out$)' | grep -v '^\.env\.example$' || true)

if [ -n "$forbidden_tracked" ]; then
  echo "error: generated, secret, cache, or private work artifact is tracked" >&2
  printf '%s\n' "$forbidden_tracked" >&2
  exit 1
fi

history_paths=$(git log --all --name-only --format= | sort -u)
if printf '%s\n' "$history_paths" | grep -E '(^|/)(__pycache__|claude_code_dumps|work)(/|$)|\.(pyc|pyo)$' >/dev/null; then
  echo "error: forbidden generated or private artifact exists in reachable history" >&2
  printf '%s\n' "$history_paths" | grep -E '(^|/)(__pycache__|claude_code_dumps|work)(/|$)|\.(pyc|pyo)$' >&2
  exit 1
fi

matches_file=${TMPDIR:-/tmp}/bytearena-arenas-hygiene-matches-$$
if git grep -I -l -E 'ByteArena-Open-Source-Pivot-Spec|roughstack-arena-generation-spec|root_byteareana|work/(ollama-runs|worktree-runs)' -- . ':(exclude)scripts/check-repository-hygiene.sh' >"$matches_file" 2>/dev/null; then
  echo "error: private workspace reference found in public files" >&2
  sed -n '1,50p' "$matches_file" >&2
  rm -f "$matches_file"
  exit 1
fi
rm -f "$matches_file"

echo "repository hygiene checks passed"
