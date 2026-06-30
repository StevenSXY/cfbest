#!/usr/bin/env bash
set -euo pipefail

REPO="${GITHUB_REPO:-}"
BRANCH="${GITHUB_BRANCH:-results}"
WORK_DIR="${GITHUB_WORKDIR:-.github-sync}"
MESSAGE="${GITHUB_MESSAGE:-Update IP results and README}"
FILES=("best_ips.txt" "full_ips.txt" "README.md")
PUSH_RETRIES="${GITHUB_PUSH_RETRIES:-3}"
PUSH_RETRY_DELAY="${GITHUB_PUSH_RETRY_DELAY:-10}"

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
WORK_DIR="$(realpath -m "$ROOT/$WORK_DIR")"

die() {
    echo "$*" >&2
    exit 1
}

get_token() {
    if [[ -n "${GITHUB_TOKEN:-}" ]]; then
        printf '%s' "$GITHUB_TOKEN"
        return
    fi
    # Extract token from parent project's git remote URL directly
    local _url
    _url=$(cd "$ROOT/.." && git remote get-url origin 2>/dev/null) || return 1
    # url = https://user:token@host/path — extract token portion
    printf '%s' "$_url" | sed -n 's|.*//[^:]*:\([^@]*\)@.*|\1|p'
}

setup_askpass() {
    local token
    token=$(get_token) || true
    if [[ -z "$token" ]]; then
        echo "Warning: No GitHub token available. Push may fail." >&2
        return
    fi
    local askpass_script
    askpass_script="$(mktemp)"
    printf '#!/usr/bin/env bash\nprintf '%%s' "%s"\n' "$token" > "$askpass_script"
    chmod 755 "$askpass_script"
    export GIT_ASKPASS="$askpass_script"
    echo "Auth: token loaded (${#token} chars)" >&2
}

cleanup_askpass() {
    [[ -n "${GIT_ASKPASS:-}" && -f "$GIT_ASKPASS" ]] && rm -f "$GIT_ASKPASS" || true
}

run_git() {
    local cwd=""
    if [[ "${1:-}" == "--cwd" ]]; then
        cwd="$2"
        shift 2
    fi
    if [[ -n "$cwd" ]]; then
        git -c "safe.directory=$cwd" -C "$cwd" "$@"
    else
        git "$@"
    fi
}

ensure_ready() {
    command -v git >/dev/null 2>&1 || die "git not found"
    command -v realpath >/dev/null 2>&1 || die "realpath not found"
    # Only require GITHUB_REPO when first-time clone (no existing worktree)
    if [[ ! -d "$WORK_DIR/.git" && -z "$REPO" ]]; then
        die 'GITHUB_REPO is required for first-time clone. Set GITHUB_REPO or run start.sh.'
    fi
    for f in "${FILES[@]}"; do
        [[ -f "$f" ]] || die "result file not found: $f"
    done
}

ensure_worktree() {
    if [[ -d "$WORK_DIR/.git" ]]; then
        run_git --cwd "$WORK_DIR" fetch origin "$BRANCH"
        run_git --cwd "$WORK_DIR" reset --hard
        run_git --cwd "$WORK_DIR" checkout -B "$BRANCH" "origin/$BRANCH"
        return
    fi
    if [[ -e "$WORK_DIR" ]] && find "$WORK_DIR" -mindepth 1 -print -quit | grep -q .; then
        die "sync dir not empty: $WORK_DIR"
    fi
    mkdir -p "$(dirname "$WORK_DIR")"
    run_git clone --branch "$BRANCH" --single-branch "$REPO" "$WORK_DIR"
}

copy_results() {
    local f
    for f in "${FILES[@]}"; do
        cp -f "$f" "$WORK_DIR/$f"
        run_git --cwd "$WORK_DIR" add "$f"
    done
}

commit_if_changed() {
    run_git --cwd "$WORK_DIR" diff --cached --quiet && return
    run_git --cwd "$WORK_DIR" \
        -c user.name="IP Update Bot" \
        -c user.email="ip-update-bot@users.noreply.github.com" \
        commit -m "$MESSAGE"
}

push_if_needed() {
    local ahead
    ahead="$(run_git --cwd "$WORK_DIR" rev-list --count "origin/$BRANCH..HEAD" 2>/dev/null || printf '0')"
    if [[ "${ahead:-0}" -le 0 ]]; then
        echo "Nothing to push: already up to date."
        return
    fi
    echo "Pushing $ahead commit(s) to $REPO ($BRANCH)..."
    local attempt=1
    while true; do
        if run_git --cwd "$WORK_DIR" push origin "$BRANCH"; then
            break
        fi
        if [[ "$attempt" -ge "$PUSH_RETRIES" ]]; then
            echo "Push failed after $attempt attempt(s)." >&2
            return 1
        fi
        echo "Push failed; retrying ($((attempt + 1))/$PUSH_RETRIES) in ${PUSH_RETRY_DELAY}s..." >&2
        sleep "$PUSH_RETRY_DELAY"
        attempt=$((attempt + 1))
    done
    echo "Push done"
}

trap cleanup_askpass EXIT
setup_askpass
ensure_ready
ensure_worktree
copy_results
commit_if_changed
push_if_needed
