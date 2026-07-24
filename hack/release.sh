#!/usr/bin/env bash
set -euo pipefail

REPO=""
IMAGE_BASE=""
UPSTREAM_REMOTE="origin"
DRY_RUN=false
FORCE=false
VERSION_OVERRIDE=""

# --- Colors ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

info()    { echo -e "${BLUE}ℹ${NC}  $*"; }
success() { echo -e "${GREEN}✔${NC}  $*"; }
warn()    { echo -e "${YELLOW}⚠${NC}  $*"; }
error()   { echo -e "${RED}✖${NC}  $*" >&2; }
header()  { echo -e "\n${BOLD}${CYAN}$*${NC}"; }
dry_run() { echo -e "   ${YELLOW}[DRY RUN]${NC} $*"; }

run_cmd() {
    if $DRY_RUN; then
        dry_run "$*"
    else
        info "Running: $*"
        eval "$@"
    fi
}

usage() {
    cat <<EOF
Usage:
  $(basename "$0") minor   [OPTIONS]    Create a new minor release (v0.X.0) from origin/main
  $(basename "$0") patch   v0.X [OPTIONS]  Create a patch release (v0.X.Y) on an existing release branch

Options:
  --dry-run              Show what would happen without making changes
  --upstream-remote NAME Override the remote name (default: origin)
  --version VERSION      Override the auto-detected version (e.g., v0.11.0)
  --force                Proceed even when there are no new commits
  -h, --help             Show this help message

Examples:
  $(basename "$0") minor --dry-run
  $(basename "$0") patch v0.10 --dry-run
  $(basename "$0") minor --version v0.11.0    # resume a partial release
EOF
    exit 0
}

# --- Pre-flight checks ---
preflight() {
    header "Pre-flight checks"

    if ! command -v gh &>/dev/null; then
        error "gh CLI not found. Install it: https://cli.github.com"
        exit 1
    fi
    if ! gh auth status &>/dev/null 2>&1; then
        error "gh CLI not authenticated. Run: gh auth login"
        exit 1
    fi
    success "gh CLI authenticated"

    if ! git remote get-url "$UPSTREAM_REMOTE" &>/dev/null; then
        error "Remote '${UPSTREAM_REMOTE}' not found."
        echo "  Add it with: git remote add ${UPSTREAM_REMOTE} git@github.com:<owner>/<repo>.git"
        exit 1
    fi

    local remote_url
    remote_url=$(git remote get-url "$UPSTREAM_REMOTE")
    REPO=$(echo "$remote_url" | sed -E 's#.*github\.com[:/]##' | sed 's/\.git$//')
    IMAGE_BASE="ghcr.io/${REPO}"
    success "Remote '${UPSTREAM_REMOTE}': ${remote_url}"
    success "Repo: ${REPO}"

    info "Fetching ${UPSTREAM_REMOTE}..."
    git fetch "$UPSTREAM_REMOTE" --tags --force --quiet
    success "Fetched latest refs and tags"
}

# --- Version helpers ---
get_all_tags() {
    git tag -l 'v*' | sort -V
}

get_latest_minor() {
    get_all_tags | { grep -oP 'v\K[0-9]+\.[0-9]+' || true; } | sort -t. -k1,1n -k2,2n | tail -1
}

get_latest_patch_for_minor() {
    local minor="$1"
    get_all_tags | { grep -P "^v${minor}\.[0-9]+$" || true; } | sort -V | tail -1
}

get_max_patch_number() {
    local minor="$1"
    get_all_tags | { grep -P "^v${minor}\.[0-9]+$" || true; } | { grep -oP '\.[0-9]+$' || true; } | tr -d '.' | sort -n | tail -1
}

get_latest_tag() {
    get_all_tags | tail -1
}

# --- Minor release ---
do_minor() {
    preflight

    header "Version detection"

    local latest_minor
    latest_minor=$(get_latest_minor)
    if [[ -z "$latest_minor" ]]; then
        error "No existing tags found. Cannot determine next version."
        exit 1
    fi

    local next_version next_minor_num release_branch prev_release_branch prev_tag

    if [[ -n "$VERSION_OVERRIDE" ]]; then
        next_version="${VERSION_OVERRIDE}"
        next_minor_num=$(echo "$next_version" | grep -oP 'v0\.\K[0-9]+')
        release_branch="release-v0.${next_minor_num}.x"

        local prev_minor_num=$((next_minor_num - 1))
        prev_release_branch="release-v0.${prev_minor_num}.x"
        prev_tag=$(get_latest_patch_for_minor "0.${prev_minor_num}")
    else
        local latest_minor_num
        latest_minor_num=$(echo "$latest_minor" | cut -d. -f2)
        next_minor_num=$((latest_minor_num + 1))
        next_version="v0.${next_minor_num}.0"
        release_branch="release-v0.${next_minor_num}.x"

        prev_release_branch="release-v0.${latest_minor_num}.x"
        prev_tag=$(get_latest_patch_for_minor "0.${latest_minor_num}")
    fi

    info "Latest minor: v${latest_minor}"
    info "Previous release branch: ${prev_release_branch}"
    info "Latest tag on branch: ${prev_tag:-none}"
    info "Next version: ${BOLD}${next_version}${NC}"

    local branch_exists=false
    if git rev-parse "${UPSTREAM_REMOTE}/${release_branch}" &>/dev/null 2>&1; then
        branch_exists=true
    fi

    if ! git rev-parse "$next_version" &>/dev/null 2>&1; then
        error "Tag ${next_version} does not exist. Create and push the tag first."
        exit 1
    fi


    local commits_since
    if git rev-parse "${UPSTREAM_REMOTE}/${prev_release_branch}" &>/dev/null 2>&1; then
        commits_since="${UPSTREAM_REMOTE}/${prev_release_branch}"
    elif [[ -n "$prev_tag" ]]; then
        commits_since="${prev_tag}"
    fi

    if [[ -n "${commits_since:-}" ]]; then
        local total_commits
        total_commits=$(git rev-list --count "${commits_since}..${UPSTREAM_REMOTE}/main")

        if (( total_commits == 0 )); then
            echo ""
            warn "No new commits on ${UPSTREAM_REMOTE}/main since ${commits_since}"
            if ! $FORCE; then
                error "Nothing to release. Use --force to proceed anyway."
                exit 1
            fi
        else
            header "Commits included (${commits_since}..${UPSTREAM_REMOTE}/main) — ${total_commits} commits"
            git log --oneline "${commits_since}..${UPSTREAM_REMOTE}/main" | head -30
            if (( total_commits > 30 )); then
                echo "  ... and $((total_commits - 30)) more commits"
            fi
            echo ""
        fi
    fi

    if $DRY_RUN; then
        header "Release plan (DRY RUN — no changes will be made)"
        echo ""
        dry_run "git branch ${release_branch} ${next_version}    # create release branch"
        dry_run "git push ${UPSTREAM_REMOTE} ${release_branch}            # push release branch"
        dry_run "gh release create ${next_version} --repo ${REPO} --generate-notes --draft --notes-start-tag ${prev_tag:-v0.0.0}"
        echo ""
        info "Images that will be built by CI:"
        echo "     ${IMAGE_BASE}:${next_version}"
        echo "     ${IMAGE_BASE}-mcp-server:${next_version}"
        echo ""
        info "To execute, run:  $(basename "$0") minor"
        return
    fi

    header "Creating minor release ${next_version}"

    if $branch_exists; then
        success "Branch ${release_branch} already exists — skipping"
    else
        run_cmd git branch "$release_branch" "$next_version"
        run_cmd git push "$UPSTREAM_REMOTE" "$release_branch"
        success "Created and pushed branch ${release_branch}"
    fi

    if gh release view "$next_version" --repo "$REPO" &>/dev/null 2>&1; then
        success "GitHub release ${next_version} already exists — skipping"
    else
        header "Creating draft GitHub release"
        local notes_flag=""
        if [[ -n "$prev_tag" ]]; then
            notes_flag="--notes-start-tag ${prev_tag}"
        fi
        run_cmd gh release create "$next_version" \
            --repo "$REPO" \
            --generate-notes \
            --draft \
            $notes_flag
    fi

    echo ""
    success "Draft release ${next_version} created!"
    info "Images:"
    echo "     ${IMAGE_BASE}:${next_version}"
    echo "     ${IMAGE_BASE}-mcp-server:${next_version}"
    info "Release: https://github.com/${REPO}/releases/tag/${next_version}"
}

# --- Patch release ---
do_patch() {
    local minor_input="$1"
    local minor
    minor=$(echo "$minor_input" | sed 's/^v//')

    if [[ ! "$minor" =~ ^[0-9]+\.[0-9]+$ ]]; then
        error "Invalid minor version format: ${minor_input}"
        echo "  Expected format: v0.X or 0.X (e.g., v0.10 or 0.10)"
        exit 1
    fi

    preflight

    header "Version detection"

    local release_branch="release-v${minor}.x"
    if ! git rev-parse "${UPSTREAM_REMOTE}/${release_branch}" &>/dev/null 2>&1; then
        error "Release branch ${UPSTREAM_REMOTE}/${release_branch} not found."
        echo "  Available release branches:"
        git branch -r --list "${UPSTREAM_REMOTE}/release-*" | sed 's/^/    /'
        exit 1
    fi
    success "Release branch: ${UPSTREAM_REMOTE}/${release_branch}"

    local latest_patch_tag
    latest_patch_tag=$(get_latest_patch_for_minor "$minor")
    local max_patch
    max_patch=$(get_max_patch_number "$minor")

    if [[ -z "$max_patch" ]]; then
        error "No existing tags found for v${minor}.x"
        exit 1
    fi

    local next_version
    if [[ -n "$VERSION_OVERRIDE" ]]; then
        next_version="${VERSION_OVERRIDE}"
        local override_patch="${next_version##*.}"
        if (( override_patch > 0 )); then
            latest_patch_tag="v${minor}.$((override_patch - 1))"
        fi
    else
        local next_patch=$((max_patch + 1))
        next_version="v${minor}.${next_patch}"
    fi

    info "Existing v${minor}.x tags: $(get_all_tags | grep -P "^v${minor}\.[0-9]+$" | tr '\n' ' ')"
    info "Latest patch: ${latest_patch_tag}"
    info "Next version: ${BOLD}${next_version}${NC}"

    if ! git rev-parse "$next_version" &>/dev/null 2>&1; then
        error "Tag ${next_version} does not exist. Create and push the tag first."
        exit 1
    fi

    local new_commits
    new_commits=$(git rev-list --count "${latest_patch_tag}..${UPSTREAM_REMOTE}/${release_branch}")

    echo ""
    if (( new_commits == 0 )); then
        warn "No new commits on ${release_branch} since ${latest_patch_tag}"
        if ! $FORCE; then
            error "Nothing to release. Use --force to proceed anyway."
            exit 1
        fi
    else
        header "Cherry-picks on ${release_branch} since ${latest_patch_tag} (${new_commits} commits)"
        git log --oneline "${latest_patch_tag}..${UPSTREAM_REMOTE}/${release_branch}"
        echo ""
    fi

    if $DRY_RUN; then
        header "Release plan (DRY RUN — no changes will be made)"
        echo ""
        dry_run "gh release create ${next_version} --repo ${REPO} --generate-notes --draft --notes-start-tag ${latest_patch_tag}"
        echo ""
        info "Images that will be built by CI:"
        echo "     ${IMAGE_BASE}:${next_version}"
        echo "     ${IMAGE_BASE}-mcp-server:${next_version}"
        echo ""
        info "To execute, run:  $(basename "$0") patch ${minor_input}"
        return
    fi

    header "Creating patch release ${next_version}"

    if gh release view "$next_version" --repo "$REPO" &>/dev/null 2>&1; then
        success "GitHub release ${next_version} already exists — skipping"
    else
        header "Creating draft GitHub release"
        run_cmd gh release create "$next_version" \
            --repo "$REPO" \
            --generate-notes \
            --draft \
            --notes-start-tag "$latest_patch_tag"
    fi

    echo ""
    success "Draft release ${next_version} created!"
    info "Images:"
    echo "     ${IMAGE_BASE}:${next_version}"
    echo "     ${IMAGE_BASE}-mcp-server:${next_version}"
    info "Release: https://github.com/${REPO}/releases/tag/${next_version}"
}

# --- Argument parsing ---
COMMAND=""
PATCH_MINOR=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        minor)
            COMMAND="minor"
            shift
            ;;
        patch)
            COMMAND="patch"
            if [[ -z "${2:-}" ]]; then
                error "patch requires a minor version argument (e.g., v0.10)"
                echo "  Usage: $(basename "$0") patch v0.X [OPTIONS]"
                exit 1
            fi
            PATCH_MINOR="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --upstream-remote)
            UPSTREAM_REMOTE="${2:-}"
            if [[ -z "$UPSTREAM_REMOTE" ]]; then
                error "--upstream-remote requires a value"
                exit 1
            fi
            shift 2
            ;;
        --version)
            VERSION_OVERRIDE="${2:-}"
            if [[ -z "$VERSION_OVERRIDE" ]]; then
                error "--version requires a value (e.g., v0.11.0)"
                exit 1
            fi
            shift 2
            ;;
        --force)
            FORCE=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            error "Unknown argument: $1"
            usage
            ;;
    esac
done

if [[ -z "$COMMAND" ]]; then
    usage
fi

case "$COMMAND" in
    minor) do_minor ;;
    patch) do_patch "$PATCH_MINOR" ;;
esac
