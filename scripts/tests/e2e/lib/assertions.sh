#!/usr/bin/env bash
# Lightweight assertion helpers for e2e test suites.

if [[ -z "${E2E_ASSERTIONS_SH_LOADED:-}" ]]; then
  E2E_ASSERTIONS_SH_LOADED=1

  _PASS_COUNT=0
  _FAIL_COUNT=0
  _SKIP_COUNT=0

  _pass() {
    ((_PASS_COUNT++)) || true
    printf "  %b[PASS]%b %s\n" "${LOG_COLOR_GREEN:-}" "${LOG_COLOR_RESET:-}" "$1"
  }

  _fail() {
    ((_FAIL_COUNT++)) || true
    printf "  %b[FAIL]%b %s\n" "${LOG_COLOR_RED:-}" "${LOG_COLOR_RESET:-}" "$1" >&2
    [[ -n "${2:-}" ]] && printf "         expected: %s\n         got:      %s\n" "$2" "$3" >&2
  }

  # Whether this suite must exercise every path it can skip. Names come from
  # E2E_STRICT_SUITES, a comma or space separated list matched against the name
  # `init_suite` was given, or the word "all".
  _suite_is_strict() {
    local list="${E2E_STRICT_SUITES:-}"
    [[ -n "$list" ]] || return 1
    local name="${_E2E_SUITE_NAME:-}"
    local entry
    for entry in ${list//,/ }; do
      if [[ "$entry" == "all" || "$entry" == "$name" ]]; then
        return 0
      fi
    done
    return 1
  }

  # A precondition this run does not meet. It counts toward the total so a
  # partial environment still reports one, and the summary says how many were
  # skipped; a suite named in E2E_STRICT_SUITES fails on it instead, which is
  # how a run that must exercise a path proves it did.
  _skip() {
    if _suite_is_strict; then
      _fail "$1" "the path to run" "skipped, and E2E_STRICT_SUITES names this suite"
      return
    fi
    ((_SKIP_COUNT++)) || true
    ((_PASS_COUNT++)) || true
    printf "  %b[SKIP]%b %s\n" "${LOG_COLOR_YELLOW:-}" "${LOG_COLOR_RESET:-}" "$1"
  }

  assert_eq() {
    local actual="$1" expected="$2" msg="${3:-assert_eq}"
    if [[ "$actual" == "$expected" ]]; then
      _pass "$msg"
    else
      _fail "$msg" "$expected" "$actual"
    fi
  }

  assert_not_eq() {
    local actual="$1" not_expected="$2" msg="${3:-assert_not_eq}"
    if [[ "$actual" != "$not_expected" ]]; then
      _pass "$msg"
    else
      _fail "$msg" "not $not_expected" "$actual"
    fi
  }

  assert_contains() {
    local haystack="$1" needle="$2" msg="${3:-assert_contains}"
    if [[ "$haystack" == *"$needle"* ]]; then
      _pass "$msg"
    else
      _fail "$msg" "*${needle}*" "$haystack"
    fi
  }

  assert_not_empty() {
    local value="$1" msg="${2:-assert_not_empty}"
    if [[ -n "$value" ]]; then
      _pass "$msg"
    else
      _fail "$msg" "(non-empty)" "(empty)"
    fi
  }

  assert_status() {
    local actual="$1" expected="$2" msg="${3:-HTTP status}"
    assert_eq "$actual" "$expected" "$msg (HTTP $expected)"
  }

  assert_json_field() {
    local json="$1" jq_expr="$2" expected="$3" msg="${4:-json field}"
    local actual
    actual=$(echo "$json" | jq -r "$jq_expr" 2>/dev/null) || actual="(jq error)"
    assert_eq "$actual" "$expected" "$msg"
  }

  # Print suite summary and return non-zero if any test failed.
  suite_summary() {
    local suite_name="${1:-suite}"
    local total=$((_PASS_COUNT + _FAIL_COUNT))
    local skipped=""
    [[ "$_SKIP_COUNT" -gt 0 ]] && skipped=" (${_SKIP_COUNT} skipped)"
    _E2E_SUITE_SUMMARY_RAN=1
    echo ""
    if [[ "$_FAIL_COUNT" -eq 0 ]]; then
      printf "%b[%s] All %d tests passed.%s%b\n" "${LOG_COLOR_GREEN:-}" "$suite_name" "$total" "$skipped" "${LOG_COLOR_RESET:-}"
    else
      printf "%b[%s] %d/%d tests failed.%s%b\n" "${LOG_COLOR_RED:-}" "$suite_name" "$_FAIL_COUNT" "$total" "$skipped" "${LOG_COLOR_RESET:-}"
    fi
    return $(( _FAIL_COUNT > 0 ? 1 : 0 ))
  }
fi
