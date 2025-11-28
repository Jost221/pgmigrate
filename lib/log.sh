#!/usr/bin/env bash

# Логирование:
# - всегда пишет всё в LOG_FILE (если задан)
# - в stderr выводит только ERROR,
#   а если VERBOSE=1 — все уровни.
#
# ВАЖНО: без внутренних helper-функций, чтобы
# экспорт через GNU parallel (export -f log ...) не ломался.

VERBOSE="${VERBOSE:-0}"

log() {
  local level="$1"; shift
  local ts line

  ts="$(date '+%Y-%m-%d %H:%M:%S')"
  line="[$ts] [$level] $*"

  # Всегда пишем в лог-файл, если задан
  if [ -n "${LOG_FILE:-}" ]; then
    echo "$line" >> "$LOG_FILE"
  fi

  # В stderr:
  # - всегда ERROR
  # - при VERBOSE=1 — все уровни
  if [ "$level" = "ERROR" ] || [ "$VERBOSE" -eq 1 ]; then
    echo "$line" >&2
  fi
}

log_info()  { log "INFO"  "$@"; }
log_warn()  { log "WARN"  "$@"; }
log_error() { log "ERROR" "$@"; }
