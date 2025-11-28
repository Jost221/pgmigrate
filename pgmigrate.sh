#!/usr/bin/env bash
# pgmigrate.sh - точка входа CLI-утилиты миграции PostgreSQL -> PostgreSQL

set -uo pipefail

VERSION="1.0.0"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Подключаем модули
source "$SCRIPT_DIR/lib/log.sh"
source "$SCRIPT_DIR/lib/config.sh"
source "$SCRIPT_DIR/lib/db.sh"
source "$SCRIPT_DIR/lib/plan.sh"
source "$SCRIPT_DIR/lib/exec.sh"

ACTION="migrate"   # migrate | plan
CONFIG_FILE=""
PLAN_FILE="plan.tsv"
STATE_FILE="state.tsv"
LOG_FILE="pgmigrate.log"
DRY_RUN=0
RESUME=0         # флаг из CLI
RESUME_MODE=0    # глобальный флаг для модулей
JOBS_OVERRIDE=0
VERBOSE=0        # выводить INFO/WARN в консоль
BACKUP=0         # делать ли бэкап target перед миграцией

usage() {
  cat >&2 <<EOF
pgmigrate v$VERSION

Использование:
  $(basename "$0") migrate --config config.yaml [--plan plan.tsv] [--state state.tsv] [--jobs N] [--dry-run] [--resume] [--backup] [--verbose]
  $(basename "$0") plan    --config config.yaml [--plan plan.tsv]

Параметры:
  migrate / plan      Режим работы: миграция данных или только построение плана.
  -c, --config PATH   Путь к YAML-конфигу.
      --plan PATH     Путь к файлу плана (по умолчанию: plan.tsv).
      --state PATH    Путь к файлу состояния (по умолчанию: state.tsv).
      --jobs N        Число потоков (перекрывает parallel.max_jobs из config.yaml).
      --dry-run       Только план и summary, без миграции.
      --resume        Использовать существующий план/state и перескочить DONE-задачи.
      --backup        Перед миграцией сделать полный логический бэкап target-БД (pg_dump -Fc).
      --verbose       Выводить INFO/WARN в консоль (по умолчанию только ERROR + прогресс).
      -h, --help      Показать помощь.
EOF
}

parse_args() {
  if [ "$#" -eq 0 ]; then
    usage
    exit 1
  fi

  case "$1" in
    migrate|plan)
      ACTION="$1"
      shift
      ;;
    *)
      log_error "Ожидалась подкоманда 'migrate' или 'plan', а получено: $1"
      usage
      exit 1
      ;;
  esac

  while [ "$#" -gt 0 ]; do
    case "$1" in
      -c|--config)
        CONFIG_FILE="$2"
        shift 2
        ;;
      --plan)
        PLAN_FILE="$2"
        shift 2
        ;;
      --state)
        STATE_FILE="$2"
        shift 2
        ;;
      --jobs)
        JOBS_OVERRIDE="$2"
        shift 2
        ;;
      --dry-run)
        DRY_RUN=1
        shift
        ;;
      --resume)
        RESUME=1
        shift
        ;;
      --backup)
        BACKUP=1
        shift
        ;;
      --verbose)
        VERBOSE=1
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        log_error "Неизвестный параметр: $1"
        usage
        exit 1
        ;;
    esac
  done

  if [ -z "$CONFIG_FILE" ]; then
    log_error "Не указан --config"
    usage
    exit 1
  fi
}

main() {
  parse_args "$@"

  # время старта для статистики
  START_TS="$(date +%s)"
  export START_TS

  # Пробрасываем режим резюма во все модули
  RESUME_MODE="$RESUME"
  export RESUME_MODE
  export STATE_FILE PLAN_FILE
  export VERBOSE LOG_FILE

  touch "$LOG_FILE"
  log_info "=== pgmigrate v$VERSION стартует (action=$ACTION, resume=$RESUME_MODE, verbose=$VERBOSE, backup=$BACKUP) ==="

  cfg_check_os_ubuntu_2004
  cfg_check_dependencies
  cfg_load "$CONFIG_FILE"
  cfg_validate

  db_check_connections_and_versions
  exec_compute_jobs "$JOBS_OVERRIDE"

  # Если требуется бэкап и это не режим plan/dry-run
  if [ "$ACTION" = "migrate" ] && [ "$DRY_RUN" -eq 0 ] && [ "$BACKUP" -eq 1 ]; then
    # Жирное предупреждение всегда в консоль
    echo "[WARN] ВНИМАНИЕ: будет выполнен полный логический бэкап target-БД перед миграцией." >&2
    echo "[WARN] Для больших БД это может занять ОЧЕНЬ много времени и потребовать много места на диске." >&2
    log_warn "Запрошен бэкап target-БД перед миграцией (--backup включен)."

    if ! db_backup_target; then
      log_error "Бэкап target-БД завершился с ошибкой, миграция прервана."
      exit 1
    fi
  fi

  # Построение или валидация плана
  plan_build "$PLAN_FILE"
  plan_summary "$PLAN_FILE"

  if [ "$ACTION" = "plan" ] || [ "$DRY_RUN" -eq 1 ]; then
    log_info "Режим plan/dry-run: выполнение миграции не производится."
    exit 0
  fi

  # Запуск миграции (exec_run_migration возвращает код, а не exit)
  if ! exec_run_migration "$PLAN_FILE" "$STATE_FILE" "$RESUME_MODE"; then
    log_error "=== pgmigrate завершён с ошибками (см. статистику выше) ==="
    exit 1
  else
    log_info "=== pgmigrate завершён успешно (см. статистику выше) ==="
  fi
}

main "$@"
