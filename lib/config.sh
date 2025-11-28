#!/usr/bin/env bash

# Модуль работы с config.yaml:
# - проверка ОС (Ubuntu 20.04)
# - проверка зависимостей
# - определение реальных бинарей pg_dump/pg_dumpall (обход pg_wrapper на Ubuntu)
# - чтение YAML через yq (mikefarah, v4+)
# - базовая валидация значений

YQ_BIN="${YQ_BIN:-yq}"

CFG_FILE=""
CFG_SOURCE_CONNINFO=""
CFG_TARGET_CONNINFO=""

CFG_SCHEMAS_MODE="all"
CFG_ROLES_MODE="all"
CFG_TABLES_MODE="all"

CFG_TABLES_DEFAULT_CHUNK_COLUMN=""
CFG_TABLES_DEFAULT_CHUNK_SIZE=500000
CFG_TABLES_DEFAULT_WHERE=""
CFG_TABLES_DEFAULT_ORDER_BY=""
CFG_TABLES_DEFAULT_LIMIT=0        # НОВОЕ: лимит строк по умолчанию (0 = без лимита)

CFG_PARALLEL_MAX_JOBS=0

CFG_OPT_CREATE_INDEXES_AFTER_DATA=1
CFG_OPT_CREATE_CONSTRAINTS_AFTER_DATA=1
CFG_OPT_ANALYZE_AFTER_LOAD=1

# Реальные бинарники pg_dump / pg_dumpall (обход /usr/bin/pg_dump оболочки)
PG_DUMP_BIN=""
PG_DUMPALL_BIN=""

cfg_check_os_ubuntu_2004() {
  if [ ! -f /etc/os-release ]; then
    log_error "Не удалось определить ОС (нет /etc/os-release). Требуется Ubuntu 20.04."
    exit 1
  fi

  # shellcheck disable=SC1091
  . /etc/os-release

  if [ "${ID:-}" != "ubuntu" ] || [ "${VERSION_ID:-}" != "20.04" ]; then
    log_error "ОС не поддерживается. Требуется Ubuntu 20.04, обнаружено: ID=${ID:-?} VERSION_ID=${VERSION_ID:-?}"
    exit 1
  fi

  log_info "Обнаружена поддерживаемая ОС: Ubuntu 20.04"
}

cfg_yq() {
  local expr="$1"
  "$YQ_BIN" eval "$expr" "$CFG_FILE" 2>/dev/null \
    | sed 's/^"//; s/"$//'
}

cfg_resolve_pg_binaries() {
  # Пытаемся найти реальные бинарники PostgreSQL 16/15
  local v
  for v in 16 15; do
    if [ -x "/usr/lib/postgresql/$v/bin/pg_dump" ] && [ -x "/usr/lib/postgresql/$v/bin/pg_dumpall" ]; then
      PG_DUMP_BIN="/usr/lib/postgresql/$v/bin/pg_dump"
      PG_DUMPALL_BIN="/usr/lib/postgresql/$v/bin/pg_dumpall"
      log_info "Использую pg_dump версии $v: $PG_DUMP_BIN"
      log_info "Использую pg_dumpall версии $v: $PG_DUMPALL_BIN"
      break
    fi
  done

  # Если не нашли в /usr/lib/postgresql, fallback на то, что есть в PATH
  if [ -z "$PG_DUMP_BIN" ]; then
    if command -v pg_dump >/dev/null 2>&1 && command -v pg_dumpall >/dev/null 2>&1; then
      PG_DUMP_BIN="$(command -v pg_dump)"
      PG_DUMPALL_BIN="$(command -v pg_dumpall)"
      log_warn "Не найдено /usr/lib/postgresql/15/16, использую pg_dump из PATH: $PG_DUMP_BIN"
    else
      log_error "Не удалось найти pg_dump/pg_dumpall ни в /usr/lib/postgresql/{15,16}/bin, ни в PATH"
      exit 1
    fi
  fi

  export PG_DUMP_BIN PG_DUMPALL_BIN

  # Дополнительная проверка: версия клиента должна быть 15+
  local dump_ver
  dump_ver="$("$PG_DUMP_BIN" --version 2>/dev/null | awk '{print $3}' | cut -d. -f1)"
  if ! [[ "$dump_ver" =~ ^[0-9]+$ ]]; then
    log_warn "Не удалось распарсить версию pg_dump, продолжаю без проверки клиентской версии."
  else
    if [ "$dump_ver" -lt 15 ]; then
      log_error "Требуется pg_dump 15+, обнаружено: $("$PG_DUMP_BIN" --version)"
      exit 1
    fi
  fi
}

cfg_check_dependencies() {
  local missing=0

  for bin in psql "$YQ_BIN" parallel; do
    if ! command -v "$bin" >/dev/null 2>&1; then
      log_error "Не найдена утилита: $bin"
      missing=1
    fi
  done

  if [ "$missing" -ne 0 ]; then
    log_error "Установите отсутствующие утилиты (psql, yq v4, parallel) и повторите попытку."
    exit 1
  fi

  # Проверим, что это mikefarah/yq v4 (есть подкоманда eval)
  if ! "$YQ_BIN" eval '.source' /dev/null >/dev/null 2>&1; then
    log_error "Похоже, установлен не тот yq (ожидается mikefarah/yq v4 с подкомандой 'eval')."
    exit 1
  fi

  # Найдём реальные бинарники pg_dump/pg_dumpall (обходим pg_wrapper с ошибкой про 12)
  cfg_resolve_pg_binaries
}

cfg_load() {
  CFG_FILE="$1"
  if [ ! -f "$CFG_FILE" ]; then
    log_error "Файл конфигурации не найден: $CFG_FILE"
    exit 1
  fi

  log_info "Чтение конфигурации из $CFG_FILE"

  CFG_SOURCE_CONNINFO="$(cfg_yq '.source.conninfo // ""')"
  CFG_TARGET_CONNINFO="$(cfg_yq '.target.conninfo // ""')"

  CFG_SCHEMAS_MODE="$(cfg_yq '.schemas.mode // "all"')"
  CFG_ROLES_MODE="$(cfg_yq '.roles.mode // "all"')"
  CFG_TABLES_MODE="$(cfg_yq '.tables.mode // "all"')"

  CFG_TABLES_DEFAULT_CHUNK_COLUMN="$(cfg_yq '.tables.default.chunk_column // ""')"
  CFG_TABLES_DEFAULT_CHUNK_SIZE="$(cfg_yq '.tables.default.chunk_size // 500000')"
  CFG_TABLES_DEFAULT_WHERE="$(cfg_yq '.tables.default.where // ""')"
  CFG_TABLES_DEFAULT_ORDER_BY="$(cfg_yq '.tables.default.order_by // ""')"
  CFG_TABLES_DEFAULT_LIMIT="$(cfg_yq '.tables.default.limit // 0')"

  CFG_PARALLEL_MAX_JOBS="$(cfg_yq '.parallel.max_jobs // 0')"

  local opt
  opt="$(cfg_yq '.options.create_indexes_after_data // "true"')"
  [ "$opt" = "true" ] && CFG_OPT_CREATE_INDEXES_AFTER_DATA=1 || CFG_OPT_CREATE_INDEXES_AFTER_DATA=0

  opt="$(cfg_yq '.options.create_constraints_after_data // "true"')"
  [ "$opt" = "true" ] && CFG_OPT_CREATE_CONSTRAINTS_AFTER_DATA=1 || CFG_OPT_CREATE_CONSTRAINTS_AFTER_DATA=0

  opt="$(cfg_yq '.options.analyze_after_load // "true"')"
  [ "$opt" = "true" ] && CFG_OPT_ANALYZE_AFTER_LOAD=1 || CFG_OPT_ANALYZE_AFTER_LOAD=0

  log_info "SCHEMAS_MODE=$CFG_SCHEMAS_MODE, TABLES_MODE=$CFG_TABLES_MODE, ROLES_MODE=$CFG_ROLES_MODE"
  log_info "DEFAULT chunk_column='$CFG_TABLES_DEFAULT_CHUNK_COLUMN', chunk_size=$CFG_TABLES_DEFAULT_CHUNK_SIZE, limit=$CFG_TABLES_DEFAULT_LIMIT"
}

cfg_validate() {
  if [ -z "$CFG_SOURCE_CONNINFO" ] || [ -z "$CFG_TARGET_CONNINFO" ]; then
    log_error "В конфиге должны быть заданы source.conninfo и target.conninfo"
    exit 1
  fi

  case "$CFG_SCHEMAS_MODE" in
    all|include|exclude) ;;
    *)
      log_error "Неверное значение schemas.mode: $CFG_SCHEMAS_MODE (ожидалось: all/include/exclude)"
      exit 1
      ;;
  esac

  case "$CFG_TABLES_MODE" in
    all|include|exclude) ;;
    *)
      log_error "Неверное значение tables.mode: $CFG_TABLES_MODE (ожидалось: all/include/exclude)"
      exit 1
      ;;
  esac

  case "$CFG_ROLES_MODE" in
    all|none) ;;
    *)
      log_error "Неверное значение roles.mode: $CFG_ROLES_MODE (ожидалось: all/none)"
      exit 1
      ;;
  esac

  if ! [[ "$CFG_TABLES_DEFAULT_CHUNK_SIZE" =~ ^[0-9]+$ ]] || [ "$CFG_TABLES_DEFAULT_CHUNK_SIZE" -le 0 ]; then
    log_warn "Некорректный tables.default.chunk_size='$CFG_TABLES_DEFAULT_CHUNK_SIZE', использую 500000"
    CFG_TABLES_DEFAULT_CHUNK_SIZE=500000
  fi

  if ! [[ "$CFG_PARALLEL_MAX_JOBS" =~ ^[0-9]+$ ]]; then
    log_warn "Некорректный parallel.max_jobs='$CFG_PARALLEL_MAX_JOBS', использую 0 (авто)"
    CFG_PARALLEL_MAX_JOBS=0
  fi

  if ! [[ "$CFG_TABLES_DEFAULT_LIMIT" =~ ^[0-9]+$ ]]; then
    log_warn "Некорректный tables.default.limit='$CFG_TABLES_DEFAULT_LIMIT', использую 0 (без лимита)"
    CFG_TABLES_DEFAULT_LIMIT=0
  fi
}
