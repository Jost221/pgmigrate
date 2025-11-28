#!/usr/bin/env bash

# DB-модуль:
# - проверка подключений и версий (PostgreSQL 15+)
# - список таблиц
# - перенос ролей
# - перенос схемы pre/post
# - ANALYZE
# - ПАРАЛЛЕЛЬНЫЙ post-data по таблицам
# - БЭКАП target-БД перед миграцией (pg_dump -Fc)
# - Очистка "битых" чанков (db_cleanup_chunk) перед повторным COPY

RESUME_MODE="${RESUME_MODE:-0}"
STATE_FILE="${STATE_FILE:-state.tsv}"

db_check_connections_and_versions() {
  log_info "Проверка подключения к source..."
  if ! psql "$CFG_SOURCE_CONNINFO" -v ON_ERROR_STOP=1 -q -c 'SELECT 1;' >>"$LOG_FILE" 2>&1; then
    log_error "Не удалось подключиться к source БД"
    exit 1
  fi

  log_info "Проверка подключения к target..."
  if ! psql "$CFG_TARGET_CONNINFO" -v ON_ERROR_STOP=1 -q -c 'SELECT 1;' >>"$LOG_FILE" 2>&1; then
    log_error "Не удалось подключиться к target БД"
    exit 1
  fi

  local src_ver tgt_ver
  src_ver="$(psql "$CFG_SOURCE_CONNINFO" -At -q -c 'SHOW server_version_num;' 2>>"$LOG_FILE" || echo "")"
  tgt_ver="$(psql "$CFG_TARGET_CONNINFO" -At -q -c 'SHOW server_version_num;' 2>>"$LOG_FILE" || echo "")"

  if [ -z "$src_ver" ] || [ -z "$tgt_ver" ]; then
    log_warn "Не удалось определить версии PostgreSQL, пропускаю проверку server_version_num"
    return 0
  fi

  log_info "Версия source PostgreSQL: $src_ver"
  log_info "Версия target PostgreSQL: $tgt_ver"

  if [ "$src_ver" -lt 150000 ] || [ "$tgt_ver" -lt 150000 ]; then
    log_error "Требуется PostgreSQL 15+, обнаружено source=$src_ver target=$tgt_ver"
    exit 1
  fi
}

# Простой парсер conninfo -> env для pg_dumpall (host/user/port/password)
_db_parse_conninfo_to_env() {
  local conn="$1"
  local kv key val

  for kv in $conn; do
    key="${kv%%=*}"
    val="${kv#*=}"
    val="${val%\"}"
    val="${val#\"}"

    case "$key" in
      host) export PGHOST="$val" ;;
      port) export PGPORT="$val" ;;
      user) export PGUSER="$val" ;;
      password) export PGPASSWORD="$val" ;;
    esac
  done
}

_db_any_data_done() {
  local sf="$1"
  [ -f "$sf" ] && awk '$2=="DONE" || $2=="FAILED"' "$sf" 2>/dev/null | grep -q .
}

_db_predata_already_applied() {
  _db_any_data_done "$STATE_FILE"
}

# ===== БЭКАП TARGET-БД =====

db_backup_target() {
  local backup_dir="${BACKUP_DIR:-./backup}"
  mkdir -p "$backup_dir" 2>/dev/null || {
    log_error "Не удалось создать каталог бэкапа: $backup_dir"
    return 1
  }

  local dbname
  dbname="$(
    printf '%s\n' "$CFG_TARGET_CONNINFO" | awk '
      {
        for (i=1;i<=NF;i++) {
          if ($i ~ /^dbname=/) {
            sub(/^dbname=/,"",$i);
            gsub(/"/,"",$i);
            print $i;
            exit;
          }
        }
      }'
  )"
  [ -z "$dbname" ] && dbname="targetdb"

  local ts
  ts="$(date +%Y%m%d_%H%M%S)"
  local backup_file="$backup_dir/pgmigrate_backup_${dbname}_${ts}.dump"

  log_warn "Старт полного логического бэкапа target-БД '$dbname' в файл: $backup_file (формат custom, pg_dump -Fc)."
  log_warn "Для больших БД бэкап может выполняться ОЧЕНЬ долго и занимать много места на диске."

  if ! "$PG_DUMP_BIN" "$CFG_TARGET_CONNINFO" -Fc -f "$backup_file" >>"$LOG_FILE" 2>&1; then
    log_error "pg_dump бэкап target-БД '$dbname' завершился с ошибкой."
    return 1
  fi

  log_info "Бэкап target-БД успешно завершён: $backup_file"
  return 0
}

db_migrate_roles() {
  if [ "$CFG_ROLES_MODE" = "none" ]; then
    log_info "Перенос ролей выключен (roles.mode=none)"
    return 0
  fi

  if [ "$RESUME_MODE" -eq 1 ] && _db_any_data_done "$STATE_FILE"; then
    log_info "resume-mode: роли уже применялись ранее, пропускаю db_migrate_roles"
    return 0
  fi

  log_info "Перенос ролей через pg_dumpall --roles-only"

  local tmp_sql
  tmp_sql="$(mktemp)"

  (
    _db_parse_conninfo_to_env "$CFG_SOURCE_CONNINFO"
    if ! "$PG_DUMPALL_BIN" --roles-only > "$tmp_sql" 2>>"$LOG_FILE"; then
      log_error "pg_dumpall --roles-only завершился с ошибкой. Проверьте права и доступ к source."
      exit 1
    fi
  )
  local rc=$?
  if [ "$rc" -ne 0 ]; then
    rm -f "$tmp_sql"
    exit 1
  fi

  if ! psql "$CFG_TARGET_CONNINFO" -v ON_ERROR_STOP=0 -q -f "$tmp_sql" >>"$LOG_FILE" 2>&1; then
    log_warn "Применение ролей на target вернуло ошибку (часть ролей могла уже существовать или не хватать прав)"
  fi

  rm -f "$tmp_sql"
}

# список таблиц всегда берём из БД, фильтрация по include/exclude — в plan.sh
db_get_tables_list() {
  psql "$CFG_SOURCE_CONNINFO" -At -F $'\t' -q <<'SQL' 2>>"$LOG_FILE"
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
  AND table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY table_schema, table_name;
SQL
}

db_is_schema_allowed() {
  local schema="$1"

  case "$CFG_SCHEMAS_MODE" in
    all|"")
      if SCHEMA="$schema" "$YQ_BIN" eval -e '.schemas.exclude[]? | select(. == env(SCHEMA))' "$CFG_FILE" >/dev/null 2>&1; then
        return 1
      fi
      return 0
      ;;
    include)
      if SCHEMA="$schema" "$YQ_BIN" eval -e '.schemas.include[]? | select(. == env(SCHEMA))' "$CFG_FILE" >/dev/null 2>&1; then
        return 0
      fi
      return 1
      ;;
    exclude)
      if SCHEMA="$schema" "$YQ_BIN" eval -e '.schemas.exclude[]? | select(. == env(SCHEMA))' "$CFG_FILE" >/dev/null 2>&1; then
        return 1
      fi
      return 0
      ;;
    *)
      return 0
      ;;
  esac
}

# ===== Очистка "битого" чанка перед повторным COPY =====
# Используется при --resume для задач со статусом FAILED.
# ВАЖНО: if where пустой или '-', чистим ВСЮ таблицу.

db_cleanup_chunk() {
  local schema="$1"
  local table="$2"
  local where="$3"

  log_warn "chunk cleanup: schema=$schema table=$table where='${where}'"
  log_warn "Рекомендуется использовать --backup перед очисткой чанков, чтобы можно было откатиться."

  local sql
  if [ -z "$where" ] || [ "$where" = "-" ]; then
    sql="DELETE FROM \"$schema\".\"$table\";"
  else
    sql="DELETE FROM \"$schema\".\"$table\" WHERE $where;"
  fi

  if ! psql "$CFG_TARGET_CONNINFO" -v ON_ERROR_STOP=1 -q -c "$sql" >>"$LOG_FILE" 2>&1; then
    log_error "chunk cleanup FAILED: $schema.$table (where='$where')"
    return 1
  fi

  log_info "chunk cleanup OK: $schema.$table (where='$where')"
  return 0
}

db_migrate_schema_pre() {
  log_info "Перенос структуры (pre-data)"

  if [ "$RESUME_MODE" -eq 1 ] && _db_predata_already_applied; then
    log_info "resume-mode: pre-data уже были применены ранее, пропускаю db_migrate_schema_pre"
    return 0
  fi

  local tmp_sql tmp_schema
  tmp_sql="$(mktemp)"
  tmp_schema="$(mktemp)"

  # Чтобы не спамить NOTICE про существующие схемы
  echo "SET client_min_messages TO warning;" >> "$tmp_schema"

  while IFS=$'\t' read -r schema table; do
    [ -z "$schema" ] && continue
    echo "CREATE SCHEMA IF NOT EXISTS \"$schema\";" >> "$tmp_schema"
  done < <(plan_extract_tables_only)

  if ! psql "$CFG_TARGET_CONNINFO" -v ON_ERROR_STOP=1 -q -f "$tmp_schema" >>"$LOG_FILE" 2>&1; then
    log_error "Не удалось создать схемы на target (CREATE SCHEMA IF NOT EXISTS ...)"
    rm -f "$tmp_sql" "$tmp_schema"
    exit 1
  fi

  local tables_args=()
  while IFS=$'\t' read -r schema table; do
    [ -z "$schema" ] && continue

    local exists
    exists="$(
      psql "$CFG_TARGET_CONNINFO" -At -F '|' -q \
        -c "SELECT 1
              FROM information_schema.tables
             WHERE table_schema = '$schema'
               AND table_name   = '$table'
             LIMIT 1;" 2>>"$LOG_FILE"
    )"

    if [ -n "$exists" ]; then
      log_info "Таблица $schema.$table уже существует на target, пропускаю её в pre-data"
      continue
    fi

    tables_args+=( "--table=${schema}.${table}" )
  done < <(plan_extract_tables_only)

  if [ "${#tables_args[@]}" -eq 0 ]; then
    log_info "Все DATA-таблицы из плана уже существуют на target, pre-data можно пропустить"
    rm -f "$tmp_sql" "$tmp_schema"
    return 0
  fi

  if ! "$PG_DUMP_BIN" "$CFG_SOURCE_CONNINFO" --schema-only --section=pre-data "${tables_args[@]}" > "$tmp_sql" 2>>"$LOG_FILE"; then
    log_error "pg_dump pre-data завершился с ошибкой"
    rm -f "$tmp_sql" "$tmp_schema"
    exit 1
  fi

  if ! psql "$CFG_TARGET_CONNINFO" -v ON_ERROR_STOP=1 -q -f "$tmp_sql" >>"$LOG_FILE" 2>&1; then
    log_error "Применение pre-data на target завершилось с ошибкой"
    rm -f "$tmp_sql" "$tmp_schema"
    exit 1
  fi

  rm -f "$tmp_sql" "$tmp_schema"
}

# ===== ПАРАЛЛЕЛЬНЫЙ post-data по таблицам =====

db_apply_postdata_for_table() {
  local schema="$1"
  local table="$2"

  # маленький локальный логгер, чтобы не зависеть от export -f log
  local ts
  ts="$(date '+%Y-%m-%d %H:%M:%S')"
  if [ -n "${LOG_FILE:-}" ]; then
    echo "[$ts] [INFO] [post-data] $schema.$table" >> "$LOG_FILE"
  fi

  local tmp_sql
  tmp_sql="$(mktemp)"

  if ! "$PG_DUMP_BIN" "$CFG_SOURCE_CONNINFO" --schema-only --section=post-data --table="${schema}.${table}" > "$tmp_sql" 2>>"$LOG_FILE"; then
    if [ -n "${LOG_FILE:-}" ]; then
      ts="$(date '+%Y-%m-%d %H:%M:%S')"
      echo "[$ts] [ERROR] [post-data] pg_dump post-data для $schema.$table завершился с ошибкой" >>"$LOG_FILE"
    fi
    rm -f "$tmp_sql"
    return 1
  fi

  if ! psql "$CFG_TARGET_CONNINFO" -v ON_ERROR_STOP=0 -q -f "$tmp_sql" >>"$LOG_FILE" 2>&1; then
    if [ -n "${LOG_FILE:-}" ]; then
      ts="$(date '+%Y-%m-%d %H:%M:%S')"
      echo "[$ts] [WARN] [post-data] для $schema.$table применён с ошибками (возможно, объекты уже существуют)" >>"$LOG_FILE"
    fi
    rm -f "$tmp_sql"
    return 0
  fi

  rm -f "$tmp_sql"
  return 0
}

db_migrate_schema_post() {
  log_info "Перенос post-data (индексы, constraints, триггеры, привилегии) в параллельном режиме"

  local tmp_list
  tmp_list="$(mktemp)"

  while IFS=$'\t' read -r schema table; do
    [ -z "$schema" ] && continue
    printf "%s\t%s\n" "$schema" "$table" >> "$tmp_list"
  done < <(plan_extract_tables_only)

  if [ ! -s "$tmp_list" ]; then
    log_warn "В плане нет DATA-таблиц, post-data пропущен"
    rm -f "$tmp_list"
    return 0
  fi

  local jobs="${EXEC_JOBS:-1}"
  log_info "post-data: таблиц $(wc -l < "$tmp_list" | tr -d ' '), потоки: $jobs"

  export PG_DUMP_BIN CFG_SOURCE_CONNINFO CFG_TARGET_CONNINFO LOG_FILE
  export -f db_apply_postdata_for_table

  if ! parallel --bar --no-notice -j "$jobs" --colsep '\t' \
      db_apply_postdata_for_table {1} {2} :::: "$tmp_list"; then
    log_warn "Некоторые post-data операции завершились с ошибкой, см. лог выше"
    rm -f "$tmp_list"
    return 0
  fi

  rm -f "$tmp_list"
  log_info "post-data для всех таблиц выполнен"
  return 0
}

db_analyze_tables() {
  if [ "$CFG_OPT_ANALYZE_AFTER_LOAD" -ne 1 ]; then
    log_info "ANALYZE после загрузки отключён (options.analyze_after_load=false)"
    return 0
  fi

  log_info "Запуск ANALYZE для мигрированных таблиц"
  local tmp_sql
  tmp_sql="$(mktemp)"

  while IFS=$'\t' read -r schema table; do
    [ -z "$schema" ] && continue
    echo "ANALYZE \"$schema\".\"$table\";" >> "$tmp_sql"
  done < <(plan_extract_tables_only)

  if ! psql "$CFG_TARGET_CONNINFO" -v ON_ERROR_STOP=1 -q -f "$tmp_sql" >>"$LOG_FILE" 2>&1; then
    log_warn "ANALYZE завершился с ошибкой (это не фатально)"
    rm -f "$tmp_sql"
    return 1
  fi

  rm -f "$tmp_sql"
}
