#!/usr/bin/env bash

EXEC_JOBS=1

exec_compute_jobs() {
  local jobs_override="$1"
  local nproc max80

  nproc="$(nproc)"
  max80=$(( nproc * 8 / 10 ))
  [ "$max80" -lt 1 ] && max80=1

  if [ "$CFG_PARALLEL_MAX_JOBS" -gt 0 ] && [ "$CFG_PARALLEL_MAX_JOBS" -lt "$max80" ]; then
    EXEC_JOBS="$CFG_PARALLEL_MAX_JOBS"
  else
    EXEC_JOBS="$max80"
  fi

  if [ "$jobs_override" -gt 0 ] && [ "$jobs_override" -lt "$EXEC_JOBS" ]; then
    EXEC_JOBS="$jobs_override"
  fi

  [ "$EXEC_JOBS" -lt 1 ] && EXEC_JOBS=1

  log_info "Доступно ядер: $nproc, ограничение 80%: $max80, выбрано потоков: $EXEC_JOBS"
}

exec_run_task_data() {
  local task_id="$1"
  local _type="$2"
  local schema="$3"
  local table="$4"
  local where="$5"
  local order_by="$6"
  local limit="$7"
  local offset="$8"

  local state_file="${STATE_FILE:?STATE_FILE not set}"

  # Локальный логгер, не зависит от log.sh и export -f
  _task_log() {
    local level="$1"; shift
    local ts
    ts="$(date '+%Y-%m-%d %H:%M:%S')"
    if [ -n "${LOG_FILE:-}" ]; then
      echo "[$ts] [$level] [task $task_id] $*" >> "$LOG_FILE"
    fi
  }

  _task_log "INFO" "копирование $schema.$table (where='$where', order_by='$order_by', limit='$limit', offset='$offset')"

  local select="SELECT * FROM \"$schema\".\"$table\""
  [ -n "$where" ]    && [ "$where"    != "-" ] && select="$select WHERE $where"
  [ -n "$order_by" ] && [ "$order_by" != "-" ] && select="$select ORDER BY $order_by"
  [ -n "$limit" ]    && [ "$limit"    != "-" ] && select="$select LIMIT $limit"
  [ -n "$offset" ]   && [ "$offset"   != "-" ] && select="$select OFFSET $offset"

  local sql_copy
  sql_copy="COPY ($select) TO STDOUT;"

  if ! printf '%s\n' "$sql_copy" \
      | psql "$CFG_SOURCE_CONNINFO" -v ON_ERROR_STOP=1 -At -q 2>>"$LOG_FILE" \
      | psql "$CFG_TARGET_CONNINFO" -v ON_ERROR_STOP=1 -q -c "COPY \"$schema\".\"$table\" FROM STDIN" >>"$LOG_FILE" 2>&1; then
    _task_log "ERROR" "ошибка при копировании данных $schema.$table"
    printf "%s\tFAILED\n" "$task_id" >> "$state_file"
    return 1
  fi

  printf "%s\tDONE\n" "$task_id" >> "$state_file"
  _task_log "INFO" "успешно ($schema.$table)"
  return 0
}

exec_execute_data_tasks() {
  local plan_file="$1"
  local state_file="$2"

  log_info "Запуск DATA-задач через GNU parallel (JOBS=$EXEC_JOBS)"

  if [ "${RESUME:-0}" -eq 0 ]; then
    : > "$state_file"
  else
    touch "$state_file"
  fi

  # 1. Если RESUME, сначала очищаем чанки для задач со статусом FAILED,
  #    чтобы при повторном COPY не ловить duplicate key.
  if [ "${RESUME:-0}" -eq 1 ]; then
    log_info "Режим resume: очистка чанков для FAILED-задач перед повторным запуском"

    while IFS=$'\t' read -r task_id type schema table where order_by limit offset; do
      [ "$type" != "DATA" ] && continue
      if grep -q -E "^${task_id}[[:space:]]+FAILED" "$state_file"; then
        log_warn "resume: очищаю чанк FAILED-задачи $task_id ($schema.$table, where='$where') перед повторным COPY"
        if ! db_cleanup_chunk "$schema" "$table" "$where"; then
          log_error "Не удалось очистить чанк для FAILED-задачи $task_id ($schema.$table), прерываю миграцию."
          return 1
        fi
      fi
    done < <(awk -F'\t' 'NR>1 {print}' "$plan_file")
  fi

  # 2. Собираем задачи, которые реально нужно запускать сейчас
  local tmp_tasks
  tmp_tasks="$(mktemp)"

  awk -F'\t' 'NR>1 && $2=="DATA" {print}' "$plan_file" \
    | while IFS=$'\t' read -r task_id type schema table where order_by limit offset; do
        # Пропускаем уже успешно выполненные (DONE), но не FAILED
        if [ "${RESUME:-0}" -eq 1 ] && grep -q -E "^${task_id}[[:space:]]+DONE" "$state_file"; then
          log_info "Пропускаю уже выполненную задачу $task_id ($schema.$table) (resume)"
          continue
        fi
        printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
          "$task_id" "$type" "$schema" "$table" "$where" "$order_by" "$limit" "$offset" >> "$tmp_tasks"
      done

  local pending
  pending="$(wc -l < "$tmp_tasks" | tr -d ' ')"
  if [ "$pending" -eq 0 ]; then
    log_info "Нет DATA-задач для выполнения (все уже DONE?)"
    rm -f "$tmp_tasks"
    return 0
  fi

  log_info "DATA-задач к запуску: $pending"

  export CFG_SOURCE_CONNINFO CFG_TARGET_CONNINFO LOG_FILE STATE_FILE
  export -f exec_run_task_data

  if ! parallel --bar --no-notice -j "$EXEC_JOBS" --colsep '\t' \
      exec_run_task_data {1} {2} {3} {4} {5} {6} {7} {8} :::: "$tmp_tasks"; then
    log_error "Некоторые DATA-задачи завершились с ошибкой, см. $state_file и лог"
    rm -f "$tmp_tasks"
    return 1
  fi

  rm -f "$tmp_tasks"
  log_info "Все DATA-задачи выполнены успешно"
  return 0
}

exec_print_summary() {
  local plan_file="$1"
  local state_file="$2"
  local start_ts="$3"
  local end_ts
  end_ts="$(date +%s)"

  local elapsed=$(( end_ts - start_ts ))

  local total_data planned_done planned_failed
  total_data="$(awk -F'\t' 'NR>1 && $2=="DATA" {c++} END{print c+0}' "$plan_file" 2>/dev/null || echo 0)"
  planned_done="$(awk '$2=="DONE"' "$state_file" 2>/dev/null | wc -l | tr -d ' ' || echo 0)"
  planned_failed="$(awk '$2=="FAILED"' "$state_file" 2>/dev/null | wc -l | tr -d ' ' || echo 0)"

  echo "===== MIGRATION SUMMARY ====="
  echo "Время работы: ${elapsed}s"
  if [ "$planned_failed" -eq 0 ]; then
    echo "Статус: OK (rc=0)"
  else
    echo "Статус: WARNING (есть FAILED-задачи)"
  fi
  echo "DATA-задач по плану: $total_data"
  echo "DONE:   $planned_done"
  echo "FAILED: $planned_failed"
  echo "============================="
}

exec_run_migration() {
  local plan_file="$1"
  local state_file="$2"

  local start_ts
  start_ts="$(date +%s)"

  PLAN_FILE_GLOBAL="$plan_file"

  db_migrate_roles
  db_migrate_schema_pre

  if ! exec_execute_data_tasks "$plan_file" "$state_file"; then
    log_error "Миграция данных завершилась с ошибкой"
    exec_print_summary "$plan_file" "$state_file" "$start_ts"
    exit 1
  fi

  db_migrate_schema_post
  db_analyze_tables || true

  log_info "Миграция успешно завершена."
  exec_print_summary "$plan_file" "$state_file" "$start_ts"
}
