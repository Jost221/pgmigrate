#!/usr/bin/env bash

PLAN_FILE_GLOBAL=""

PLAN_TC_WHERE=""
PLAN_TC_ORDER_BY=""
PLAN_TC_CHUNK_COLUMN=""
PLAN_TC_CHUNK_SIZE=0
PLAN_TC_LIMIT=0   # НОВОЕ: лимит строк для текущей таблицы (0 = без лимита)

plan_get_table_config() {
  local schema="$1"
  local table="$2"

  PLAN_TC_WHERE="$CFG_TABLES_DEFAULT_WHERE"
  PLAN_TC_ORDER_BY="$CFG_TABLES_DEFAULT_ORDER_BY"
  PLAN_TC_CHUNK_COLUMN="$CFG_TABLES_DEFAULT_CHUNK_COLUMN"
  PLAN_TC_CHUNK_SIZE="$CFG_TABLES_DEFAULT_CHUNK_SIZE"
  PLAN_TC_LIMIT="$CFG_TABLES_DEFAULT_LIMIT"

  # 1) исключённые таблицы
  if SCHEMA="$schema" TABLE="$table" "$YQ_BIN" eval -e '.tables.exclude[]? | select(.schema == env(SCHEMA) and .name == env(TABLE))' "$CFG_FILE" >/dev/null 2>&1; then
    return 1
  fi

  # 2) проверка, есть ли таблица в tables.include
  local in_include=0
  if SCHEMA="$schema" TABLE="$table" "$YQ_BIN" eval -e '.tables.include[]? | select(.schema == env(SCHEMA) and .name == env(TABLE))' "$CFG_FILE" >/dev/null 2>&1; then
    in_include=1
  fi

  # 3) если режим include и таблицы нет в include — выкидываем
  if [ "$CFG_TABLES_MODE" = "include" ] && [ "$in_include" -ne 1 ]; then
    return 1
  fi

  # 4) если таблица есть в include — переопределяем настройки (where, order_by, chunk, limit)
  if [ "$in_include" -eq 1 ]; then
    local expr='.tables.include[]? | select(.schema == env(SCHEMA) and .name == env(TABLE))'

    PLAN_TC_WHERE="$(
      SCHEMA="$schema" TABLE="$table" "$YQ_BIN" eval "$expr | .where // \"\"" "$CFG_FILE" 2>/dev/null \
        | sed 's/^"//; s/"$//' \
        || echo "$PLAN_TC_WHERE"
    )"

    PLAN_TC_ORDER_BY="$(
      SCHEMA="$schema" TABLE="$table" "$YQ_BIN" eval "$expr | .order_by // \"\"" "$CFG_FILE" 2>/dev/null \
        | sed 's/^"//; s/"$//' \
        || echo "$PLAN_TC_ORDER_BY"
    )"

    PLAN_TC_CHUNK_COLUMN="$(
      SCHEMA="$schema" TABLE="$table" "$YQ_BIN" eval "$expr | .chunk_column // \"$PLAN_TC_CHUNK_COLUMN\"" "$CFG_FILE" 2>/dev/null \
        | sed 's/^"//; s/"$//' \
        || echo "$PLAN_TC_CHUNK_COLUMN"
    )"

    local size
    size="$(
      SCHEMA="$schema" TABLE="$table" "$YQ_BIN" eval "$expr | .chunk_size // \"$PLAN_TC_CHUNK_SIZE\"" "$CFG_FILE" 2>/dev/null \
        | sed 's/^"//; s/"$//' \
        || echo "$PLAN_TC_CHUNK_SIZE"
    )"
    PLAN_TC_CHUNK_SIZE="$size"

    local limit
    limit="$(
      SCHEMA="$schema" TABLE="$table" "$YQ_BIN" eval "$expr | .limit // \"$PLAN_TC_LIMIT\"" "$CFG_FILE" 2>/dev/null \
        | sed 's/^"//; s/"$//' \
        || echo "$PLAN_TC_LIMIT"
    )"
    PLAN_TC_LIMIT="$limit"
  fi

  # валидируем числа
  if ! [[ "$PLAN_TC_CHUNK_SIZE" =~ ^[0-9]+$ ]] || [ "$PLAN_TC_CHUNK_SIZE" -le 0 ]; then
    PLAN_TC_CHUNK_SIZE="$CFG_TABLES_DEFAULT_CHUNK_SIZE"
  fi

  if ! [[ "$PLAN_TC_LIMIT" =~ ^[0-9]+$ ]]; then
    PLAN_TC_LIMIT=0
  fi

  return 0
}


plan_build() {
  local plan_file="$1"
  PLAN_FILE_GLOBAL="$plan_file"

  log_info "Построение плана миграции -> $plan_file"

  : > "$plan_file"
  echo -e "task_id\ttype\tschema\ttable\twhere\torder_by\tlimit\toffset" >> "$plan_file"

  local task_id=1

  # Роли
  echo -e "$task_id\tROLE\t-\t-\t-\t-\t-\t-" >> "$plan_file"
  task_id=$((task_id+1))

  # Pre-schema
  echo -e "$task_id\tSCHEMA_PRE\t-\t-\t-\t-\t-\t-" >> "$plan_file"
  task_id=$((task_id+1))

  local t_schema t_table
  while IFS=$'\t' read -r t_schema t_table; do
    [ -z "$t_schema" ] && continue

    if ! db_is_schema_allowed "$t_schema"; then
      continue
    fi

    if ! plan_get_table_config "$t_schema" "$t_table"; then
      continue
    fi

    # НОВОЕ ПРАВИЛО:
    # Если для таблицы указали LIMIT > 0, то:
    #   - НЕ режем её на чанки (даже если chunk_column задан)
    #   - создаём одну DATA-задачу с limit=<PLAN_TC_LIMIT>
    if [ "$PLAN_TC_LIMIT" -gt 0 ]; then
      echo -e "$task_id\tDATA\t$t_schema\t$t_table\t$PLAN_TC_WHERE\t$PLAN_TC_ORDER_BY\t$PLAN_TC_LIMIT\t-" >> "$plan_file"
      task_id=$((task_id+1))
      continue
    fi

    # Если нет chunk_column — обычная одна задача без LIMIT
    if [ -z "$PLAN_TC_CHUNK_COLUMN" ]; then
      echo -e "$task_id\tDATA\t$t_schema\t$t_table\t$PLAN_TC_WHERE\t$PLAN_TC_ORDER_BY\t-\t-" >> "$plan_file"
      task_id=$((task_id+1))
      continue
    fi

    # Ниже — логика чанков (как раньше), без limit
    local sql stats min_id max_id row_count
    sql="SELECT COALESCE(MIN(\"$PLAN_TC_CHUNK_COLUMN\"),0), COALESCE(MAX(\"$PLAN_TC_CHUNK_COLUMN\"),0), COUNT(*) FROM \"$t_schema\".\"$t_table\""
    if [ -n "$PLAN_TC_WHERE" ]; then
      sql="$sql WHERE $PLAN_TC_WHERE"
    fi

    stats="$(printf '%s\n' "$sql" | psql "$CFG_SOURCE_CONNINFO" -v ON_ERROR_STOP=1 -At -F '|' 2>/dev/null || true)"
    if [ -z "$stats" ]; then
      log_warn "Не удалось получить статистику для $t_schema.$t_table, создаю одну задачу без чанков"
      echo -e "$task_id\tDATA\t$t_schema\t$t_table\t$PLAN_TC_WHERE\t$PLAN_TC_ORDER_BY\t-\t-" >> "$plan_file"
      task_id=$((task_id+1))
      continue
    fi

    min_id="${stats%%|*}"
    local tmp_rest="${stats#*|}"
    max_id="${tmp_rest%%|*}"
    row_count="${tmp_rest##*|}"

    if [ "$row_count" -eq 0 ]; then
      log_info "Таблица $t_schema.$t_table пуста, пропускаю"
      continue
    fi

    if [ "$row_count" -le "$PLAN_TC_CHUNK_SIZE" ] || [ "$min_id" = "$max_id" ]; then
      echo -e "$task_id\tDATA\t$t_schema\t$t_table\t$PLAN_TC_WHERE\t$PLAN_TC_ORDER_BY\t-\t-" >> "$plan_file"
      task_id=$((task_id+1))
      continue
    fi

    local n_chunks=$(( (row_count + PLAN_TC_CHUNK_SIZE - 1) / PLAN_TC_CHUNK_SIZE ))
    [ "$n_chunks" -lt 1 ] && n_chunks=1

    local range=$(( max_id - min_id + 1 ))
    local step=$(( (range + n_chunks - 1) / n_chunks ))
    [ "$step" -lt 1 ] && step=1

    log_info "Планирование $n_chunks чанков для $t_schema.$t_table (rows=$row_count, id_range=$min_id..$max_id, step=$step)"

    local i start_id end_id chunk_where chunk_order_by
    chunk_order_by="$PLAN_TC_CHUNK_COLUMN"

    i=0
    while [ "$i" -lt "$n_chunks" ]; do
      start_id=$(( min_id + i * step ))
      end_id=$(( start_id + step - 1 ))
      if [ "$i" -eq $(( n_chunks - 1 )) ]; then
        end_id="$max_id"
      fi

      if [ -n "$PLAN_TC_WHERE" ]; then
        chunk_where="($PLAN_TC_WHERE) AND \"$PLAN_TC_CHUNK_COLUMN\" BETWEEN $start_id AND $end_id"
      else
        chunk_where="\"$PLAN_TC_CHUNK_COLUMN\" BETWEEN $start_id AND $end_id"
      fi

      echo -e "$task_id\tDATA\t$t_schema\t$t_table\t$chunk_where\t$chunk_order_by\t-\t-" >> "$plan_file"
      task_id=$((task_id+1))
      i=$((i+1))
    done

  done < <(db_get_tables_list)

  # post-schema
  echo -e "$task_id\tSCHEMA_POST\t-\t-\t-\t-\t-\t-" >> "$plan_file"

  log_info "План построен, последняя task_id: $task_id"
}

plan_summary() {
  local plan_file="$1"
  local total_tasks data_tasks table_count

  total_tasks="$(awk 'NR>1 {c++} END{print c+0}' "$plan_file")"
  data_tasks="$(awk -F'\t' 'NR>1 && $2=="DATA" {c++} END{print c+0}' "$plan_file")"
  table_count="$(awk -F'\t' 'NR>1 && $2=="DATA" {t[$3"."$4]=1} END{print length(t)+0}' "$plan_file")"

  log_info "План: задач всего = $total_tasks, таблиц (DATA) = $table_count, чанков (DATA) = $data_tasks"
}

plan_extract_tables_only() {
  awk -F'\t' 'NR>1 && $2=="DATA" {print $3"\t"$4}' "$PLAN_FILE_GLOBAL" | sort -u
}
