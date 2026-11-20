#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"
guard_edge
source "$(dirname "$0")/hive_env.sh"

[ -x "${PREFECT_VENV}/bin/python" ] || die "Prefect не установлен — сначала ./deploy.sh prefect-install"
svc_running metastore || die "Hive Metastore не запущен — ./deploy.sh hive-start"
svc_running hiveserver2 || die "HiveServer2 не запущен — ./deploy.sh hive-start"
busy="$(ss -ltn | grep -E "$(ports_regex ${SPARK_PORTS})" || true)"
[ -z "${busy}" ] || { echo "${busy}"; die "порты Spark Team C заняты (уже идёт другой запуск?)"; }

export SPARK_HOME="${SPARK_DIR}"
py4j="$(ls "${SPARK_DIR}"/python/lib/py4j-*-src.zip | sed -n 1p)"
export PYTHONPATH="${SPARK_DIR}/python:${py4j}"
export PYSPARK_PYTHON=python3
export PREFECT_HOME="${PREFECT_HOME_DIR}"
export PREFECT_SERVER_ANALYTICS_ENABLED=false
mkdir -p "${PREFECT_HOME_DIR}"

log "=== Запуск потока Prefect team28c-sales-pipeline ==="
LOG="${HIVE_TMP}/prefect_flow.log"
set +e
"${PREFECT_VENV}/bin/python" "${REPO_DIR}/flows/sales_flow.py" \
  "hdfs://${NN_HOST}:${PORT_NN_RPC}${SPARK_INPUT}" "${SPARK_DB}" "${FLOW_TABLE}" \
  "hdfs://${NN_HOST}:${PORT_NN_RPC}${FLOW_TABLE_PATH}" "${HS2_URL}" "${TEAM_USER}" > "${LOG}" 2>&1
rc=$?
set -e
grep -E '\| (INFO|ERROR|WARNING|CRITICAL) +\| (Flow run|Task run)|^[|+]' "${LOG}" | grep -v 'Created task run' || true
if [ "${rc}" != 0 ] || ! grep -q "Flow run .* Finished in state Completed" "${LOG}"; then
  grep -E 'Error|Exception' "${LOG}" | tail -n 20
  die "поток завершился с ошибкой, полный лог: ${LOG}"
fi

log "=== Spark-приложение потока в YARN ==="
yarn application -list -appStates FINISHED 2>/dev/null | grep 'team28c-prefect-sales' | tail -n 1 || true

echo
log "PREFECT TEAM C РАБОТАЕТ: поток выполнил все шаги (Spark на YARN -> HDFS -> чтение -> трансформации -> таблица ${SPARK_DB}.${FLOW_TABLE} -> проверка в Hive)."
echo "Полный лог: ${LOG}"
