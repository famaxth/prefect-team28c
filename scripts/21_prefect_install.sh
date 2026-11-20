#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"
guard_edge

log "=== Prefect в отдельном окружении Python (${PREFECT_VENV}) ==="
if [ ! -x "${PREFECT_VENV}/bin/python" ]; then
  if ! python3 -m venv "${PREFECT_VENV}" 2>/dev/null; then
    rm -rf "${PREFECT_VENV}"
    mkdir -p "${DIST_DIR}"
    curl -fsSL --retry 3 -o "${DIST_DIR}/virtualenv.pyz" https://bootstrap.pypa.io/virtualenv.pyz
    python3 "${DIST_DIR}/virtualenv.pyz" -q "${PREFECT_VENV}"
  fi
fi
"${PREFECT_VENV}/bin/python" -m pip install -q --upgrade pip
"${PREFECT_VENV}/bin/python" -m pip install -q "${PREFECT_VERSION_SPEC}"
"${PREFECT_VENV}/bin/python" -c "import prefect, sys; print('Python', sys.version.split()[0], '| Prefect', prefect.__version__)"
[ -x "${SPARK_DIR}/bin/spark-submit" ] || die "Spark не установлен — сначала ./deploy.sh spark-install"
log "Установка Prefect завершена."
