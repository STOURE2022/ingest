# WAX Ingestion – Modularized (Fixed)

## Run locally
```bash
python -m src.main
```

Override params with environment variables:
```
ZIP_PATH=... EXCEL_PATH=... EXTRACT_DIR=... LOG_EXEC_PATH=... LOG_QUALITY_PATH=... ENV=dev VERSION=v1 python -m src.main
```

## Notes
- Detects Databricks widgets if running in a notebook, otherwise falls back to env vars.
- Writes Delta tables to `/mnt/wax/<env>/<zone>/<version>/<table>_{all,last}`.
- Execution logs: `/mnt/logs/wax_execution_logs_delta`
- Quality logs: `/mnt/logs/wax_data_quality_errors_delta`


setx HADOOP_HOME "C:\Users\ZZ7MQ\databricks\wax_ingestion_main\winutils"
setx PATH "%PATH%;%HADOOP_HOME%\bin"
