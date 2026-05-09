**Refer Ansh lamba's spotify video for asset Bundles **

Configured Databricks CLI Authentication
```bash
databricks configure --host https://adb-7405606599051163.3.azuredatabricks.net/
# Enter your personal access token when prompted
```

Validated the Bundle
```bash
databricks bundle validate
# Ensure "Validation OK!" appears with no errors
```

Deployed to Dev
```bash
databricks bundle deploy -t dev
```

Ran the Job
```bash
databricks bundle run -t dev end_to_end_healthcare_job_run```


## Useful Commands
```bash
# Validate bundle config
databricks bundle validate

# Deploy to dev
databricks bundle deploy -t dev

# Deploy to prod
databricks bundle deploy -t prod

# Run the job manually
databricks bundle run -t dev end_to_end_healthcare_job_run

# Destroy deployed resources (cleanup)
databricks bundle destroy -t dev
```

## GitHub Secrets Setup
Go to your repo -> Settings -> Secrets and variables -> Actions -> New repository secret:
1. `DATABRICKS_HOST` = `https://adb-7405606599051163.3.azuredatabricks.net/`
2. `DATABRICKS_TOKEN` = your Databricks personal access token
