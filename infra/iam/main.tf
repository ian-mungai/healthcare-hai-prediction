provider "aws" {
  profile             = var.admin_profile
  region              = var.aws_region
  allowed_account_ids = [var.expected_account_id]

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# The project user's permissions are one policy file per service in policies/, rendered and applied by the owner with
# scripts/manage_policies.py (README, Install); this stack does not manage them.

# The secret and parameter names are root variables whose defaults are the real names; the policy files take them from those
# defaults (scripts/infrastructure/secret_store.py reads variables.tf), so this file holds no second list of values, only the
# grouping that the output below checks. The four shared API keys stay in Secrets Manager and are read only; every project
# secret is a Parameter Store SecureString, and the Airflow administrator password is read only.
locals {
  shared_secret_names = [var.census_secret_name, var.bls_secret_name, var.hud_secret_name, var.jev_secret_name]
  managed_parameter_names = concat([
    var.postgres_password_name,
    var.polaris_root_secret_name,
    var.polaris_reader_client_id_name,
    var.polaris_reader_client_secret_name,
    var.polaris_gold_client_id_name,
    var.polaris_gold_client_secret_name,
    var.polaris_silver_client_id_name,
    var.polaris_silver_client_secret_name,
    var.airflow_db_password_name,
    var.airflow_fernet_key_name,
    var.airflow_jwt_secret_name,
    var.airflow_api_secret_key_name,
    var.marquez_db_password_name,
    var.redis_password_name,
    var.mlflow_db_password_name,
    var.mlflow_admin_password_name,
  ], var.monitoring_enabled ? [var.grafana_admin_password_name] : [], var.model_api_prepared ? [var.model_api_key_hashes_name, var.model_api_mlflow_user_name, var.model_api_mlflow_credential_name, var.streamlit_model_api_key_name] : [])
  read_parameter_names = concat([var.airflow_admin_secret_name], local.managed_parameter_names)
}
