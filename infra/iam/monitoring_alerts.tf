# Grafana runs in the monitoring task under the control plane role and sends its alert notifications to the platform alerts topic through its Amazon SNS contact
# point (monitoring/grafana/provisioning/alerting). The contact point signs with this role through the default credential chain. The role gains two actions on
# the one topic and nothing else; a null topic grants nothing, like the promote task's notice grant.
resource "aws_iam_role_policy" "grafana_alerts" {
  count = var.alerts_topic_arn == null ? 0 : 1

  name   = "grafana_alerts"
  role   = aws_iam_role.control_plane_task.name
  policy = templatefile("${path.module}/owner_policies/grafana_alerts.json", { ALERTS_TOPIC_ARN = var.alerts_topic_arn })
}

# The host state exporter (monitoring/host_state_exporter.py) reads the live capacity of the host from ECS under the same role: it lists the tasks and the container
# instances of the one cluster and describes the instances. It runs no task and stops none. The read follows the same switch as the alert grant, because both
# belong to the monitoring task.
resource "aws_iam_role_policy" "host_state_read" {
  count = var.alerts_topic_arn == null ? 0 : 1

  name = "host_state_read"
  role = aws_iam_role.control_plane_task.name
  policy = templatefile("${path.module}/owner_policies/host_state_read.json", {
    CLUSTER_ARN                    = "${local.ecs_arn}:cluster/${local.cluster_name}"
    CONTAINER_INSTANCE_ARN_PATTERN = "${local.ecs_arn}:container-instance/${local.cluster_name}/*"
  })
}
