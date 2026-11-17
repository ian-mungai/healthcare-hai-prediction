# Grafana runs in the monitoring task under the control plane role and sends its alert notifications to the platform alerts topic through its Amazon SNS contact
# point (monitoring/grafana/provisioning/alerting). The contact point signs with this role through the default credential chain. The role gains two actions on
# the one topic and nothing else; a null topic grants nothing, like the promote task's notice grant.
resource "aws_iam_role_policy" "grafana_alerts" {
  count = var.alerts_topic_arn == null ? 0 : 1

  name   = "grafana_alerts"
  role   = aws_iam_role.control_plane_task.name
  policy = templatefile("${path.module}/owner_policies/grafana_alerts.json", { ALERTS_TOPIC_ARN = var.alerts_topic_arn })
}
