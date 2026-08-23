# VPC Flow Logs.
#
# Trivy taramasi (AWS-0178) bunu eksik olarak isaretledi ve haklıydı: bu proje
# bastan sona gozlemlenebilirlik uzerine kurulu ama AG katmani gorunmezdi.
# Uygulama metrikleri "consumer kac mesaj isledi"yi soyluyor; hangi IP'nin
# hangi porta baglandigini, reddedilen bir baglanti olup olmadigini
# soyleyebilecek tek kaynak flow log'lar.
#
# Maliyet sinirli tutuldu:
#   - retention 7 gun (varsayilan: sonsuz)
#   - yalnizca REJECT degil TUM trafik kaydediliyor ama bu ENI seviyesindedir;
#     50k msg/sn'lik ic trafik Docker bridge uzerinde akar ve VPC ENI'sine
#     hic ugramaz. Yani hacim, dis trafik (harita ziyaretcileri, imaj
#     indirmeleri) kadardir - kucuk.

resource "aws_cloudwatch_log_group" "flow_logs" {
  name              = "/aws/vpc/${var.project_name}-flow-logs"
  retention_in_days = 7

  tags = {
    Name = "${var.project_name}-flow-logs"
  }
}

data "aws_iam_policy_document" "flow_logs_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["vpc-flow-logs.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "flow_logs" {
  name               = "${var.project_name}-flow-logs-role"
  assume_role_policy = data.aws_iam_policy_document.flow_logs_assume.json

  tags = {
    Name = "${var.project_name}-flow-logs-role"
  }
}

data "aws_iam_policy_document" "flow_logs" {
  statement {
    effect = "Allow"

    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams",
    ]

    # Yalnizca bu projenin log grubu - hesabin tamamindaki loglara degil.
    resources = ["${aws_cloudwatch_log_group.flow_logs.arn}:*"]
  }
}

resource "aws_iam_role_policy" "flow_logs" {
  name   = "${var.project_name}-flow-logs"
  role   = aws_iam_role.flow_logs.id
  policy = data.aws_iam_policy_document.flow_logs.json
}

resource "aws_flow_log" "main" {
  vpc_id          = aws_vpc.main.id
  traffic_type    = "ALL"
  iam_role_arn    = aws_iam_role.flow_logs.arn
  log_destination = aws_cloudwatch_log_group.flow_logs.arn

  tags = {
    Name = "${var.project_name}-flow-log"
  }
}
