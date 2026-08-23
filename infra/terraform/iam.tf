# Secret tasima: SSM Parameter Store + instance profile.
#
# NEDEN user_data DEGIL:
# Grafana admin parolasini cloud-init icine gomomek en kolay yol olurdu ama
# EC2 user-data gizli bir kanal DEGILDIR:
#   - Instance uzerindeki HERHANGI bir surec IMDS'ten okuyabilir
#     (curl http://169.254.169.254/latest/user-data)
#   - AWS konsolunda ve `aws ec2 describe-instance-attribute` ciktisinda
#     duz metin gorunur
#   - Terraform state'ine de duz metin olarak yazilir
#
# Bunun yerine parola SSM'de SecureString olarak (KMS ile sifreli) durur ve
# instance onu KENDI kimligiyle, yalnizca kendi parametre yoluna erisim veren
# bir IAM politikasiyla ceker. Parola makineye hic "gonderilmez", makine onu
# yetkisi oldugu icin okur.
#
# Bu ayni zamanda bir sonraki adimin (SOPS+age) zeminini kuruyor: SOPS
# depodaki dosyalari sifreleyecek, SSM ise calisma zamaninda cozulen
# degerleri tasiyacak.

resource "random_password" "grafana_admin" {
  length  = 24
  special = true
  # SSM ve compose .env dosyasi uzerinden gececek; kabuk yorumlamasi
  # yaratabilecek karakterler disarida birakildi.
  override_special = "-_=+."
}

resource "aws_ssm_parameter" "grafana_admin_password" {
  name        = "/${var.project_name}/grafana/admin_password"
  description = "PulseCity Grafana admin parolasi"
  type        = "SecureString"
  value       = random_password.grafana_admin.result

  tags = {
    Name = "${var.project_name}-grafana-admin-password"
  }
}

# --- age ozel anahtari (Faz 15) --------------------------------------------
#
# Sunucunun bilmesi gereken TEK bootstrap sirri budur. Geri kalan her sey
# (Slack webhook'u vb.) depoda, SOPS ile sifreli ve surumlenmis halde durur;
# sunucu onlari bu anahtarla cozer.
#
# Bu, "her sir icin bir SSM parametresi" yaklasimindan daha iyi: her yeni sir
# icin bir `terraform apply` gerekmez, degisiklikler git gecmisinde gorunur ve
# gozden gecirilir. SSM yalnizca zinciri baslatan anahtari tasir.
#
# Deger BOS birakilabilir (varsayilan): o durumda SOPS akisi devre disidir ve
# cloud-init sir cozmeyi atlar. Slack bildirimi istendiginde doldurulur.
resource "aws_ssm_parameter" "age_private_key" {
  count = var.age_private_key == "" ? 0 : 1

  name        = "/${var.project_name}/sops/age_private_key"
  description = "PulseCity SOPS age ozel anahtari - depodaki sirlari cozmek icin"
  type        = "SecureString"
  value       = var.age_private_key

  tags = {
    Name = "${var.project_name}-age-private-key"
  }
}

# --- Instance rolu ----------------------------------------------------------

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "instance" {
  name               = "${var.project_name}-instance-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json

  tags = {
    Name = "${var.project_name}-instance-role"
  }
}

# En az yetki: yalnizca BU projenin parametre yolu, yalnizca okuma.
# `ssm:GetParameter*` yerine acik acik iki eylem; wildcard eylem, wildcard
# kaynaktan daha sinsi bir genisleme kaynagidir.
data "aws_iam_policy_document" "ssm_read" {
  statement {
    sid    = "ReadOwnParameters"
    effect = "Allow"

    actions = [
      "ssm:GetParameter",
      "ssm:GetParameters",
    ]

    # Yalnizca bu iki parametre - projenin tum yolu (/pulsecity/*) bile degil.
    resources = concat(
      [aws_ssm_parameter.grafana_admin_password.arn],
      aws_ssm_parameter.age_private_key[*].arn,
    )
  }

  # SecureString'i cozmek icin KMS izni de gerekir. AWS yonetimli
  # `alias/aws/ssm` anahtari kullaniliyor; kosul, izni yalnizca SSM uzerinden
  # yapilan cozumlere daraltir - rol dogrudan KMS'e gidip baska bir sey
  # cozemez.
  statement {
    sid       = "DecryptViaSSM"
    effect    = "Allow"
    actions   = ["kms:Decrypt"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["ssm.${var.aws_region}.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "ssm_read" {
  name   = "${var.project_name}-ssm-read"
  role   = aws_iam_role.instance.id
  policy = data.aws_iam_policy_document.ssm_read.json
}

resource "aws_iam_instance_profile" "instance" {
  name = "${var.project_name}-instance-profile"
  role = aws_iam_role.instance.name
}
