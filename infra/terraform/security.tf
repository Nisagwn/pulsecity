# Security Group.
#
# Kurallar `aws_vpc_security_group_*_rule` kaynaklariyla AYRI yazildi, grup
# icine gomulu (inline) `ingress {}` bloklariyla degil. Inline kurallar
# Terraform tarafindan "grubun tamami" olarak yonetilir: disaridan (konsoldan)
# eklenen bir kural sessizce silinir ve her kural degisikligi tum grubun
# yeniden hesaplanmasina yol acar. Ayri kaynaklar her kurali bagimsiz olarak
# izler, plan ciktisi da hangi kuralin degistigini tek tek gosterir.

resource "aws_security_group" "app" {
  name        = "${var.project_name}-sg"
  description = "PulseCity: harita/Grafana (HTTP) ve yonetim (SSH) erisimi"
  vpc_id      = aws_vpc.main.id

  tags = {
    Name = "${var.project_name}-sg"
  }

  # Grup, kendisine bagli kurallardan once silinemez; kural degisikliklerinde
  # kesintisiz gecis icin.
  lifecycle {
    create_before_destroy = true
  }
}

# --- Giris kurallari --------------------------------------------------------

# SSH: yalnizca allowed_ssh_cidr. Bu degiskenin varsayilani YOK ve 0.0.0.0/0
# degeri validation ile reddediliyor (bkz. variables.tf).
resource "aws_vpc_security_group_ingress_rule" "ssh" {
  security_group_id = aws_security_group.app.id
  description       = "SSH - yalnizca operator IP adresi"

  cidr_ipv4   = var.allowed_ssh_cidr
  ip_protocol = "tcp"
  from_port   = 22
  to_port     = 22
}

# HTTP: Nginx/Caddy buradan servis ediyor (harita "/" ve Grafana "/grafana/").
resource "aws_vpc_security_group_ingress_rule" "http" {
  security_group_id = aws_security_group.app.id
  description       = "HTTP - harita ve Grafana"

  cidr_ipv4   = var.allowed_http_cidr
  ip_protocol = "tcp"
  from_port   = 80
  to_port     = 80
}

# HTTPS: TLS adimi (P1-3, Caddy) icin simdiden acik. Caddy'nin ACME HTTP-01
# dogrulamasi 80'i, sertifika sunumu 443'u kullanir.
resource "aws_vpc_security_group_ingress_rule" "https" {
  security_group_id = aws_security_group.app.id
  description       = "HTTPS - TLS sonlandirma"

  cidr_ipv4   = var.allowed_http_cidr
  ip_protocol = "tcp"
  from_port   = 443
  to_port     = 443
}

# DIKKAT: Prometheus (9090), Alertmanager (9093), Grafana (3000), ScyllaDB
# (9042) ve Kafka (9092) BILEREK acik degil. Bunlar compose ag'i icinde
# konusuyor; prod override'i zaten portlarini disa yayinlamiyor
# (deploy/docker-compose.prod.yml -> ports: []). Arayuzlerine bakmak icin
# SSH tuneli kullan:
#
#   ssh -L 9090:localhost:9090 -L 9093:localhost:9093 ubuntu@<ip>

# --- Cikis kurali -----------------------------------------------------------

# KABUL EDILMIS ISTISNA — Trivy AWS-0104 (CRITICAL)
#
# Tarama bu kurali "sinirsiz cikis" olarak isaretliyor ve kural genel olarak
# dogru. Burada bilincli olarak kabul ediliyor:
#
# Sunucunun ulasmasi gereken hedefler (Docker Hub, GHCR, Ubuntu apt aynalari,
# Let's Encrypt ACME) CDN arkasinda, genis ve DEGISKEN IP araliklarinda.
# Bunlari CIDR listesiyle daraltmak ya kirilgan bir liste ureti (aynalar
# degisince build durur) ya da VPC endpoint + NAT Gateway + egress proxy
# gerektirir - ayda ~35 USD NAT ucreti ve belirgin bir karmasiklik, korudugu
# seyle orantisiz.
#
# Riski sinirlayan diger katmanlar: instance'in IAM rolu yalnizca TEK bir SSM
# parametresini okuyabiliyor (iam.tf), IMDSv2 zorunlu ve hop_limit=1 oldugu
# icin konteynerlerden metadata'ya erisilemiyor (ec2.tf).
#
# Bu istisna, gercek bir uretim ortaminda (odeme verisi, musteri verisi)
# YENIDEN DEGERLENDIRILMELIDIR.
#
# trivy:ignore:AWS-0104
resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.app.id
  description       = "Tum cikis - imaj/paket indirme, ACME"

  cidr_ipv4   = "0.0.0.0/0"
  ip_protocol = "-1"
}
