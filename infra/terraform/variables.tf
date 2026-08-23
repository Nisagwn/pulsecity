variable "aws_region" {
  description = "Kaynaklarin olusturulacagi AWS bolgesi."
  type        = string
  default     = "eu-central-1"
}

variable "project_name" {
  description = "Kaynak adlarinda ve etiketlerde kullanilan proje adi."
  type        = string
  default     = "pulsecity"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,30}$", var.project_name))
    error_message = "project_name kucuk harf/rakam/tire olmali ve harfle baslamali."
  }
}

variable "repo_url" {
  description = "cloud-init'in sunucuya klonlayacagi depo."
  type        = string
  default     = "https://github.com/Nisagwn/pulsecity.git"
}

variable "repo_branch" {
  description = "Klonlanacak dal."
  type        = string
  default     = "main"
}

# DEPLOY.md'deki boyutlandirma tablosunun koda dokulmus hali:
#   t3.large  (2 vCPU / 8GB)  - sadece demo
#   t3.xlarge (4 vCPU / 16GB) - benchmark de kosulacaksa
# Free tier t2/t3.micro (1GB) bu proje icin CALISMAZ - tek basina ScyllaDB
# 1.5GB istiyor.
variable "instance_type" {
  description = "EC2 instance tipi."
  type        = string
  default     = "t3.large"

  validation {
    # Bellek tabanli bir dogrulama Terraform'da mumkun degil; bilinen yetersiz
    # tipleri acikca disliyoruz ki "neden ayaga kalkmiyor" turu bir hata
    # apply'dan sonra degil, plan'dan once yakalansin.
    condition     = !contains(["t2.micro", "t3.micro", "t2.small", "t3.small", "t3.nano", "t2.nano"], var.instance_type)
    error_message = "Bu instance tipi PulseCity icin yetersiz (>=8GB RAM gerekir). Bkz. deploy/DEPLOY.md boyutlandirma tablosu."
  }
}

variable "root_volume_size_gb" {
  description = "Kok EBS diskinin boyutu (GB). Kafka retention ~6 GiB + ScyllaDB + imajlar."
  type        = number
  default     = 30

  validation {
    condition     = var.root_volume_size_gb >= 20
    error_message = "20 GB altinda imajlar ve Kafka segmentleri icin yer kalmaz."
  }
}

# --- Ag erisimi -------------------------------------------------------------

variable "allowed_ssh_cidr" {
  description = <<-EOT
    SSH'a (22) izin verilen CIDR blogu. Ornek: "203.0.113.42/32".
    Kendi IP'ni ogrenmek icin: curl -s https://checkip.amazonaws.com
  EOT
  type        = string
  # VARSAYILAN YOK - bilerek. Bir varsayilan koymak, en guvenli degeri bile
  # secsek, bu alani "dusunulmesi gerekmeyen" bir alan haline getirir.
  # Zorunlu birakmak her apply'da bilincli bir karar dayatiyor.

  validation {
    condition     = var.allowed_ssh_cidr != "0.0.0.0/0"
    error_message = "SSH'i tum internete acma. Kendi IP'ni /32 olarak ver (curl https://checkip.amazonaws.com)."
  }

  validation {
    condition     = can(cidrhost(var.allowed_ssh_cidr, 0))
    error_message = "allowed_ssh_cidr gecerli bir CIDR blogu olmali (or. 203.0.113.42/32)."
  }
}

variable "allowed_http_cidr" {
  description = "HTTP/HTTPS'e izin verilen CIDR. Harita ve Grafana herkese acik bir demo oldugu icin varsayilan tum internet."
  type        = string
  default     = "0.0.0.0/0"
}

variable "ssh_public_key" {
  description = "Sunucuya yetkilendirilecek SSH acik anahtari (ssh-ed25519 ... biciminde)."
  type        = string

  validation {
    condition     = can(regex("^(ssh-ed25519|ssh-rsa|ecdsa-sha2-) ", var.ssh_public_key))
    error_message = "Gecerli bir SSH ACIK anahtari ver (id_ed25519.pub icerigi). Ozel anahtari ASLA buraya yazma."
  }
}

# --- Ag topolojisi ----------------------------------------------------------

variable "vpc_cidr" {
  description = "VPC icin CIDR blogu."
  type        = string
  default     = "10.20.0.0/16"
}

variable "subnet_cidr" {
  description = "Public subnet icin CIDR blogu."
  type        = string
  default     = "10.20.1.0/24"
}

# --- AMI --------------------------------------------------------------------

variable "ami_name_filter" {
  description = "Kullanilacak Ubuntu AMI ad kalibi. Canonical'in resmi imajlari kullanilir."
  type        = string
  default     = "ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"
}
