# Canonical'in resmi Ubuntu AMI'si. Sabit bir AMI ID yazmak yerine data
# source: AMI ID'leri BOLGEYE OZELDIR, sabit yazilan bir ID baska bir bolgede
# apply edildiginde ya hata verir ya da (daha kotusu) baska birinin imajina
# denk gelir.
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = [var.ami_name_filter]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
}

resource "aws_key_pair" "operator" {
  key_name   = "${var.project_name}-key"
  public_key = var.ssh_public_key

  tags = {
    Name = "${var.project_name}-key"
  }
}

resource "aws_instance" "app" {
  ami           = data.aws_ami.ubuntu.id
  instance_type = var.instance_type

  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.app.id]
  key_name               = aws_key_pair.operator.key_name
  iam_instance_profile   = aws_iam_instance_profile.instance.name

  # IMDSv2 ZORUNLU.
  #
  # IMDSv1'de metadata servisi kimlik dogrulamasiz bir GET ile okunabilir.
  # Uygulamada bir SSRF acigi varsa, saldirgan uygulamaya
  # http://169.254.169.254/... adresini cagirtarak instance rolunun gecici
  # kimlik bilgilerini disari sizdirabilir - bu, gercek ihlallerde defalarca
  # kullanilmis bir zincirdir. IMDSv2 once PUT ile token istedigi icin bu
  # zinciri kirar.
  #
  # hop_limit = 1: token'i alan yanit konteyner ag katmanini asamaz, yani
  # konteyner icinden IMDS'e ulasilamaz. Uygulamalarimizin AWS kimligine
  # ihtiyaci yok; SSM'i cloud-init host uzerinde okuyor.
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
    instance_metadata_tags      = "disabled"
  }

  # CPU KREDI MODU - yalnizca T ailesinde.
  #
  # T ailesi (t2/t3/t3a/t4g) burstable'dir: taban performansin (t3.large icin
  # vCPU basina %30) ustu CPU kredisiyle karsilanir. AWS varsayilani
  # `unlimited`: krediler bitince instance yavaslamaz, vCPU-saat basina EK
  # UCRET islemeye baslar. Surekli akan bir boru hattinda CPU hep mesgul
  # oldugu icin bu, faturanin ust sinirini tamamen kaldirir. `standard` ise
  # kredi bitince taban hiza duser - demo yavaslar, fatura ongorulebilir kalir.
  #
  # NEDEN DINAMIK: credit_specification T ailesi DISINDA gecersizdir. Varsayilan
  # tipimiz artik m7i-flex.large (bkz. variables.tf: Free Plan kisiti) ve
  # "flex" tipler kredi sayaci kullanmaz - taban %40 CPU verir, ustune burst
  # yapar, ek ucret mekanizmasi yoktur. Blogu kosulsuz birakmak, free-tier
  # uygun tiple apply edildiginde hata verirdi.
  dynamic "credit_specification" {
    for_each = startswith(var.instance_type, "t") ? [1] : []
    content {
      cpu_credits = "standard"
    }
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = var.root_volume_size_gb
    encrypted   = true

    # Instance silindiginde disk de silinsin - uzerinde kalici deger yok,
    # veri zaten yeniden uretilebilir simulasyon verisi.
    delete_on_termination = true

    tags = {
      Name = "${var.project_name}-root"
    }
  }

  # Kaza sonucu terminate'e karsi koruma bilerek KAPALI: bu bir demo
  # ortami ve `terraform destroy` ile kolayca yikilabilmesi isteniyor
  # (README'nin maliyet bilinci: demo bitince kaynagi kapat).
  disable_api_termination = false

  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/cloud-init.yaml", {
    project_name     = var.project_name
    repo_url         = var.repo_url
    repo_branch      = var.repo_branch
    aws_region       = var.aws_region
    grafana_ssm_path = aws_ssm_parameter.grafana_admin_password.name
    age_ssm_path     = var.age_private_key == "" ? "" : aws_ssm_parameter.age_private_key[0].name
    domain           = var.domain
    acme_email       = var.acme_email

    # Yuk profili: cloud-init bunlari .env'e yazmazsa prod override 50.000 msg/sn
    # varsayilanina duser. Gerekce: variables.tf "Yuk profili" bolumu.
    target_rate_per_sec = var.target_rate_per_sec
    vehicle_count       = var.vehicle_count
  })

  tags = {
    Name = "${var.project_name}-app"
  }

  # cloud-init SSM'den parola okuyor; politika henuz baglanmamisken instance
  # ayaga kalkarsa ilk okuma basarisiz olur.
  depends_on = [aws_iam_role_policy.ssm_read]
}

# Elastic IP.
#
# DEPLOY.md'nin notu: EC2'nin public IP'si instance her durdurulup
# baslatildiginda DEGISIR. Demo linkini paylasacaksan bu kabul edilemez -
# EIP adresi sabitler. Instance CALISIRKEN ucretsizdir; instance kapaliyken
# ayrilmis IP icin ucret islemeye baslar (yine DEPLOY.md'de not edilmis).
resource "aws_eip" "app" {
  domain   = "vpc"
  instance = aws_instance.app.id

  tags = {
    Name = "${var.project_name}-eip"
  }

  depends_on = [aws_internet_gateway.main]
}
