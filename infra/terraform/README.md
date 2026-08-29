# PulseCity — Altyapı (Terraform / AWS EC2)

`deploy/DEPLOY.md`'deki elle SSH adımlarının koda dökülmüş hali. Rehber hâlâ
okunmaya değer — *ne yapıldığını* anlatır — ama artık yapan o değil.

## Ne oluşturur

| Kaynak | Not |
|---|---|
| VPC + public subnet + IGW + route table | Varsayılan VPC kullanılmıyor: her hesapta farklı yapılandırılmış olabilir |
| Security Group | SSH yalnızca `allowed_ssh_cidr`; 80/443 herkese açık |
| EC2 instance | `t3.large` (2 vCPU / 8GB), gp3 + **EBS şifreleme**, **IMDSv2 zorunlu** |
| Elastic IP | Instance durdurulup başlatıldığında IP değişmesin diye |
| SSM Parameter (SecureString) | Grafana admin parolası |
| IAM rolü + instance profile | Yalnızca **o tek parametreyi** okuma yetkisi |
| VPC Flow Logs + CloudWatch log grubu | 7 gün saklama |

Sunucu hazırlığı (Docker, depo, `.env`, stack) `cloud-init.yaml` ile yapılır.

## Kullanım

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
# terraform.tfvars'ı doldur: allowed_ssh_cidr ve ssh_public_key ZORUNLU

terraform init
terraform plan     # AWS kimlik bilgisi gerektirir
terraform apply
```

Çıktılar:

```bash
terraform output map_url          # http://<eip>/
terraform output grafana_url      # http://<eip>/grafana/
terraform output ssh_command
terraform output tunnel_command   # Prometheus/Alertmanager için SSH tüneli

# Grafana parolası — çıktı olarak DÖNDÜRÜLMEZ, SSM'den okunur:
eval "$(terraform output -raw grafana_admin_password_command)"
```

Yıkmak için:

```bash
terraform destroy
```

## Tasarım kararları

**Grafana parolası `user_data`'ya gömülmüyor.** EC2 user-data gizli bir kanal
değildir: instance üzerindeki herhangi bir süreç IMDS'ten okuyabilir, AWS
konsolunda düz metin görünür ve Terraform state'ine de düz metin yazılır.
Parola SSM'de SecureString olarak durur; instance onu *kendi* IAM kimliğiyle,
yalnızca o parametre yoluna erişim veren bir politikayla çeker. Parola
makineye gönderilmez — makine onu yetkisi olduğu için okur.

**IMDSv2 zorunlu, `hop_limit = 1`.** IMDSv1'de metadata servisi kimlik
doğrulamasız bir GET ile okunur; uygulamada bir SSRF açığı varsa saldırgan
instance rolünün geçici kimlik bilgilerini sızdırabilir. `hop_limit = 1`
ayrıca konteyner ağ katmanından IMDS'e erişimi keser.

**`allowed_ssh_cidr`'ın varsayılanı yok.** En güvenli değeri bile varsayılan
yapmak, bu alanı "düşünülmesi gerekmeyen" bir alan haline getirir. Zorunlu
bırakmak her `apply`'da bilinçli bir karar dayatıyor. `0.0.0.0/0` verilmesi
`validation` ile reddedilir.

**Tek public subnet, NAT yok.** Private subnet + NAT Gateway ayda ~35 USD
getirir ve burada koruduğu bir şey yoktur (korunacak private kaynak yok).
NAT'ın gerekli olacağı nokta, veritabanı ayrı bir instance'a taşındığında
gelir.

**State yerelde.** Tek operatör, tek ortam — uzak state'in çözdüğü problem
(aynı state'e eş zamanlı iki apply) burada yok. Ekip çalışmasına ya da CI'dan
apply'a geçilirse `versions.tf`'teki S3 backend bloğu açılmalı; o noktada
yerel state artık yanlış tercihtir. **`terraform.tfstate` düz metin olarak
hassas veri içerir ve `.gitignore`'dadır — asla commit etme.**

**Kabul edilmiş istisna: sınırsız egress (Trivy AWS-0104).** Docker Hub, GHCR,
apt aynaları ve Let's Encrypt CDN arkasında, değişken IP aralıklarında.
CIDR listesiyle daraltmak ya kırılgan bir liste üretir ya da NAT + VPC
endpoint + egress proxy gerektirir. `security.tf` içinde `trivy:ignore` ile
**gerekçesiyle** işaretlendi — sessizce bastırılmadı. Gerçek bir üretim
ortamında yeniden değerlendirilmelidir.

## Maliyet

`t3.large` sürekli açık ≈ **60 USD/ay** (+ EBS ~2.4 USD, EIP instance
çalışırken ücretsiz, CloudWatch flow log ingest'i düşük). `t3.xlarge`
≈ 140 USD/ay.

**CPU kredi modu `standard`.** T3 burstable bir ailedir ve AWS varsayılanı
`unlimited`'dir: krediler bitince instance yavaşlamaz, vCPU-saat başına ek
ücret işlemeye başlar. Sürekli akan bir boru hattında CPU hep meşgul olduğu
için bu, yukarıdaki hesabın üst sınırını kaldırırdı. `standard` modda kredi
bitince instance taban hızına düşer — demo yavaşlar, fatura öngörülebilir
kalır. Aynı gerekçeyle `target_rate_per_sec` varsayılanı 5.000'dir (50.000
değil): bkz. `variables.tf` "Yuk profili".

**Demo bittiğinde `terraform destroy` çalıştır.** Elastic IP, instance
*kapalıyken* ücret işletir — instance'ı durdurup bırakmak yerine tamamen
yıkmak daha ucuzdur.

## CI

`.github/workflows/ci.yml` → `iac` job'ı her push'ta `fmt -check`, `validate`
ve Trivy yapılandırma taraması çalıştırır. **`plan`/`apply` yok** — bu iş
akışının bulut kimlik bilgisi tutması gerekmiyor.
