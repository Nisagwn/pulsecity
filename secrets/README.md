# secrets/

SOPS + age ile şifrelenmiş, **depoda duran** yapılandırma.

`*.example` dosyaları düz metin şablonlardır (gerçek değer içermezler).
`*.enc` / `*.enc.*` dosyaları şifrelidir ve commit edilebilir.

Kurulum ve gerekçe: [`.sops.yaml`](../.sops.yaml) ve
[`scripts/secrets.sh`](../scripts/secrets.sh).

```bash
./scripts/secrets.sh init                        # age anahtarı üret
cp secrets/prod.env.example secrets/prod.env.enc # doldur
./scripts/secrets.sh encrypt secrets/prod.env.enc
./scripts/secrets.sh check                       # hepsi şifreli mi
```

**Grafana admin parolası burada değil** — onu Terraform üretir ve SSM'de tutar.
Makine tarafından üretilen bir sır operatörün elinde dolaşmamalı. Burası
yalnızca *dışarıdan alınan* (Slack webhook gibi) ve hangi sürümün hangi değerle
çalıştığı git geçmişinden okunabilmesi gereken değerler için.
