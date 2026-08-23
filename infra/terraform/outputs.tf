output "public_ip" {
  description = "Sunucunun sabit (Elastic) IP adresi."
  value       = aws_eip.app.public_ip
}

output "ssh_command" {
  description = "Sunucuya baglanma komutu."
  value       = "ssh ubuntu@${aws_eip.app.public_ip}"
}

output "map_url" {
  description = "Canli harita adresi."
  value       = "http://${aws_eip.app.public_ip}/"
}

output "grafana_url" {
  description = "Grafana adresi."
  value       = "http://${aws_eip.app.public_ip}/grafana/"
}

output "tunnel_command" {
  description = "Disa acilmayan arayuzler (Prometheus, Alertmanager) icin SSH tuneli."
  value       = "ssh -L 9090:localhost:9090 -L 9093:localhost:9093 ubuntu@${aws_eip.app.public_ip}"
}

output "grafana_admin_password_ssm_path" {
  description = "Grafana parolasinin SSM'deki yolu. Deger cikti olarak DONDURULMUYOR - okumak icin asagidaki komutu kullan."
  value       = aws_ssm_parameter.grafana_admin_password.name
}

output "grafana_admin_password_command" {
  description = "Grafana parolasini okuma komutu."
  value       = "aws ssm get-parameter --name ${aws_ssm_parameter.grafana_admin_password.name} --with-decryption --region ${var.aws_region} --query Parameter.Value --output text"
}

# Parolanin KENDISI bilerek cikti olarak verilmiyor.
#
# `sensitive = true` degeri terminalde maskeler ama state dosyasinda ve
# `terraform output -json` ciktisinda duz metin olarak durur - yani korumasi
# gorsel. Degeri hic cikti yapmayip SSM'den okutmak, parolanin tek bir
# yetkili kaynakta kalmasini saglar.
#
# (random_password.grafana_admin.result zaten state'te duruyor; bu yuzden
#  versions.tf state'in gizli veri icerdigini ve commit edilmemesi gerektigini
#  ayrica not ediyor.)
