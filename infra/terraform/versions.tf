terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.62"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # --- State yonetimi ------------------------------------------------------
  #
  # Bu kurulumda state BILEREK yerelde tutuluyor: tek operator, tek ortam,
  # tek instance. Uzak state'in cozdugu problem (ayni state'e es zamanli iki
  # apply) burada yok, S3 bucket + DynamoDB tablosu ise apply edilmeden once
  # elle olusturulmasi gereken bir "tavuk-yumurta" adimi ekler.
  #
  # ANCAK: terraform.tfstate ACIK METIN olarak hassas veri icerir (bu projede
  # SSM parametre degerleri ve random_password ciktisi). .gitignore bu yuzden
  # *.tfstate'i disliyor - state dosyasini asla commit etme.
  #
  # Ekip calismasina ya da CI'dan apply'a gecilecekse asagidaki blok acilmali;
  # o noktada yerel state artik yanlis tercihtir (kilit yok, paylasim yok,
  # laptop kaybi = altyapi kontrolunun kaybi).
  #
  # backend "s3" {
  #   bucket         = "pulsecity-tfstate"
  #   key            = "prod/terraform.tfstate"
  #   region         = "eu-central-1"
  #   encrypt        = true
  #   dynamodb_table = "pulsecity-tflock"
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = var.project_name
      ManagedBy = "terraform"
      Repo      = var.repo_url
    }
  }
}
