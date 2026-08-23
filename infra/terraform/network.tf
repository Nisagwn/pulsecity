# Ag katmani.
#
# Varsayilan VPC kullanmak yerine kendi VPC'mizi olusturuyoruz: varsayilan VPC
# her hesapta farkli sekilde yapilandirilmis olabilir (ve genelde tum
# subnetleri public'tir), yani "benim makinemde calisiyordu" sinifindan bir
# belirsizlik kaynagidir. Kendi VPC'miz altyapiyi tekrarlanabilir kilar.
#
# Tek public subnet bilerek: bu kurulumda tek bir EC2 var, private subnet +
# NAT Gateway eklemek ayda ~35 USD'lik bir NAT ucreti getirir ve korudugu bir
# sey yoktur (korunacak private kaynak yok). NAT'in gerekli olacagi nokta,
# veritabani ayri bir instance'a tasindiginda gelir.

data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "main" {
  cidr_block = var.vpc_cidr

  # EC2'nin ic DNS adiyla cozulebilmesi icin ikisi de gerekli.
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${var.project_name}-vpc"
  }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.project_name}-igw"
  }
}

resource "aws_subnet" "public" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = var.subnet_cidr
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = {
    Name = "${var.project_name}-public"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "${var.project_name}-public-rt"
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}
