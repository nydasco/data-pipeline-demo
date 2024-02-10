terraform {
  required_providers {
    minio = {
      source  = "aminueza/minio"
      version = ">= 2.0.0"
    }
  }
}

provider minio {
  minio_server   = "172.20.0.2:9000"
  minio_user     = "admin"
  minio_password = "some_password"
}

# Create buckets.
resource "minio_s3_bucket" "bucket_bronze" {
  bucket = "bronze"
}

resource "minio_s3_bucket" "bucket_silver" {
  bucket = "silver"
}

resource "minio_s3_bucket" "bucket_gold" {
  bucket = "gold"
}

# Create canned policies with specified permissions.
resource "minio_iam_policy" "read_write_bronze" {
  name = "read_write_bronze"
  policy = <<EOT
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "s3:GetObject"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::bronze/*"
      ]
    }
  ]
}
EOT
}

resource "minio_iam_policy" "read_write_silver" {
  name = "read_write_silver"
  policy = <<EOT
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "s3:GetObject"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::silver/*"
      ]
    }
  ]
}
EOT
}

resource "minio_iam_policy" "read_write_gold" {
  name = "read_write_gold"
  policy = <<EOT
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "s3:GetObject"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::gold/*"
      ]
    }
  ]
}
EOT
}

# Create groups with specified policies.
resource "minio_iam_group" "bronze_group" {
  name = "bronze"
}

resource "minio_iam_group" "silver_group" {
  name = "silver"
}

resource "minio_iam_group" "gold_group" {
  name = "gold"
}

resource "minio_iam_group_policy_attachment" "bronze_group" {
  group_name  = minio_iam_group.bronze_group.name
  policy_name = minio_iam_policy.read_write_bronze.id
}

resource "minio_iam_group_policy_attachment" "silver_group" {
  group_name  = minio_iam_group.silver_group.name
  policy_name = minio_iam_policy.read_write_silver.id
}

resource "minio_iam_group_policy_attachment" "gold_group" {
  group_name  = minio_iam_group.gold_group.name
  policy_name = minio_iam_policy.read_write_gold.id
}

# Create a user with specified access credentials, policies and group membership.
resource "minio_iam_user" "data_pipeline_user" {
  name = "data_pipeline_user"
}


resource "minio_iam_group_user_attachment" "data_pipeline_user_bronze" {
  group_name = minio_iam_group.bronze_group.name
  user_name  = minio_iam_user.data_pipeline_user.name
}