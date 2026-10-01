terraform {
  required_version = ">= 1.8.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  glue_arn_base  = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}"
  glue_db_prefix = replace(var.name_prefix, "-", "_")
}

# ── Shared least-privilege data-access policy ─────────────────────────────────
data "aws_iam_policy_document" "data_access" {
  statement {
    sid       = "BucketList"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [var.lakehouse_bucket_arn, var.artifacts_bucket_arn]
  }
  statement {
    sid       = "LakehouseReadWrite"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["${var.lakehouse_bucket_arn}/*"]
  }
  statement {
    sid       = "ArtifactsRead"
    actions   = ["s3:GetObject"]
    resources = ["${var.artifacts_bucket_arn}/*"]
  }
  statement {
    sid       = "LogsWrite"
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${var.logs_bucket_arn}/*"]
  }
  statement {
    sid       = "ReadPipelineSecrets"
    actions   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
    resources = ["${var.secrets_arn_prefix}*"]
  }
  dynamic "statement" {
    for_each = var.kms_key_arn == "" ? [] : [var.kms_key_arn]
    content {
      sid       = "UseDataKey"
      actions   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
      resources = [statement.value]
    }
  }
  # Glue access is limited to this environment's databases/tables (names start with the
  # environment prefix), never account-wide.
  statement {
    sid     = "GlueCatalog"
    actions = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartitions", "glue:CreateTable", "glue:UpdateTable"]
    resources = [
      "${local.glue_arn_base}:catalog",
      "${local.glue_arn_base}:database/${local.glue_db_prefix}*",
      "${local.glue_arn_base}:table/${local.glue_db_prefix}*/*",
    ]
  }
}

resource "aws_iam_policy" "data_access" {
  name   = "${var.name_prefix}-data-access"
  policy = data.aws_iam_policy_document.data_access.json
  tags   = var.tags
}

# ── EMR Serverless job execution role ─────────────────────────────────────────
data "aws_iam_policy_document" "emr_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "emr_job" {
  name               = "${var.name_prefix}-emr-job"
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "emr_job_data" {
  role       = aws_iam_role.emr_job.name
  policy_arn = aws_iam_policy.data_access.arn
}

# ── Redshift COPY role ────────────────────────────────────────────────────────
data "aws_iam_policy_document" "redshift_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

# COPY only reads the staged Gold files — no writes, deletes, secrets or Glue.
data "aws_iam_policy_document" "redshift_copy" {
  statement {
    sid       = "ListLakehouse"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [var.lakehouse_bucket_arn]
  }
  statement {
    sid       = "ReadGoldAndStaging"
    actions   = ["s3:GetObject"]
    resources = ["${var.lakehouse_bucket_arn}/*"]
  }
  dynamic "statement" {
    for_each = var.kms_key_arn == "" ? [] : [var.kms_key_arn]
    content {
      sid       = "DecryptOnly"
      actions   = ["kms:Decrypt", "kms:DescribeKey"]
      resources = [statement.value]
    }
  }
}

resource "aws_iam_policy" "redshift_copy" {
  name   = "${var.name_prefix}-redshift-copy"
  policy = data.aws_iam_policy_document.redshift_copy.json
  tags   = var.tags
}

resource "aws_iam_role" "redshift" {
  name               = "${var.name_prefix}-redshift"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "redshift_copy" {
  role       = aws_iam_role.redshift.name
  policy_arn = aws_iam_policy.redshift_copy.arn
}

# ── ECS task role (FastAPI service) ───────────────────────────────────────────
data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "api_secrets" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["${var.secrets_arn_prefix}*"]
  }
}

resource "aws_iam_role" "api_task" {
  name               = "${var.name_prefix}-api-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "api_task" {
  name   = "read-secrets"
  role   = aws_iam_role.api_task.id
  policy = data.aws_iam_policy_document.api_secrets.json
}
