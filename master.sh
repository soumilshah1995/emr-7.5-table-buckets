#!/bin/bash

# Set environment variables
export CLUSTER_NAME="My_Spark_Iceberg_Cluster"
export RELEASE_LABEL="emr-7.5.0"
export REGION="us-east-1"
export LOG_URI="s3://$S3_BUCKET_NAME/"
export SERVICE_ROLE="arn:aws:iam::${AWS_ACCOUNT_ID}:role/EMRServiceRole"
export EC2_PROFILE="EMR_EC2_DefaultRole"
export SUBNET_ID="XXXX"
export KEY_NAME="MacBookEMR"
export CONFIG_FILE="/path/to/configurations.json"
export JOB_PATH="/path/to/iceberg-job.py"
export S3_JOB_PATH="s3://$S3_BUCKET_NAME/jobs/iceberg-job.py"

# Upload PySpark job to S3
echo "Uploading PySpark job to S3..."
aws s3 cp "$JOB_PATH" "$S3_JOB_PATH"

if [[ $? -ne 0 ]]; then
  echo "Error: Failed to upload PySpark job to S3."
  exit 1
fi

echo "PySpark job uploaded to $S3_JOB_PATH."

# Create EMR cluster and submit the step
echo "Creating EMR cluster and submitting step..."

CLUSTER_ID=$(aws emr create-cluster \
  --release-label "$RELEASE_LABEL" \
  --applications Name=Spark \
  --configurations file://"$CONFIG_FILE" \
  --region "$REGION" \
  --name "$CLUSTER_NAME" \
  --log-uri "$LOG_URI" \
  --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge \
  --service-role "$SERVICE_ROLE" \
  --ec2-attributes InstanceProfile="$EC2_PROFILE",SubnetId="$SUBNET_ID",KeyName="$KEY_NAME" \
  --auto-terminate \
  --steps '[{"Name":"Iceberg PySpark Job","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Args":["spark-submit","--packages","software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.0,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.6.0","s3://$S3_BUCKET_NAME/jobs/iceberg-job.py"],"Type":"CUSTOM_JAR"}]' \
  --query 'ClusterId' --output text)

if [[ $? -ne 0 || -z "$CLUSTER_ID" ]]; then
  echo "Error: Failed to create EMR cluster."
  exit 1
fi

echo "Cluster created successfully with ID: $CLUSTER_ID"
