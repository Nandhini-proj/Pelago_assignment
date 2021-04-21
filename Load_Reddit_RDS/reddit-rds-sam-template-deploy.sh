#!/bin/bash

# variables
TEMPLATE_IN=reddit_rds_sam_template.yaml
TEMPLATE_OUT=deploy_reddit_rds_sam_template.yaml
S3_BUCKET=datalake-codefiles
STACK_NAME=datalake-reddit-rds-stack

# package (upload artifact to rds)
echo "SAM is now packaging..."
sam package --template-file $TEMPLATE_IN --output-template-file $TEMPLATE_OUT --s3-bucket $S3_BUCKET

# deploy (CloudFormation changesets)
echo "SAM is now deploying..."
sam deploy --template-file $TEMPLATE_OUT --stack-name $STACK_NAME --capabilities CAPABILITY_IAM
