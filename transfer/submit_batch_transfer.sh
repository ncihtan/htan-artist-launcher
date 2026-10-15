ACCOUNT_B_ID=${ACCOUNT_B_ID:?set to the destination AWS account ID}
ROLE_ARN=${ROLE_ARN:?set to the IAM role ARN for the S3 Batch Operations job}
MANIFEST_OBJ_ARN=arn:aws:s3:::htan-assets/transfer-manifests/artist_oxqp_output_ls.csv
MANIFEST_ETAG=9e7af477741bb56193745695198a2751-3
DEST_BUCKET_ARN=arn:aws:s3:::htan-assets/syn/

aws s3control create-job \
  --account-id $ACCOUNT_B_ID \
  --operation '{
      "S3PutObjectCopy": {
        "TargetResource": "'"$DEST_BUCKET_ARN"'",
        "CannedAccessControlList": "bucket-owner-full-control",
        "MetadataDirective": "COPY",
        "TaggingDirective": "COPY",
        "BucketKeyEnabled": false,
        "ObjectOwnership": "BucketOwnerPreferred"
      }
    }' \
  --manifest '{
      "Spec": {
        "Format": "S3BatchOperations_CSV_20180820",
        "Fields": ["Bucket","Key"]
      },
      "Location": {
        "ObjectArn": "'"$MANIFEST_OBJ_ARN"'",
        "ETag": "'"$MANIFEST_ETAG"'"
      }
    }' \
  --report '{
      "Bucket": "arn:aws:s3:::<report-bucket>",
      "Format": "Report_CSV_20180820",
      "Enabled": true,
      "Prefix": "batch-reports/<job-id>/",
      "ReportScope": "AllTasks"
    }' \
  --priority 10 \
  --role-arn $ROLE_ARN \
  --confirmation-required