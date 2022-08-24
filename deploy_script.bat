gcloud functions deploy update_table_dv3_cost_spends ^
    --runtime python39 ^
    --region us-west1 ^
    --project nyo-yoptima ^
    --set-env-vars ENVIRONEMNT=Production ^
    --entry-point subscribe ^
    --service-account alerts@nyo-yoptima.iam.gserviceaccount.com ^
    --trigger-topic deepm_dv3_data_v2_updated ^
    --min-instances 0 ^
    --max-instances 1 ^
    --memory 256MB ^

    