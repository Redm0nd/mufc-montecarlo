# mufc-montecarlo

Monte Carlo Premier League season simulator on a 30-minute AWS Lambda.
SQS → Lambda (1,800 s timeout, arm64, Python 3.14) → S3, deployed with Terraform.
Data from [football-data.co.uk](https://www.football-data.co.uk/englandm.php).

## Deploy

```bash
bash build.sh              # packages handler + numpy for Lambda arm64
cd terraform
terraform init
terraform apply
```

## Run

```bash
aws sqs send-message \
  --queue-url "$(terraform output -raw queue_url)" \
  --message-body '{"request_id":"mufc-2026-11-23","season":"2627","team":"Man United","bootstrap_fits":2000,"sims_per_fit":10000}'

aws logs tail /aws/lambda/mufc-montecarlo --follow

aws s3 cp "s3://$(terraform output -raw results_bucket)/results/mufc-2026-11-23.json" - | jq
```

Message fields: `request_id` (also the idempotency key), `season` (football-data code, e.g. `2627`),
`team` (football-data name, e.g. `Man United`), optional `bootstrap_fits`, `sims_per_fit`, `seed`.

## Run locally

```bash
pip install numpy boto3
cd test
python3 run_local.py <path-to-E0.csv> 50 2000   # fits, sims per fit
```

## Clean up

```bash
cd terraform && terraform destroy
```
