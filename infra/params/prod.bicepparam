using '../main.bicep'

env = 'prod'
suffix = 'ord01'
location = 'centralindia'
tags = {
  project: 'serverless-order-pipeline'
  owner: 'platform-team'
  environment: 'prod'
  costCentre: 'commerce-001'
  dataClassification: 'confidential'
}
