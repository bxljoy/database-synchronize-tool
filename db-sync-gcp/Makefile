# load environment variables from .env file
ifneq (,$(wildcard ./.env))
    include .env
    export
endif

# service variables
PROJECT_ID=your-project-id
SERVICE_REGION=your-region
SERVICE_NAME=your-service-name
REGION=your-region
STAGE=stage

# artifact variables
ARTIFACT_REGION=your-region
ARTIFACT_REPO_NAME=your-artifact-repo-name
ARTIFACT_PATH=$(ARTIFACT_REGION)-docker.pkg.dev
ARTIFACT_IMAGE_NAME=$(SERVICE_NAME)
DB_SECRET_INFO=your-db-secret-info

# service account email
SERVICE_ACCOUNT_EMAIL=your-service-account-email

# jobs scheduler variables
SCHEDULER_SCHEDULE="0 0 * * *"
SCHEDULER_HTTP_METHOD=POST
SCHEDULER_URI="your-scheduler-uri"

build.local:
	docker build -t $(SERVICE_NAME) .

push.local:
	docker tag $(SERVICE_NAME) $(ARTIFACT_PATH)/$(PROJECT_ID)/$(ARTIFACT_REPO_NAME)/$(ARTIFACT_IMAGE_NAME)
	docker push $(ARTIFACT_PATH)/$(PROJECT_ID)/$(ARTIFACT_REPO_NAME)/$(ARTIFACT_IMAGE_NAME)

jobs.deploy:
	@gcloud run jobs deploy $(SERVICE_NAME) \
		--image $(ARTIFACT_PATH)/$(PROJECT_ID)/$(ARTIFACT_REPO_NAME)/$(ARTIFACT_IMAGE_NAME):latest \
		--region $(SERVICE_REGION) \
		--tasks 1 \
		--set-secrets  "DB_SECRET_INFO=$(DB_SECRET_INFO):latest" \
		--set-env-vars "SOURCE_GCS_BUCKET_1=$(SOURCE_GCS_BUCKET_1),DEST_GCS_BUCKET_1=$(DEST_GCS_BUCKET_1),SOURCE_GCS_BUCKET_2=$(SOURCE_GCS_BUCKET_2),DEST_GCS_BUCKET_2=$(DEST_GCS_BUCKET_2)" \
		--set-cloudsql-instances "$(PROD_INSTANCE_CONNECTION_NAME_INVENTORY),$(STAGE_INSTANCE_CONNECTION_NAME_INVENTORY),$(PROD_INSTANCE_CONNECTION_NAME_MERCHANT),$(STAGE_INSTANCE_CONNECTION_NAME_MERCHANT),$(PROD_INSTANCE_CONNECTION_NAME_ORDER),$(STAGE_INSTANCE_CONNECTION_NAME_ORDER)"

scheduler.deploy:
	@if gcloud scheduler jobs describe $(SERVICE_NAME)-scheduler --location $(SERVICE_REGION) > /dev/null 2>&1; then \
		gcloud scheduler jobs update http $(SERVICE_NAME)-scheduler \
			--location $(SERVICE_REGION) \
			--schedule=$(SCHEDULER_SCHEDULE) \
			--uri=$(SCHEDULER_URI) \
			--http-method $(SCHEDULER_HTTP_METHOD) \
			--oauth-service-account-email $(SERVICE_ACCOUNT_EMAIL); \
	else \
		gcloud scheduler jobs create http $(SERVICE_NAME)-scheduler \
			--location $(SERVICE_REGION) \
			--schedule=$(SCHEDULER_SCHEDULE) \
			--uri=$(SCHEDULER_URI) \
			--http-method $(SCHEDULER_HTTP_METHOD) \
			--oauth-service-account-email $(SERVICE_ACCOUNT_EMAIL); \
	fi

jobs.exec:
	gcloud run jobs execute $(SERVICE_NAME) \
		--region $(SERVICE_REGION)

deploy: build.local push.local jobs.deploy scheduler.deploy
exec: jobs.exec