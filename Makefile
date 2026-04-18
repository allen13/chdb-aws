TF_MAIN      := terraform/main
TF_BOOTSTRAP := terraform/bootstrap
TERRATEST    := terratest

.PHONY: help \
        bootstrap-init bootstrap-plan bootstrap-apply bootstrap-destroy \
        init plan apply destroy \
        test fmt validate

help:
	@echo "State bucket (one-time, local state):"
	@echo "  bootstrap-apply   create S3 bucket that holds the main stack's state"
	@echo "  bootstrap-plan / bootstrap-destroy"
	@echo ""
	@echo "Main stack (S3 backend, native locking):"
	@echo "  init / plan / apply / destroy"
	@echo ""
	@echo "Terratest dev environment:"
	@echo "  test              deploy dev fixture, assert outputs, destroy"
	@echo ""
	@echo "  fmt / validate"

# --- bootstrap (state bucket) ------------------------------------------------

bootstrap-init:
	cd $(TF_BOOTSTRAP) && terraform init

bootstrap-plan: bootstrap-init
	cd $(TF_BOOTSTRAP) && terraform plan

bootstrap-apply: bootstrap-init
	cd $(TF_BOOTSTRAP) && terraform apply

bootstrap-destroy: bootstrap-init
	cd $(TF_BOOTSTRAP) && terraform destroy

# --- main stack --------------------------------------------------------------

init:
	cd $(TF_MAIN) && terraform init

plan: init
	cd $(TF_MAIN) && terraform plan

apply: init
	cd $(TF_MAIN) && terraform apply

destroy: init
	cd $(TF_MAIN) && terraform destroy

# --- terratest dev environment ----------------------------------------------

test:
	cd $(TERRATEST) && go test -v -timeout 60m ./...

# --- housekeeping ------------------------------------------------------------

fmt:
	terraform fmt -recursive terraform terratest

validate: init
	cd $(TF_MAIN) && terraform validate
