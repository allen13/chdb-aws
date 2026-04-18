package test

import (
	"testing"

	"github.com/gruntwork-io/terratest/modules/terraform"
	"github.com/stretchr/testify/assert"
)

func TestChdbAwsDev(t *testing.T) {
	t.Parallel()

	opts := &terraform.Options{
		TerraformDir: "./dev",
		VarFiles:     []string{"dev.tfvars"},
	}

	defer terraform.Destroy(t, opts)
	terraform.InitAndApply(t, opts)

	assert.NotEmpty(t, terraform.Output(t, opts, "data_bucket_name"))
	assert.NotEmpty(t, terraform.Output(t, opts, "table_bucket_arn"))
	assert.NotEmpty(t, terraform.Output(t, opts, "ecr_repository_url"))

	tableArns := terraform.OutputMap(t, opts, "table_arns")
	assert.Contains(t, tableArns, "events")
}
