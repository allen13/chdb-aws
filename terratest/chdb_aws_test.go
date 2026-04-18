package test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/gruntwork-io/terratest/modules/terraform"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	devTfvars      = "dev.tfvars"
	terraformDir   = "./dev"
	awsRegion      = "us-east-1"
	rowsPerAsset   = 25
	uploadPollFor  = 120 * time.Second
	uploadPollTick = 3 * time.Second
)

// Each asset carries the single analytical query we run against it after ingest.
type assetProbe struct {
	name string
	sql  string
}

var assetProbes = []assetProbe{
	{name: "events", sql: "SELECT count() FROM ${asset}"},
	{name: "test_users", sql: "SELECT count() FROM ${asset} WHERE is_active IS NOT NULL"},
	{name: "test_orders", sql: "SELECT sum(amount), count(DISTINCT user_id) FROM ${asset}"},
}

func TestChdbAwsDev(t *testing.T) {
	imageURI := os.Getenv("CHDB_AWS_IMAGE_URI")
	if imageURI == "" {
		t.Skip("CHDB_AWS_IMAGE_URI not set; run scripts/build-image.sh && scripts/push-image.sh first")
	}

	repoRoot, err := filepath.Abs("..")
	require.NoError(t, err)

	opts := &terraform.Options{
		TerraformDir: terraformDir,
		VarFiles:     []string{devTfvars},
		Vars: map[string]interface{}{
			"image_uri": imageURI,
		},
	}

	defer terraform.Destroy(t, opts)
	terraform.InitAndApply(t, opts)

	dataBucket := terraform.Output(t, opts, "data_bucket_name")
	readFn := terraform.Output(t, opts, "read_lambda_function_name")
	require.NotEmpty(t, dataBucket, "data_bucket_name output")
	require.NotEmpty(t, readFn, "read_lambda_function_name output (Lambda not created?)")

	tableArns := terraform.OutputMap(t, opts, "table_arns")
	for _, probe := range assetProbes {
		assert.Contains(t, tableArns, probe.name, "table ARN for %s", probe.name)
	}

	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(awsRegion))
	require.NoError(t, err)
	s3c := s3.NewFromConfig(cfg)

	tfvarsAbsPath := filepath.Join(repoRoot, "terratest", "dev", devTfvars)

	for _, probe := range assetProbes {
		t.Run(probe.name, func(t *testing.T) {
			runLifecycle(t, ctx, s3c, repoRoot, tfvarsAbsPath, dataBucket, readFn, probe)
		})
	}
}

func runLifecycle(
	t *testing.T,
	ctx context.Context,
	s3c *s3.Client,
	repoRoot, tfvarsPath, dataBucket, readFn string,
	probe assetProbe,
) {
	t.Helper()

	parquetPath := filepath.Join(t.TempDir(), probe.name+".parquet")
	runPy(t, repoRoot,
		"scripts/generate_test_data.py",
		"--tfvars", tfvarsPath,
		"--asset", probe.name,
		"--rows", fmt.Sprintf("%d", rowsPerAsset),
		"--output", parquetPath,
		"--seed", "42",
	)

	filename := fmt.Sprintf("%s-%d.parquet", probe.name, time.Now().UnixNano())
	dropzoneKey := fmt.Sprintf("assets/%s/dropzone/%s", probe.name, filename)
	archiveKey := fmt.Sprintf("assets/%s/archive/%s", probe.name, filename)

	body, err := os.ReadFile(parquetPath)
	require.NoError(t, err)

	_, err = s3c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: awsv2.String(dataBucket),
		Key:    awsv2.String(dropzoneKey),
		Body:   strings.NewReader(string(body)),
	})
	require.NoError(t, err, "uploading dropzone parquet for %s", probe.name)

	waitForArchive(t, ctx, s3c, dataBucket, dropzoneKey, archiveKey)

	output := runPy(t, repoRoot,
		"scripts/query.py",
		"--function-name", readFn,
		"--asset", probe.name,
		"--sql", probe.sql,
		"--region", awsRegion,
	)
	assert.NotEmpty(t, strings.TrimSpace(output), "read lambda returned empty body for %s", probe.name)
	t.Logf("%s query result: %s", probe.name, strings.TrimSpace(output))
	// JSONCompact wraps every scalar in an array; "0" never appears alone — any
	// non-empty numeric result that contains a digit > 0 implies rows were read.
	assert.Regexp(t, `[1-9]`, output, "expected non-zero numeric result for %s", probe.name)
}

func waitForArchive(
	t *testing.T,
	ctx context.Context,
	s3c *s3.Client,
	bucket, dropzoneKey, archiveKey string,
) {
	t.Helper()
	deadline := time.Now().Add(uploadPollFor)
	for time.Now().Before(deadline) {
		dropGone := !objectExists(ctx, s3c, bucket, dropzoneKey)
		archivePresent := objectExists(ctx, s3c, bucket, archiveKey)
		if dropGone && archivePresent {
			return
		}
		time.Sleep(uploadPollTick)
	}
	t.Fatalf("timeout waiting for dropzone→archive: bucket=%s dropzone=%s archive=%s", bucket, dropzoneKey, archiveKey)
}

func objectExists(ctx context.Context, s3c *s3.Client, bucket, key string) bool {
	_, err := s3c.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: awsv2.String(bucket),
		Key:    awsv2.String(key),
	})
	if err == nil {
		return true
	}
	var nf *s3types.NotFound
	if errors.As(err, &nf) {
		return false
	}
	// HeadObject returns a generic 404 as an smithy error when the key is missing.
	if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "status code: 404") {
		return false
	}
	// Any other error — surface it. The caller will timeout if this keeps happening.
	return false
}

func runPy(t *testing.T, repoRoot string, args ...string) string {
	t.Helper()
	full := append([]string{"run"}, args...)
	cmd := exec.Command("uv", full...)
	cmd.Dir = repoRoot
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	require.NoError(t, err, "uv run %s failed", strings.Join(args, " "))
	return string(out)
}
