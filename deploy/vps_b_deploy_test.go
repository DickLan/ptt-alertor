package deploy

import (
	"os"
	"strings"
	"testing"
)

func readDeploymentFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func TestDeployScriptRejectsCurrentCommitBeforeReleaseMutation(t *testing.T) {
	script := readDeploymentFile(t, "vps-b-deploy.sh")
	guard := strings.Index(script, `previous_release == "$release_dir"`)
	markerWrite := strings.Index(script, `>"$release_dir/image"`)
	dockerLoad := strings.Index(script, `docker load`)
	if guard < 0 || markerWrite < 0 || dockerLoad < 0 {
		t.Fatalf("same-commit guard or deployment mutation marker is missing")
	}
	if guard > markerWrite || markerWrite > dockerLoad {
		t.Fatalf("same-commit guard must run before release marker writes and docker load")
	}
}

func TestDeployScriptRequiresFailClosedVPSBRoleAndRegularArtifacts(t *testing.T) {
	script := readDeploymentFile(t, "vps-b-deploy.sh")
	for _, required := range []string{
		"require_exact_setting DEPLOYMENT_ROLE vps-b",
		"require_exact_setting BOARD_ALLOWLIST HardwareSale,MacShop,PC_Shopping",
		"require_exact_setting STOCK_QUANT_NOTIFY_ENABLE false",
		"require_exact_setting PTT_COMMENT_JOBS_ENABLED false",
		"require_exact_setting PTT_PUSHSUM_JOBS_ENABLED false",
		"-L $artifact",
		"deployment artifact owner does not match the deployment user",
	} {
		if !strings.Contains(script, required) {
			t.Fatalf("deployment safety check %q is missing", required)
		}
	}
}

func TestWorkflowUsesPrivateRandomRemoteStagingDirectory(t *testing.T) {
	workflow := readDeploymentFile(t, "../.github/workflows/deploy-vps-b.yml")
	for _, required := range []string{
		"mktemp -d /tmp/ptt-alertor-vps-b.XXXXXXXX",
		"$REMOTE_DIR/image.tar.gz",
		"$REMOTE_DIR/deploy.sh",
	} {
		if !strings.Contains(workflow, required) {
			t.Fatalf("workflow staging control %q is missing", required)
		}
	}
	if strings.Contains(workflow, "/tmp/ptt-alertor-vps-b-$COMMIT-") {
		t.Fatal("workflow still uses predictable shared /tmp artifact names")
	}
}
