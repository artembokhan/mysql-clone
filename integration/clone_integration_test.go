//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	testcontainers "github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	defaultImage         = "percona/percona-server:8.0.28"
	defaultRootPassword  = "rootpass"
	defaultReplUser      = "repl"
	defaultReplPassword  = "replpass"
	defaultDatasetRows   = 1000
	defaultConcurrency   = 8
	mysqlPort            = "3306/tcp"
	mysqlSocket          = "/tmp/mysql.sock"
	replicationTimeout   = 90 * time.Second
	replicationWaitSleep = 1 * time.Second
	startupTimeout       = 2 * time.Minute
)

type testConfig struct {
	image        string
	rootPassword string
	replUser     string
	replPassword string
	datasetRows  int
	concurrency  int
}

type stepper struct {
	t *testing.T
}

func newStepper(t *testing.T) *stepper {
	return &stepper{t: t}
}

func (s *stepper) Run(name string, fn func()) {
	s.t.Helper()
	start := time.Now()
	ok := false
	defer func() {
		elapsed := time.Since(start).Truncate(100 * time.Millisecond)
		if ok {
			s.t.Logf("[STEP] %s: OK (%s)", name, elapsed)
		} else {
			s.t.Logf("[STEP] %s: FAIL (%s)", name, elapsed)
		}
	}()
	s.t.Logf("[STEP] %s: START", name)
	fn()
	ok = true
}

func TestCloneReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	steps := newStepper(t)
	cfg := loadConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	var (
		netw     *testcontainers.DockerNetwork
		master   testcontainers.Container
		slave    testcontainers.Container
		expected string
		datadir  string
	)

	steps.Run("Create network", func() {
		netw = newNetwork(ctx, t)
	})

	steps.Run("Start master", func() {
		masterDir := filepath.Join(t.TempDir(), "master-datadir")
		if err := os.MkdirAll(masterDir, 0o755); err != nil {
			t.Fatalf("create master datadir: %v", err)
		}
		master = startMaster(ctx, t, cfg, netw.Name, masterDir)
		waitMySQLReady(ctx, t, master, cfg.rootPassword, "")
	})

	steps.Run("Prepare donor dataset", func() {
		prepareDonor(ctx, t, master, cfg)
	})

	steps.Run("Fingerprint donor", func() {
		expected = datasetFingerprint(ctx, t, master, cfg.rootPassword, "")
	})

	steps.Run("Clone dry-run", func() {
		runCloneDryRun(ctx, t, cfg, master)
	})

	steps.Run("Clone dump", func() {
		cloneOut := filepath.Join(t.TempDir(), "clone-out")
		if err := os.MkdirAll(cloneOut, 0o755); err != nil {
			t.Fatalf("create clone output dir: %v", err)
		}
		runCloneDump(ctx, t, cfg, master, cloneOut)

		datadir = filepath.Join(cloneOut, "innodb")
		_ = os.Remove(filepath.Join(datadir, "auto.cnf"))
		sizeBytes, err := dirSize(datadir)
		if err != nil {
			t.Fatalf("compute clone datadir size: %v", err)
		}
		t.Logf("[INFO] clone datadir size: %s", formatBytes(sizeBytes))
		slave = startSlaveFromDatadir(ctx, t, cfg, netw.Name, datadir, "slave")
		waitMySQLReady(ctx, t, slave, cfg.rootPassword, mysqlSocket)
	})

	steps.Run("InnoDB checksum clone", func() {
		runInnoDBChecksum(ctx, t, datadir)
	})

	steps.Run("Fingerprint clone", func() {
		actual := datasetFingerprint(ctx, t, slave, cfg.rootPassword, mysqlSocket)
		if actual != expected {
			t.Fatalf("dataset fingerprint mismatch after clone: expected=%s actual=%s", expected, actual)
		}
	})

	steps.Run("Start replication", func() {
		startReplication(ctx, t, master, slave, cfg)
	})

	var marker string
	steps.Run("Insert replication marker", func() {
		marker = fmt.Sprintf("repl_probe_%d_%d", time.Now().Unix(), time.Now().UnixNano()%100000)
		insertMarker(ctx, t, master, cfg.rootPassword, marker)
	})

	steps.Run("Wait for replication marker", func() {
		waitForMarker(ctx, t, slave, cfg.rootPassword, mysqlSocket, marker)
	})
}

func TestNativeCloneReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	steps := newStepper(t)
	cfg := loadConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	var (
		netw      *testcontainers.DockerNetwork
		master    testcontainers.Container
		slave     testcontainers.Container
		expected  string
		nativeDir string
	)

	steps.Run("Create network", func() {
		netw = newNetwork(ctx, t)
	})

	steps.Run("Start master", func() {
		masterDir := filepath.Join(t.TempDir(), "master-datadir")
		if err := os.MkdirAll(masterDir, 0o755); err != nil {
			t.Fatalf("create master datadir: %v", err)
		}
		master = startMaster(ctx, t, cfg, netw.Name, masterDir)
		waitMySQLReady(ctx, t, master, cfg.rootPassword, "")
	})

	steps.Run("Prepare donor dataset", func() {
		prepareDonor(ctx, t, master, cfg)
	})

	steps.Run("Fingerprint donor", func() {
		expected = datasetFingerprint(ctx, t, master, cfg.rootPassword, "")
	})

	steps.Run("Native clone", func() {
		nativeDir = filepath.Join(t.TempDir(), "native-datadir")
		if err := os.MkdirAll(nativeDir, 0o755); err != nil {
			t.Fatalf("create native datadir: %v", err)
		}
		cloneContainer := startEmptySlave(ctx, t, cfg, netw.Name, nativeDir, "native-clone")
		waitMySQLReady(ctx, t, cloneContainer, cfg.rootPassword, "")
		ensureClonePlugin(ctx, t, cloneContainer, cfg.rootPassword, "")
		runNativeClone(ctx, t, cloneContainer, cfg.rootPassword, "master")
		waitContainerStopped(ctx, t, cloneContainer)
		_ = cloneContainer.Terminate(ctx)
	})

	steps.Run("Start slave from native clone", func() {
		_ = os.Remove(filepath.Join(nativeDir, "auto.cnf"))
		slave = startSlaveFromDatadir(ctx, t, cfg, netw.Name, nativeDir, "native-slave")
		waitMySQLReady(ctx, t, slave, cfg.rootPassword, mysqlSocket)
	})

	steps.Run("Fingerprint native clone", func() {
		actual := datasetFingerprint(ctx, t, slave, cfg.rootPassword, mysqlSocket)
		if actual != expected {
			t.Fatalf("dataset fingerprint mismatch after native clone: expected=%s actual=%s", expected, actual)
		}
	})

	steps.Run("Start replication", func() {
		startReplication(ctx, t, master, slave, cfg)
	})

	var marker string
	steps.Run("Insert replication marker", func() {
		marker = fmt.Sprintf("native_probe_%d_%d", time.Now().Unix(), time.Now().UnixNano()%100000)
		insertMarker(ctx, t, master, cfg.rootPassword, marker)
	})

	steps.Run("Wait for replication marker", func() {
		waitForMarker(ctx, t, slave, cfg.rootPassword, mysqlSocket, marker)
	})
}

func loadConfig(t *testing.T) testConfig {
	t.Helper()
	cfg := testConfig{
		image:        defaultImage,
		rootPassword: defaultRootPassword,
		replUser:     defaultReplUser,
		replPassword: defaultReplPassword,
		datasetRows:  defaultDatasetRows,
		concurrency:  defaultConcurrency,
	}

	if v := strings.TrimSpace(os.Getenv("MYSQL_IMAGE")); v != "" {
		cfg.image = v
	}
	if v := strings.TrimSpace(os.Getenv("ROOT_PASSWORD")); v != "" {
		cfg.rootPassword = v
	}
	if v := strings.TrimSpace(os.Getenv("REPL_USER")); v != "" {
		cfg.replUser = v
	}
	if v := strings.TrimSpace(os.Getenv("REPL_PASSWORD")); v != "" {
		cfg.replPassword = v
	}
	if v := strings.TrimSpace(os.Getenv("DATASET_ROWS")); v != "" {
		rows, err := strconv.Atoi(v)
		if err != nil || rows <= 0 {
			t.Fatalf("invalid DATASET_ROWS=%q", v)
		}
		cfg.datasetRows = rows
	}
	if v := strings.TrimSpace(os.Getenv("CLONE_CONCURRENCY")); v != "" {
		value, err := strconv.Atoi(v)
		if err != nil || value <= 0 {
			t.Fatalf("invalid CLONE_CONCURRENCY=%q", v)
		}
		cfg.concurrency = value
	}

	return cfg
}

func newNetwork(ctx context.Context, t *testing.T) *testcontainers.DockerNetwork {
	t.Helper()
	netw, err := network.New(ctx)
	if err != nil {
		t.Skipf("docker network unavailable: %v", err)
	}
	t.Cleanup(func() { _ = netw.Remove(ctx) })
	return netw
}

func startMaster(ctx context.Context, t *testing.T, cfg testConfig, networkName, datadir string) testcontainers.Container {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        cfg.image,
		Env:          map[string]string{"MYSQL_ROOT_PASSWORD": cfg.rootPassword},
		ExposedPorts: []string{mysqlPort},
		User:         hostUser(),
		Networks:     []string{networkName},
		NetworkAliases: map[string][]string{
			networkName: {"master"},
		},
		Mounts: testcontainers.Mounts(
			testcontainers.BindMount(datadir, testcontainers.ContainerMountTarget("/var/lib/mysql")),
		),
		Cmd: []string{
			"--datadir=/var/lib/mysql",
			"--server-id=1",
			"--log-bin=mysql-bin",
			"--binlog-format=ROW",
			"--pid-file=/tmp/mysqld.pid",
		},
		WaitingFor: wait.ForLog("ready for connections").WithStartupTimeout(startupTimeout),
	}
	return startContainer(ctx, t, req)
}

func startEmptySlave(ctx context.Context, t *testing.T, cfg testConfig, networkName, datadir, alias string) testcontainers.Container {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        cfg.image,
		Env:          map[string]string{"MYSQL_ROOT_PASSWORD": cfg.rootPassword},
		ExposedPorts: []string{mysqlPort},
		User:         hostUser(),
		Networks:     []string{networkName},
		NetworkAliases: map[string][]string{
			networkName: {alias},
		},
		Mounts: testcontainers.Mounts(
			testcontainers.BindMount(datadir, testcontainers.ContainerMountTarget("/var/lib/mysql")),
		),
		Cmd: []string{
			"--server-id=2",
			"--relay-log=relay-bin",
			"--pid-file=/tmp/mysqld.pid",
		},
		WaitingFor: wait.ForLog("ready for connections").WithStartupTimeout(startupTimeout),
	}
	return startContainer(ctx, t, req)
}

func startSlaveFromDatadir(ctx context.Context, t *testing.T, cfg testConfig, networkName, datadir, alias string) testcontainers.Container {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        cfg.image,
		Env:          map[string]string{"MYSQL_ROOT_PASSWORD": cfg.rootPassword},
		ExposedPorts: []string{mysqlPort},
		User:         hostUser(),
		Networks:     []string{networkName},
		NetworkAliases: map[string][]string{
			networkName: {alias},
		},
		Mounts: testcontainers.Mounts(
			testcontainers.BindMount(datadir, testcontainers.ContainerMountTarget("/var/lib/mysql")),
		),
		Cmd: []string{
			"--datadir=/var/lib/mysql",
			"--server-id=2",
			"--relay-log=relay-bin",
			"--socket=" + mysqlSocket,
			"--pid-file=/tmp/mysqld.pid",
			"--skip-log-bin",
		},
		WaitingFor: wait.ForLog("ready for connections").WithStartupTimeout(startupTimeout),
	}
	return startContainer(ctx, t, req)
}

func startContainer(ctx context.Context, t *testing.T, req testcontainers.ContainerRequest) testcontainers.Container {
	t.Helper()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		logs := ""
		if c != nil {
			logs = containerLogs(ctx, c)
			_ = c.Terminate(ctx)
		}
		t.Fatalf("start container: %v\nlogs:\n%s", err, logs)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })
	return c
}

func waitMySQLReady(ctx context.Context, t *testing.T, c testcontainers.Container, rootPassword, socket string) {
	t.Helper()
	deadline := time.Now().Add(startupTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		_, err := execMySQL(ctx, c, rootPassword, socket, "SELECT 1")
		if err == nil {
			return
		}
		lastErr = err
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("mysql did not become ready: %v\nlogs:\n%s", lastErr, containerLogs(ctx, c))
}

func prepareDonor(ctx context.Context, t *testing.T, master testcontainers.Container, cfg testConfig) {
	t.Helper()
	ensureClonePlugin(ctx, t, master, cfg.rootPassword, "")

	sql := fmt.Sprintf(`
CREATE DATABASE IF NOT EXISTS testdump;
USE testdump;

SET @rows := %d;
SET SESSION cte_max_recursion_depth = GREATEST(1000, @rows + 10);

DROP TABLE IF EXISTS seq_numbers;
CREATE TABLE seq_numbers (
  n INT NOT NULL PRIMARY KEY
) ENGINE=InnoDB;

INSERT INTO seq_numbers (n)
WITH RECURSIVE seq AS (
  SELECT 1 AS n
  UNION ALL
  SELECT n + 1 FROM seq WHERE n < @rows
)
SELECT n FROM seq;

DROP TABLE IF EXISTS accounts;
CREATE TABLE accounts (
  id BIGINT NOT NULL PRIMARY KEY,
  username VARCHAR(64) NOT NULL UNIQUE,
  balance DECIMAL(12,2) NOT NULL,
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL
) ENGINE=InnoDB;

INSERT INTO accounts
SELECT
  n,
  CONCAT('user_', n),
  ROUND(((n * 1731) %% 100000) / 100, 2),
  NOW(6),
  NOW(6)
FROM seq_numbers;

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
  id BIGINT NOT NULL PRIMARY KEY,
  account_id BIGINT NOT NULL,
  amount DECIMAL(12,2) NOT NULL,
  created_at DATETIME(6) NOT NULL,
  KEY idx_account (account_id)
) ENGINE=InnoDB;

INSERT INTO orders
SELECT
  n,
  ((n - 1) %% @rows) + 1,
  ROUND(((n * 973) %% 50000) / 100, 2),
  NOW(6)
FROM seq_numbers;

CREATE TABLE IF NOT EXISTS repl_probe (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  marker VARCHAR(128) NOT NULL UNIQUE,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '%s'@'%%';
FLUSH PRIVILEGES;
`, cfg.datasetRows, cfg.replUser, cfg.replPassword, cfg.replUser)

	if _, err := execMySQL(ctx, master, cfg.rootPassword, "", sql); err != nil {
		t.Fatalf("prepare donor: %v", err)
	}
}

func ensureClonePlugin(ctx context.Context, t *testing.T, c testcontainers.Container, rootPassword, socket string) {
	t.Helper()
	installed, err := queryScalar(ctx, c, rootPassword, socket, "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME='clone'")
	if err != nil {
		t.Fatalf("check clone plugin: %v", err)
	}
	if installed == "ACTIVE" {
		return
	}
	_, err = execMySQL(ctx, c, rootPassword, socket, "INSTALL PLUGIN clone SONAME 'mysql_clone.so';")
	if err != nil {
		t.Fatalf("install clone plugin: %v", err)
	}
}

func datasetFingerprint(ctx context.Context, t *testing.T, c testcontainers.Container, rootPassword, socket string) string {
	t.Helper()
	const fingerprintSQL = `
SELECT SHA2(CONCAT_WS('|',
  IFNULL((SELECT COUNT(*) FROM testdump.accounts), 0),
  IFNULL((SELECT SUM(id) FROM testdump.accounts), 0),
  IFNULL((SELECT SUM(CAST(balance * 100 AS SIGNED)) FROM testdump.accounts), 0),
  IFNULL((SELECT COUNT(*) FROM testdump.orders), 0),
  IFNULL((SELECT SUM(id) FROM testdump.orders), 0),
  IFNULL((SELECT SUM(CAST(amount * 100 AS SIGNED)) FROM testdump.orders), 0)
), 256);
`
	fp, err := queryScalar(ctx, c, rootPassword, socket, fingerprintSQL)
	if err != nil {
		t.Fatalf("fingerprint query: %v", err)
	}
	if fp == "" {
		t.Fatalf("empty fingerprint")
	}
	return fp
}

func runCloneDump(ctx context.Context, t *testing.T, cfg testConfig, master testcontainers.Container, outDir string) {
	t.Helper()
	addr := hostPort(ctx, t, master)
	bin := buildCloneBinary(ctx, t)
	cmd := exec.CommandContext(ctx, bin,
		"--addr", addr,
		"--user", "root",
		"--password", cfg.rootPassword,
		"--out", outDir,
		"--mode", "innodb",
		"--concurrency", strconv.Itoa(cfg.concurrency),
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mysql-clone failed: %v\n%s", err, string(out))
	}
}

func runCloneDryRun(ctx context.Context, t *testing.T, cfg testConfig, master testcontainers.Container) {
	t.Helper()
	addr := hostPort(ctx, t, master)
	bin := buildCloneBinary(ctx, t)
	cmd := exec.CommandContext(ctx, bin,
		"--addr", addr,
		"--user", "root",
		"--password", cfg.rootPassword,
		"--mode", "innodb",
		"--dry-run",
		"--concurrency", strconv.Itoa(cfg.concurrency),
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mysql-clone dry-run failed: %v\n%s", err, string(out))
	}
}

func runNativeClone(ctx context.Context, t *testing.T, slave testcontainers.Container, rootPassword, masterAlias string) {
	t.Helper()
	_, err := execMySQL(ctx, slave, rootPassword, "", fmt.Sprintf("SET GLOBAL clone_valid_donor_list = '%s:3306';", masterAlias))
	if err != nil {
		t.Fatalf("set clone_valid_donor_list: %v", err)
	}

	sql := fmt.Sprintf("CLONE INSTANCE FROM 'root'@'%s':3306 IDENTIFIED BY '%s';", masterAlias, rootPassword)
	code, out, err := execMySQLRaw(ctx, slave, rootPassword, "", sql)
	if err != nil {
		t.Fatalf("native clone exec failed: %v", err)
	}
	if code != 0 && !isCloneDisconnect(string(out)) {
		t.Fatalf("native clone failed: %s", string(out))
	}
}

func waitContainerStopped(ctx context.Context, t *testing.T, c testcontainers.Container) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		state, err := c.State(ctx)
		if err == nil && state != nil && !state.Running {
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("container did not stop after native clone\nlogs:\n%s", containerLogs(ctx, c))
}

func startReplication(ctx context.Context, t *testing.T, master, slave testcontainers.Container, cfg testConfig) {
	t.Helper()
	status, err := queryRow(ctx, master, cfg.rootPassword, "", "SHOW MASTER STATUS")
	if err != nil {
		t.Fatalf("master status: %v", err)
	}
	fields := strings.Fields(status)
	if len(fields) < 2 {
		t.Fatalf("unexpected master status: %q", status)
	}
	binlogFile := fields[0]
	binlogPos := fields[1]

	sql := fmt.Sprintf(`
STOP REPLICA;
RESET REPLICA ALL;
CHANGE REPLICATION SOURCE TO
  SOURCE_HOST='master',
  SOURCE_PORT=3306,
  SOURCE_USER='%s',
  SOURCE_PASSWORD='%s',
  SOURCE_LOG_FILE='%s',
  SOURCE_LOG_POS=%s,
  GET_SOURCE_PUBLIC_KEY=1;
START REPLICA;`, cfg.replUser, cfg.replPassword, binlogFile, binlogPos)

	if _, err := execMySQL(ctx, slave, cfg.rootPassword, mysqlSocket, sql); err != nil {
		t.Fatalf("configure replication: %v", err)
	}
	waitReplicationRunning(ctx, t, slave, cfg.rootPassword, mysqlSocket)
}

func waitReplicationRunning(ctx context.Context, t *testing.T, slave testcontainers.Container, rootPassword, socket string) {
	t.Helper()
	deadline := time.Now().Add(replicationTimeout)
	for time.Now().Before(deadline) {
		ioState, _ := queryScalar(ctx, slave, rootPassword, socket, "SELECT IFNULL(SERVICE_STATE,'') FROM performance_schema.replication_connection_status LIMIT 1")
		sqlState, _ := queryScalar(ctx, slave, rootPassword, socket, "SELECT IFNULL(SERVICE_STATE,'') FROM performance_schema.replication_applier_status LIMIT 1")
		if ioState == "ON" && sqlState == "ON" {
			return
		}
		ioErr, _ := queryScalar(ctx, slave, rootPassword, socket, "SELECT IFNULL(LAST_ERROR_MESSAGE,'') FROM performance_schema.replication_connection_status LIMIT 1")
		sqlErr, _ := queryScalar(ctx, slave, rootPassword, socket, "SELECT IFNULL(LAST_ERROR_MESSAGE,'') FROM performance_schema.replication_applier_status LIMIT 1")
		if ioErr != "" || sqlErr != "" {
			t.Fatalf("replica reported errors: io=%s sql=%s", ioErr, sqlErr)
		}
		time.Sleep(replicationWaitSleep)
	}
	t.Fatalf("replica did not reach running state in %s", replicationTimeout)
}

func insertMarker(ctx context.Context, t *testing.T, master testcontainers.Container, rootPassword, marker string) {
	t.Helper()
	sql := fmt.Sprintf("INSERT INTO testdump.repl_probe(marker) VALUES('%s');", marker)
	if _, err := execMySQL(ctx, master, rootPassword, "", sql); err != nil {
		t.Fatalf("insert marker: %v", err)
	}
}

func waitForMarker(ctx context.Context, t *testing.T, slave testcontainers.Container, rootPassword, socket, marker string) {
	t.Helper()
	deadline := time.Now().Add(replicationTimeout)
	for time.Now().Before(deadline) {
		count, _ := queryScalar(ctx, slave, rootPassword, socket, fmt.Sprintf("SELECT COUNT(*) FROM testdump.repl_probe WHERE marker='%s'", marker))
		if count == "1" {
			return
		}
		time.Sleep(replicationWaitSleep)
	}
	t.Fatalf("replication marker not applied: %s", marker)
}

func hostPort(ctx context.Context, t *testing.T, c testcontainers.Container) string {
	t.Helper()
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("container host: %v", err)
	}
	mapped, err := c.MappedPort(ctx, nat.Port(mysqlPort))
	if err != nil {
		t.Fatalf("container port: %v", err)
	}
	return net.JoinHostPort(host, mapped.Port())
}

func buildCloneBinary(ctx context.Context, t *testing.T) string {
	t.Helper()
	clone, _ := buildBinaries(ctx, t)
	return clone
}

func hostUser() string {
	return fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
}

func buildChecksumBinary(ctx context.Context, t *testing.T) string {
	t.Helper()
	_, checksum := buildBinaries(ctx, t)
	return checksum
}

var (
	buildOnce      sync.Once
	buildClonePath string
	buildChecksum  string
	buildErr       error
)

func buildBinaries(ctx context.Context, t *testing.T) (string, string) {
	t.Helper()
	buildOnce.Do(func() {
		root := repoRoot(t)
		outDir, err := os.MkdirTemp("", "clone-it-bins-*")
		if err != nil {
			buildErr = fmt.Errorf("create temp bin dir: %w", err)
			return
		}

		buildClonePath = filepath.Join(outDir, "mysql-clone")
		cmd := exec.CommandContext(ctx, "go", "build", "-o", buildClonePath, "./cmd/mysql-clone")
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if out, err := cmd.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("build mysql-clone: %w\n%s", err, string(out))
			return
		}

		buildChecksum = filepath.Join(outDir, "innodb-checksum")
		cmd = exec.CommandContext(ctx, "go", "build", "-o", buildChecksum, "./cmd/innodb-checksum")
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if out, err := cmd.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("build innodb-checksum: %w\n%s", err, string(out))
			return
		}
	})
	if buildErr != nil {
		t.Fatalf("build tools: %v", buildErr)
	}
	return buildClonePath, buildChecksum
}

func runInnoDBChecksum(ctx context.Context, t *testing.T, datadir string) {
	t.Helper()
	bin := buildChecksumBinary(ctx, t)
	cmd := exec.CommandContext(ctx, bin, datadir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("innodb-checksum failed: %v\n%s", err, string(out))
	}
}

func dirSize(root string) (int64, error) {
	var total int64
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}

func formatBytes(size int64) string {
	return fmt.Sprintf("%d bytes (%.2f MiB)", size, float64(size)/(1024.0*1024.0))
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if fileExists(filepath.Join(wd, "go.mod")) {
		return wd
	}
	parent := filepath.Dir(wd)
	if fileExists(filepath.Join(parent, "go.mod")) {
		return parent
	}
	t.Fatalf("cannot locate repo root from %s", wd)
	return ""
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func execMySQL(ctx context.Context, c testcontainers.Container, rootPassword, socket, sql string) (string, error) {
	code, out, err := execMySQLRaw(ctx, c, rootPassword, socket, sql)
	if err != nil {
		return "", err
	}
	if code != 0 {
		return "", fmt.Errorf("mysql exit %d: %s", code, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

func execMySQLRaw(ctx context.Context, c testcontainers.Container, rootPassword, socket, sql string) (int, []byte, error) {
	cmd := []string{"mysql", "-uroot"}
	if socket != "" {
		cmd = append(cmd, fmt.Sprintf("--socket=%s", socket))
	}
	cmd = append(cmd, "-Nse", sql)
	code, reader, err := c.Exec(ctx, cmd, tcexec.Multiplexed(), tcexec.WithEnv([]string{fmt.Sprintf("MYSQL_PWD=%s", rootPassword)}))
	if err != nil {
		return code, nil, err
	}
	out, err := io.ReadAll(reader)
	if err != nil {
		return code, nil, err
	}
	return code, out, nil
}

func queryScalar(ctx context.Context, c testcontainers.Container, rootPassword, socket, sql string) (string, error) {
	out, err := execMySQL(ctx, c, rootPassword, socket, sql)
	if err != nil {
		return "", err
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) == 0 {
		return "", nil
	}
	return strings.TrimSpace(lines[len(lines)-1]), nil
}

func queryRow(ctx context.Context, c testcontainers.Container, rootPassword, socket, sql string) (string, error) {
	out, err := execMySQL(ctx, c, rootPassword, socket, sql)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

func isCloneDisconnect(output string) bool {
	msg := strings.ToLower(output)
	return strings.Contains(msg, "lost connection") ||
		strings.Contains(msg, "server has gone away") ||
		strings.Contains(msg, "error 2013") ||
		strings.Contains(msg, "error 2006") ||
		strings.Contains(msg, "error 3707") ||
		strings.Contains(msg, "restart server failed")
}

func containerLogs(ctx context.Context, c testcontainers.Container) string {
	rc, err := c.Logs(ctx)
	if err != nil {
		return ""
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return ""
	}
	return string(data)
}
