package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestRebindPostgresPlaceholders(t *testing.T) {
	query := `SELECT * FROM ventas WHERE producto_id = ? AND notas <> '?' AND metodo_pago = ?`
	got := rebindPostgresPlaceholders(query)
	want := `SELECT * FROM ventas WHERE producto_id = $1 AND notas <> '?' AND metodo_pago = $2`
	if got != want {
		t.Fatalf("unexpected rebound query\nwant: %s\ngot:  %s", want, got)
	}
}

func TestLoadDatabaseConfigPostgres(t *testing.T) {
	t.Setenv("DB_ENGINE", "postgres")
	t.Setenv("DB_DSN", "postgres://stocki:secret@localhost:5432/stocki")
	t.Setenv("DB_PATH", "")
	t.Setenv("DATABASE_URL", "")

	cfg := loadDatabaseConfig("data.db")
	if cfg.Engine != dbEnginePostgres {
		t.Fatalf("expected postgres engine, got %s", cfg.Engine)
	}
	if !strings.Contains(cfg.DSN, "postgres://stocki:secret") {
		t.Fatalf("unexpected dsn: %s", cfg.DSN)
	}
}

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE unidades (
			id TEXT PRIMARY KEY,
			tenant_id INTEGER NOT NULL DEFAULT 1,
			producto_id TEXT NOT NULL,
			estado TEXT NOT NULL,
			creado_en TEXT NOT NULL
		);`)
	if err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return db
}

func TestSelectAndMarkUnitsSoldFIFO(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	created := []string{
		time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC).Format(time.RFC3339),
		time.Date(2024, 1, 2, 10, 0, 0, 0, time.UTC).Format(time.RFC3339),
		time.Date(2024, 1, 3, 10, 0, 0, 0, time.UTC).Format(time.RFC3339),
	}
	_, err := db.Exec(`INSERT INTO unidades (id, producto_id, estado, creado_en) VALUES
		('U-001', 'P-001', 'Disponible', ?),
		('U-002', 'P-001', 'Disponible', ?),
		('U-003', 'P-001', 'Vendida', ?)
	`, created[0], created[1], created[2])
	if err != nil {
		t.Fatalf("insert unidades: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	ids, err := selectAndMarkUnitsSold(tx, defaultTenantID, "P-001", 2)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("selectAndMarkUnitsSold: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if len(ids) != 2 || ids[0] != "U-001" || ids[1] != "U-002" {
		t.Fatalf("fifo ids inesperados: %v", ids)
	}

	rows, err := db.Query(`SELECT id, estado FROM unidades WHERE producto_id = 'P-001' ORDER BY id`)
	if err != nil {
		t.Fatalf("query unidades: %v", err)
	}
	defer rows.Close()

	estados := map[string]string{}
	for rows.Next() {
		var id, estado string
		if err := rows.Scan(&id, &estado); err != nil {
			t.Fatalf("scan unidad: %v", err)
		}
		estados[id] = estado
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}

	if estados["U-001"] != "Vendida" || estados["U-002"] != "Vendida" || estados["U-003"] != "Vendida" {
		t.Fatalf("estados inesperados: %v", estados)
	}
}

func TestSelectAndMarkUnitsSoldInsufficient(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	created := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC).Format(time.RFC3339)
	_, err := db.Exec(`INSERT INTO unidades (id, producto_id, estado, creado_en) VALUES
		('U-010', 'P-002', 'Disponible', ?)
	`, created)
	if err != nil {
		t.Fatalf("insert unidades: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	_, err = selectAndMarkUnitsSold(tx, defaultTenantID, "P-002", 2)
	if err == nil {
		_ = tx.Rollback()
		t.Fatalf("expected error")
	}
	_ = tx.Rollback()
	if err != errInsufficientStock {
		t.Fatalf("expected errInsufficientStock, got %v", err)
	}
}

func setupOperationsTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE productos (
			sku TEXT PRIMARY KEY,
			tenant_id INTEGER NOT NULL DEFAULT 1,
			id TEXT,
			nombre TEXT NOT NULL,
			precio_venta REAL NOT NULL DEFAULT 0,
			retoma_enabled INTEGER NOT NULL DEFAULT 0,
			retoma_price REAL,
			owner_user_id INTEGER
		);
		CREATE TABLE unidades (
			id TEXT PRIMARY KEY,
			tenant_id INTEGER NOT NULL DEFAULT 1,
			producto_id TEXT NOT NULL,
			estado TEXT NOT NULL,
			creado_en TEXT NOT NULL,
			caducidad TEXT
		);
		CREATE TABLE retomas (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			tenant_id INTEGER NOT NULL DEFAULT 1,
			producto_id TEXT NOT NULL,
			cantidad INTEGER NOT NULL,
			valor_recibido REAL NOT NULL,
			estado_recibido TEXT NOT NULL,
			publicado_stock INTEGER NOT NULL DEFAULT 0,
			precio_publicado REAL,
			notas TEXT NOT NULL DEFAULT '',
			fecha TEXT NOT NULL
		);
		CREATE TABLE movimientos (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			tenant_id INTEGER NOT NULL DEFAULT 1,
			producto_id TEXT NOT NULL,
			unidad_id TEXT NOT NULL,
			tipo TEXT NOT NULL,
			nota TEXT NOT NULL DEFAULT '',
			usuario TEXT NOT NULL DEFAULT '',
			fecha TEXT NOT NULL
		);
		CREATE TABLE audit_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			tenant_id INTEGER NOT NULL DEFAULT 1,
			event_type TEXT NOT NULL,
			entity_type TEXT NOT NULL,
			entity_id TEXT NOT NULL,
			user_id INTEGER,
			source TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			created_at TEXT NOT NULL
		);
		CREATE TABLE movement_settings (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			tenant_id INTEGER NOT NULL DEFAULT 1,
			movement_type TEXT NOT NULL UNIQUE,
			enabled INTEGER NOT NULL DEFAULT 1,
			updated_at TEXT NOT NULL
		);
	`)
	if err != nil {
		t.Fatalf("create operations schema: %v", err)
	}
	return db
}

func mustLoadTestUser(t *testing.T, db *sql.DB, username string) *User {
	t.Helper()

	var user User
	var isActive int
	if err := db.QueryRow(`SELECT id, username, role, tenant_id, is_active FROM users WHERE username = ?`, username).Scan(&user.ID, &user.Username, &user.Role, &user.TenantID, &isActive); err != nil {
		t.Fatalf("load test user %q: %v", username, err)
	}
	user.IsActive = isActive == 1
	return &user
}

func newTenantWriteAPIHandler(db *sql.DB) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/retomas", handleAPIRetomas(db, nil))
	mux.HandleFunc("/api/sales", handleAPISales(db))
	mux.HandleFunc("/api/credits", handleAPICredits(db))
	mux.HandleFunc("/api/credits/installments", handleAPICreditInstallments(db))
	mux.HandleFunc("/api/swaps", handleAPISwaps(db))
	return authMiddleware(db, mux)
}

func setupTenantWriteAPIHarness(t *testing.T) (*sql.DB, http.Handler, *Tenant, string) {
	t.Helper()
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "tenant-write-api.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}

	usersCols, err := tableColumns(db, "users")
	if err != nil {
		t.Fatalf("tableColumns users: %v", err)
	}

	platformAdmin := mustLoadTestUser(t, db, "admin")
	provisioned, err := createTenantWithSeed(db, platformAdmin, usersCols, "Tenant Dos", "tenant-dos", "tenant2.admin", "TenantDos123!")
	if err != nil {
		t.Fatalf("createTenantWithSeed: %v", err)
	}

	return db, newTenantWriteAPIHandler(db), provisioned.Tenant, provisioned.InitialAPIToken
}

func seedTenantProductWithUnits(t *testing.T, db *sql.DB, tenantID int, sku, name string, salePrice float64, units int) {
	t.Helper()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO productos (sku, tenant_id, id, linea, nombre, precio_venta, retoma_enabled)
		VALUES (?, ?, ?, 'Linea Test', ?, ?, 0)
	`, sku, tenantID, sku, name, salePrice); err != nil {
		t.Fatalf("insert product %s: %v", sku, err)
	}
	for i := 0; i < units; i++ {
		unitID := sku + "-U-" + strconv.Itoa(i+1)
		if _, err := db.Exec(`
			INSERT INTO unidades (id, tenant_id, producto_id, estado, creado_en)
			VALUES (?, ?, ?, 'Disponible', ?)
		`, unitID, tenantID, sku, now); err != nil {
			t.Fatalf("insert unit %s: %v", unitID, err)
		}
	}
}

func seedTenantRetomaProductWithUnits(t *testing.T, db *sql.DB, tenantID int, sku, name string, salePrice, retomaPrice float64, units int) {
	t.Helper()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO productos (sku, tenant_id, id, linea, nombre, precio_venta, retoma_enabled, retoma_price)
		VALUES (?, ?, ?, 'Linea Test', ?, ?, 1, ?)
	`, sku, tenantID, sku, name, salePrice, retomaPrice); err != nil {
		t.Fatalf("insert retoma product %s: %v", sku, err)
	}
	for i := 0; i < units; i++ {
		unitID := sku + "-U-" + strconv.Itoa(i+1)
		if _, err := db.Exec(`
			INSERT INTO unidades (id, tenant_id, producto_id, estado, creado_en)
			VALUES (?, ?, ?, 'Disponible', ?)
		`, unitID, tenantID, sku, now); err != nil {
			t.Fatalf("insert retoma unit %s: %v", unitID, err)
		}
	}
}

func seedTenantCreditSale(t *testing.T, db *sql.DB, tenantID int, productID string, installmentsTotal, installmentsPaid int, totalValue, installmentValue float64) int {
	t.Helper()

	now := time.Now().Format(time.RFC3339)
	result, err := db.Exec(`
		INSERT INTO credit_sales (tenant_id, product_id, quantity, debtor_name, debtor_document_type, debtor_document_number, debtor_phone, installments_total, installments_paid, total_value, interest_percent, installment_value, notes, created_at, created_by)
		VALUES (?, ?, 1, 'Cliente Test', 'CC', '123456', '3001112233', ?, ?, ?, 0, ?, 'credito test', ?, 1)
	`, tenantID, productID, installmentsTotal, installmentsPaid, totalValue, installmentValue, now)
	if err != nil {
		t.Fatalf("insert credit sale %s: %v", productID, err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("last insert credit sale id: %v", err)
	}
	return int(id)
}

func performAPIJSONRequest(t *testing.T, handler http.Handler, method, path, token string, payload any) *httptest.ResponseRecorder {
	t.Helper()

	var body *bytes.Reader
	if payload == nil {
		body = bytes.NewReader(nil)
	} else {
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		body = bytes.NewReader(raw)
	}

	req := httptest.NewRequest(method, path, body)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func decodeAPIResponse(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v body=%s", err, rec.Body.String())
	}
	return payload
}

func TestRegisterRetomaPublishToStock(t *testing.T) {
	db := setupOperationsTestDB(t)
	defer db.Close()

	now := time.Date(2026, 3, 22, 10, 0, 0, 0, time.UTC).Format(time.RFC3339)
	_, err := db.Exec(`
		INSERT INTO productos (sku, id, nombre, precio_venta, retoma_enabled, retoma_price)
		VALUES ('P-001', 'P-001', 'Crema corporal', 25000, 1, 12000);
		INSERT INTO movement_settings (movement_type, enabled, updated_at)
		VALUES ('retoma', 1, ?)
	`, now)
	if err != nil {
		t.Fatalf("seed db: %v", err)
	}

	finalSalePrice := 30000.0
	result, err := registerRetoma(db, &User{ID: 1, Username: "admin", Role: "admin"}, retomaOperationInput{
		ProductID:      "P-001",
		Quantity:       2,
		ValueReceived:  15000,
		ReceivedState:  "Usado",
		PublishToStock: true,
		FinalSalePrice: &finalSalePrice,
		Notes:          "retoma test",
	}, "api", nil)
	if err != nil {
		t.Fatalf("registerRetoma: %v", err)
	}
	if !result.PublishedToStock || result.UnitsCreated != 2 {
		t.Fatalf("unexpected result: %+v", result)
	}

	var availableCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM unidades WHERE producto_id = 'P-001' AND estado = 'Disponible'`).Scan(&availableCount); err != nil {
		t.Fatalf("count unidades: %v", err)
	}
	if availableCount != 2 {
		t.Fatalf("expected 2 available units, got %d", availableCount)
	}

	var updatedPrice float64
	if err := db.QueryRow(`SELECT precio_venta FROM productos WHERE sku = 'P-001'`).Scan(&updatedPrice); err != nil {
		t.Fatalf("query updated price: %v", err)
	}
	if updatedPrice != finalSalePrice {
		t.Fatalf("expected updated price %.2f, got %.2f", finalSalePrice, updatedPrice)
	}

	var auditCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit_events WHERE event_type = 'retoma_registered' AND source = 'api'`).Scan(&auditCount); err != nil {
		t.Fatalf("count audit events: %v", err)
	}
	if auditCount != 1 {
		t.Fatalf("expected 1 audit event, got %d", auditCount)
	}
}

func TestInitDBBootstrapsDefaultTenant(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "tenant-bootstrap.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	var (
		slug   string
		name   string
		active int
	)
	if err := db.QueryRow(`SELECT slug, name, active FROM tenants WHERE id = ?`, defaultTenantID).Scan(&slug, &name, &active); err != nil {
		t.Fatalf("query default tenant: %v", err)
	}
	if slug != defaultTenantSlug || name != defaultTenantName || active != 1 {
		t.Fatalf("default tenant inesperado: slug=%q name=%q active=%d", slug, name, active)
	}

	for _, table := range []string{
		"users",
		"sessions",
		"api_keys",
		"productos",
		"unidades",
		"ventas",
		"retomas",
		"credit_sales",
		"credit_installments",
		"movimientos",
		"audit_events",
		"business_settings",
		"business_lines",
		"payment_methods",
		"movement_settings",
	} {
		cols, err := tableColumns(db, table)
		if err != nil {
			t.Fatalf("tableColumns(%s): %v", table, err)
		}
		if !cols["tenant_id"] {
			t.Fatalf("%s sin tenant_id", table)
		}
	}

	var adminTenantID int
	if err := db.QueryRow(`SELECT tenant_id FROM users WHERE username = 'admin'`).Scan(&adminTenantID); err != nil {
		t.Fatalf("query admin tenant: %v", err)
	}
	if adminTenantID != defaultTenantID {
		t.Fatalf("tenant admin inesperado: %d", adminTenantID)
	}
	var adminRole string
	if err := db.QueryRow(`SELECT role FROM users WHERE username = 'admin'`).Scan(&adminRole); err != nil {
		t.Fatalf("query admin role: %v", err)
	}
	if adminRole != rolePlatformAdmin {
		t.Fatalf("role admin inesperado: %q", adminRole)
	}
}

func TestTenantScopedConfigTablesAllowDuplicateNamesAcrossTenants(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "tenant-config.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO tenants (id, slug, name, active, created_at, updated_at)
		VALUES (2, 'tenant-b', 'Tenant B', 1, ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert tenant 2: %v", err)
	}

	if _, err := db.Exec(`
		INSERT INTO business_lines (tenant_id, name, active, created_at, updated_at)
		VALUES (1, 'Dermocosmética', 1, ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert business line tenant 1: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO business_lines (tenant_id, name, active, created_at, updated_at)
		VALUES (2, 'Dermocosmética', 1, ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert business line tenant 2: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO payment_methods (tenant_id, name, active, sort_order, created_at, updated_at)
		VALUES (2, 'Efectivo', 1, 1, ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert payment method tenant 2: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO movement_settings (tenant_id, movement_type, enabled, updated_at)
		VALUES (2, 'venta', 1, ?)
	`, now); err != nil {
		t.Fatalf("insert movement setting tenant 2: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO business_settings (tenant_id, business_name, logo_path, primary_color, currency, date_format, updated_at)
		VALUES (1, 'Tenant A', '', '#0ea5c9', 'COP', '2006-01-02', ?)
	`, now); err != nil {
		t.Fatalf("insert business settings tenant 1: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO business_settings (tenant_id, business_name, logo_path, primary_color, currency, date_format, updated_at)
		VALUES (2, 'Tenant B', '', '#000000', 'COP', '2006-01-02', ?)
	`, now); err != nil {
		t.Fatalf("insert business settings tenant 2: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM business_lines WHERE name = 'Dermocosmética'`).Scan(&count); err != nil {
		t.Fatalf("count business lines: %v", err)
	}
	if count < 2 {
		t.Fatalf("expected duplicated business line across tenants, got %d", count)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM payment_methods WHERE name = 'Efectivo'`).Scan(&count); err != nil {
		t.Fatalf("count payment methods: %v", err)
	}
	if count < 2 {
		t.Fatalf("expected duplicated payment method across tenants, got %d", count)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM movement_settings WHERE movement_type = 'venta'`).Scan(&count); err != nil {
		t.Fatalf("count movement settings: %v", err)
	}
	if count < 2 {
		t.Fatalf("expected duplicated movement setting across tenants, got %d", count)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM business_settings`).Scan(&count); err != nil {
		t.Fatalf("count business settings: %v", err)
	}
	if count < 2 {
		t.Fatalf("expected business settings per tenant, got %d", count)
	}
}

func TestCreateTenantWithSeedCopiesOperationalCatalogs(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "tenant-create.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO business_lines (tenant_id, name, active, created_at, updated_at)
		VALUES (?, 'Smartphones', 1, ?, ?)
	`, defaultTenantID, now, now); err != nil {
		t.Fatalf("insert business line: %v", err)
	}

	adminUser := &User{ID: 1, Username: "admin", Role: rolePlatformAdmin, TenantID: defaultTenantID}
	usersCols, err := tableColumns(db, "users")
	if err != nil {
		t.Fatalf("tableColumns(users): %v", err)
	}
	provisioned, err := createTenantWithSeed(db, adminUser, usersCols, "Tenant Norte", "", "tenant.norte.admin", "Secreta123")
	if err != nil {
		t.Fatalf("createTenantWithSeed: %v", err)
	}
	tenant := provisioned.Tenant

	if tenant == nil || tenant.ID <= defaultTenantID {
		t.Fatalf("tenant inesperado: %+v", tenant)
	}
	if tenant.Slug != "tenant-norte" {
		t.Fatalf("slug inesperado: %q", tenant.Slug)
	}

	var businessName string
	if err := db.QueryRow(`SELECT business_name FROM business_settings WHERE tenant_id = ?`, tenant.ID).Scan(&businessName); err != nil {
		t.Fatalf("query business settings: %v", err)
	}
	if businessName != "Tenant Norte" {
		t.Fatalf("business_name inesperado: %q", businessName)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM business_lines WHERE tenant_id = ?`, tenant.ID).Scan(&count); err != nil {
		t.Fatalf("count business lines: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 business line, got %d", count)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM payment_methods WHERE tenant_id = ?`, tenant.ID).Scan(&count); err != nil {
		t.Fatalf("count payment methods: %v", err)
	}
	if count != len(defaultPaymentMethodNames()) {
		t.Fatalf("expected %d payment methods, got %d", len(defaultPaymentMethodNames()), count)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM movement_settings WHERE tenant_id = ?`, tenant.ID).Scan(&count); err != nil {
		t.Fatalf("count movement settings: %v", err)
	}
	if count != len(defaultMovementTypes()) {
		t.Fatalf("expected %d movement settings, got %d", len(defaultMovementTypes()), count)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit_events WHERE event_type = 'tenant_created' AND entity_id = ?`, strconv.Itoa(tenant.ID)).Scan(&count); err != nil {
		t.Fatalf("count audit events: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 tenant_created audit event, got %d", count)
	}
	var adminTenantID int
	if err := db.QueryRow(`SELECT tenant_id FROM users WHERE username = ?`, "tenant.norte.admin").Scan(&adminTenantID); err != nil {
		t.Fatalf("query tenant admin: %v", err)
	}
	if adminTenantID != tenant.ID {
		t.Fatalf("tenant admin inesperado: got %d want %d", adminTenantID, tenant.ID)
	}
	if provisioned.InitialAPIToken == "" {
		t.Fatalf("expected initial api token")
	}
	if provisioned.InitialAPIKeyName != "tenant-norte-inicial" {
		t.Fatalf("api key name inesperado: %q", provisioned.InitialAPIKeyName)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM api_keys WHERE tenant_id = ? AND name = ?`, tenant.ID, provisioned.InitialAPIKeyName).Scan(&count); err != nil {
		t.Fatalf("count tenant api keys: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 tenant api key, got %d", count)
	}
}

func TestRotateTenantInitialAPIKeyKeepsSingleCanonicalCredential(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "tenant-rotate-key.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	adminUser := &User{ID: 1, Username: "admin", Role: rolePlatformAdmin, TenantID: defaultTenantID}
	usersCols, err := tableColumns(db, "users")
	if err != nil {
		t.Fatalf("tableColumns(users): %v", err)
	}
	provisioned, err := createTenantWithSeed(db, adminUser, usersCols, "Tenant Norte", "", "tenant.norte.admin", "Secreta123")
	if err != nil {
		t.Fatalf("createTenantWithSeed: %v", err)
	}

	tenant, err := updateTenantBasics(db, adminUser, provisioned.Tenant.ID, "Tenant Norte", "tenant-renombrado")
	if err != nil {
		t.Fatalf("updateTenantBasics: %v", err)
	}

	var oldHash string
	if err := db.QueryRow(`SELECT token_hash FROM api_keys WHERE tenant_id = ? AND name = ?`, tenant.ID, "tenant-norte-inicial").Scan(&oldHash); err != nil {
		t.Fatalf("query old token hash: %v", err)
	}

	keyName, token, err := rotateTenantInitialAPIKey(db, adminUser, tenant.ID)
	if err != nil {
		t.Fatalf("rotateTenantInitialAPIKey: %v", err)
	}
	if keyName != "tenant-renombrado-inicial" {
		t.Fatalf("canonical initial key name inesperado: %q", keyName)
	}
	if token == "" {
		t.Fatalf("expected rotated token")
	}

	var (
		count   int
		newHash string
		active  int
	)
	if err := db.QueryRow(`SELECT COUNT(*) FROM api_keys WHERE tenant_id = ? AND name = ?`, tenant.ID, keyName).Scan(&count); err != nil {
		t.Fatalf("count canonical initial key: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 canonical initial key, got %d", count)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM api_keys WHERE tenant_id = ? AND name = ?`, tenant.ID, "tenant-norte-inicial").Scan(&count); err != nil {
		t.Fatalf("count old initial key name: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected old initial key name to be replaced, got %d rows", count)
	}
	if err := db.QueryRow(`SELECT token_hash, active FROM api_keys WHERE tenant_id = ? AND name = ?`, tenant.ID, keyName).Scan(&newHash, &active); err != nil {
		t.Fatalf("query rotated key: %v", err)
	}
	if newHash == oldHash {
		t.Fatalf("expected rotated key hash to change")
	}
	if newHash != hashAPIToken(token) {
		t.Fatalf("rotated key hash does not match returned token")
	}
	if active != 1 {
		t.Fatalf("expected rotated key to remain active, got %d", active)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit_events WHERE event_type = 'tenant_initial_api_key_rotated' AND entity_id = ?`, strconv.Itoa(tenant.ID)).Scan(&count); err != nil {
		t.Fatalf("count rotation audit events: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 tenant_initial_api_key_rotated audit event, got %d", count)
	}
}

func TestUpdateTenantAPIKeyRejectsCrossTenantAccess(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "tenant-api-scope.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO tenants (id, slug, name, active, created_at, updated_at)
		VALUES (2, 'tenant-dos', 'Tenant Dos', 1, ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert tenant 2: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO api_keys (name, token_hash, tenant_id, active, created_at, updated_at)
		VALUES ('tenant-dos-key', ?, 2, 1, ?, ?)
	`, hashAPIToken("tenant-two-token"), now, now); err != nil {
		t.Fatalf("insert tenant 2 api key: %v", err)
	}

	var keyID int
	if err := db.QueryRow(`SELECT id FROM api_keys WHERE tenant_id = 2 AND name = 'tenant-dos-key'`).Scan(&keyID); err != nil {
		t.Fatalf("query tenant 2 api key id: %v", err)
	}

	err = updateTenantAPIKey(db, &User{ID: 10, Username: "tenant1.admin", Role: roleAdmin, TenantID: defaultTenantID}, keyID, "renamed-key", true)
	var reqErr requestError
	if !errors.As(err, &reqErr) || reqErr.Status != http.StatusNotFound {
		t.Fatalf("expected not found for cross-tenant update, got %v", err)
	}
}

func TestUpdateTenantAPIKeyProtectsInitialKeyFromGenericEdit(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "tenant-api-initial-protect.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO api_keys (name, token_hash, tenant_id, active, created_at, updated_at)
		VALUES ('default-inicial', ?, 1, 1, ?, ?)
	`, hashAPIToken("default-token"), now, now); err != nil {
		t.Fatalf("insert default initial key: %v", err)
	}

	var keyID int
	if err := db.QueryRow(`SELECT id FROM api_keys WHERE tenant_id = 1 AND name = 'default-inicial'`).Scan(&keyID); err != nil {
		t.Fatalf("query default initial key id: %v", err)
	}

	err = updateTenantAPIKey(db, &User{ID: 1, Username: "admin", Role: rolePlatformAdmin, TenantID: defaultTenantID}, keyID, "default-inicial-renombrada", false)
	var reqErr requestError
	if !errors.As(err, &reqErr) || reqErr.Status != http.StatusBadRequest {
		t.Fatalf("expected bad request for initial key generic edit, got %v", err)
	}
}

func TestCanManageTenantsRequiresPlatformAdmin(t *testing.T) {
	if canManageTenants(&User{Role: roleAdmin, TenantID: defaultTenantID}) {
		t.Fatalf("default tenant admin should not manage tenants without platform role")
	}
	if !canManageTenants(&User{Role: rolePlatformAdmin, TenantID: defaultTenantID}) {
		t.Fatalf("platform admin should manage tenants")
	}
}

func TestPlatformAdminOnlyMiddleware(t *testing.T) {
	called := false
	handler := platformAdminOnly(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodPost, "/configuracion/tenants/create", nil)
	req = req.WithContext(context.WithValue(req.Context(), userContextKey, &User{Role: roleAdmin, TenantID: defaultTenantID}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for tenant admin, got %d", rec.Code)
	}
	if called {
		t.Fatalf("handler should not execute for tenant admin")
	}

	called = false
	req = httptest.NewRequest(http.MethodPost, "/configuracion/tenants/create", nil)
	req = req.WithContext(context.WithValue(req.Context(), userContextKey, &User{Role: rolePlatformAdmin, TenantID: defaultTenantID}))
	rec = httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for platform admin, got %d", rec.Code)
	}
	if !called {
		t.Fatalf("handler should execute for platform admin")
	}
}

func TestCanManagePlatformUser(t *testing.T) {
	if canManagePlatformUser(&User{Role: roleAdmin, TenantID: defaultTenantID}, rolePlatformAdmin) {
		t.Fatalf("tenant admin should not manage platform admin users")
	}
	if !canManagePlatformUser(&User{Role: rolePlatformAdmin, TenantID: defaultTenantID}, rolePlatformAdmin) {
		t.Fatalf("platform admin should manage platform admin users")
	}
	if !canManagePlatformUser(&User{Role: roleAdmin, TenantID: defaultTenantID}, roleAdmin) {
		t.Fatalf("tenant admin should still manage tenant admin users")
	}
}

func TestListTenantsIncludesOperationalContext(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "tenant-list.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	adminUser := &User{ID: 1, Username: "admin", Role: rolePlatformAdmin, TenantID: defaultTenantID}
	usersCols, err := tableColumns(db, "users")
	if err != nil {
		t.Fatalf("tableColumns(users): %v", err)
	}
	provisioned, err := createTenantWithSeed(db, adminUser, usersCols, "Tenant Norte", "", "tenant.norte.admin", "Secreta123")
	if err != nil {
		t.Fatalf("createTenantWithSeed: %v", err)
	}

	tenants, err := listTenants(db)
	if err != nil {
		t.Fatalf("listTenants: %v", err)
	}

	var found Tenant
	for _, tenant := range tenants {
		if tenant.ID == provisioned.Tenant.ID {
			found = tenant
			break
		}
	}
	if found.ID != provisioned.Tenant.ID {
		t.Fatalf("tenant not found in list: %d", provisioned.Tenant.ID)
	}
	if found.InitialAdminUsername != "tenant.norte.admin" {
		t.Fatalf("unexpected initial admin username: %q", found.InitialAdminUsername)
	}
	if found.InitialAPIKeyName != provisioned.InitialAPIKeyName {
		t.Fatalf("unexpected initial api key name: %q", found.InitialAPIKeyName)
	}
	if found.IsDefault {
		t.Fatalf("expected provisioned tenant to not be default")
	}
}

func TestUpdateTenantBasicsAndToggleState(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "tenant-manage.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	adminUser := &User{ID: 1, Username: "admin", Role: rolePlatformAdmin, TenantID: defaultTenantID}
	usersCols, err := tableColumns(db, "users")
	if err != nil {
		t.Fatalf("tableColumns(users): %v", err)
	}
	provisioned, err := createTenantWithSeed(db, adminUser, usersCols, "Tenant Norte", "", "tenant.norte.admin", "Secreta123")
	if err != nil {
		t.Fatalf("createTenantWithSeed: %v", err)
	}

	updatedTenant, err := updateTenantBasics(db, adminUser, provisioned.Tenant.ID, "Tenant Sur", "tenant-sur")
	if err != nil {
		t.Fatalf("updateTenantBasics: %v", err)
	}
	if updatedTenant.Name != "Tenant Sur" || updatedTenant.Slug != "tenant-sur" {
		t.Fatalf("unexpected updated tenant: %+v", updatedTenant)
	}

	var tenantAdminID int
	if err := db.QueryRow(`SELECT id FROM users WHERE username = ?`, "tenant.norte.admin").Scan(&tenantAdminID); err != nil {
		t.Fatalf("query tenant admin id: %v", err)
	}
	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO sessions (token, user_id, tenant_id, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?)
	`, "tenant-session", tenantAdminID, provisioned.Tenant.ID, now, time.Now().Add(24*time.Hour).Format(time.RFC3339)); err != nil {
		t.Fatalf("insert session: %v", err)
	}

	inactiveTenant, err := setTenantActiveState(db, adminUser, provisioned.Tenant.ID, false)
	if err != nil {
		t.Fatalf("setTenantActiveState(false): %v", err)
	}
	if inactiveTenant.Active {
		t.Fatalf("expected inactive tenant, got %+v", inactiveTenant)
	}

	var sessionCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE tenant_id = ?`, provisioned.Tenant.ID).Scan(&sessionCount); err != nil {
		t.Fatalf("count sessions: %v", err)
	}
	if sessionCount != 0 {
		t.Fatalf("expected tenant sessions to be deleted, got %d", sessionCount)
	}

	reactivatedTenant, err := setTenantActiveState(db, adminUser, provisioned.Tenant.ID, true)
	if err != nil {
		t.Fatalf("setTenantActiveState(true): %v", err)
	}
	if !reactivatedTenant.Active {
		t.Fatalf("expected active tenant after reactivation, got %+v", reactivatedTenant)
	}
}

func TestTenantBaseRestrictions(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "tenant-restrictions.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	adminUser := &User{ID: 1, Username: "admin", Role: rolePlatformAdmin, TenantID: defaultTenantID}

	if _, err := updateTenantBasics(db, adminUser, defaultTenantID, "Base Renombrada", "otro-slug"); err == nil {
		t.Fatalf("expected slug edit restriction for default tenant")
	}
	if _, err := setTenantActiveState(db, adminUser, defaultTenantID, false); err == nil {
		t.Fatalf("expected active toggle restriction for default tenant")
	}
}

func TestAPIAuthFromRequestUsesTenantScopedIntegrationUser(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "tenant-auth.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO tenants (id, slug, name, active, created_at, updated_at)
		VALUES (2, 'tenant-dos', 'Tenant Dos', 1, ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert tenant 2: %v", err)
	}

	token := "tenant-two-token"
	if _, err := db.Exec(`
		INSERT INTO api_keys (name, token_hash, tenant_id, active, created_at, updated_at)
		VALUES ('tenant-dos-key', ?, 2, 1, ?, ?)
	`, hashAPIToken(token), now, now); err != nil {
		t.Fatalf("insert tenant api key: %v", err)
	}

	req := httptest.NewRequest("GET", "/api/inventory", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	user, integrationName, authMode, err := apiAuthFromRequest(db, req)
	if err != nil {
		t.Fatalf("apiAuthFromRequest: %v", err)
	}
	if integrationName != "tenant-dos-key" {
		t.Fatalf("integration name inesperado: %q", integrationName)
	}
	if authMode != "api_key" {
		t.Fatalf("auth mode inesperado: %q", authMode)
	}
	if user == nil || user.Username != "api:tenant-dos-key" || user.TenantID != 2 || user.Role != roleAdmin || user.ID != 0 {
		t.Fatalf("usuario tenant inesperado: %+v", user)
	}
}

func TestAPIAuthFromRequestPrefersBearerOverSession(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "tenant-auth-priority.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO tenants (id, slug, name, active, created_at, updated_at)
		VALUES (2, 'tenant-dos', 'Tenant Dos', 1, ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert tenant 2: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO api_keys (name, token_hash, tenant_id, active, created_at, updated_at)
		VALUES ('tenant-dos-key', ?, 2, 1, ?, ?)
	`, hashAPIToken("tenant-two-token"), now, now); err != nil {
		t.Fatalf("insert tenant api key: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO sessions (token, user_id, tenant_id, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?)
	`, "default-session", 1, defaultTenantID, now, time.Now().Add(24*time.Hour).Format(time.RFC3339)); err != nil {
		t.Fatalf("insert default session: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	req.AddCookie(&http.Cookie{Name: "session_token", Value: "default-session"})
	req.Header.Set("Authorization", "Bearer tenant-two-token")

	user, integrationName, authMode, err := apiAuthFromRequest(db, req)
	if err != nil {
		t.Fatalf("apiAuthFromRequest: %v", err)
	}
	if authMode != "api_key" {
		t.Fatalf("expected api_key auth mode, got %q", authMode)
	}
	if integrationName != "tenant-dos-key" {
		t.Fatalf("unexpected integration name: %q", integrationName)
	}
	if user == nil || user.TenantID != 2 || user.Username != "api:tenant-dos-key" {
		t.Fatalf("expected bearer tenant context, got %+v", user)
	}
}

func TestAPIBusinessSettingsForRequestUsesTenantContext(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "tenant-api-business.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO tenants (id, slug, name, active, created_at, updated_at)
		VALUES (2, 'tenant-dos', 'Tenant Dos', 1, ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert tenant 2: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO business_settings (tenant_id, business_name, logo_path, primary_color, currency, date_format, updated_at)
		VALUES (2, 'Tenant Dos Brand', '/static/logo.png', '#112233', 'USD', '2006-01-02', ?)
	`, now); err != nil {
		t.Fatalf("insert tenant 2 business settings: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/agent/business", nil)
	req = req.WithContext(context.WithValue(req.Context(), tenantContextKey, &Tenant{ID: 2, Slug: "tenant-dos", Name: "Tenant Dos", Active: true}))

	settings, err := apiBusinessSettingsForRequest(db, req)
	if err != nil {
		t.Fatalf("apiBusinessSettingsForRequest: %v", err)
	}
	if settings.BusinessName != "Tenant Dos Brand" || settings.PrimaryColor != "#112233" || settings.Currency != "USD" {
		t.Fatalf("unexpected tenant settings: %+v", settings)
	}
}

func TestAPIAssignableUsersForRequestUsesTenantContext(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "tenant-api-owners.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO tenants (id, slug, name, active, created_at, updated_at)
		VALUES (2, 'tenant-dos', 'Tenant Dos', 1, ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert tenant 2: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO users (username, password_hash, role, tenant_id, created_at, is_active)
		VALUES ('tenant2.staff', 'hash', 'empleado', 2, ?, 1)
	`, now); err != nil {
		t.Fatalf("insert tenant 2 user: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/settings/owners", nil)
	req = req.WithContext(context.WithValue(req.Context(), tenantContextKey, &Tenant{ID: 2, Slug: "tenant-dos", Name: "Tenant Dos", Active: true}))

	users, err := apiAssignableUsersForRequest(db, req)
	if err != nil {
		t.Fatalf("apiAssignableUsersForRequest: %v", err)
	}
	if len(users) != 1 || users[0].Username != "tenant2.staff" {
		t.Fatalf("unexpected tenant-scoped owners: %+v", users)
	}
}

func TestUserFromRequestInvalidatesMismatchedTenantSession(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "tenant-session-mismatch.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO tenants (id, slug, name, active, created_at, updated_at)
		VALUES (2, 'tenant-dos', 'Tenant Dos', 1, ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert tenant 2: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO users (username, password_hash, role, tenant_id, created_at, is_active)
		VALUES ('tenant_user', 'hash', 'admin', 2, ?, 1)
	`, now); err != nil {
		t.Fatalf("insert tenant user: %v", err)
	}

	var userID int
	if err := db.QueryRow(`SELECT id FROM users WHERE username = ?`, "tenant_user").Scan(&userID); err != nil {
		t.Fatalf("query tenant user id: %v", err)
	}

	token := "tenant-session-mismatch"
	if _, err := db.Exec(`
		INSERT INTO sessions (token, user_id, tenant_id, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?)
	`, token, userID, defaultTenantID, now, time.Now().Add(24*time.Hour).Format(time.RFC3339)); err != nil {
		t.Fatalf("insert mismatched session: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/inventario", nil)
	req.AddCookie(&http.Cookie{Name: "session_token", Value: token})

	user, err := userFromRequest(db, req)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got user=%+v err=%v", user, err)
	}

	var sessionCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE token = ?`, token).Scan(&sessionCount); err != nil {
		t.Fatalf("count sessions: %v", err)
	}
	if sessionCount != 0 {
		t.Fatalf("expected mismatched session to be deleted, got %d rows", sessionCount)
	}
}

func TestListSalesForUserIsTenantScoped(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "tenant-sales-scope.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO tenants (id, slug, name, active, created_at, updated_at)
		VALUES (2, 'tenant-dos', 'Tenant Dos', 1, ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert tenant 2: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO productos (sku, tenant_id, id, nombre, linea, precio_venta, retoma_enabled)
		VALUES
			('P-001', 1, 'P-001', 'Producto Uno', 'Linea Uno', 10000, 0),
			('P-002', 2, 'P-002', 'Producto Dos', 'Linea Dos', 20000, 0)
	`); err != nil {
		t.Fatalf("insert products: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO ventas (tenant_id, producto_id, cantidad, precio_final, metodo_pago, channel, sold_by, notas, fecha)
		VALUES
			(1, 'P-001', 1, 10000, 'Efectivo', 'Local', 'maria', 'venta t1', ?),
			(2, 'P-002', 1, 20000, 'Transferencia', 'WhatsApp', 'carlos', 'venta t2', ?)
	`, now, now); err != nil {
		t.Fatalf("insert sales: %v", err)
	}

	items, err := listSalesForUser(db, &User{Role: roleAdmin, TenantID: 2}, "", "", "", 100)
	if err != nil {
		t.Fatalf("listSalesForUser: %v", err)
	}
	if len(items) != 1 || items[0]["product_id"] != "P-002" {
		t.Fatalf("unexpected tenant-scoped sales: %+v", items)
	}
}

func TestListRetomasForUserIsTenantScoped(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "tenant-retomas-scope.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO tenants (id, slug, name, active, created_at, updated_at)
		VALUES (2, 'tenant-dos', 'Tenant Dos', 1, ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert tenant 2: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO productos (sku, tenant_id, id, nombre, linea, precio_venta, retoma_enabled)
		VALUES
			('R-001', 1, 'R-001', 'Retoma Uno', 'Linea Uno', 10000, 1),
			('R-002', 2, 'R-002', 'Retoma Dos', 'Linea Dos', 20000, 1)
	`); err != nil {
		t.Fatalf("insert products: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO retomas (tenant_id, producto_id, cantidad, valor_recibido, estado_recibido, publicado_stock, precio_publicado, notas, fecha)
		VALUES
			(1, 'R-001', 1, 5000, 'Usado', 1, 10000, 'retoma t1', ?),
			(2, 'R-002', 1, 7000, 'Usado', 1, 20000, 'retoma t2', ?)
	`, now, now); err != nil {
		t.Fatalf("insert retomas: %v", err)
	}

	items, err := listRetomasForUser(db, &User{Role: roleAdmin, TenantID: 2}, "", 100)
	if err != nil {
		t.Fatalf("listRetomasForUser: %v", err)
	}
	if len(items) != 1 || items[0]["product_id"] != "R-002" {
		t.Fatalf("unexpected tenant-scoped retomas: %+v", items)
	}
}

func TestListCreditsForUserIsTenantScoped(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "tenant-credits-scope.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO tenants (id, slug, name, active, created_at, updated_at)
		VALUES (2, 'tenant-dos', 'Tenant Dos', 1, ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert tenant 2: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO productos (sku, tenant_id, id, nombre, linea, precio_venta, retoma_enabled)
		VALUES
			('C-001', 1, 'C-001', 'Credito Uno', 'Linea Uno', 10000, 0),
			('C-002', 2, 'C-002', 'Credito Dos', 'Linea Dos', 20000, 0)
	`); err != nil {
		t.Fatalf("insert products: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO credit_sales (tenant_id, product_id, quantity, debtor_name, debtor_document_type, debtor_document_number, debtor_phone, installments_total, installments_paid, total_value, interest_percent, installment_value, notes, created_at, created_by)
		VALUES
			(1, 'C-001', 1, 'Ana Uno', 'CC', '111', '3001111111', 4, 1, 40000, 0, 10000, 'credito t1', ?, 1),
			(2, 'C-002', 1, 'Ana Dos', 'CC', '222', '3002222222', 6, 2, 120000, 0, 20000, 'credito t2', ?, 1)
	`, now, now); err != nil {
		t.Fatalf("insert credits: %v", err)
	}

	items, err := listCreditsForUser(db, &User{Role: roleAdmin, TenantID: 2}, "", 100)
	if err != nil {
		t.Fatalf("listCreditsForUser: %v", err)
	}
	if len(items) != 1 || items[0]["product_id"] != "C-002" {
		t.Fatalf("unexpected tenant-scoped credits: %+v", items)
	}
}

func TestRegisterRetomaRejectsCrossTenantProductAccess(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "tenant-retoma-cross.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO tenants (id, slug, name, active, created_at, updated_at)
		VALUES (2, 'tenant-dos', 'Tenant Dos', 1, ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert tenant 2: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO productos (sku, tenant_id, id, nombre, linea, precio_venta, retoma_enabled, retoma_price)
		VALUES ('R-900', 2, 'R-900', 'Retoma Restringida', 'Linea Dos', 10000, 1, 7000)
	`); err != nil {
		t.Fatalf("insert tenant 2 product: %v", err)
	}

	_, err = registerRetoma(db, &User{ID: 1, Username: "admin", Role: rolePlatformAdmin, TenantID: defaultTenantID}, retomaOperationInput{
		ProductID:      "R-900",
		Quantity:       1,
		ValueReceived:  5000,
		ReceivedState:  "Usado",
		PublishToStock: false,
	}, "api", nil)
	var reqErr requestError
	if !errors.As(err, &reqErr) || reqErr.Status != http.StatusForbidden {
		t.Fatalf("expected forbidden cross-tenant retoma, got %v", err)
	}
}

func TestAddCreditInstallmentRejectsCrossTenantCredit(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "tenant-credit-cross.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO tenants (id, slug, name, active, created_at, updated_at)
		VALUES (2, 'tenant-dos', 'Tenant Dos', 1, ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert tenant 2: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO productos (sku, tenant_id, id, nombre, linea, precio_venta, retoma_enabled)
		VALUES ('C-900', 2, 'C-900', 'Credito Restringido', 'Linea Dos', 15000, 0)
	`); err != nil {
		t.Fatalf("insert tenant 2 product: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO credit_sales (tenant_id, product_id, quantity, debtor_name, debtor_document_type, debtor_document_number, debtor_phone, installments_total, installments_paid, total_value, interest_percent, installment_value, notes, created_at, created_by)
		VALUES (2, 'C-900', 1, 'Pedro', 'CC', '999', '3009999999', 4, 0, 60000, 0, 15000, 'credito t2', ?, 1)
	`, now); err != nil {
		t.Fatalf("insert tenant 2 credit: %v", err)
	}

	var creditSaleID int
	if err := db.QueryRow(`SELECT id FROM credit_sales WHERE tenant_id = 2 AND product_id = 'C-900'`).Scan(&creditSaleID); err != nil {
		t.Fatalf("query credit sale id: %v", err)
	}

	_, err = addCreditInstallment(db, creditSaleID, nil, &User{ID: 1, Username: "admin", Role: rolePlatformAdmin, TenantID: defaultTenantID}, "api", nil)
	var reqErr requestError
	if !errors.As(err, &reqErr) || reqErr.Status != http.StatusNotFound {
		t.Fatalf("expected not found cross-tenant credit installment, got %v", err)
	}
}

func TestSelectAndMarkUnitsSoldRespectsTenantScope(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO unidades (id, tenant_id, producto_id, estado, creado_en)
		VALUES
			('T1-U1', 1, 'P-001', 'Disponible', ?),
			('T2-U1', 2, 'P-001', 'Disponible', ?)
	`, now, now); err != nil {
		t.Fatalf("insert units: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	ids, err := selectAndMarkUnitsSold(tx, 2, "P-001", 1)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("selectAndMarkUnitsSold: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(ids) != 1 || ids[0] != "T2-U1" {
		t.Fatalf("unexpected sold ids: %v", ids)
	}
}

func TestSelectAndMarkUnitsByStatusRespectsTenantScope(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO unidades (id, tenant_id, producto_id, estado, creado_en)
		VALUES
			('T1-U1', 1, 'P-001', 'Disponible', ?),
			('T2-U1', 2, 'P-001', 'Disponible', ?)
	`, now, now); err != nil {
		t.Fatalf("insert units: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	ids, err := selectAndMarkUnitsByStatus(tx, 2, "P-001", 1, "Cambio")
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("selectAndMarkUnitsByStatus: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(ids) != 1 || ids[0] != "T2-U1" {
		t.Fatalf("unexpected swapped ids: %v", ids)
	}
}

func TestAPISalesEndpointRespectsTenantScopeByAPIKey(t *testing.T) {
	db, handler, tenant, token := setupTenantWriteAPIHarness(t)
	defer db.Close()

	seedTenantProductWithUnits(t, db, defaultTenantID, "T1-SALE-001", "Producto Base", 10000, 1)
	seedTenantProductWithUnits(t, db, tenant.ID, "T2-SALE-001", "Producto Tenant Dos", 25000, 2)

	valid := performAPIJSONRequest(t, handler, http.MethodPost, "/api/sales", token, map[string]any{
		"product_id":     "T2-SALE-001",
		"quantity":       1,
		"payment_method": "Efectivo",
		"sale_price":     25000,
		"channel":        "n8n",
		"sold_by":        "agent",
		"notes":          "venta tenant 2",
	})
	if valid.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", valid.Code, valid.Body.String())
	}
	if tenantHeader := valid.Header().Get("X-Stocki-Tenant-ID"); tenantHeader != strconv.Itoa(tenant.ID) {
		t.Fatalf("unexpected tenant header: %q", tenantHeader)
	}
	if authMode := valid.Header().Get("X-Stocki-Auth-Mode"); authMode != "api_key" {
		t.Fatalf("unexpected auth mode header: %q", authMode)
	}
	validBody := decodeAPIResponse(t, valid)
	if validBody["product_id"] != "T2-SALE-001" {
		t.Fatalf("unexpected sales response: %+v", validBody)
	}

	var tenantSales int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ventas WHERE tenant_id = ? AND producto_id = ?`, tenant.ID, "T2-SALE-001").Scan(&tenantSales); err != nil {
		t.Fatalf("count tenant sales: %v", err)
	}
	if tenantSales != 1 {
		t.Fatalf("expected 1 tenant sale, got %d", tenantSales)
	}

	var soldTenantUnits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM unidades WHERE tenant_id = ? AND producto_id = ? AND estado = 'Vendida'`, tenant.ID, "T2-SALE-001").Scan(&soldTenantUnits); err != nil {
		t.Fatalf("count sold tenant units: %v", err)
	}
	if soldTenantUnits != 1 {
		t.Fatalf("expected 1 sold tenant unit, got %d", soldTenantUnits)
	}

	cross := performAPIJSONRequest(t, handler, http.MethodPost, "/api/sales", token, map[string]any{
		"product_id":     "T1-SALE-001",
		"quantity":       1,
		"payment_method": "Efectivo",
		"sale_price":     10000,
	})
	if cross.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for cross-tenant sale, got %d body=%s", cross.Code, cross.Body.String())
	}
	crossBody := decodeAPIResponse(t, cross)
	fields, _ := crossBody["fields"].(map[string]any)
	if _, ok := fields["product_id"]; !ok {
		t.Fatalf("expected product_id validation error, got %+v", crossBody)
	}

	var crossSales int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ventas WHERE producto_id = ?`, "T1-SALE-001").Scan(&crossSales); err != nil {
		t.Fatalf("count cross product sales: %v", err)
	}
	if crossSales != 0 {
		t.Fatalf("expected no sales for cross-tenant product, got %d", crossSales)
	}
}

func TestAPICreditsEndpointRespectsTenantScopeByAPIKey(t *testing.T) {
	db, handler, tenant, token := setupTenantWriteAPIHarness(t)
	defer db.Close()

	seedTenantProductWithUnits(t, db, defaultTenantID, "T1-CREDIT-001", "Credito Base", 18000, 1)
	seedTenantProductWithUnits(t, db, tenant.ID, "T2-CREDIT-001", "Credito Tenant Dos", 32000, 2)

	valid := performAPIJSONRequest(t, handler, http.MethodPost, "/api/credits", token, map[string]any{
		"product_id":             "T2-CREDIT-001",
		"quantity":               1,
		"debtor_name":            "Cliente Tenant Dos",
		"debtor_document_type":   "CC",
		"debtor_document_number": "123456",
		"debtor_phone":           "3001112233",
		"installments_total":     4,
		"total_value":            128000,
		"interest_percent":       0,
		"notes":                  "credito tenant 2",
	})
	if valid.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", valid.Code, valid.Body.String())
	}
	validBody := decodeAPIResponse(t, valid)
	if validBody["product_id"] != "T2-CREDIT-001" {
		t.Fatalf("unexpected credits response: %+v", validBody)
	}

	var tenantCredits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM credit_sales WHERE tenant_id = ? AND product_id = ?`, tenant.ID, "T2-CREDIT-001").Scan(&tenantCredits); err != nil {
		t.Fatalf("count tenant credits: %v", err)
	}
	if tenantCredits != 1 {
		t.Fatalf("expected 1 tenant credit sale, got %d", tenantCredits)
	}

	var soldTenantUnits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM unidades WHERE tenant_id = ? AND producto_id = ? AND estado = 'Vendida'`, tenant.ID, "T2-CREDIT-001").Scan(&soldTenantUnits); err != nil {
		t.Fatalf("count sold credit units: %v", err)
	}
	if soldTenantUnits != 1 {
		t.Fatalf("expected 1 sold tenant credit unit, got %d", soldTenantUnits)
	}

	cross := performAPIJSONRequest(t, handler, http.MethodPost, "/api/credits", token, map[string]any{
		"product_id":             "T1-CREDIT-001",
		"quantity":               1,
		"debtor_name":            "Cliente Cruzado",
		"debtor_document_type":   "CC",
		"debtor_document_number": "999999",
		"debtor_phone":           "3009998877",
		"installments_total":     3,
		"total_value":            54000,
	})
	if cross.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for cross-tenant credit, got %d body=%s", cross.Code, cross.Body.String())
	}
	crossBody := decodeAPIResponse(t, cross)
	fields, _ := crossBody["fields"].(map[string]any)
	if _, ok := fields["product_id"]; !ok {
		t.Fatalf("expected product_id validation error, got %+v", crossBody)
	}

	var crossCredits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM credit_sales WHERE product_id = ?`, "T1-CREDIT-001").Scan(&crossCredits); err != nil {
		t.Fatalf("count cross product credits: %v", err)
	}
	if crossCredits != 0 {
		t.Fatalf("expected no credits for cross-tenant product, got %d", crossCredits)
	}
}

func TestAPISwapsEndpointRespectsTenantScopeByAPIKey(t *testing.T) {
	db, handler, tenant, token := setupTenantWriteAPIHarness(t)
	defer db.Close()

	seedTenantProductWithUnits(t, db, defaultTenantID, "T1-SWAP-001", "Swap Base", 15000, 0)
	seedTenantProductWithUnits(t, db, tenant.ID, "T2-SWAP-OUT", "Swap Salida Tenant Dos", 28000, 1)
	seedTenantProductWithUnits(t, db, tenant.ID, "T2-SWAP-IN", "Swap Entrada Tenant Dos", 30000, 0)

	valid := performAPIJSONRequest(t, handler, http.MethodPost, "/api/swaps", token, map[string]any{
		"product_id":            "T2-SWAP-OUT",
		"quantity":              1,
		"persona_del_cambio":    "Agente Tenant Dos",
		"notes":                 "cambio tenant 2",
		"incoming_mode":         "existing",
		"incoming_existing_id":  "T2-SWAP-IN",
		"incoming_existing_qty": 2,
	})
	if valid.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", valid.Code, valid.Body.String())
	}
	validBody := decodeAPIResponse(t, valid)
	if validBody["incoming_product_id"] != "T2-SWAP-IN" {
		t.Fatalf("unexpected swaps response: %+v", validBody)
	}

	var changedOutgoing int
	if err := db.QueryRow(`SELECT COUNT(*) FROM unidades WHERE tenant_id = ? AND producto_id = ? AND estado = 'Cambio'`, tenant.ID, "T2-SWAP-OUT").Scan(&changedOutgoing); err != nil {
		t.Fatalf("count changed outgoing units: %v", err)
	}
	if changedOutgoing != 1 {
		t.Fatalf("expected 1 changed outgoing unit, got %d", changedOutgoing)
	}

	var incomingTenantUnits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM unidades WHERE tenant_id = ? AND producto_id = ? AND estado = 'Disponible'`, tenant.ID, "T2-SWAP-IN").Scan(&incomingTenantUnits); err != nil {
		t.Fatalf("count incoming tenant units: %v", err)
	}
	if incomingTenantUnits != 2 {
		t.Fatalf("expected 2 incoming tenant units, got %d", incomingTenantUnits)
	}

	var incomingDefaultUnits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM unidades WHERE tenant_id = ? AND producto_id = ?`, defaultTenantID, "T2-SWAP-IN").Scan(&incomingDefaultUnits); err != nil {
		t.Fatalf("count incoming default-tenant units: %v", err)
	}
	if incomingDefaultUnits != 0 {
		t.Fatalf("expected no leaked incoming units in default tenant, got %d", incomingDefaultUnits)
	}

	cross := performAPIJSONRequest(t, handler, http.MethodPost, "/api/swaps", token, map[string]any{
		"product_id":            "T2-SWAP-OUT",
		"quantity":              1,
		"persona_del_cambio":    "Agente Tenant Dos",
		"incoming_mode":         "existing",
		"incoming_existing_id":  "T1-SWAP-001",
		"incoming_existing_qty": 1,
	})
	if cross.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for cross-tenant swap, got %d body=%s", cross.Code, cross.Body.String())
	}
	crossBody := decodeAPIResponse(t, cross)
	fields, _ := crossBody["fields"].(map[string]any)
	if _, ok := fields["incoming_existing_id"]; !ok {
		t.Fatalf("expected incoming_existing_id validation error, got %+v", crossBody)
	}

	var leakedCrossUnits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM unidades WHERE tenant_id = ? AND producto_id = ?`, defaultTenantID, "T1-SWAP-001").Scan(&leakedCrossUnits); err != nil {
		t.Fatalf("count cross-tenant incoming units: %v", err)
	}
	if leakedCrossUnits != 0 {
		t.Fatalf("expected no units created for cross-tenant incoming product, got %d", leakedCrossUnits)
	}
}

func TestAPIRetomasEndpointRespectsTenantScopeByAPIKey(t *testing.T) {
	db, handler, tenant, token := setupTenantWriteAPIHarness(t)
	defer db.Close()

	seedTenantRetomaProductWithUnits(t, db, defaultTenantID, "T1-RET-001", "Retoma Base", 12000, 7000, 0)
	seedTenantRetomaProductWithUnits(t, db, tenant.ID, "T2-RET-001", "Retoma Tenant Dos", 22000, 14000, 0)

	finalSalePrice := 21000.0
	valid := performAPIJSONRequest(t, handler, http.MethodPost, "/api/retomas", token, map[string]any{
		"product_id":       "T2-RET-001",
		"quantity":         2,
		"value_received":   15000,
		"received_state":   "Usado",
		"publish_to_stock": true,
		"final_sale_price": finalSalePrice,
		"notes":            "retoma tenant 2",
	})
	if valid.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", valid.Code, valid.Body.String())
	}
	validBody := decodeAPIResponse(t, valid)
	if validBody["product_id"] != "T2-RET-001" {
		t.Fatalf("unexpected retomas response: %+v", validBody)
	}

	var tenantRetomas int
	if err := db.QueryRow(`SELECT COUNT(*) FROM retomas WHERE tenant_id = ? AND producto_id = ?`, tenant.ID, "T2-RET-001").Scan(&tenantRetomas); err != nil {
		t.Fatalf("count tenant retomas: %v", err)
	}
	if tenantRetomas != 1 {
		t.Fatalf("expected 1 tenant retoma, got %d", tenantRetomas)
	}

	var publishedUnits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM unidades WHERE tenant_id = ? AND producto_id = ? AND estado = 'Disponible'`, tenant.ID, "T2-RET-001").Scan(&publishedUnits); err != nil {
		t.Fatalf("count published retoma units: %v", err)
	}
	if publishedUnits != 2 {
		t.Fatalf("expected 2 published retoma units, got %d", publishedUnits)
	}

	cross := performAPIJSONRequest(t, handler, http.MethodPost, "/api/retomas", token, map[string]any{
		"product_id":       "T1-RET-001",
		"quantity":         1,
		"value_received":   5000,
		"received_state":   "Usado",
		"publish_to_stock": false,
	})
	if cross.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for cross-tenant retoma, got %d body=%s", cross.Code, cross.Body.String())
	}

	var crossRetomas int
	if err := db.QueryRow(`SELECT COUNT(*) FROM retomas WHERE producto_id = ?`, "T1-RET-001").Scan(&crossRetomas); err != nil {
		t.Fatalf("count cross retomas: %v", err)
	}
	if crossRetomas != 0 {
		t.Fatalf("expected no retomas for cross-tenant product, got %d", crossRetomas)
	}
}

func TestAPICreditInstallmentsEndpointRespectsTenantScopeByAPIKey(t *testing.T) {
	db, handler, tenant, token := setupTenantWriteAPIHarness(t)
	defer db.Close()

	seedTenantProductWithUnits(t, db, defaultTenantID, "T1-CREDIT-INS-001", "Credito Base", 18000, 0)
	seedTenantProductWithUnits(t, db, tenant.ID, "T2-CREDIT-INS-001", "Credito Tenant Dos", 28000, 0)

	tenantCreditID := seedTenantCreditSale(t, db, tenant.ID, "T2-CREDIT-INS-001", 4, 1, 112000, 28000)
	crossCreditID := seedTenantCreditSale(t, db, defaultTenantID, "T1-CREDIT-INS-001", 3, 0, 54000, 18000)

	valid := performAPIJSONRequest(t, handler, http.MethodPost, "/api/credits/installments", token, map[string]any{
		"credit_sale_id": tenantCreditID,
		"amount_paid":    28000,
	})
	if valid.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", valid.Code, valid.Body.String())
	}
	validBody := decodeAPIResponse(t, valid)
	if int(validBody["credit_sale_id"].(float64)) != tenantCreditID {
		t.Fatalf("unexpected credit installments response: %+v", validBody)
	}

	var installmentsCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM credit_installments WHERE tenant_id = ? AND credit_sale_id = ?`, tenant.ID, tenantCreditID).Scan(&installmentsCount); err != nil {
		t.Fatalf("count tenant installments: %v", err)
	}
	if installmentsCount != 1 {
		t.Fatalf("expected 1 tenant installment, got %d", installmentsCount)
	}

	var installmentsPaid int
	if err := db.QueryRow(`SELECT installments_paid FROM credit_sales WHERE tenant_id = ? AND id = ?`, tenant.ID, tenantCreditID).Scan(&installmentsPaid); err != nil {
		t.Fatalf("query installments_paid: %v", err)
	}
	if installmentsPaid != 2 {
		t.Fatalf("expected installments_paid=2, got %d", installmentsPaid)
	}

	cross := performAPIJSONRequest(t, handler, http.MethodPost, "/api/credits/installments", token, map[string]any{
		"credit_sale_id": crossCreditID,
		"amount_paid":    18000,
	})
	if cross.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for cross-tenant installment, got %d body=%s", cross.Code, cross.Body.String())
	}

	var crossInstallments int
	if err := db.QueryRow(`SELECT COUNT(*) FROM credit_installments WHERE credit_sale_id = ?`, crossCreditID).Scan(&crossInstallments); err != nil {
		t.Fatalf("count cross installments: %v", err)
	}
	if crossInstallments != 0 {
		t.Fatalf("expected no installments for cross-tenant credit, got %d", crossInstallments)
	}
}

func TestAdjustInventoryProductUpdatesStockAndRetoma(t *testing.T) {
	db := setupOperationsTestDB(t)
	defer db.Close()

	now := time.Date(2026, 3, 22, 10, 0, 0, 0, time.UTC).Format(time.RFC3339)
	_, err := db.Exec(`
		INSERT INTO productos (sku, id, nombre, precio_venta, retoma_enabled)
		VALUES ('P-002', 'P-002', 'Producto test', 20000, 0);
		INSERT INTO unidades (id, producto_id, estado, creado_en)
		VALUES ('U-1', 'P-002', 'Disponible', ?);
		INSERT INTO movement_settings (movement_type, enabled, updated_at)
		VALUES ('retoma', 1, ?)
	`, now, now)
	if err != nil {
		t.Fatalf("seed db: %v", err)
	}

	target := 3
	salePrice := 22000.0
	retomaEnabled := true
	retomaPrice := 12000.0
	result, err := adjustInventoryProduct(db, &User{ID: 1, Username: "admin", Role: "admin"}, inventoryAdjustInput{
		ProductID:      "P-002",
		TargetQuantity: &target,
		Notes:          "ajuste test",
		SalePrice:      &salePrice,
		RetomaEnabled:  &retomaEnabled,
		RetomaPrice:    &retomaPrice,
	}, "api", nil)
	if err != nil {
		t.Fatalf("adjustInventoryProduct: %v", err)
	}
	if result.Delta != 2 || result.CurrentQuantity != 3 {
		t.Fatalf("unexpected result: %+v", result)
	}

	var (
		availableCount  int
		updatedPrice    float64
		retomaEnabledDB int
		retomaPriceDB   sql.NullFloat64
	)
	if err := db.QueryRow(`SELECT COUNT(*) FROM unidades WHERE producto_id = 'P-002' AND estado = 'Disponible'`).Scan(&availableCount); err != nil {
		t.Fatalf("count unidades: %v", err)
	}
	if availableCount != 3 {
		t.Fatalf("expected 3 available units, got %d", availableCount)
	}
	if err := db.QueryRow(`SELECT precio_venta, retoma_enabled, retoma_price FROM productos WHERE sku = 'P-002'`).Scan(&updatedPrice, &retomaEnabledDB, &retomaPriceDB); err != nil {
		t.Fatalf("query product: %v", err)
	}
	if updatedPrice != salePrice || retomaEnabledDB != 1 || !retomaPriceDB.Valid || retomaPriceDB.Float64 != retomaPrice {
		t.Fatalf("unexpected product state: price=%.2f enabled=%d retoma=%v", updatedPrice, retomaEnabledDB, retomaPriceDB)
	}

	var inventoryAuditCount, productAuditCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit_events WHERE event_type = 'inventory_adjusted' AND source = 'api'`).Scan(&inventoryAuditCount); err != nil {
		t.Fatalf("count inventory audit: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit_events WHERE event_type = 'product_updated' AND source = 'api'`).Scan(&productAuditCount); err != nil {
		t.Fatalf("count product audit: %v", err)
	}
	if inventoryAuditCount != 1 || productAuditCount != 1 {
		t.Fatalf("unexpected audit counts inventory=%d product=%d", inventoryAuditCount, productAuditCount)
	}
}
