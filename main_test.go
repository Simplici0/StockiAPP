package main

import (
	"database/sql"
	"net/http/httptest"
	"path/filepath"
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
	if _, err := db.Exec(`
		INSERT INTO users (username, password_hash, role, tenant_id, created_at, is_active)
		VALUES ('admin_tenant_2', 'hash', 'admin', 2, ?, 1)
	`, now); err != nil {
		t.Fatalf("insert tenant 2 admin: %v", err)
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

	user, integrationName, err := apiAuthFromRequest(db, req)
	if err != nil {
		t.Fatalf("apiAuthFromRequest: %v", err)
	}
	if integrationName != "tenant-dos-key" {
		t.Fatalf("integration name inesperado: %q", integrationName)
	}
	if user == nil || user.Username != "admin_tenant_2" || user.TenantID != 2 {
		t.Fatalf("usuario tenant inesperado: %+v", user)
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
