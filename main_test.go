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

func newTenantWriteAPIHandler(db *sql.DB, usersCols map[string]bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/retomas", handleAPIRetomas(db, nil))
	mux.HandleFunc("/api/sales", handleAPISales(db))
	mux.HandleFunc("/api/customers", handleAPICustomers(db))
	mux.HandleFunc("/api/customers/", handleAPICustomerRoutes(db))
	mux.HandleFunc("/api/invoices", handleAPIInvoices(db))
	mux.HandleFunc("/api/invoices/", handleAPIInvoiceRoutes(db))
	mux.HandleFunc("/api/users", handleAPIUsers(db, usersCols))
	mux.HandleFunc("/api/users/", handleAPIUserRoutes(db, usersCols))
	mux.HandleFunc("/api/agent/customers/search", handleAPIAgentCustomerSearch(db))
	mux.HandleFunc("/api/agent/invoices", handleAPIAgentInvoices(db))
	mux.HandleFunc("/api/credits", handleAPICredits(db))
	mux.HandleFunc("/api/credits/edited", handleAPICreditsEditedReport(db))
	mux.HandleFunc("/api/credits/", handleAPICreditRoutes(db))
	mux.HandleFunc("/api/credits/installments", handleAPICreditInstallments(db))
	mux.HandleFunc("/api/agent/credits", handleAPIAgentCredits(db))
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

	return db, newTenantWriteAPIHandler(db, usersCols), provisioned.Tenant, provisioned.InitialAPIToken
}

func TestCreateManagedUserSupportsTelegramIDAndTenantScope(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "managed-users-create.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	usersCols, err := tableColumns(db, "users")
	if err != nil {
		t.Fatalf("tableColumns users: %v", err)
	}
	if !usersCols["telegram_id"] {
		t.Fatalf("expected telegram_id column in users schema")
	}

	platformAdmin := mustLoadTestUser(t, db, "admin")
	provisioned, err := createTenantWithSeed(db, platformAdmin, usersCols, "Tenant Tres", "tenant-tres", "tenant3.admin", "TenantTres123!")
	if err != nil {
		t.Fatalf("createTenantWithSeed: %v", err)
	}
	tenantAdmin := mustLoadTestUser(t, db, "tenant3.admin")

	created, err := createManagedUser(db, tenantAdmin, tenantAdmin.TenantID, usersCols, managedUserInput{
		Username:   "tenant3.ops",
		Password:   "OpsSegura123!",
		Role:       roleEmployee,
		IsActive:   true,
		TelegramID: "99887766",
	}, "manual", nil)
	if err != nil {
		t.Fatalf("createManagedUser: %v", err)
	}
	if created.TenantID != provisioned.Tenant.ID {
		t.Fatalf("expected tenant_id=%d, got %+v", provisioned.Tenant.ID, created)
	}
	if created.TelegramID != "99887766" {
		t.Fatalf("expected telegram_id persisted, got %+v", created)
	}

	var persistedTelegram string
	var persistedTenantID int
	if err := db.QueryRow(`SELECT telegram_id, tenant_id FROM users WHERE username = ?`, "tenant3.ops").Scan(&persistedTelegram, &persistedTenantID); err != nil {
		t.Fatalf("query created user: %v", err)
	}
	if persistedTelegram != "99887766" {
		t.Fatalf("unexpected persisted telegram_id: %q", persistedTelegram)
	}
	if persistedTenantID != provisioned.Tenant.ID {
		t.Fatalf("unexpected persisted tenant_id: %d", persistedTenantID)
	}
}

func TestUpdateManagedUserUsesTenantScopedAdminSafeguardAndTelegramID(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "managed-users-update.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	usersCols, err := tableColumns(db, "users")
	if err != nil {
		t.Fatalf("tableColumns users: %v", err)
	}

	platformAdmin := mustLoadTestUser(t, db, "admin")
	provisioned, err := createTenantWithSeed(db, platformAdmin, usersCols, "Tenant Cuatro", "tenant-cuatro", "tenant4.admin", "TenantCuatro123!")
	if err != nil {
		t.Fatalf("createTenantWithSeed: %v", err)
	}
	tenantAdmin := mustLoadTestUser(t, db, "tenant4.admin")

	updated, err := createManagedUser(db, tenantAdmin, tenantAdmin.TenantID, usersCols, managedUserInput{
		Username:   "tenant4.staff",
		Password:   "StaffSegura123!",
		Role:       roleEmployee,
		IsActive:   true,
		TelegramID: "123123123",
	}, "manual", nil)
	if err != nil {
		t.Fatalf("createManagedUser employee: %v", err)
	}

	updated, err = updateManagedUser(db, tenantAdmin, tenantAdmin.TenantID, updated.ID, usersCols, managedUserInput{
		Username:   updated.Username,
		Role:       roleEmployee,
		IsActive:   true,
		TelegramID: "555000111",
	}, "manual", nil)
	if err != nil {
		t.Fatalf("updateManagedUser employee: %v", err)
	}
	if updated.TelegramID != "555000111" {
		t.Fatalf("expected telegram_id updated, got %+v", updated)
	}

	_, err = updateManagedUser(db, tenantAdmin, tenantAdmin.TenantID, tenantAdmin.ID, usersCols, managedUserInput{
		Username: tenantAdmin.Username,
		Role:     roleEmployee,
		IsActive: false,
	}, "manual", nil)
	if err == nil {
		t.Fatalf("expected tenant-scoped admin safeguard error")
	}
	var reqErr requestError
	if !errors.As(err, &reqErr) {
		t.Fatalf("expected requestError, got %v", err)
	}
	if reqErr.Message != "Debe existir al menos un admin activo." {
		t.Fatalf("unexpected request error: %+v", reqErr)
	}

	var tenantAdminActive int
	if err := db.QueryRow(`SELECT is_active FROM users WHERE id = ? AND tenant_id = ?`, tenantAdmin.ID, provisioned.Tenant.ID).Scan(&tenantAdminActive); err != nil {
		t.Fatalf("query tenant admin active state: %v", err)
	}
	if tenantAdminActive != 1 {
		t.Fatalf("tenant admin should remain active after rejected update, got %d", tenantAdminActive)
	}
}

func TestProductLabelItemsForUserRespectsTenantScope(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "product-labels.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	usersCols, err := tableColumns(db, "users")
	if err != nil {
		t.Fatalf("tableColumns users: %v", err)
	}

	platformAdmin := mustLoadTestUser(t, db, "admin")
	provisioned, err := createTenantWithSeed(db, platformAdmin, usersCols, "Tenant Etiquetas", "tenant-etiquetas", "tenant-labels.admin", "TenantLabels123!")
	if err != nil {
		t.Fatalf("createTenantWithSeed: %v", err)
	}
	tenantAdmin := mustLoadTestUser(t, db, "tenant-labels.admin")

	seedTenantProductWithUnits(t, db, defaultTenantID, "P-DEFAULT", "Producto Global", 18000, 1)
	seedTenantProductWithUnits(t, db, provisioned.Tenant.ID, "P-TENANT", "Producto Tenant", 26500, 1)

	items, widthMM, heightMM, err := productLabelItemsForUser(db, tenantAdmin, []string{"P-DEFAULT", "P-TENANT"}, "60x40")
	if err != nil {
		t.Fatalf("productLabelItemsForUser: %v", err)
	}
	if widthMM != 60 || heightMM != 40 {
		t.Fatalf("unexpected label dimensions: %dx%d", widthMM, heightMM)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 visible label, got %d", len(items))
	}
	if items[0].ID != "P-TENANT" {
		t.Fatalf("unexpected label item: %+v", items[0])
	}
	if items[0].BarcodeDataURI == "" {
		t.Fatalf("expected barcode data uri")
	}
}

func TestVisibleProductsAndAgentItemsIncludeLocation(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "product-location.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	usersCols, err := tableColumns(db, "users")
	if err != nil {
		t.Fatalf("tableColumns users: %v", err)
	}
	platformAdmin := mustLoadTestUser(t, db, "admin")
	provisioned, err := createTenantWithSeed(db, platformAdmin, usersCols, "Tenant Locacion", "tenant-locacion", "tenantloc.admin", "TenantLocacion123!")
	if err != nil {
		t.Fatalf("createTenantWithSeed: %v", err)
	}
	tenantAdmin := mustLoadTestUser(t, db, "tenantloc.admin")

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO productos (sku, tenant_id, id, linea, nombre, location, precio_venta, fecha_ingreso)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, "LOC-001", provisioned.Tenant.ID, "LOC-001", "Linea API", "Producto con locacion", "Estante B-04", 32000, now); err != nil {
		t.Fatalf("insert product: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO unidades (id, tenant_id, producto_id, estado, creado_en) VALUES (?, ?, ?, 'Disponible', ?)`, "LOC-001-U-1", provisioned.Tenant.ID, "LOC-001", now); err != nil {
		t.Fatalf("insert unit: %v", err)
	}

	productsSnapshot, err := loadVisibleProductsForUser(db, tenantAdmin)
	if err != nil {
		t.Fatalf("loadVisibleProductsForUser: %v", err)
	}
	if len(productsSnapshot) != 1 {
		t.Fatalf("expected 1 visible product, got %d", len(productsSnapshot))
	}
	if productsSnapshot[0].Location != "Estante B-04" {
		t.Fatalf("expected location in visible product, got %+v", productsSnapshot[0])
	}

	countsByProduct, err := loadInventoryCountsForProducts(db, provisioned.Tenant.ID, []string{"LOC-001"})
	if err != nil {
		t.Fatalf("loadInventoryCountsForProducts: %v", err)
	}
	item := agentProductItem(productsSnapshot[0], countsByProduct["LOC-001"], true)
	if item["location"] != "Estante B-04" {
		t.Fatalf("expected location in agent product item, got %+v", item)
	}
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

	_, err = addCreditInstallment(db, creditSaleID, nil, "", &User{ID: 1, Username: "admin", Role: rolePlatformAdmin, TenantID: defaultTenantID}, "api", nil)
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
		"customer_city":          "Bogota",
		"customer_address":       "Calle 1",
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
	var tenantCustomers int
	if err := db.QueryRow(`SELECT COUNT(*) FROM customers WHERE tenant_id = ? AND document_number = ?`, tenant.ID, "123456").Scan(&tenantCustomers); err != nil {
		t.Fatalf("count tenant customers: %v", err)
	}
	if tenantCustomers != 1 {
		t.Fatalf("expected 1 tenant customer, got %d", tenantCustomers)
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
		"customer_city":          "Medellin",
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

func TestAPICreditsEndpointSupportsCashLoanWithoutInventory(t *testing.T) {
	db, handler, tenant, token := setupTenantWriteAPIHarness(t)
	defer db.Close()

	resp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/credits", token, map[string]any{
		"kind":                     "cash_loan",
		"customer_name":            "Cliente Prestamo",
		"customer_phone":           "3004567890",
		"customer_document_type":   "CC",
		"customer_document_number": "456789",
		"customer_city":            "Cali",
		"installments_total":       5,
		"total_value":              500000,
		"interest_percent":         0,
		"notes":                    "prestamo tenant 2",
	})
	if resp.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", resp.Code, resp.Body.String())
	}
	body := decodeAPIResponse(t, resp)
	if body["kind"] != "cash_loan" {
		t.Fatalf("unexpected response: %+v", body)
	}
	if body["product_id"] != "" {
		t.Fatalf("cash loan should not have product_id: %+v", body)
	}

	var creditCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM credit_sales WHERE tenant_id = ? AND kind = 'cash_loan'`, tenant.ID).Scan(&creditCount); err != nil {
		t.Fatalf("count cash loans: %v", err)
	}
	if creditCount != 1 {
		t.Fatalf("expected 1 cash loan, got %d", creditCount)
	}
	var unitsTouched int
	if err := db.QueryRow(`SELECT COUNT(*) FROM unidades WHERE tenant_id = ?`, tenant.ID).Scan(&unitsTouched); err != nil {
		t.Fatalf("count tenant units: %v", err)
	}
	if unitsTouched != 0 {
		t.Fatalf("cash loan should not create or modify inventory units, got %d rows", unitsTouched)
	}
	var movementsTouched int
	if err := db.QueryRow(`SELECT COUNT(*) FROM movimientos WHERE tenant_id = ?`, tenant.ID).Scan(&movementsTouched); err != nil {
		t.Fatalf("count tenant movements: %v", err)
	}
	if movementsTouched != 0 {
		t.Fatalf("cash loan should not create inventory movements, got %d rows", movementsTouched)
	}
}

func TestAPIAgentCreditsEndpointDefaultsToCashLoanWithoutInventory(t *testing.T) {
	db, handler, tenant, token := setupTenantWriteAPIHarness(t)
	defer db.Close()

	resp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/agent/credits", token, map[string]any{
		"customer_name":            "Cliente Agente",
		"customer_phone":           "3005678901",
		"customer_document_type":   "CC",
		"customer_document_number": "567890",
		"customer_city":            "Barranquilla",
		"installments_total":       6,
		"total_value":              720000,
		"interest_percent":         0,
		"notes":                    "prestamo desde agent",
	})
	if resp.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", resp.Code, resp.Body.String())
	}
	if authMode := resp.Header().Get("X-Stocki-Auth-Mode"); authMode != "api_key" {
		t.Fatalf("unexpected auth mode header: %q", authMode)
	}
	body := decodeAPIResponse(t, resp)
	if body["kind"] != "cash_loan" {
		t.Fatalf("unexpected response: %+v", body)
	}
	if body["product_id"] != "" {
		t.Fatalf("agent cash loan should not have product_id: %+v", body)
	}

	var creditCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM credit_sales WHERE tenant_id = ? AND kind = 'cash_loan'`, tenant.ID).Scan(&creditCount); err != nil {
		t.Fatalf("count tenant cash loans: %v", err)
	}
	if creditCount != 1 {
		t.Fatalf("expected 1 tenant cash loan, got %d", creditCount)
	}

	var movementCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM movimientos WHERE tenant_id = ?`, tenant.ID).Scan(&movementCount); err != nil {
		t.Fatalf("count tenant movements: %v", err)
	}
	if movementCount != 0 {
		t.Fatalf("cash loan should not create inventory movements, got %d", movementCount)
	}

	var unitCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM unidades WHERE tenant_id = ?`, tenant.ID).Scan(&unitCount); err != nil {
		t.Fatalf("count tenant units: %v", err)
	}
	if unitCount != 0 {
		t.Fatalf("cash loan should not touch tenant units, got %d", unitCount)
	}

	var customerEvents int
	if err := db.QueryRow(`SELECT COUNT(*) FROM customer_events WHERE tenant_id = ? AND event_type = 'credit_created'`, tenant.ID).Scan(&customerEvents); err != nil {
		t.Fatalf("count customer events: %v", err)
	}
	if customerEvents != 1 {
		t.Fatalf("expected 1 customer credit_created event, got %d", customerEvents)
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

func TestAPICreditsCanUpdateProductCreditAndPersistDerivedState(t *testing.T) {
	db, handler, tenant, token := setupTenantWriteAPIHarness(t)
	defer db.Close()

	seedTenantProductWithUnits(t, db, tenant.ID, "T2-EDIT-CREDIT-001", "Producto Editado", 45000, 3)

	customerResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/customers", token, map[string]any{
		"customer_name":            "Cliente Editado",
		"customer_phone":           "3009991111",
		"customer_document_type":   "CC",
		"customer_document_number": "900111",
		"customer_city":            "Bogota",
	})
	if customerResp.Code != http.StatusCreated {
		t.Fatalf("expected 201 customer, got %d body=%s", customerResp.Code, customerResp.Body.String())
	}
	customerBody := decodeAPIResponse(t, customerResp)
	customerID := int(customerBody["customer"].(map[string]any)["id"].(float64))

	createResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/credits", token, map[string]any{
		"kind":               "product_credit",
		"product_id":         "T2-EDIT-CREDIT-001",
		"quantity":           1,
		"customer_id":        customerID,
		"installments_total": 4,
		"total_value":        120000,
		"interest_percent":   0,
		"installment_value":  30000,
		"notes":              "credito original",
	})
	if createResp.Code != http.StatusCreated {
		t.Fatalf("expected 201 credit, got %d body=%s", createResp.Code, createResp.Body.String())
	}
	createBody := decodeAPIResponse(t, createResp)
	creditSaleID := int(createBody["credit_sale_id"].(float64))

	updateResp := performAPIJSONRequest(t, handler, http.MethodPatch, "/api/credits/"+strconv.Itoa(creditSaleID), token, map[string]any{
		"installments_total": 6,
		"installments_paid":  1,
		"installment_value":  30000,
		"notes":              "credito reprogramado",
		"status":             "suspended",
	})
	if updateResp.Code != http.StatusOK {
		t.Fatalf("expected 200 updating credit, got %d body=%s", updateResp.Code, updateResp.Body.String())
	}
	updateBody := decodeAPIResponse(t, updateResp)
	credit := updateBody["credit"].(map[string]any)
	if credit["status"] != "suspended" {
		t.Fatalf("unexpected status after update: %+v", credit)
	}
	if int(credit["installments_total"].(float64)) != 6 || int(credit["installments_paid"].(float64)) != 1 {
		t.Fatalf("unexpected installments after update: %+v", credit)
	}
	if credit["notes"] != "credito reprogramado" {
		t.Fatalf("unexpected notes after update: %+v", credit)
	}
	if credit["current_debt"].(float64) != 150000 {
		t.Fatalf("unexpected current_debt after update: %+v", credit)
	}

	detailResp := performAPIJSONRequest(t, handler, http.MethodGet, "/api/credits/"+strconv.Itoa(creditSaleID), token, nil)
	if detailResp.Code != http.StatusOK {
		t.Fatalf("expected 200 detail, got %d body=%s", detailResp.Code, detailResp.Body.String())
	}
	detailBody := decodeAPIResponse(t, detailResp)
	if detailBody["credit"].(map[string]any)["status"] != "suspended" {
		t.Fatalf("unexpected detail after update: %+v", detailBody)
	}

	var (
		installmentsTotal int
		installmentsPaid  int
		totalValue        float64
		status            string
		notes             string
	)
	if err := db.QueryRow(`SELECT installments_total, installments_paid, total_value, status, notes FROM credit_sales WHERE tenant_id = ? AND id = ?`, tenant.ID, creditSaleID).Scan(&installmentsTotal, &installmentsPaid, &totalValue, &status, &notes); err != nil {
		t.Fatalf("query updated credit: %v", err)
	}
	if installmentsTotal != 6 || installmentsPaid != 1 || totalValue != 180000 || status != "suspended" || notes != "credito reprogramado" {
		t.Fatalf("unexpected persisted credit state total=%d paid=%d totalValue=%.0f status=%s notes=%s", installmentsTotal, installmentsPaid, totalValue, status, notes)
	}
}

func TestAPICreditsUpdatePreservesRecordedPaymentsAndScopesTenant(t *testing.T) {
	db, handler, _, token := setupTenantWriteAPIHarness(t)
	defer db.Close()

	customerResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/customers", token, map[string]any{
		"customer_name":            "Cliente Prestamo",
		"customer_phone":           "3007770000",
		"customer_document_type":   "CC",
		"customer_document_number": "770000",
		"customer_city":            "Cali",
	})
	if customerResp.Code != http.StatusCreated {
		t.Fatalf("expected 201 customer, got %d body=%s", customerResp.Code, customerResp.Body.String())
	}
	customerID := int(decodeAPIResponse(t, customerResp)["customer"].(map[string]any)["id"].(float64))

	loanResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/agent/credits", token, map[string]any{
		"customer_id":        customerID,
		"installments_total": 4,
		"total_value":        100000,
		"interest_percent":   0,
		"notes":              "prestamo editable",
	})
	if loanResp.Code != http.StatusCreated {
		t.Fatalf("expected 201 loan, got %d body=%s", loanResp.Code, loanResp.Body.String())
	}
	loanID := int(decodeAPIResponse(t, loanResp)["credit_sale_id"].(float64))

	installmentResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/credits/installments", token, map[string]any{
		"credit_sale_id": loanID,
		"amount_paid":    25000,
		"payment_type":   "cuota",
	})
	if installmentResp.Code != http.StatusCreated {
		t.Fatalf("expected 201 installment, got %d body=%s", installmentResp.Code, installmentResp.Body.String())
	}

	invalidUpdate := performAPIJSONRequest(t, handler, http.MethodPatch, "/api/credits/"+strconv.Itoa(loanID), token, map[string]any{
		"installments_total": 4,
		"installments_paid":  0,
		"installment_value":  25000,
		"notes":              "intento invalido",
		"status":             "active",
	})
	if invalidUpdate.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when lowering installments below recorded payments, got %d body=%s", invalidUpdate.Code, invalidUpdate.Body.String())
	}

	validUpdate := performAPIJSONRequest(t, handler, http.MethodPatch, "/api/credits/"+strconv.Itoa(loanID), token, map[string]any{
		"installments_total": 5,
		"installments_paid":  2,
		"installment_value":  30000,
		"notes":              "prestamo reprogramado",
		"status":             "cancelled",
	})
	if validUpdate.Code != http.StatusOK {
		t.Fatalf("expected 200 valid loan update, got %d body=%s", validUpdate.Code, validUpdate.Body.String())
	}
	validBody := decodeAPIResponse(t, validUpdate)
	credit := validBody["credit"].(map[string]any)
	if credit["status"] != "cancelled" || credit["current_debt"].(float64) != 90000 {
		t.Fatalf("unexpected updated loan payload: %+v", credit)
	}

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO customers (tenant_id, name, phone, document_type, document_number, address, city, notes, created_at, updated_at)
		VALUES (1, 'Cliente Ajeno', '3000000000', 'CC', '100001', '', 'Bogota', '', ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert cross customer: %v", err)
	}
	var crossCustomerID int
	if err := db.QueryRow(`SELECT id FROM customers WHERE tenant_id = 1 AND document_number = '100001'`).Scan(&crossCustomerID); err != nil {
		t.Fatalf("query cross customer id: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO credit_sales (tenant_id, customer_id, kind, product_id, quantity, debtor_name, debtor_document_type, debtor_document_number, debtor_phone, installments_total, installments_paid, total_value, interest_percent, installment_value, notes, status, created_at, created_by)
		VALUES (1, ?, 'cash_loan', NULL, 1, 'Cliente Ajeno', 'CC', '100001', '3000000000', 3, 0, 90000, 0, 30000, 'prestamo ajeno', 'active', ?, 1)
	`, crossCustomerID, now); err != nil {
		t.Fatalf("insert cross credit: %v", err)
	}
	var crossCreditID int
	if err := db.QueryRow(`SELECT id FROM credit_sales WHERE tenant_id = 1 AND customer_id = ?`, crossCustomerID).Scan(&crossCreditID); err != nil {
		t.Fatalf("query cross credit id: %v", err)
	}
	crossUpdate := performAPIJSONRequest(t, handler, http.MethodPatch, "/api/credits/"+strconv.Itoa(crossCreditID), token, map[string]any{
		"installments_total": 3,
		"installments_paid":  1,
		"installment_value":  30000,
		"notes":              "no deberia editar",
		"status":             "active",
	})
	if crossUpdate.Code != http.StatusNotFound {
		t.Fatalf("expected 404 cross-tenant update, got %d body=%s", crossUpdate.Code, crossUpdate.Body.String())
	}
}

func TestAPICreditHistoryAndCustomerEventsReflectEditChanges(t *testing.T) {
	db, handler, tenant, token := setupTenantWriteAPIHarness(t)
	defer db.Close()

	seedTenantProductWithUnits(t, db, tenant.ID, "T2-HISTORY-CREDIT-001", "Producto Historial", 50000, 2)

	customerResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/customers", token, map[string]any{
		"customer_name":            "Cliente Historial",
		"customer_phone":           "3001112233",
		"customer_document_type":   "CC",
		"customer_document_number": "9112233",
		"customer_city":            "Bogota",
	})
	if customerResp.Code != http.StatusCreated {
		t.Fatalf("expected 201 customer, got %d body=%s", customerResp.Code, customerResp.Body.String())
	}
	customerID := int(decodeAPIResponse(t, customerResp)["customer"].(map[string]any)["id"].(float64))

	createResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/credits", token, map[string]any{
		"kind":               "product_credit",
		"product_id":         "T2-HISTORY-CREDIT-001",
		"quantity":           1,
		"customer_id":        customerID,
		"installments_total": 4,
		"total_value":        120000,
		"interest_percent":   0,
		"installment_value":  30000,
		"notes":              "credito base historial",
	})
	if createResp.Code != http.StatusCreated {
		t.Fatalf("expected 201 credit, got %d body=%s", createResp.Code, createResp.Body.String())
	}
	creditSaleID := int(decodeAPIResponse(t, createResp)["credit_sale_id"].(float64))

	updateResp := performAPIJSONRequest(t, handler, http.MethodPatch, "/api/credits/"+strconv.Itoa(creditSaleID), token, map[string]any{
		"installments_total": 5,
		"installments_paid":  1,
		"installment_value":  32000,
		"notes":              "credito ajustado para soporte",
		"status":             "suspended",
	})
	if updateResp.Code != http.StatusOK {
		t.Fatalf("expected 200 updating credit, got %d body=%s", updateResp.Code, updateResp.Body.String())
	}

	historyResp := performAPIJSONRequest(t, handler, http.MethodGet, "/api/credits/"+strconv.Itoa(creditSaleID)+"/history?limit=5", token, nil)
	if historyResp.Code != http.StatusOK {
		t.Fatalf("expected 200 history, got %d body=%s", historyResp.Code, historyResp.Body.String())
	}
	historyBody := decodeAPIResponse(t, historyResp)
	if int(historyBody["count"].(float64)) != 1 {
		t.Fatalf("expected 1 history item, got %+v", historyBody)
	}
	historyItems := historyBody["items"].([]any)
	entry := historyItems[0].(map[string]any)
	if entry["event_type"] != "credit_sale_updated" {
		t.Fatalf("unexpected history event: %+v", entry)
	}
	if int(entry["change_count"].(float64)) < 4 {
		t.Fatalf("expected multiple changed fields, got %+v", entry)
	}
	changes := entry["changes"].([]any)
	fields := map[string]bool{}
	for _, raw := range changes {
		change := raw.(map[string]any)
		fields[change["field"].(string)] = true
	}
	for _, field := range []string{"installments_total", "installments_paid", "installment_value", "notes", "status"} {
		if !fields[field] {
			t.Fatalf("expected field %q in history changes, got %+v", field, fields)
		}
	}
	impact := entry["impact"].(map[string]any)
	if impact["status_after"] != "suspended" {
		t.Fatalf("unexpected history impact: %+v", impact)
	}

	customerEventsResp := performAPIJSONRequest(t, handler, http.MethodGet, "/api/customers/"+strconv.Itoa(customerID)+"/events?limit=10", token, nil)
	if customerEventsResp.Code != http.StatusOK {
		t.Fatalf("expected 200 customer events, got %d body=%s", customerEventsResp.Code, customerEventsResp.Body.String())
	}
	customerEventsBody := decodeAPIResponse(t, customerEventsResp)
	eventItems := customerEventsBody["items"].([]any)
	foundUpdate := false
	for _, raw := range eventItems {
		item := raw.(map[string]any)
		if item["event_type"] != "credit_updated" {
			continue
		}
		payload := item["payload"].(map[string]any)
		if int(payload["change_count"].(float64)) < 4 {
			t.Fatalf("expected change_count in customer event payload, got %+v", payload)
		}
		if payload["status"] != "suspended" {
			t.Fatalf("unexpected customer event payload: %+v", payload)
		}
		foundUpdate = true
		break
	}
	if !foundUpdate {
		t.Fatalf("expected credit_updated event in customer timeline, got %+v", eventItems)
	}
}

func TestAPICreditsEditedReportSupportsFiltersAndTenantScope(t *testing.T) {
	db, handler, tenant, token := setupTenantWriteAPIHarness(t)
	defer db.Close()

	seedTenantProductWithUnits(t, db, tenant.ID, "T2-REPORT-CREDIT-001", "Producto Reporte", 42000, 2)

	customerResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/customers", token, map[string]any{
		"customer_name":            "Cliente Reporte",
		"customer_phone":           "3004445566",
		"customer_document_type":   "CC",
		"customer_document_number": "94445566",
		"customer_city":            "Medellin",
	})
	if customerResp.Code != http.StatusCreated {
		t.Fatalf("expected 201 customer, got %d body=%s", customerResp.Code, customerResp.Body.String())
	}
	customerID := int(decodeAPIResponse(t, customerResp)["customer"].(map[string]any)["id"].(float64))

	createResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/credits", token, map[string]any{
		"kind":               "product_credit",
		"product_id":         "T2-REPORT-CREDIT-001",
		"quantity":           1,
		"customer_id":        customerID,
		"installments_total": 4,
		"total_value":        120000,
		"interest_percent":   0,
		"installment_value":  30000,
		"notes":              "credito reporte",
	})
	if createResp.Code != http.StatusCreated {
		t.Fatalf("expected 201 credit, got %d body=%s", createResp.Code, createResp.Body.String())
	}
	creditSaleID := int(decodeAPIResponse(t, createResp)["credit_sale_id"].(float64))

	updateResp := performAPIJSONRequest(t, handler, http.MethodPatch, "/api/credits/"+strconv.Itoa(creditSaleID), token, map[string]any{
		"installments_total": 5,
		"installments_paid":  1,
		"installment_value":  32000,
		"notes":              "credito reporte ajustado",
		"status":             "suspended",
	})
	if updateResp.Code != http.StatusOK {
		t.Fatalf("expected 200 updating credit, got %d body=%s", updateResp.Code, updateResp.Body.String())
	}

	reportResp := performAPIJSONRequest(t, handler, http.MethodGet, "/api/credits/edited?status=suspended&kind=product_credit&customer=94445566&credit_sale_id="+strconv.Itoa(creditSaleID), token, nil)
	if reportResp.Code != http.StatusOK {
		t.Fatalf("expected 200 report, got %d body=%s", reportResp.Code, reportResp.Body.String())
	}
	reportBody := decodeAPIResponse(t, reportResp)
	if int(reportBody["count"].(float64)) != 1 {
		t.Fatalf("expected 1 report item, got %+v", reportBody)
	}
	item := reportBody["items"].([]any)[0].(map[string]any)
	if int(item["credit_sale_id"].(float64)) != creditSaleID {
		t.Fatalf("unexpected credit_sale_id: %+v", item)
	}
	if item["tenant_id"].(float64) != float64(tenant.ID) {
		t.Fatalf("unexpected tenant scope: %+v", item)
	}
	if item["status_after"] != "suspended" {
		t.Fatalf("unexpected status_after: %+v", item)
	}
	if int(item["change_count"].(float64)) == 0 {
		t.Fatalf("expected report changes, got %+v", item)
	}

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO customers (tenant_id, name, phone, document_type, document_number, address, city, notes, created_at, updated_at)
		VALUES (1, 'Cliente Ajeno Reporte', '3000009999', 'CC', '10009999', '', 'Bogota', '', ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert cross customer: %v", err)
	}
	var crossCustomerID int
	if err := db.QueryRow(`SELECT id FROM customers WHERE tenant_id = 1 AND document_number = '10009999'`).Scan(&crossCustomerID); err != nil {
		t.Fatalf("query cross customer id: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO credit_sales (tenant_id, customer_id, kind, product_id, quantity, debtor_name, debtor_document_type, debtor_document_number, debtor_phone, installments_total, installments_paid, total_value, interest_percent, installment_value, notes, status, created_at, created_by)
		VALUES (1, ?, 'cash_loan', NULL, 1, 'Cliente Ajeno Reporte', 'CC', '10009999', '3000009999', 3, 0, 90000, 0, 30000, 'prestamo ajeno reporte', 'active', ?, 1)
	`, crossCustomerID, now); err != nil {
		t.Fatalf("insert cross credit: %v", err)
	}
	var crossCreditID int
	if err := db.QueryRow(`SELECT id FROM credit_sales WHERE tenant_id = 1 AND customer_id = ?`, crossCustomerID).Scan(&crossCreditID); err != nil {
		t.Fatalf("query cross credit id: %v", err)
	}
	crossPayload := `{"changes":[{"field":"status","label":"Estado","before":"active","after":"cancelled"}],"change_count":1,"changed_fields":["status"],"impact":{"status_before":"active","status_after":"cancelled","status_label_before":"Crédito activo","status_label_after":"Crédito cancelado","current_debt_before":90000,"current_debt_after":90000,"debt_total_before":90000,"debt_total_after":90000,"total_paid_before":0,"total_paid_after":0,"installments_due_after":3}}`
	if _, err := db.Exec(`
		INSERT INTO audit_events (tenant_id, event_type, entity_type, entity_id, user_id, source, payload_json, created_at)
		VALUES (1, 'credit_sale_updated', 'credit_sale', ?, 1, 'api', ?, ?)
	`, strconv.Itoa(crossCreditID), crossPayload, now); err != nil {
		t.Fatalf("insert cross audit event: %v", err)
	}

	crossReportResp := performAPIJSONRequest(t, handler, http.MethodGet, "/api/credits/edited", token, nil)
	if crossReportResp.Code != http.StatusOK {
		t.Fatalf("expected 200 report without cross tenant, got %d body=%s", crossReportResp.Code, crossReportResp.Body.String())
	}
	crossReportBody := decodeAPIResponse(t, crossReportResp)
	if int(crossReportBody["count"].(float64)) != 1 {
		t.Fatalf("expected only tenant-scoped report item, got %+v", crossReportBody)
	}
}

func TestAddCreditInstallmentSupportsCashLoan(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "cash-loan-installments.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO customers (tenant_id, name, phone, document_type, document_number, address, city, notes, created_at, updated_at)
		VALUES (1, 'Laura Dinero', '3007778899', 'CC', '700700', '', 'Bogota', '', ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert customer: %v", err)
	}
	var customerID int
	if err := db.QueryRow(`SELECT id FROM customers WHERE tenant_id = 1 AND document_number = '700700'`).Scan(&customerID); err != nil {
		t.Fatalf("query customer id: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO credit_sales (tenant_id, customer_id, kind, product_id, quantity, debtor_name, debtor_document_type, debtor_document_number, debtor_phone, installments_total, installments_paid, total_value, interest_percent, installment_value, notes, created_at, created_by)
		VALUES (1, ?, 'cash_loan', NULL, 1, 'Laura Dinero', 'CC', '700700', '3007778899', 4, 1, 400000, 0, 100000, 'prestamo cash', ?, 1)
	`, customerID, now); err != nil {
		t.Fatalf("insert cash loan: %v", err)
	}
	var creditSaleID int
	if err := db.QueryRow(`SELECT id FROM credit_sales WHERE tenant_id = 1 AND kind = 'cash_loan' ORDER BY id DESC LIMIT 1`).Scan(&creditSaleID); err != nil {
		t.Fatalf("query credit sale id: %v", err)
	}

	result, err := addCreditInstallment(db, creditSaleID, nil, "cuota", &User{ID: 1, Username: "admin", Role: roleAdmin, TenantID: 1}, "api", nil)
	if err != nil {
		t.Fatalf("addCreditInstallment: %v", err)
	}
	if result.Kind != creditSaleKindCash {
		t.Fatalf("unexpected kind: %+v", result)
	}
	if result.ProductID != "" {
		t.Fatalf("cash loan should keep empty product_id: %+v", result)
	}
	if result.InstallmentsPaid != 2 {
		t.Fatalf("expected installments_paid=2, got %+v", result)
	}
}

func TestListCreditsForUserIncludesCustomerAndDebtMetrics(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "credits-customer-metrics.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO customers (tenant_id, name, phone, document_type, document_number, address, city, notes, created_at, updated_at)
		VALUES (1, 'Maria Gomez', '3001234567', 'CC', '101010', 'Calle 10', 'Bogota', 'Cliente frecuente', ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert customer: %v", err)
	}
	var customerID int
	if err := db.QueryRow(`SELECT id FROM customers WHERE tenant_id = 1 AND document_number = '101010'`).Scan(&customerID); err != nil {
		t.Fatalf("query customer id: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO productos (sku, tenant_id, id, nombre, linea, precio_venta, retoma_enabled)
		VALUES ('CRM-001', 1, 'CRM-001', 'Producto CRM', 'Linea Uno', 20000, 0)
	`); err != nil {
		t.Fatalf("insert product: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO credit_sales (tenant_id, customer_id, product_id, quantity, debtor_name, debtor_document_type, debtor_document_number, debtor_phone, installments_total, installments_paid, total_value, interest_percent, installment_value, notes, created_at, created_by)
		VALUES (1, ?, 'CRM-001', 1, 'Maria Gomez', 'CC', '101010', '3001234567', 4, 1, 80000, 0, 20000, 'credito crm', ?, 1)
	`, customerID, now); err != nil {
		t.Fatalf("insert credit sale: %v", err)
	}
	var creditSaleID int
	if err := db.QueryRow(`SELECT id FROM credit_sales WHERE tenant_id = 1 AND product_id = 'CRM-001'`).Scan(&creditSaleID); err != nil {
		t.Fatalf("query credit sale id: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO credit_installments (tenant_id, credit_sale_id, product_id, installment_number, amount_paid, payment_type, created_at, created_by)
		VALUES
			(1, ?, 'CRM-001', 1, 20000, 'cuota', ?, 1),
			(1, ?, 'CRM-001', 1, 5000, 'abono', ?, 1)
	`, creditSaleID, now, creditSaleID, now); err != nil {
		t.Fatalf("insert installments: %v", err)
	}

	items, err := listCreditsForUser(db, &User{Role: roleAdmin, TenantID: 1}, "maria", 20)
	if err != nil {
		t.Fatalf("listCreditsForUser: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 credit item, got %d", len(items))
	}
	item := items[0]
	if int(item["customer_id"].(int)) != customerID {
		t.Fatalf("unexpected customer_id: %+v", item)
	}
	if item["customer_city"] != "Bogota" {
		t.Fatalf("unexpected customer city: %+v", item)
	}
	if item["total_paid"].(float64) != 25000 {
		t.Fatalf("unexpected total_paid: %+v", item)
	}
	if item["current_debt"].(float64) != 55000 {
		t.Fatalf("unexpected current_debt: %+v", item)
	}
	if item["paid_installments_count"].(int) != 1 {
		t.Fatalf("unexpected paid_installments_count: %+v", item)
	}
}

func TestListCreditsForUserIncludesCashLoanKind(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "credits-cash-loan-kind.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO customers (tenant_id, name, phone, document_type, document_number, address, city, notes, created_at, updated_at)
		VALUES (1, 'Carlos Prestamo', '3002223344', 'CC', '808080', '', 'Bogota', '', ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert customer: %v", err)
	}
	var customerID int
	if err := db.QueryRow(`SELECT id FROM customers WHERE tenant_id = 1 AND document_number = '808080'`).Scan(&customerID); err != nil {
		t.Fatalf("query customer id: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO credit_sales (tenant_id, customer_id, kind, product_id, quantity, debtor_name, debtor_document_type, debtor_document_number, debtor_phone, installments_total, installments_paid, total_value, interest_percent, installment_value, notes, created_at, created_by)
		VALUES (1, ?, 'cash_loan', NULL, 1, 'Carlos Prestamo', 'CC', '808080', '3002223344', 6, 1, 600000, 0, 100000, 'prestamo de dinero', ?, 1)
	`, customerID, now); err != nil {
		t.Fatalf("insert cash loan: %v", err)
	}

	items, err := listCreditsForUser(db, &User{Role: roleAdmin, TenantID: 1}, "prestamo", 20)
	if err != nil {
		t.Fatalf("listCreditsForUser: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 cash loan item, got %d", len(items))
	}
	item := items[0]
	if item["kind"] != "cash_loan" {
		t.Fatalf("unexpected kind: %+v", item)
	}
	if item["product"] != "Préstamo de dinero" {
		t.Fatalf("unexpected product label: %+v", item)
	}
}

func TestAddCreditInstallmentAbonoDoesNotIncreaseInstallmentsPaid(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "credits-abono.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO customers (tenant_id, name, phone, document_type, document_number, address, city, notes, created_at, updated_at)
		VALUES (1, 'Pedro Pago', '3000000000', 'CC', '454545', '', 'Bogota', '', ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert customer: %v", err)
	}
	var customerID int
	if err := db.QueryRow(`SELECT id FROM customers WHERE tenant_id = 1 AND document_number = '454545'`).Scan(&customerID); err != nil {
		t.Fatalf("query customer id: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO productos (sku, tenant_id, id, nombre, linea, precio_venta, retoma_enabled)
		VALUES ('ABONO-001', 1, 'ABONO-001', 'Producto Abono', 'Linea Uno', 15000, 0)
	`); err != nil {
		t.Fatalf("insert product: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO credit_sales (tenant_id, customer_id, product_id, quantity, debtor_name, debtor_document_type, debtor_document_number, debtor_phone, installments_total, installments_paid, total_value, interest_percent, installment_value, notes, created_at, created_by)
		VALUES (1, ?, 'ABONO-001', 1, 'Pedro Pago', 'CC', '454545', '3000000000', 4, 1, 60000, 0, 15000, 'credito abono', ?, 1)
	`, customerID, now); err != nil {
		t.Fatalf("insert credit sale: %v", err)
	}
	var creditSaleID int
	if err := db.QueryRow(`SELECT id FROM credit_sales WHERE tenant_id = 1 AND product_id = 'ABONO-001'`).Scan(&creditSaleID); err != nil {
		t.Fatalf("query credit sale id: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO credit_installments (tenant_id, credit_sale_id, product_id, installment_number, amount_paid, payment_type, created_at, created_by)
		VALUES (1, ?, 'ABONO-001', 1, 15000, 'cuota', ?, 1)
	`, creditSaleID, now); err != nil {
		t.Fatalf("insert initial cuota: %v", err)
	}

	amountPaid := 5000.0
	result, err := addCreditInstallment(db, creditSaleID, &amountPaid, "abono", &User{ID: 1, Username: "admin", Role: roleAdmin, TenantID: 1}, "api", nil)
	if err != nil {
		t.Fatalf("addCreditInstallment abono: %v", err)
	}
	if result.PaymentType != creditPaymentTypeAbono {
		t.Fatalf("expected payment type abono, got %q", result.PaymentType)
	}
	if result.InstallmentsPaid != 1 {
		t.Fatalf("expected installments_paid to remain 1, got %d", result.InstallmentsPaid)
	}
	if result.TotalPaid != 20000 {
		t.Fatalf("unexpected total paid after abono: %+v", result)
	}
	if result.CurrentDebt != 40000 {
		t.Fatalf("unexpected current debt after abono: %+v", result)
	}

	var installmentsPaid int
	if err := db.QueryRow(`SELECT installments_paid FROM credit_sales WHERE tenant_id = 1 AND id = ?`, creditSaleID).Scan(&installmentsPaid); err != nil {
		t.Fatalf("query installments_paid: %v", err)
	}
	if installmentsPaid != 1 {
		t.Fatalf("expected persisted installments_paid to remain 1, got %d", installmentsPaid)
	}
	var paymentType string
	if err := db.QueryRow(`SELECT payment_type FROM credit_installments WHERE tenant_id = 1 AND credit_sale_id = ? ORDER BY id DESC LIMIT 1`, creditSaleID).Scan(&paymentType); err != nil {
		t.Fatalf("query payment_type: %v", err)
	}
	if paymentType != "abono" {
		t.Fatalf("expected payment_type abono, got %q", paymentType)
	}
}

func TestAPICustomersEndpointCreatesReusesAndScopesByTenant(t *testing.T) {
	db, handler, tenant, token := setupTenantWriteAPIHarness(t)
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO customers (tenant_id, name, phone, document_type, document_number, address, city, notes, created_at, updated_at)
		VALUES (1, 'Cliente Base', '3000001111', 'CC', '111111', 'Carrera 1', 'Bogota', '', ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert default customer: %v", err)
	}

	created := performAPIJSONRequest(t, handler, http.MethodPost, "/api/customers", token, map[string]any{
		"customer_name":            "Carolina Tenant",
		"customer_phone":           "3002223333",
		"customer_document_type":   "CC",
		"customer_document_number": "222222",
		"customer_address":         "Calle 22",
		"customer_city":            "Medellin",
		"customer_notes":           "Cliente nueva",
	})
	if created.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", created.Code, created.Body.String())
	}
	createdBody := decodeAPIResponse(t, created)
	if createdBody["created"] != true {
		t.Fatalf("expected created=true, got %+v", createdBody)
	}

	reused := performAPIJSONRequest(t, handler, http.MethodPost, "/api/customers", token, map[string]any{
		"customer_name":            "Carolina Tenant Actualizada",
		"customer_phone":           "3002223333",
		"customer_document_type":   "CC",
		"customer_document_number": "222222",
		"customer_address":         "Calle 23",
		"customer_city":            "Medellin",
		"customer_notes":           "Cliente actualizada",
	})
	if reused.Code != http.StatusOK {
		t.Fatalf("expected 200 for reused customer, got %d body=%s", reused.Code, reused.Body.String())
	}
	reusedBody := decodeAPIResponse(t, reused)
	if reusedBody["reused"] != true {
		t.Fatalf("expected reused=true, got %+v", reusedBody)
	}

	var tenantCustomers int
	if err := db.QueryRow(`SELECT COUNT(*) FROM customers WHERE tenant_id = ? AND document_number = ?`, tenant.ID, "222222").Scan(&tenantCustomers); err != nil {
		t.Fatalf("count tenant customers: %v", err)
	}
	if tenantCustomers != 1 {
		t.Fatalf("expected 1 tenant customer after reuse, got %d", tenantCustomers)
	}

	list := performAPIJSONRequest(t, handler, http.MethodGet, "/api/customers?q=carolina", token, nil)
	if list.Code != http.StatusOK {
		t.Fatalf("expected 200 list, got %d body=%s", list.Code, list.Body.String())
	}
	listBody := decodeAPIResponse(t, list)
	if int(listBody["count"].(float64)) != 1 {
		t.Fatalf("expected 1 listed customer, got %+v", listBody)
	}
	items := listBody["items"].([]any)
	item := items[0].(map[string]any)
	if item["document_number"] != "222222" {
		t.Fatalf("unexpected customer list item: %+v", item)
	}
}

func TestAPIAgentCustomerSearchReturnsCompactTenantScopedResults(t *testing.T) {
	db, handler, tenant, token := setupTenantWriteAPIHarness(t)
	defer db.Close()

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO customers (tenant_id, name, phone, document_type, document_number, address, city, notes, created_at, updated_at)
		VALUES (1, 'Cliente Base Search', '3001002000', 'CC', '100200300', 'Calle Base', 'Bogota', '', ?, ?)
	`, now, now); err != nil {
		t.Fatalf("insert default customer: %v", err)
	}

	createResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/customers", token, map[string]any{
		"customer_name":            "Diana Search",
		"customer_phone":           "3007778888",
		"customer_document_type":   "CC",
		"customer_document_number": "777888999",
		"customer_city":            "Cartagena",
	})
	if createResp.Code != http.StatusCreated {
		t.Fatalf("expected 201 creating customer, got %d body=%s", createResp.Code, createResp.Body.String())
	}
	createBody := decodeAPIResponse(t, createResp)
	customer := createBody["customer"].(map[string]any)
	customerID := int(customer["id"].(float64))

	creditResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/agent/credits", token, map[string]any{
		"customer_id":        customerID,
		"installments_total": 4,
		"total_value":        400000,
		"interest_percent":   0,
		"notes":              "prestamo para search",
	})
	if creditResp.Code != http.StatusCreated {
		t.Fatalf("expected 201 credit, got %d body=%s", creditResp.Code, creditResp.Body.String())
	}

	searchResp := performAPIJSONRequest(t, handler, http.MethodGet, "/api/agent/customers/search?q=777888999", token, nil)
	if searchResp.Code != http.StatusOK {
		t.Fatalf("expected 200 search, got %d body=%s", searchResp.Code, searchResp.Body.String())
	}
	searchBody := decodeAPIResponse(t, searchResp)
	if int(searchBody["count"].(float64)) != 1 {
		t.Fatalf("expected 1 compact customer result, got %+v", searchBody)
	}
	items := searchBody["items"].([]any)
	item := items[0].(map[string]any)
	if int(item["id"].(float64)) != customerID {
		t.Fatalf("unexpected compact customer id: %+v", item)
	}
	if item["document_number"] != "777888999" {
		t.Fatalf("unexpected compact customer document: %+v", item)
	}
	if _, ok := item["address"]; ok {
		t.Fatalf("agent search should not expose address in compact payload: %+v", item)
	}
	if item["credits_count"].(float64) != 1 {
		t.Fatalf("unexpected credits_count: %+v", item)
	}
	if item["current_debt"].(float64) != 400000 {
		t.Fatalf("unexpected current_debt: %+v", item)
	}
	if item["active_credits"].(float64) != 1 {
		t.Fatalf("unexpected active_credits: %+v", item)
	}

	crossResp := performAPIJSONRequest(t, handler, http.MethodGet, "/api/agent/customers/search?q=100200300", token, nil)
	if crossResp.Code != http.StatusOK {
		t.Fatalf("expected 200 cross search, got %d body=%s", crossResp.Code, crossResp.Body.String())
	}
	crossBody := decodeAPIResponse(t, crossResp)
	if int(crossBody["count"].(float64)) != 0 {
		t.Fatalf("expected no cross-tenant customers, got %+v", crossBody)
	}

	var tenantCustomerCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM customers WHERE tenant_id = ?`, tenant.ID).Scan(&tenantCustomerCount); err != nil {
		t.Fatalf("count tenant customers: %v", err)
	}
	if tenantCustomerCount != 1 {
		t.Fatalf("expected 1 tenant customer, got %d", tenantCustomerCount)
	}
}

func TestAPICustomerDetailAndEventsRespectTenantScope(t *testing.T) {
	db, handler, tenant, token := setupTenantWriteAPIHarness(t)
	defer db.Close()

	seedTenantProductWithUnits(t, db, tenant.ID, "T2-CUSTOMER-001", "Producto Cliente", 25000, 1)
	seedTenantProductWithUnits(t, db, defaultTenantID, "T1-CUSTOMER-001", "Producto Base", 12000, 1)

	createResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/customers", token, map[string]any{
		"customer_name":            "Laura Cliente",
		"customer_phone":           "3005556666",
		"customer_document_type":   "CC",
		"customer_document_number": "555666",
		"customer_city":            "Cali",
	})
	if createResp.Code != http.StatusCreated {
		t.Fatalf("expected 201 creating customer, got %d body=%s", createResp.Code, createResp.Body.String())
	}
	createBody := decodeAPIResponse(t, createResp)
	customer := createBody["customer"].(map[string]any)
	customerID := int(customer["id"].(float64))

	creditResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/credits", token, map[string]any{
		"product_id":         "T2-CUSTOMER-001",
		"quantity":           1,
		"customer_id":        customerID,
		"installments_total": 4,
		"total_value":        100000,
		"interest_percent":   0,
		"notes":              "credito con customer_id",
	})
	if creditResp.Code != http.StatusCreated {
		t.Fatalf("expected 201 credit, got %d body=%s", creditResp.Code, creditResp.Body.String())
	}
	creditBody := decodeAPIResponse(t, creditResp)
	creditSaleID := int(creditBody["credit_sale_id"].(float64))

	installmentResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/credits/installments", token, map[string]any{
		"credit_sale_id": creditSaleID,
		"amount_paid":    5000,
		"payment_type":   "abono",
	})
	if installmentResp.Code != http.StatusCreated {
		t.Fatalf("expected 201 installment, got %d body=%s", installmentResp.Code, installmentResp.Body.String())
	}

	detail := performAPIJSONRequest(t, handler, http.MethodGet, "/api/customers/"+strconv.Itoa(customerID), token, nil)
	if detail.Code != http.StatusOK {
		t.Fatalf("expected 200 detail, got %d body=%s", detail.Code, detail.Body.String())
	}
	detailBody := decodeAPIResponse(t, detail)
	detailCustomer := detailBody["customer"].(map[string]any)
	if int(detailCustomer["id"].(float64)) != customerID {
		t.Fatalf("unexpected customer detail: %+v", detailCustomer)
	}
	if detailCustomer["current_debt"].(float64) != 95000 {
		t.Fatalf("unexpected current_debt in detail: %+v", detailCustomer)
	}
	recentCredits := detailCustomer["recent_credits"].([]any)
	if len(recentCredits) != 1 {
		t.Fatalf("expected 1 recent credit, got %+v", detailCustomer)
	}

	events := performAPIJSONRequest(t, handler, http.MethodGet, "/api/customers/"+strconv.Itoa(customerID)+"/events", token, nil)
	if events.Code != http.StatusOK {
		t.Fatalf("expected 200 events, got %d body=%s", events.Code, events.Body.String())
	}
	eventsBody := decodeAPIResponse(t, events)
	if int(eventsBody["count"].(float64)) < 3 {
		t.Fatalf("expected at least 3 customer events, got %+v", eventsBody)
	}

	defaultCustomerID := 0
	if err := db.QueryRow(`SELECT id FROM customers WHERE tenant_id = 1 AND document_number = '555666'`).Scan(&defaultCustomerID); err != nil && err != sql.ErrNoRows {
		t.Fatalf("query cross-tenant customer: %v", err)
	}
	if defaultCustomerID != 0 {
		t.Fatalf("unexpected cross-tenant duplicate customer id=%d", defaultCustomerID)
	}
	if _, err := db.Exec(`
		INSERT INTO customers (tenant_id, name, phone, document_type, document_number, address, city, notes, created_at, updated_at)
		VALUES (1, 'Cliente Base Ajeno', '3000001111', 'CC', '909090', '', 'Bogota', '', ?, ?)
	`, time.Now().Format(time.RFC3339), time.Now().Format(time.RFC3339)); err != nil {
		t.Fatalf("insert cross tenant customer: %v", err)
	}
	var crossCustomerID int
	if err := db.QueryRow(`SELECT id FROM customers WHERE tenant_id = 1 AND document_number = '909090'`).Scan(&crossCustomerID); err != nil {
		t.Fatalf("query cross customer id: %v", err)
	}
	crossDetail := performAPIJSONRequest(t, handler, http.MethodGet, "/api/customers/"+strconv.Itoa(crossCustomerID), token, nil)
	if crossDetail.Code != http.StatusNotFound {
		t.Fatalf("expected 404 cross-tenant detail, got %d body=%s", crossDetail.Code, crossDetail.Body.String())
	}
}

func TestAPIUsersEndpointsReuseSharedManagedUserLogic(t *testing.T) {
	db, handler, _, token := setupTenantWriteAPIHarness(t)
	defer db.Close()

	createResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/users", token, map[string]any{
		"username":    "tenant2.ops",
		"name":        "Operador Dos",
		"email":       "tenant2.ops@example.com",
		"password":    "OpsSegura123!",
		"role":        "empleado",
		"is_active":   true,
		"telegram_id": "44556677",
	})
	if createResp.Code != http.StatusCreated {
		t.Fatalf("expected 201 creating user, got %d body=%s", createResp.Code, createResp.Body.String())
	}
	createBody := decodeAPIResponse(t, createResp)
	user := createBody["user"].(map[string]any)
	userID := int(user["id"].(float64))
	if user["telegram_id"] != "44556677" {
		t.Fatalf("unexpected telegram_id in create response: %+v", user)
	}
	if user["role"] != "empleado" {
		t.Fatalf("unexpected role in create response: %+v", user)
	}

	listResp := performAPIJSONRequest(t, handler, http.MethodGet, "/api/users", token, nil)
	if listResp.Code != http.StatusOK {
		t.Fatalf("expected 200 listing users, got %d body=%s", listResp.Code, listResp.Body.String())
	}
	listBody := decodeAPIResponse(t, listResp)
	if int(listBody["count"].(float64)) < 2 {
		t.Fatalf("expected at least tenant admin + created user, got %+v", listBody)
	}

	detailResp := performAPIJSONRequest(t, handler, http.MethodGet, "/api/users/"+strconv.Itoa(userID), token, nil)
	if detailResp.Code != http.StatusOK {
		t.Fatalf("expected 200 getting user detail, got %d body=%s", detailResp.Code, detailResp.Body.String())
	}
	detailBody := decodeAPIResponse(t, detailResp)
	detailUser := detailBody["user"].(map[string]any)
	if detailUser["username"] != "tenant2.ops" {
		t.Fatalf("unexpected detail payload: %+v", detailUser)
	}

	toggleOffResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/users/"+strconv.Itoa(userID)+"/toggle", token, map[string]any{
		"is_active": false,
	})
	if toggleOffResp.Code != http.StatusOK {
		t.Fatalf("expected 200 toggling user off, got %d body=%s", toggleOffResp.Code, toggleOffResp.Body.String())
	}
	toggleOffBody := decodeAPIResponse(t, toggleOffResp)
	toggledOffUser := toggleOffBody["user"].(map[string]any)
	if toggledOffUser["is_active"] != false {
		t.Fatalf("unexpected toggle-off payload: %+v", toggledOffUser)
	}

	updateResp := performAPIJSONRequest(t, handler, http.MethodPatch, "/api/users/"+strconv.Itoa(userID), token, map[string]any{
		"name":        "Operador Dos Actualizado",
		"telegram_id": "88990011",
		"is_active":   true,
	})
	if updateResp.Code != http.StatusOK {
		t.Fatalf("expected 200 updating user, got %d body=%s", updateResp.Code, updateResp.Body.String())
	}
	updateBody := decodeAPIResponse(t, updateResp)
	updatedUser := updateBody["user"].(map[string]any)
	if updatedUser["telegram_id"] != "88990011" || updatedUser["is_active"] != true {
		t.Fatalf("unexpected updated payload: %+v", updatedUser)
	}

	now := time.Now().Add(24 * time.Hour).Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO sessions (token, user_id, tenant_id, created_at, expires_at)
		VALUES ('user-pass-reset', ?, 2, ?, ?)
	`, userID, time.Now().Format(time.RFC3339), now); err != nil {
		t.Fatalf("insert session for password reset: %v", err)
	}
	passwordResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/users/"+strconv.Itoa(userID)+"/password", token, map[string]any{
		"password": "NuevaClave123!",
	})
	if passwordResp.Code != http.StatusOK {
		t.Fatalf("expected 200 updating password, got %d body=%s", passwordResp.Code, passwordResp.Body.String())
	}
	var remainingSessions int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE user_id = ?`, userID).Scan(&remainingSessions); err != nil {
		t.Fatalf("count user sessions after password reset: %v", err)
	}
	if remainingSessions != 0 {
		t.Fatalf("expected sessions to be invalidated after password reset, got %d", remainingSessions)
	}

	crossTenantResp := performAPIJSONRequest(t, handler, http.MethodGet, "/api/users/1", token, nil)
	if crossTenantResp.Code != http.StatusNotFound {
		t.Fatalf("expected 404 cross-tenant user detail, got %d body=%s", crossTenantResp.Code, crossTenantResp.Body.String())
	}

	forbiddenRoleResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/users", token, map[string]any{
		"username":  "tenant2.platform",
		"password":  "PlatformSegura123!",
		"role":      "platform_admin",
		"is_active": true,
	})
	if forbiddenRoleResp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when tenant admin tries to create platform admin, got %d body=%s", forbiddenRoleResp.Code, forbiddenRoleResp.Body.String())
	}
}

func TestAPIUsersRejectsDeactivatingLastTenantAdmin(t *testing.T) {
	db, handler, _, token := setupTenantWriteAPIHarness(t)
	defer db.Close()

	tenantAdmin := mustLoadTestUser(t, db, "tenant2.admin")
	resp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/users/"+strconv.Itoa(tenantAdmin.ID)+"/toggle", token, map[string]any{
		"is_active": false,
	})
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when deactivating last tenant admin, got %d body=%s", resp.Code, resp.Body.String())
	}
	body := decodeAPIResponse(t, resp)
	if body["error"] != "Debe existir al menos un admin activo." {
		t.Fatalf("unexpected error payload: %+v", body)
	}
}

func TestAPIInvoicesSupportSalesCreditsAndTenantScope(t *testing.T) {
	db, handler, tenant, token := setupTenantWriteAPIHarness(t)
	defer db.Close()

	seedTenantProductWithUnits(t, db, tenant.ID, "INV-001", "Producto Facturable", 45000, 2)

	saleResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/sales", token, map[string]any{
		"product_id":     "INV-001",
		"quantity":       1,
		"payment_method": "Efectivo",
		"sale_price":     45000,
		"channel":        "web",
		"sold_by":        "tester",
	})
	if saleResp.Code != http.StatusCreated {
		t.Fatalf("expected sale 201, got %d body=%s", saleResp.Code, saleResp.Body.String())
	}
	saleBody := decodeAPIResponse(t, saleResp)
	saleID := int(saleBody["sale_id"].(float64))

	invoiceSaleResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/invoices", token, map[string]any{
		"sale_id":                  saleID,
		"customer_name":            "Cliente Factura Venta",
		"customer_phone":           "3001234567",
		"customer_document_type":   "CC",
		"customer_document_number": "99887766",
		"customer_city":            "Bogota",
		"customer_address":         "Calle 10 # 1-20",
		"notes":                    "factura venta api",
	})
	if invoiceSaleResp.Code != http.StatusCreated {
		t.Fatalf("expected invoice 201, got %d body=%s", invoiceSaleResp.Code, invoiceSaleResp.Body.String())
	}
	invoiceSaleBody := decodeAPIResponse(t, invoiceSaleResp)
	invoiceSale, _ := invoiceSaleBody["invoice"].(map[string]any)
	if invoiceSale["source_type"] != "sale" {
		t.Fatalf("unexpected sale invoice payload: %+v", invoiceSaleBody)
	}
	invoiceSaleID := int(invoiceSale["id"].(float64))

	invoiceDetailResp := performAPIJSONRequest(t, handler, http.MethodGet, "/api/invoices/"+strconv.Itoa(invoiceSaleID), token, nil)
	if invoiceDetailResp.Code != http.StatusOK {
		t.Fatalf("expected invoice detail 200, got %d body=%s", invoiceDetailResp.Code, invoiceDetailResp.Body.String())
	}
	invoiceDetailBody := decodeAPIResponse(t, invoiceDetailResp)
	invoiceDetail, _ := invoiceDetailBody["invoice"].(map[string]any)
	if invoiceDetail["customer_document_number"] != "99887766" {
		t.Fatalf("unexpected invoice detail: %+v", invoiceDetailBody)
	}

	listResp := performAPIJSONRequest(t, handler, http.MethodGet, "/api/invoices?q=99887766", token, nil)
	if listResp.Code != http.StatusOK {
		t.Fatalf("expected invoice list 200, got %d body=%s", listResp.Code, listResp.Body.String())
	}
	listBody := decodeAPIResponse(t, listResp)
	if int(listBody["count"].(float64)) < 1 {
		t.Fatalf("expected invoice list with items, got %+v", listBody)
	}

	creditResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/credits", token, map[string]any{
		"kind":                     "cash_loan",
		"customer_name":            "Cliente Factura Credito",
		"customer_phone":           "3015556677",
		"customer_document_type":   "CC",
		"customer_document_number": "55443322",
		"customer_city":            "Medellin",
		"installments_total":       4,
		"total_value":              320000,
		"interest_percent":         0,
		"notes":                    "credito para factura",
	})
	if creditResp.Code != http.StatusCreated {
		t.Fatalf("expected credit 201, got %d body=%s", creditResp.Code, creditResp.Body.String())
	}
	creditBody := decodeAPIResponse(t, creditResp)
	creditSaleID := int(creditBody["credit_sale_id"].(float64))

	invoiceCreditResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/agent/invoices", token, map[string]any{
		"credit_sale_id": creditSaleID,
		"notes":          "factura credito agent",
	})
	if invoiceCreditResp.Code != http.StatusCreated {
		t.Fatalf("expected credit invoice 201, got %d body=%s", invoiceCreditResp.Code, invoiceCreditResp.Body.String())
	}
	invoiceCreditBody := decodeAPIResponse(t, invoiceCreditResp)
	invoiceCredit, _ := invoiceCreditBody["invoice"].(map[string]any)
	if invoiceCredit["source_type"] != "credit" {
		t.Fatalf("unexpected credit invoice payload: %+v", invoiceCreditBody)
	}

	reusedResp := performAPIJSONRequest(t, handler, http.MethodPost, "/api/agent/invoices", token, map[string]any{
		"credit_sale_id": creditSaleID,
	})
	if reusedResp.Code != http.StatusOK {
		t.Fatalf("expected reused credit invoice 200, got %d body=%s", reusedResp.Code, reusedResp.Body.String())
	}
	reusedBody := decodeAPIResponse(t, reusedResp)
	if created, _ := reusedBody["created"].(bool); created {
		t.Fatalf("expected reused invoice, got %+v", reusedBody)
	}

	usersCols, err := tableColumns(db, "users")
	if err != nil {
		t.Fatalf("tableColumns users: %v", err)
	}
	platformAdmin := mustLoadTestUser(t, db, "admin")
	provisioned, err := createTenantWithSeed(db, platformAdmin, usersCols, "Tenant Facturas", "tenant-facturas", "tenantfact.admin", "TenantFacturas123!")
	if err != nil {
		t.Fatalf("createTenantWithSeed second tenant: %v", err)
	}
	crossResp := performAPIJSONRequest(t, handler, http.MethodGet, "/api/invoices/"+strconv.Itoa(invoiceSaleID), provisioned.InitialAPIToken, nil)
	if crossResp.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for cross-tenant invoice detail, got %d body=%s", crossResp.Code, crossResp.Body.String())
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

func TestCreateAndCloseProductLoanUpdatesInventoryAndTraceability(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "product-loan.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	admin := mustLoadTestUser(t, db, "admin")
	now := time.Now().Add(-24 * time.Hour).Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO productos (sku, tenant_id, id, nombre, linea, precio_venta, retoma_enabled)
		VALUES ('PLOAN-001', 1, 'PLOAN-001', 'Producto prestable', 'Operaciones', 80000, 0);
		INSERT INTO unidades (id, tenant_id, producto_id, estado, creado_en)
		VALUES
			('UPL-001', 1, 'PLOAN-001', 'Disponible', ?),
			('UPL-002', 1, 'PLOAN-001', 'Disponible', ?);
	`, now, now); err != nil {
		t.Fatalf("seed product loan db: %v", err)
	}

	createResult, err := createProductLoan(db, admin, productLoanCreateInput{
		ProductID: "PLOAN-001",
		Quantity:  1,
		Customer: customerInput{
			Name:           "Cliente Prestamo",
			Phone:          "3001112233",
			DocumentType:   "CC",
			DocumentNumber: "10101010",
			City:           "Bogota",
		},
		DueAt: "2026-04-05",
		Notes: "prestamo operativo",
	}, "web", nil)
	if err != nil {
		t.Fatalf("createProductLoan: %v", err)
	}
	if createResult.ProductLoanID <= 0 || createResult.Status != productLoanStatusActive {
		t.Fatalf("unexpected create result: %+v", createResult)
	}

	var availableAfterCreate, loanedAfterCreate int
	if err := db.QueryRow(`SELECT COUNT(*) FROM unidades WHERE tenant_id = 1 AND producto_id = 'PLOAN-001' AND estado = 'Disponible'`).Scan(&availableAfterCreate); err != nil {
		t.Fatalf("count available after create: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM unidades WHERE tenant_id = 1 AND producto_id = 'PLOAN-001' AND estado = 'Prestada'`).Scan(&loanedAfterCreate); err != nil {
		t.Fatalf("count loaned after create: %v", err)
	}
	if availableAfterCreate != 1 || loanedAfterCreate != 1 {
		t.Fatalf("unexpected stock after create available=%d loaned=%d", availableAfterCreate, loanedAfterCreate)
	}

	var createdAuditCount, createdCustomerEventCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit_events WHERE tenant_id = 1 AND event_type = 'product_loan_created'`).Scan(&createdAuditCount); err != nil {
		t.Fatalf("count created audit: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM customer_events WHERE tenant_id = 1 AND event_type = 'product_loan_created'`).Scan(&createdCustomerEventCount); err != nil {
		t.Fatalf("count created customer event: %v", err)
	}
	if createdAuditCount != 1 || createdCustomerEventCount != 1 {
		t.Fatalf("unexpected create traceability audit=%d customer=%d", createdAuditCount, createdCustomerEventCount)
	}

	closeResult, err := closeProductLoan(db, admin, productLoanCloseInput{
		ProductLoanID: createResult.ProductLoanID,
		Status:        productLoanStatusReturned,
		Notes:         "retornado en buen estado",
	}, "web", nil)
	if err != nil {
		t.Fatalf("closeProductLoan: %v", err)
	}
	if closeResult.Status != productLoanStatusReturned {
		t.Fatalf("unexpected close result: %+v", closeResult)
	}

	var availableAfterClose, loanedAfterClose int
	if err := db.QueryRow(`SELECT COUNT(*) FROM unidades WHERE tenant_id = 1 AND producto_id = 'PLOAN-001' AND estado = 'Disponible'`).Scan(&availableAfterClose); err != nil {
		t.Fatalf("count available after close: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM unidades WHERE tenant_id = 1 AND producto_id = 'PLOAN-001' AND estado = 'Prestada'`).Scan(&loanedAfterClose); err != nil {
		t.Fatalf("count loaned after close: %v", err)
	}
	if availableAfterClose != 2 || loanedAfterClose != 0 {
		t.Fatalf("unexpected stock after close available=%d loaned=%d", availableAfterClose, loanedAfterClose)
	}

	var (
		storedStatus string
		closedAt     sql.NullString
	)
	if err := db.QueryRow(`SELECT status, closed_at FROM product_loans WHERE tenant_id = 1 AND id = ?`, createResult.ProductLoanID).Scan(&storedStatus, &closedAt); err != nil {
		t.Fatalf("query product loan: %v", err)
	}
	if storedStatus != string(productLoanStatusReturned) || !closedAt.Valid || strings.TrimSpace(closedAt.String) == "" {
		t.Fatalf("unexpected persisted product loan status=%q closed_at=%v", storedStatus, closedAt)
	}

	var closedAuditCount, closedCustomerEventCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit_events WHERE tenant_id = 1 AND event_type = 'product_loan_closed'`).Scan(&closedAuditCount); err != nil {
		t.Fatalf("count closed audit: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM customer_events WHERE tenant_id = 1 AND event_type = 'product_loan_closed'`).Scan(&closedCustomerEventCount); err != nil {
		t.Fatalf("count closed customer event: %v", err)
	}
	if closedAuditCount != 1 || closedCustomerEventCount != 1 {
		t.Fatalf("unexpected close traceability audit=%d customer=%d", closedAuditCount, closedCustomerEventCount)
	}
}

func TestListProductLoansReportSupportsOverdueAndTenantScope(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "SuperSecreto123")

	db, err := initDB(filepath.Join(t.TempDir(), "product-loan-report.db"), defaultPaymentMethodNames())
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	defer db.Close()

	admin := mustLoadTestUser(t, db, "admin")
	now := time.Now().Add(-72 * time.Hour).Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO productos (sku, tenant_id, id, nombre, linea, precio_venta, retoma_enabled)
		VALUES
			('PLOAN-R1', 1, 'PLOAN-R1', 'Producto Vencido', 'Operaciones', 50000, 0),
			('PLOAN-R2', 1, 'PLOAN-R2', 'Producto En Fecha', 'Operaciones', 60000, 0),
			('PLOAN-RX', 2, 'PLOAN-RX', 'Producto Otro Tenant', 'Operaciones', 70000, 0);
		INSERT INTO customers (tenant_id, name, phone, document_type, document_number, address, city, notes, created_at, updated_at)
		VALUES
			(1, 'Cliente Vencido', '3001110000', 'CC', '7001', '', 'Bogota', '', ?, ?),
			(1, 'Cliente Activo', '3001110001', 'CC', '7002', '', 'Medellin', '', ?, ?),
			(2, 'Cliente Externo', '3001110002', 'CC', '7003', '', 'Cali', '', ?, ?);
	`, now, now, now, now, now, now); err != nil {
		t.Fatalf("seed report db: %v", err)
	}

	var customerExpiredID, customerActiveID, customerOtherTenantID int
	if err := db.QueryRow(`SELECT id FROM customers WHERE tenant_id = 1 AND document_number = '7001'`).Scan(&customerExpiredID); err != nil {
		t.Fatalf("customer expired id: %v", err)
	}
	if err := db.QueryRow(`SELECT id FROM customers WHERE tenant_id = 1 AND document_number = '7002'`).Scan(&customerActiveID); err != nil {
		t.Fatalf("customer active id: %v", err)
	}
	if err := db.QueryRow(`SELECT id FROM customers WHERE tenant_id = 2 AND document_number = '7003'`).Scan(&customerOtherTenantID); err != nil {
		t.Fatalf("customer other tenant id: %v", err)
	}

	if _, err := db.Exec(`
		INSERT INTO product_loans (
			tenant_id, product_id, customer_id, quantity, borrower_name, borrower_phone,
			borrower_document_type, borrower_document_number, borrower_address, borrower_city,
			notes, status, loaned_at, due_at, created_by
		) VALUES
			(1, 'PLOAN-R1', ?, 1, 'Cliente Vencido', '3001110000', 'CC', '7001', '', 'Bogota', 'prestamo vencido', 'active', ?, '2026-03-01', 1),
			(1, 'PLOAN-R2', ?, 1, 'Cliente Activo', '3001110001', 'CC', '7002', '', 'Medellin', 'prestamo activo', 'active', ?, '2099-03-01', 1),
			(2, 'PLOAN-RX', ?, 1, 'Cliente Externo', '3001110002', 'CC', '7003', '', 'Cali', 'otro tenant', 'active', ?, '2026-03-01', 1)
	`, customerExpiredID, now, customerActiveID, now, customerOtherTenantID, now); err != nil {
		t.Fatalf("insert product loans: %v", err)
	}

	var loanExpiredID, loanActiveID, loanOtherTenantID int
	if err := db.QueryRow(`SELECT id FROM product_loans WHERE tenant_id = 1 AND product_id = 'PLOAN-R1'`).Scan(&loanExpiredID); err != nil {
		t.Fatalf("loan expired id: %v", err)
	}
	if err := db.QueryRow(`SELECT id FROM product_loans WHERE tenant_id = 1 AND product_id = 'PLOAN-R2'`).Scan(&loanActiveID); err != nil {
		t.Fatalf("loan active id: %v", err)
	}
	if err := db.QueryRow(`SELECT id FROM product_loans WHERE tenant_id = 2 AND product_id = 'PLOAN-RX'`).Scan(&loanOtherTenantID); err != nil {
		t.Fatalf("loan other tenant id: %v", err)
	}

	if _, err := db.Exec(`
		INSERT INTO product_loan_units (tenant_id, product_loan_id, unit_id)
		VALUES
			(1, ?, 'ULR-001'),
			(1, ?, 'ULR-002'),
			(2, ?, 'ULR-003')
	`, loanExpiredID, loanActiveID, loanOtherTenantID); err != nil {
		t.Fatalf("insert product loan units: %v", err)
	}

	items, err := listProductLoansReport(db, admin, 1, productLoanReportFilters{
		Limit: 50,
	})
	if err != nil {
		t.Fatalf("listProductLoansReport: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 tenant-scoped items, got %d", len(items))
	}

	overdueItems, err := listProductLoansReport(db, admin, 1, productLoanReportFilters{
		Overdue: "yes",
		Limit:   50,
	})
	if err != nil {
		t.Fatalf("listProductLoansReport overdue: %v", err)
	}
	if len(overdueItems) != 1 || !overdueItems[0].IsOverdue || overdueItems[0].ProductID != "PLOAN-R1" {
		t.Fatalf("unexpected overdue items: %+v", overdueItems)
	}
	if overdueItems[0].UnitIDsText != "ULR-001" {
		t.Fatalf("expected unit ids in overdue item, got %+v", overdueItems[0])
	}
}
