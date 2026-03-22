package main

import (
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE unidades (
			id TEXT PRIMARY KEY,
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
	ids, err := selectAndMarkUnitsSold(tx, "P-001", 2)
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
	_, err = selectAndMarkUnitsSold(tx, "P-002", 2)
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
			id TEXT,
			nombre TEXT NOT NULL,
			precio_venta REAL NOT NULL DEFAULT 0,
			retoma_enabled INTEGER NOT NULL DEFAULT 0,
			retoma_price REAL,
			owner_user_id INTEGER
		);
		CREATE TABLE unidades (
			id TEXT PRIMARY KEY,
			producto_id TEXT NOT NULL,
			estado TEXT NOT NULL,
			creado_en TEXT NOT NULL,
			caducidad TEXT
		);
		CREATE TABLE retomas (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
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
			producto_id TEXT NOT NULL,
			unidad_id TEXT NOT NULL,
			tipo TEXT NOT NULL,
			nota TEXT NOT NULL DEFAULT '',
			usuario TEXT NOT NULL DEFAULT '',
			fecha TEXT NOT NULL
		);
		CREATE TABLE audit_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
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
