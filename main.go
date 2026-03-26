package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"image/png"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/boombuler/barcode"
	"github.com/boombuler/barcode/code128"
	pgxstdlib "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	"golang.org/x/crypto/bcrypt"
)

var appTimeLocation = func() *time.Location {
	loc, err := time.LoadLocation("America/Bogota")
	if err != nil {
		return time.FixedZone("America/Bogota", -5*60*60)
	}
	return loc
}()

func init() {
	time.Local = appTimeLocation
	sql.Register(postgresDriverName, postgresPlaceholderDriver{base: pgxstdlib.GetDefaultDriver()})
}

type inventoryPageData struct {
	Title              string
	Subtitle           string
	RoutePrefix        string
	Flash              string
	ReceiptSaleID      int
	ReceiptViewURL     string
	ReceiptDownloadURL string
	ThermalTicketURL   string
	MetodoPagos        []string
	Products           []inventoryProduct
	EditableLines      []string
	AssignableUsers    []assignableUser
	CanSell            bool
	CanSwap            bool
	CanRetoma          bool
	CanLoan            bool
	CanCredit          bool
	CurrentUser        *User
}

type saleReceiptData struct {
	Title            string
	Subtitle         string
	PaperSize        string
	PaperWidthMM     int
	PaperDPI         int
	PaperClass       string
	SaleID           int
	ReceiptNumber    string
	SaleDate         string
	SaleTime         string
	SaleDateTime     string
	OperationType    string
	ProductoID       string
	ProductoNom      string
	Cantidad         int
	PrecioUnitario   string
	Total            string
	MetodoPago       string
	SoldBy           string
	Channel          string
	Notas            string
	BuyerName        string
	BuyerDocument    string
	NeedsBuyerData   bool
	DownloadURL      string
	ThermalURL       string
	InvoiceCreateURL string
	CanLoan          bool
	CanCredit        bool
	CurrentUser      *User
	Settings         BusinessSettings
}

type productLabelItem struct {
	ID             string
	Name           string
	Price          string
	BarcodeDataURI template.URL
}

type productLabelsPageData struct {
	Title       string
	Subtitle    string
	Size        string
	WidthMM     int
	HeightMM    int
	PaperDPI    int
	PaperClass  string
	Items       []productLabelItem
	CanLoan     bool
	CanCredit   bool
	CurrentUser *User
	Settings    BusinessSettings
}

type invoiceItemData struct {
	ProductID     string
	Description   string
	Quantity      int
	UnitPrice     float64
	UnitPriceText string
	LineTotal     float64
	LineTotalText string
}

type invoiceViewData struct {
	Title                  string
	Subtitle               string
	Flash                  string
	PaperSize              string
	PaperWidthMM           int
	PaperDPI               int
	PaperClass             string
	InvoiceID              int
	InvoiceNumber          string
	SourceType             string
	SourceLabel            string
	SaleID                 int
	CreditSaleID           int
	Status                 string
	StatusLabel            string
	CreatedAt              string
	CustomerID             int
	CustomerName           string
	CustomerPhone          string
	CustomerDocumentType   string
	CustomerDocumentNumber string
	CustomerAddress        string
	CustomerCity           string
	Notes                  string
	SubtotalText           string
	TotalText              string
	Items                  []invoiceItemData
	CanLoan                bool
	CanCredit              bool
	CurrentUser            *User
	Settings               BusinessSettings
}

type invoiceFormData struct {
	Title                  string
	Subtitle               string
	Flash                  string
	Error                  string
	SourceType             string
	SourceLabel            string
	SaleID                 int
	CreditSaleID           int
	ProductName            string
	Quantity               int
	UnitPriceText          string
	TotalText              string
	PaymentMethod          string
	CustomerID             int
	CustomerName           string
	CustomerPhone          string
	CustomerDocumentType   string
	CustomerDocumentNumber string
	CustomerAddress        string
	CustomerCity           string
	Notes                  string
	CanLoan                bool
	CanCredit              bool
	CurrentUser            *User
}

type invoiceSourceSnapshot struct {
	SourceType   string
	SourceLabel  string
	SaleID       int
	CreditSaleID int
	Customer     *Customer
	Item         invoiceItemData
}

type unitOption struct {
	ID string
}

type productOption struct {
	ID                string
	Name              string
	Line              string
	Location          string
	CreditEnabled     bool
	DebtorName        string
	InstallmentsTotal int
	InstallmentsPaid  int
	TotalValue        float64
	InstallmentValue  float64
	Notes             string
	FechaIngreso      string
	SalePrice         float64
	RetomaEnabled     bool
	RetomaPrice       float64
	HasRetomaPrice    bool
	OwnerUserID       int
	HasOwner          bool
	Units             []unitOption
}

type csvFailedRow struct {
	Row   int    `json:"row"`
	SKU   string `json:"sku"`
	Error string `json:"error"`
}

type csvUploadResponse struct {
	CreatedProducts int            `json:"created_products"`
	UpdatedProducts int            `json:"updated_products"`
	CreatedUnits    int            `json:"created_units"`
	ProductIDs      []string       `json:"product_ids,omitempty"`
	LabelPrintURL   string         `json:"label_print_url,omitempty"`
	FailedRows      []csvFailedRow `json:"failed_rows"`
}

type inventoryUnit struct {
	ID          string
	Estado      string
	EstadoClass string
	CreadoEn    string
	Caducidad   string
	FIFO        string
}

type inventoryProduct struct {
	EntryType             string
	CreditSaleID          int
	ProductLoanID         int
	CustomerID            int
	CreditKind            string
	BaseProductID         string
	ID                    string
	Name                  string
	Line                  string
	Location              string
	CreditEnabled         bool
	InterestPercent       float64
	DebtorName            string
	DebtorDocumentType    string
	DebtorDocumentNumber  string
	DebtorPhone           string
	CustomerAddress       string
	CustomerCity          string
	CustomerNotes         string
	ManagedByName         string
	DueAt                 string
	ClosedAt              string
	CloseStatus           string
	InstallmentsTotal     int
	InstallmentsPaid      int
	PaidInstallmentsCount int
	TotalValue            float64
	DebtTotal             float64
	TotalPaid             float64
	CurrentDebt           float64
	InstallmentValue      float64
	LastPaymentAmount     float64
	LastPaymentAt         string
	LastPaymentType       string
	Notes                 string
	EstadoLabel           string
	EstadoClass           string
	Disponible            int
	Unidades              []inventoryUnit
	DisabledSale          bool
	FechaIngreso          string
	MesesEnStock          int
	AlertaPermanencia     bool
	SalePrice             float64
	RetomaEnabled         bool
	RetomaPrice           float64
	HasRetomaPrice        bool
	OwnerUserID           int
	HasOwner              bool
}

type productInventoryCounts struct {
	Available int
	Reserved  int
	Swapped   int
	Damaged   int
}

type BusinessSettings struct {
	ID                int
	BusinessName      string
	LogoPath          string
	PrimaryColor      string
	Currency          string
	DateFormat        string
	LabelPaperWidth   string
	InvoicePaperWidth string
	TicketPaperWidth  string
	UpdatedAt         string
}

type BusinessLine struct {
	ID        int
	Name      string
	Active    bool
	CreatedAt string
	UpdatedAt string
}

type PaymentMethod struct {
	ID        int
	Name      string
	Active    bool
	SortOrder int
	CreatedAt string
	UpdatedAt string
}

type MovementSetting struct {
	ID           int
	MovementType string
	Enabled      bool
	UpdatedAt    string
}

type assignableUser struct {
	ID       int
	Username string
}

type Customer struct {
	ID             int
	TenantID       int
	Name           string
	Phone          string
	DocumentType   string
	DocumentNumber string
	Address        string
	City           string
	Notes          string
	CreatedAt      string
	UpdatedAt      string
}

type customerListViewItem struct {
	ID              int
	Name            string
	Phone           string
	DocumentType    string
	DocumentNumber  string
	Address         string
	City            string
	Notes           string
	CreatedAt       string
	UpdatedAt       string
	CreditsCount    int
	UnitsOnCredit   int
	ActiveCredits   int
	DebtTotalText   string
	TotalPaidText   string
	CurrentDebtText string
	LastCreditAt    string
	DetailURL       string
}

type customerCreditViewItem struct {
	ID                int
	CreatedAt         string
	Kind              string
	KindLabel         string
	ProductID         string
	ProductName       string
	Quantity          int
	InstallmentsTotal int
	InstallmentsPaid  int
	InstallmentValue  string
	DebtTotalText     string
	TotalPaidText     string
	CurrentDebtText   string
}

type customerInvoiceViewItem struct {
	ID            int
	InvoiceNumber string
	SourceType    string
	SourceLabel   string
	Status        string
	StatusLabel   string
	TotalText     string
	CreatedAt     string
	ViewURL       string
}

type customerProductLoanViewItem struct {
	ID          int
	ProductID   string
	ProductName string
	Quantity    int
	Status      string
	StatusLabel string
	LoanedAt    string
	DueAt       string
	ClosedAt    string
	IsOverdue   bool
	DetailURL   string
}

type customerProductViewItem struct {
	ProductID   string
	ProductName string
	Quantity    int
	TotalText   string
	LastAt      string
	SourcesText string
}

type customerTimelineViewItem struct {
	EventType  string
	EventLabel string
	RefType    string
	RefID      string
	Summary    string
	AmountText string
	CreatedAt  string
	CreatedBy  string
	DetailURL  string
}

type customerDetailViewData struct {
	Summary  customerListViewItem
	Credits  []customerCreditViewItem
	Invoices []customerInvoiceViewItem
	Loans    []customerProductLoanViewItem
	Products []customerProductViewItem
	Timeline []customerTimelineViewItem
}

type creditPaymentType string

const (
	creditPaymentTypeCuota creditPaymentType = "cuota"
	creditPaymentTypeAbono creditPaymentType = "abono"
)

type creditSaleKind string

const (
	creditSaleKindProduct creditSaleKind = "product_credit"
	creditSaleKindCash    creditSaleKind = "cash_loan"
)

type creditStatus string

const (
	creditStatusActive    creditStatus = "active"
	creditStatusCompleted creditStatus = "completed"
	creditStatusSuspended creditStatus = "suspended"
	creditStatusCancelled creditStatus = "cancelled"
)

type productLoanStatus string

const (
	productLoanStatusActive    productLoanStatus = "active"
	productLoanStatusReturned  productLoanStatus = "returned"
	productLoanStatusPaid      productLoanStatus = "paid"
	productLoanStatusCancelled productLoanStatus = "cancelled"
)

type productLoanCreateInput struct {
	ProductID string
	Quantity  int
	Customer  customerInput
	DueAt     string
	Notes     string
}

type productLoanCloseInput struct {
	ProductLoanID int
	Status        productLoanStatus
	Notes         string
}

type productLoanOperationResult struct {
	ProductLoanID int
	ProductID     string
	CustomerID    int
	BorrowerName  string
	Quantity      int
	Status        productLoanStatus
	LoanedAt      string
	DueAt         string
	ClosedAt      string
}

type AuditEvent struct {
	ID          int
	EventType   string
	EntityType  string
	EntityID    string
	UserID      int
	HasUserID   bool
	Username    string
	Source      string
	PayloadJSON string
	CreatedAt   string
}

type creditEditReportChange struct {
	Field      string
	Label      string
	Before     any
	After      any
	BeforeText string
	AfterText  string
}

type creditEditReportItem struct {
	AuditID            int
	CreditSaleID       int
	CreatedAt          string
	Source             string
	Username           string
	TenantID           int
	TenantSlug         string
	TenantName         string
	Kind               string
	KindLabel          string
	ProductID          string
	ProductName        string
	CustomerID         int
	CustomerName       string
	CustomerDocument   string
	CustomerPhone      string
	Status             string
	StatusLabel        string
	StatusBefore       string
	StatusAfter        string
	StatusLabelBefore  string
	StatusLabelAfter   string
	ChangedFields      []string
	ChangedFieldsText  string
	ChangeCount        int
	Changes            []creditEditReportChange
	DebtTotalBefore    float64
	DebtTotalAfter     float64
	TotalPaidBefore    float64
	TotalPaidAfter     float64
	CurrentDebtBefore  float64
	CurrentDebtAfter   float64
	CurrentDebtDelta   float64
	InstallmentsDueNow int
}

type productLoanReportItem struct {
	ProductLoanID        int
	ProductID            string
	ProductName          string
	Quantity             int
	CustomerID           int
	CustomerName         string
	CustomerDocumentType string
	CustomerDocument     string
	CustomerPhone        string
	CustomerCity         string
	ManagedByName        string
	LoanedAt             string
	DueAt                string
	ClosedAt             string
	Status               string
	StatusLabel          string
	IsOverdue            bool
	OverdueLabel         string
	Notes                string
	CloseNotes           string
	UnitIDs              []string
	UnitIDsText          string
}

type cashLoanReportItem struct {
	CreditSaleID         int
	CreatedAt            string
	CustomerID           int
	CustomerName         string
	CustomerDocumentType string
	CustomerDocument     string
	CustomerPhone        string
	CustomerCity         string
	ManagedByName        string
	InstallmentsTotal    int
	InstallmentsPaid     int
	InstallmentsPending  int
	TotalValue           float64
	DebtTotal            float64
	TotalPaid            float64
	CurrentDebt          float64
	InterestPercent      float64
	InstallmentValue     float64
	Status               string
	StatusLabel          string
	KindLabel            string
	Notes                string
	LastPaymentAt        string
	LastPaymentAmount    float64
	LastPaymentType      string
	DetailURL            string
}

type cashLoanReportSummary struct {
	Count          int
	ActiveCount    int
	CompletedCount int
	SuspendedCount int
	CancelledCount int
	TotalValue     float64
	TotalPaid      float64
	CurrentDebt    float64
}

type productLoanTimelineItem struct {
	CreatedAt string
	EventType string
	Label     string
	Username  string
	Source    string
	Notes     string
}

type Tenant struct {
	ID                   int
	Slug                 string
	Name                 string
	Active               bool
	CreatedAt            string
	UpdatedAt            string
	InitialAdminUsername string
	InitialAPIKeyName    string
	IsDefault            bool
}

type APIKey struct {
	ID        int
	Name      string
	TenantID  int
	Active    bool
	IsInitial bool
	CreatedAt string
	UpdatedAt string
}

type tenantProvisionResult struct {
	Tenant            *Tenant
	InitialAPIKeyName string
	InitialAPIToken   string
	InitialAdminUser  string
}

const (
	rolePlatformAdmin = "platform_admin"
	roleAdmin         = "admin"
	roleEmployee      = "empleado"

	defaultTenantID   = 1
	defaultTenantSlug = "default"
	defaultTenantName = "Default tenant"
)

type dbEngine string

const (
	dbEngineSQLite   dbEngine = "sqlite"
	dbEnginePostgres dbEngine = "postgres"

	postgresDriverName = "stocki-postgres"
)

type databaseConfig struct {
	Engine dbEngine
	DSN    string
	Label  string
}

var (
	errInsufficientStock = fmt.Errorf("stock insuficiente")

	businessSettingsMu sync.RWMutex
	businessSettings   = defaultBusinessSettings()
	activeDBEngine     = dbEngineSQLite
)

func defaultBusinessSettings() BusinessSettings {
	return BusinessSettings{
		ID:                1,
		BusinessName:      "Stocki App",
		LogoPath:          "/static/img/logo1.svg",
		PrimaryColor:      "#0ea5c9",
		Currency:          "COP",
		DateFormat:        "2006-01-02",
		LabelPaperWidth:   "58mm",
		InvoicePaperWidth: "58mm",
		TicketPaperWidth:  "58mm",
	}
}

type sqlExecer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

type sqlQueryExecer interface {
	sqlExecer
	QueryRow(query string, args ...any) *sql.Row
}

type postgresPlaceholderDriver struct {
	base driver.Driver
}

type postgresPlaceholderConn struct {
	driver.Conn
}

func (d postgresPlaceholderDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.base.Open(name)
	if err != nil {
		return nil, err
	}
	return postgresPlaceholderConn{Conn: conn}, nil
}

func (c postgresPlaceholderConn) Prepare(query string) (driver.Stmt, error) {
	return c.Conn.Prepare(rebindPostgresPlaceholders(query))
}

func (c postgresPlaceholderConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if preparer, ok := c.Conn.(driver.ConnPrepareContext); ok {
		return preparer.PrepareContext(ctx, rebindPostgresPlaceholders(query))
	}
	return c.Conn.Prepare(rebindPostgresPlaceholders(query))
}

func (c postgresPlaceholderConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if execer, ok := c.Conn.(driver.ExecerContext); ok {
		return execer.ExecContext(ctx, rebindPostgresPlaceholders(query), args)
	}
	return nil, driver.ErrSkip
}

func (c postgresPlaceholderConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if queryer, ok := c.Conn.(driver.QueryerContext); ok {
		return queryer.QueryContext(ctx, rebindPostgresPlaceholders(query), args)
	}
	return nil, driver.ErrSkip
}

func (c postgresPlaceholderConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if beginner, ok := c.Conn.(driver.ConnBeginTx); ok {
		return beginner.BeginTx(ctx, opts)
	}
	return nil, driver.ErrSkip
}

func (c postgresPlaceholderConn) Ping(ctx context.Context) error {
	if pinger, ok := c.Conn.(driver.Pinger); ok {
		return pinger.Ping(ctx)
	}
	return nil
}

func (c postgresPlaceholderConn) CheckNamedValue(value *driver.NamedValue) error {
	if checker, ok := c.Conn.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(value)
	}
	return nil
}

func currentDBEngine() dbEngine {
	return activeDBEngine
}

func isPostgresDB() bool {
	return currentDBEngine() == dbEnginePostgres
}

func loadDatabaseConfig(defaultSQLitePath string) databaseConfig {
	engineRaw := strings.TrimSpace(strings.ToLower(os.Getenv("DB_ENGINE")))
	engine := dbEngineSQLite
	if engineRaw == string(dbEnginePostgres) {
		engine = dbEnginePostgres
	}

	if engine == dbEnginePostgres {
		dsn := strings.TrimSpace(os.Getenv("DB_DSN"))
		if dsn == "" {
			dsn = strings.TrimSpace(os.Getenv("DATABASE_URL"))
		}
		if dsn == "" {
			dsn = strings.TrimSpace(os.Getenv("DB_PATH"))
		}
		return databaseConfig{
			Engine: engine,
			DSN:    dsn,
			Label:  "Postgres",
		}
	}

	dsn := strings.TrimSpace(os.Getenv("DB_PATH"))
	if dsn == "" {
		dsn = defaultSQLitePath
	}
	return databaseConfig{
		Engine: dbEngineSQLite,
		DSN:    dsn,
		Label:  "SQLite",
	}
}

func rebindPostgresPlaceholders(query string) string {
	if !strings.Contains(query, "?") {
		return query
	}

	var builder strings.Builder
	builder.Grow(len(query) + 8)

	inSingleQuote := false
	inDoubleQuote := false
	placeholderIndex := 1

	for i := 0; i < len(query); i++ {
		ch := query[i]
		switch ch {
		case '\'':
			builder.WriteByte(ch)
			if inDoubleQuote {
				continue
			}
			if inSingleQuote && i+1 < len(query) && query[i+1] == '\'' {
				builder.WriteByte(query[i+1])
				i++
				continue
			}
			inSingleQuote = !inSingleQuote
		case '"':
			builder.WriteByte(ch)
			if inSingleQuote {
				continue
			}
			inDoubleQuote = !inDoubleQuote
		case '?':
			if inSingleQuote || inDoubleQuote {
				builder.WriteByte(ch)
				continue
			}
			builder.WriteByte('$')
			builder.WriteString(strconv.Itoa(placeholderIndex))
			placeholderIndex++
		default:
			builder.WriteByte(ch)
		}
	}

	return builder.String()
}

func normalizeSchemaSQLForEngine(schema string, engine dbEngine) string {
	if engine != dbEnginePostgres {
		return schema
	}
	normalized := strings.ReplaceAll(schema, "INTEGER PRIMARY KEY AUTOINCREMENT", "INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY")
	normalized = strings.ReplaceAll(normalized, "DEFAULT CURRENT_TIMESTAMP", "DEFAULT (CURRENT_TIMESTAMP::text)")
	normalized = strings.ReplaceAll(normalized, "DEFAULT (CURRENT_TIMESTAMP)", "DEFAULT (CURRENT_TIMESTAMP::text)")
	return normalized
}

func insertAndReturnID(exec sqlQueryExecer, query string, args ...any) (int64, error) {
	if isPostgresDB() {
		var id int64
		if err := exec.QueryRow(query+" RETURNING id", args...).Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	result, err := exec.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func sqlDatePrefixExpr(column string) string {
	return fmt.Sprintf("substr(%s, 1, 10)", column)
}

func upsertProducto(exec sqlExecer, tenantID int, sku, nombre, linea, now string) error {
	// productos table is part of the existing DB schema and uses sku as the primary key.
	// Other columns (prices, discount, notes) have defaults so manual creation can omit them.
	_ = now // kept for backwards-compat in case we later add created_at.
	_, err := exec.Exec(`
		INSERT INTO productos (sku, tenant_id, id, linea, nombre, fecha_ingreso)
		VALUES (?, ?, ?, ?, ?, COALESCE((SELECT fecha_ingreso FROM productos WHERE sku = ? AND tenant_id = ?), CURRENT_TIMESTAMP::text))
		ON CONFLICT(sku) DO UPDATE SET
			tenant_id = excluded.tenant_id,
			id = excluded.id,
			linea = excluded.linea,
			nombre = excluded.nombre
	`, sku, normalizeTenantID(tenantID), sku, linea, nombre, sku, normalizeTenantID(tenantID))
	return err
}

func normalizeCreditKey(value string) string {
	replacer := strings.NewReplacer(
		"á", "a",
		"é", "e",
		"í", "i",
		"ó", "o",
		"ú", "u",
		"Á", "a",
		"É", "e",
		"Í", "i",
		"Ó", "o",
		"Ú", "u",
	)
	return strings.ToLower(strings.TrimSpace(replacer.Replace(value)))
}

func isCreditProduct(product productOption) bool {
	return product.CreditEnabled
}

func seedProductosIfMissing(db *sql.DB, defaults []productOption) error {
	// Backfill unknown products that already exist in inventory units.
	if _, err := db.Exec(`
		INSERT INTO productos (sku, tenant_id, id, nombre, linea, fecha_ingreso)
		SELECT DISTINCT producto_id, COALESCE(NULLIF(tenant_id, 0), ?), producto_id, producto_id, 'Sin línea', CURRENT_TIMESTAMP
		FROM unidades
		ON CONFLICT(sku) DO NOTHING
	`, defaultTenantID); err != nil {
		return err
	}

	for _, p := range defaults {
		if _, err := db.Exec(`
			INSERT INTO productos (sku, tenant_id, id, nombre, linea, fecha_ingreso)
			VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
			ON CONFLICT(sku) DO NOTHING
		`, p.ID, defaultTenantID, p.ID, p.Name, p.Line); err != nil {
			return err
		}
	}
	return nil
}

func loadProductosForTenant(db *sql.DB, tenantID int) ([]productOption, error) {
	rows, err := db.Query(`
		SELECT sku, nombre, linea, COALESCE(location, ''), COALESCE(credit_enabled, 0), COALESCE(debtor_name, ''), COALESCE(installments_total, 0), COALESCE(installments_paid, 0), COALESCE(total_value, 0), COALESCE(installment_value, 0), COALESCE(anotaciones, ''), COALESCE(fecha_ingreso, ''), COALESCE(precio_venta, 0), COALESCE(retoma_enabled, 0), retoma_price, owner_user_id
		FROM productos
		WHERE tenant_id = ?
		ORDER BY sku
	`, normalizeTenantID(tenantID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	products := []productOption{}
	for rows.Next() {
		var p productOption
		var creditEnabled int
		var retomaEnabled int
		var retomaPrice sql.NullFloat64
		var ownerUserID sql.NullInt64
		if err := rows.Scan(&p.ID, &p.Name, &p.Line, &p.Location, &creditEnabled, &p.DebtorName, &p.InstallmentsTotal, &p.InstallmentsPaid, &p.TotalValue, &p.InstallmentValue, &p.Notes, &p.FechaIngreso, &p.SalePrice, &retomaEnabled, &retomaPrice, &ownerUserID); err != nil {
			return nil, err
		}
		p.CreditEnabled = creditEnabled == 1
		p.RetomaEnabled = retomaEnabled == 1
		p.HasRetomaPrice = retomaPrice.Valid
		if retomaPrice.Valid {
			p.RetomaPrice = retomaPrice.Float64
		}
		p.HasOwner = ownerUserID.Valid
		if ownerUserID.Valid {
			p.OwnerUserID = int(ownerUserID.Int64)
		}
		products = append(products, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return products, nil
}

func loadProductos(db *sql.DB) ([]productOption, error) {
	return loadProductosForTenant(db, defaultTenantID)
}

func loadVisibleProductsForUser(db *sql.DB, user *User) ([]productOption, error) {
	products, err := loadProductosForTenant(db, tenantIDFromUser(user))
	if err != nil {
		return nil, err
	}
	return filterProductsForUser(products, user), nil
}

func loadAssignableUsersForTenant(db *sql.DB, tenantID int) ([]assignableUser, error) {
	rows, err := db.Query(`
		SELECT id, username
		FROM users
		WHERE is_active = 1 AND tenant_id = ?
		ORDER BY LOWER(username), id
	`, normalizeTenantID(tenantID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	users := make([]assignableUser, 0)
	for rows.Next() {
		var user assignableUser
		if err := rows.Scan(&user.ID, &user.Username); err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return users, nil
}

func loadAssignableUsers(db *sql.DB) ([]assignableUser, error) {
	return loadAssignableUsersForTenant(db, defaultTenantID)
}

func canAccessProduct(user *User, product productOption) bool {
	if user != nil && isAdminRole(user.Role) {
		return true
	}
	if !product.HasOwner {
		return true
	}
	if user == nil {
		return false
	}
	return product.OwnerUserID == user.ID
}

func filterProductsForUser(products []productOption, user *User) []productOption {
	if user != nil && isAdminRole(user.Role) {
		return products
	}
	filtered := make([]productOption, 0, len(products))
	for _, product := range products {
		if canAccessProduct(user, product) {
			filtered = append(filtered, product)
		}
	}
	return filtered
}

func productAccessibleByID(db *sql.DB, user *User, productID string) (bool, error) {
	var ownerUserID sql.NullInt64
	err := db.QueryRow(`
		SELECT owner_user_id
		FROM productos
		WHERE tenant_id = ? AND (sku = ? OR id = ?)
		LIMIT 1
	`, tenantIDFromUser(user), productID, productID).Scan(&ownerUserID)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if user != nil && isAdminRole(user.Role) {
		return true, nil
	}
	if !ownerUserID.Valid {
		return true, nil
	}
	if user == nil {
		return false, nil
	}
	return int(ownerUserID.Int64) == user.ID, nil
}

func productVisibilityPredicate(alias string, user *User) (string, []any) {
	if alias == "" {
		alias = "p"
	}
	tenantID := tenantIDFromUser(user)
	if user != nil && isAdminRole(user.Role) {
		return fmt.Sprintf("%s.tenant_id = ?", alias), []any{tenantID}
	}
	if user == nil {
		return fmt.Sprintf("(%s.tenant_id = ? AND (%s.sku IS NULL OR %s.owner_user_id IS NULL))", alias, alias, alias), []any{tenantID}
	}
	return fmt.Sprintf("(%s.tenant_id = ? AND (%s.sku IS NULL OR %s.owner_user_id IS NULL OR %s.owner_user_id = ?))", alias, alias, alias, alias), []any{tenantID, user.ID}
}

func tenantScopedProductAccessPredicate(entityAlias, productAlias string, user *User) (string, []any) {
	if entityAlias == "" {
		entityAlias = "t"
	}
	if productAlias == "" {
		productAlias = "p"
	}
	tenantID := tenantIDFromUser(user)
	if user != nil && isAdminRole(user.Role) {
		return fmt.Sprintf("%s.tenant_id = ?", entityAlias), []any{tenantID}
	}
	if user == nil {
		return fmt.Sprintf("(%s.tenant_id = ? AND (%s.sku IS NULL OR %s.owner_user_id IS NULL))", entityAlias, productAlias, productAlias), []any{tenantID}
	}
	return fmt.Sprintf("(%s.tenant_id = ? AND (%s.sku IS NULL OR %s.owner_user_id IS NULL OR %s.owner_user_id = ?))", entityAlias, productAlias, productAlias, productAlias), []any{tenantID, user.ID}
}

func creditVisibilityPredicate(creditAlias string, user *User) (string, []any) {
	if creditAlias == "" {
		creditAlias = "cs"
	}
	tenantID := tenantIDFromUser(user)
	if user != nil && isAdminRole(user.Role) {
		return fmt.Sprintf("%s.tenant_id = ?", creditAlias), []any{tenantID}
	}
	if user == nil {
		return fmt.Sprintf(`(
			%s.tenant_id = ?
			AND (
				COALESCE(%s.kind, '%s') = '%s'
				OR EXISTS (
					SELECT 1
					FROM productos pvis
					WHERE pvis.tenant_id = %s.tenant_id
					  AND pvis.sku = %s.product_id
					  AND pvis.owner_user_id IS NULL
				)
			)
		)`, creditAlias, creditAlias, creditSaleKindProduct, creditSaleKindCash, creditAlias, creditAlias), []any{tenantID}
	}
	return fmt.Sprintf(`(
		%s.tenant_id = ?
		AND (
			COALESCE(%s.kind, '%s') = '%s'
			OR EXISTS (
				SELECT 1
				FROM productos pvis
				WHERE pvis.tenant_id = %s.tenant_id
				  AND pvis.sku = %s.product_id
				  AND (pvis.owner_user_id IS NULL OR pvis.owner_user_id = ?)
			)
		)
	)`, creditAlias, creditAlias, creditSaleKindProduct, creditSaleKindCash, creditAlias, creditAlias), []any{tenantID, user.ID}
}

func listRecentSalesForUser(db *sql.DB, user *User, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 50
	}
	accessSQL, accessArgs := tenantScopedProductAccessPredicate("v", "p", user)
	args := append([]any{}, accessArgs...)
	args = append(args, limit)
	rows, err := db.Query(`
		SELECT v.id, v.fecha, v.producto_id, COALESCE(p.nombre, v.producto_id), v.cantidad, v.precio_final, COALESCE(v.metodo_pago, '')
		FROM ventas v
		LEFT JOIN productos p ON p.sku = v.producto_id AND p.tenant_id = v.tenant_id
		WHERE `+accessSQL+`
		ORDER BY v.fecha DESC, v.id DESC
		LIMIT ?
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0, limit)
	for rows.Next() {
		var id int
		var fecha, productoID, producto, metodo string
		var cantidad int
		var precioFinal float64
		if err := rows.Scan(&id, &fecha, &productoID, &producto, &cantidad, &precioFinal, &metodo); err != nil {
			return nil, err
		}
		items = append(items, map[string]any{
			"id":           id,
			"fecha":        formatDateWithSettings(fecha),
			"producto_id":  productoID,
			"producto":     producto,
			"cantidad":     cantidad,
			"precio_final": precioFinal,
			"metodo_pago":  metodo,
			"total":        precioFinal * float64(cantidad),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func listSalesForUser(db *sql.DB, user *User, q, fromStr, toStr string, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 100
	}
	q = strings.TrimSpace(strings.ToLower(q))
	accessSQL, accessArgs := tenantScopedProductAccessPredicate("v", "p", user)
	args := append([]any{}, accessArgs...)
	query := `
		SELECT
			v.id,
			v.fecha,
			v.producto_id,
			COALESCE(p.nombre, v.producto_id),
			v.cantidad,
			v.precio_final,
			COALESCE(v.channel, ''),
			COALESCE(v.sold_by, ''),
			COALESCE(v.notas, ''),
			COALESCE(v.metodo_pago, '')
		FROM ventas v
		LEFT JOIN productos p ON p.sku = v.producto_id AND p.tenant_id = v.tenant_id
		WHERE ` + accessSQL
	if q != "" {
		query += ` AND (LOWER(v.producto_id) LIKE ? OR LOWER(COALESCE(p.nombre, '')) LIKE ? OR LOWER(COALESCE(v.channel, '')) LIKE ? OR LOWER(COALESCE(v.sold_by, '')) LIKE ? OR LOWER(COALESCE(v.notas, '')) LIKE ? OR LOWER(COALESCE(v.metodo_pago, '')) LIKE ?)`
		qLike := "%" + q + "%"
		args = append(args, qLike, qLike, qLike, qLike, qLike, qLike)
	}
	if fromStr != "" {
		query += ` AND ` + sqlDatePrefixExpr("v.fecha") + ` >= ?`
		args = append(args, fromStr)
	}
	if toStr != "" {
		query += ` AND ` + sqlDatePrefixExpr("v.fecha") + ` <= ?`
		args = append(args, toStr)
	}
	query += ` ORDER BY v.fecha DESC, v.id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0, limit)
	for rows.Next() {
		var (
			id            int
			fecha         string
			productID     string
			productName   string
			quantity      int
			salePrice     float64
			channel       string
			soldBy        string
			notes         string
			paymentMethod string
		)
		if err := rows.Scan(&id, &fecha, &productID, &productName, &quantity, &salePrice, &channel, &soldBy, &notes, &paymentMethod); err != nil {
			return nil, err
		}
		items = append(items, map[string]any{
			"id":             id,
			"fecha":          formatDateWithSettings(fecha),
			"product_id":     productID,
			"product_name":   productName,
			"quantity":       quantity,
			"sale_price":     salePrice,
			"channel":        channel,
			"sold_by":        soldBy,
			"notes":          notes,
			"payment_method": paymentMethod,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func listRetomasForUser(db *sql.DB, user *User, q string, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 100
	}
	q = strings.TrimSpace(strings.ToLower(q))
	accessSQL, accessArgs := tenantScopedProductAccessPredicate("r", "p", user)
	args := append([]any{}, accessArgs...)
	query := `
		SELECT
			r.id,
			r.fecha,
			r.producto_id,
			COALESCE(p.nombre, r.producto_id),
			r.cantidad,
			r.valor_recibido,
			r.estado_recibido,
			r.publicado_stock,
			r.precio_publicado,
			COALESCE(r.notas, '')
		FROM retomas r
		LEFT JOIN productos p ON p.sku = r.producto_id AND p.tenant_id = r.tenant_id
		WHERE ` + accessSQL
	if q != "" {
		query += ` AND (LOWER(r.producto_id) LIKE ? OR LOWER(COALESCE(p.nombre, '')) LIKE ? OR LOWER(COALESCE(r.estado_recibido, '')) LIKE ? OR LOWER(COALESCE(r.notas, '')) LIKE ?)`
		qLike := "%" + q + "%"
		args = append(args, qLike, qLike, qLike, qLike)
	}
	query += ` ORDER BY r.fecha DESC, r.id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0, limit)
	for rows.Next() {
		var (
			id               int
			fecha            string
			productID        string
			productName      string
			quantity         int
			valueReceived    float64
			receivedState    string
			publishedToStock int
			finalSalePrice   sql.NullFloat64
			notes            string
		)
		if err := rows.Scan(&id, &fecha, &productID, &productName, &quantity, &valueReceived, &receivedState, &publishedToStock, &finalSalePrice, &notes); err != nil {
			return nil, err
		}
		var publishedPrice any = nil
		if finalSalePrice.Valid {
			publishedPrice = finalSalePrice.Float64
		}
		items = append(items, map[string]any{
			"id":                 id,
			"fecha":              formatDateWithSettings(fecha),
			"product_id":         productID,
			"product_name":       productName,
			"quantity":           quantity,
			"value_received":     valueReceived,
			"received_state":     receivedState,
			"published_to_stock": publishedToStock == 1,
			"final_sale_price":   publishedPrice,
			"notes":              notes,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func listCreditsForUser(db *sql.DB, user *User, q string, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 100
	}
	q = strings.TrimSpace(strings.ToLower(q))
	accessSQL, accessArgs := creditVisibilityPredicate("cs", user)
	args := append([]any{}, accessArgs...)
	query := `
		SELECT
			cs.id,
			cs.created_at,
			COALESCE(cs.kind, ?),
			COALESCE(cs.product_id, ''),
			CASE
				WHEN COALESCE(cs.kind, ?) = ? THEN 'Préstamo de dinero'
				ELSE COALESCE(NULLIF(p.nombre, ''), cs.product_id)
			END,
			cs.quantity,
			COALESCE(cs.customer_id, 0),
			COALESCE(c.name, cs.debtor_name, ''),
			COALESCE(c.document_type, cs.debtor_document_type, ''),
			COALESCE(c.document_number, cs.debtor_document_number, ''),
			COALESCE(c.phone, cs.debtor_phone, ''),
			COALESCE(c.address, ''),
			COALESCE(c.city, ''),
			COALESCE(c.notes, ''),
			COALESCE(cs.installments_total, 0),
			COALESCE(cs.installments_paid, 0),
			COALESCE(cs.total_value, 0),
			COALESCE(cs.interest_percent, 0),
			COALESCE(cs.installment_value, 0),
			COALESCE((
				SELECT SUM(ci.amount_paid)
				FROM credit_installments ci
				WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id
			), 0),
			COALESCE((
				SELECT COUNT(*)
				FROM credit_installments ci
				WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id AND COALESCE(ci.payment_type, 'cuota') = 'cuota'
			), COALESCE(cs.installments_paid, 0)),
			COALESCE(cs.notes, ''),
			COALESCE((
				SELECT ci.amount_paid
				FROM credit_installments ci
				WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id
				ORDER BY ci.created_at DESC, ci.id DESC
				LIMIT 1
			), 0),
			COALESCE((
				SELECT ci.created_at
				FROM credit_installments ci
				WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id
				ORDER BY ci.created_at DESC, ci.id DESC
				LIMIT 1
			), ''),
			COALESCE((
				SELECT COALESCE(ci.payment_type, 'cuota')
				FROM credit_installments ci
				WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id
				ORDER BY ci.created_at DESC, ci.id DESC
				LIMIT 1
			), '')
			,
			COALESCE(cs.status, '')
		FROM credit_sales cs
		LEFT JOIN productos p ON p.sku = cs.product_id AND p.tenant_id = cs.tenant_id
		LEFT JOIN customers c ON c.id = cs.customer_id AND c.tenant_id = cs.tenant_id
		WHERE ` + accessSQL
	args = append([]any{string(creditSaleKindProduct), string(creditSaleKindProduct), string(creditSaleKindCash)}, args...)
	if q != "" {
		query += ` AND (
			LOWER(COALESCE(cs.product_id, '')) LIKE ?
			OR LOWER(COALESCE(p.nombre, '')) LIKE ?
			OR LOWER(COALESCE(cs.kind, '')) LIKE ?
			OR (COALESCE(cs.kind, ?) = ? AND LOWER('prestamo de dinero') LIKE ?)
			OR LOWER(COALESCE(c.name, cs.debtor_name, '')) LIKE ?
			OR LOWER(COALESCE(c.document_type, cs.debtor_document_type, '')) LIKE ?
			OR LOWER(COALESCE(c.document_number, cs.debtor_document_number, '')) LIKE ?
			OR LOWER(COALESCE(c.phone, cs.debtor_phone, '')) LIKE ?
			OR LOWER(COALESCE(c.city, '')) LIKE ?
		)`
		qLike := "%" + q + "%"
		args = append(args, qLike, qLike, qLike, string(creditSaleKindProduct), string(creditSaleKindCash), qLike, qLike, qLike, qLike, qLike, qLike)
	}
	query += ` ORDER BY cs.created_at DESC, cs.id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0, limit)
	for rows.Next() {
		var (
			id                    int
			createdAt             string
			kindRaw               string
			productID             string
			productName           string
			quantity              int
			customerID            int
			debtorName            string
			debtorDocType         string
			debtorDocNumber       string
			debtorPhone           string
			customerAddress       string
			customerCity          string
			customerNotes         string
			installmentsTotal     int
			installmentsPaid      int
			totalValue            float64
			interestPercent       float64
			installmentValue      float64
			totalPaid             float64
			paidInstallmentsCount int
			notes                 string
			lastPaymentAmount     float64
			lastPaymentAt         string
			lastPaymentType       string
			statusRaw             string
		)
		if err := rows.Scan(&id, &createdAt, &kindRaw, &productID, &productName, &quantity, &customerID, &debtorName, &debtorDocType, &debtorDocNumber, &debtorPhone, &customerAddress, &customerCity, &customerNotes, &installmentsTotal, &installmentsPaid, &totalValue, &interestPercent, &installmentValue, &totalPaid, &paidInstallmentsCount, &notes, &lastPaymentAmount, &lastPaymentAt, &lastPaymentType, &statusRaw); err != nil {
			return nil, err
		}
		kind := normalizeCreditSaleKind(kindRaw)
		if paidInstallmentsCount < installmentsPaid {
			paidInstallmentsCount = installmentsPaid
		}
		legacyTotalPaid := math.Round((float64(installmentsPaid)*installmentValue)*100) / 100
		if totalPaid < legacyTotalPaid {
			totalPaid = legacyTotalPaid
		}
		debtTotal := creditDebtTotal(installmentsTotal, installmentValue)
		currentDebt := creditCurrentDebt(debtTotal, totalPaid)
		status := effectiveCreditStatus(statusRaw, currentDebt, debtTotal)
		item := map[string]any{
			"id":                       id,
			"created_at":               formatDateWithSettings(createdAt),
			"kind":                     string(kind),
			"kind_label":               creditKindLabel(kind),
			"product_id":               productID,
			"product":                  productName,
			"quantity":                 quantity,
			"customer_id":              customerID,
			"customer_name":            debtorName,
			"customer_phone":           debtorPhone,
			"customer_document_type":   debtorDocType,
			"customer_document_number": debtorDocNumber,
			"customer_address":         customerAddress,
			"customer_city":            customerCity,
			"customer_notes":           customerNotes,
			"debtor_name":              debtorName,
			"debtor_document_type":     debtorDocType,
			"debtor_document_number":   debtorDocNumber,
			"debtor_phone":             debtorPhone,
			"installments_total":       installmentsTotal,
			"installments_paid":        paidInstallmentsCount,
			"paid_installments_count":  paidInstallmentsCount,
			"installments_pending":     max(installmentsTotal-paidInstallmentsCount, 0),
			"total_value":              totalValue,
			"debt_total":               debtTotal,
			"total_paid":               totalPaid,
			"current_debt":             currentDebt,
			"interest_percent":         interestPercent,
			"installment_value":        installmentValue,
			"notes":                    notes,
			"status":                   string(status),
			"status_label":             creditStatusLabel(status),
			"last_payment_type":        normalizeCreditPaymentType(lastPaymentType),
		}
		if lastPaymentAt != "" {
			item["last_payment_at"] = formatDateWithSettings(lastPaymentAt)
			item["last_payment_amount"] = lastPaymentAmount
		} else {
			item["last_payment_at"] = ""
			item["last_payment_amount"] = 0
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func creditDetailForUser(db *sql.DB, user *User, creditSaleID int) (map[string]any, error) {
	if creditSaleID <= 0 {
		return nil, sql.ErrNoRows
	}
	accessSQL, accessArgs := creditVisibilityPredicate("cs", user)
	args := append([]any{string(creditSaleKindProduct), string(creditSaleKindProduct), string(creditSaleKindCash)}, accessArgs...)
	args = append(args, creditSaleID)
	row := db.QueryRow(`
		SELECT
			cs.id,
			cs.created_at,
			COALESCE(cs.kind, ?),
			COALESCE(cs.product_id, ''),
			CASE
				WHEN COALESCE(cs.kind, ?) = ? THEN 'Préstamo de dinero'
				ELSE COALESCE(NULLIF(p.nombre, ''), cs.product_id)
			END,
			cs.quantity,
			COALESCE(cs.customer_id, 0),
			COALESCE(c.name, cs.debtor_name, ''),
			COALESCE(c.document_type, cs.debtor_document_type, ''),
			COALESCE(c.document_number, cs.debtor_document_number, ''),
			COALESCE(c.phone, cs.debtor_phone, ''),
			COALESCE(c.address, ''),
			COALESCE(c.city, ''),
			COALESCE(c.notes, ''),
			COALESCE(cs.installments_total, 0),
			COALESCE(cs.installments_paid, 0),
			COALESCE(cs.total_value, 0),
			COALESCE(cs.interest_percent, 0),
			COALESCE(cs.installment_value, 0),
			COALESCE((
				SELECT SUM(ci.amount_paid)
				FROM credit_installments ci
				WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id
			), 0),
			COALESCE((
				SELECT COUNT(*)
				FROM credit_installments ci
				WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id AND COALESCE(ci.payment_type, 'cuota') = 'cuota'
			), COALESCE(cs.installments_paid, 0)),
			COALESCE(cs.notes, ''),
			COALESCE((
				SELECT ci.amount_paid
				FROM credit_installments ci
				WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id
				ORDER BY ci.created_at DESC, ci.id DESC
				LIMIT 1
			), 0),
			COALESCE((
				SELECT ci.created_at
				FROM credit_installments ci
				WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id
				ORDER BY ci.created_at DESC, ci.id DESC
				LIMIT 1
			), ''),
			COALESCE((
				SELECT COALESCE(ci.payment_type, 'cuota')
				FROM credit_installments ci
				WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id
				ORDER BY ci.created_at DESC, ci.id DESC
				LIMIT 1
			), ''),
			COALESCE(cs.status, '')
		FROM credit_sales cs
		LEFT JOIN productos p ON p.sku = cs.product_id AND p.tenant_id = cs.tenant_id
		LEFT JOIN customers c ON c.id = cs.customer_id AND c.tenant_id = cs.tenant_id
		WHERE `+accessSQL+` AND cs.id = ?
		LIMIT 1
	`, args...)
	var (
		id                    int
		createdAt             string
		kindRaw               string
		productID             string
		productName           string
		quantity              int
		customerID            int
		debtorName            string
		debtorDocType         string
		debtorDocNumber       string
		debtorPhone           string
		customerAddress       string
		customerCity          string
		customerNotes         string
		installmentsTotal     int
		installmentsPaid      int
		totalValue            float64
		interestPercent       float64
		installmentValue      float64
		totalPaid             float64
		paidInstallmentsCount int
		notes                 string
		lastPaymentAmount     float64
		lastPaymentAt         string
		lastPaymentType       string
		statusRaw             string
	)
	if err := row.Scan(&id, &createdAt, &kindRaw, &productID, &productName, &quantity, &customerID, &debtorName, &debtorDocType, &debtorDocNumber, &debtorPhone, &customerAddress, &customerCity, &customerNotes, &installmentsTotal, &installmentsPaid, &totalValue, &interestPercent, &installmentValue, &totalPaid, &paidInstallmentsCount, &notes, &lastPaymentAmount, &lastPaymentAt, &lastPaymentType, &statusRaw); err != nil {
		return nil, err
	}
	kind := normalizeCreditSaleKind(kindRaw)
	if paidInstallmentsCount < installmentsPaid {
		paidInstallmentsCount = installmentsPaid
	}
	legacyTotalPaid := math.Round((float64(installmentsPaid)*installmentValue)*100) / 100
	if totalPaid < legacyTotalPaid {
		totalPaid = legacyTotalPaid
	}
	debtTotal := creditDebtTotal(installmentsTotal, installmentValue)
	currentDebt := creditCurrentDebt(debtTotal, totalPaid)
	status := effectiveCreditStatus(statusRaw, currentDebt, debtTotal)
	item := map[string]any{
		"id":                       id,
		"created_at":               formatDateWithSettings(createdAt),
		"kind":                     string(kind),
		"kind_label":               creditKindLabel(kind),
		"product_id":               productID,
		"product":                  productName,
		"quantity":                 quantity,
		"customer_id":              customerID,
		"customer_name":            debtorName,
		"customer_phone":           debtorPhone,
		"customer_document_type":   debtorDocType,
		"customer_document_number": debtorDocNumber,
		"customer_address":         customerAddress,
		"customer_city":            customerCity,
		"customer_notes":           customerNotes,
		"debtor_name":              debtorName,
		"debtor_document_type":     debtorDocType,
		"debtor_document_number":   debtorDocNumber,
		"debtor_phone":             debtorPhone,
		"installments_total":       installmentsTotal,
		"installments_paid":        paidInstallmentsCount,
		"paid_installments_count":  paidInstallmentsCount,
		"installments_pending":     max(installmentsTotal-paidInstallmentsCount, 0),
		"total_value":              totalValue,
		"debt_total":               debtTotal,
		"total_paid":               totalPaid,
		"current_debt":             currentDebt,
		"interest_percent":         interestPercent,
		"installment_value":        installmentValue,
		"notes":                    notes,
		"status":                   string(status),
		"status_label":             creditStatusLabel(status),
		"last_payment_type":        normalizeCreditPaymentType(lastPaymentType),
	}
	if lastPaymentAt != "" {
		item["last_payment_at"] = formatDateWithSettings(lastPaymentAt)
		item["last_payment_amount"] = lastPaymentAmount
	} else {
		item["last_payment_at"] = ""
		item["last_payment_amount"] = 0
	}
	return item, nil
}

func creditAccessibleByID(db *sql.DB, user *User, creditSaleID int) (bool, error) {
	if creditSaleID <= 0 {
		return false, nil
	}
	accessSQL, accessArgs := creditVisibilityPredicate("cs", user)
	args := append(accessArgs, creditSaleID)
	var exists int
	if err := db.QueryRow(`SELECT 1 FROM credit_sales cs WHERE `+accessSQL+` AND cs.id = ? LIMIT 1`, args...).Scan(&exists); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func creditEditHistoryForUser(db *sql.DB, user *User, creditSaleID, limit int) ([]map[string]any, error) {
	allowed, err := creditAccessibleByID(db, user, creditSaleID)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, sql.ErrNoRows
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}

	rows, err := db.Query(`
		SELECT a.id, a.event_type, a.source, COALESCE(a.payload_json, '{}'), a.created_at, COALESCE(u.username, '')
		FROM audit_events a
		LEFT JOIN users u ON u.id = a.user_id AND u.tenant_id = a.tenant_id
		WHERE a.tenant_id = ? AND a.entity_type = 'credit_sale' AND a.entity_id = ? AND a.event_type = 'credit_sale_updated'
		ORDER BY a.created_at DESC, a.id DESC
		LIMIT ?
	`, tenantIDFromUser(user), strconv.Itoa(creditSaleID), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0, limit)
	for rows.Next() {
		var (
			id         int
			eventType  string
			source     string
			payloadRaw string
			createdAt  string
			createdBy  string
		)
		if err := rows.Scan(&id, &eventType, &source, &payloadRaw, &createdAt, &createdBy); err != nil {
			return nil, err
		}
		payload := map[string]any{}
		if strings.TrimSpace(payloadRaw) != "" {
			_ = json.Unmarshal([]byte(payloadRaw), &payload)
		}
		changes := make([]map[string]any, 0)
		if rawChanges, ok := payload["changes"].([]any); ok {
			for _, raw := range rawChanges {
				change, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				changes = append(changes, map[string]any{
					"field":  change["field"],
					"label":  change["label"],
					"before": change["before"],
					"after":  change["after"],
				})
			}
		}
		impact := map[string]any{}
		if rawImpact, ok := payload["impact"].(map[string]any); ok {
			impact = rawImpact
		}
		items = append(items, map[string]any{
			"id":           id,
			"event_type":   eventType,
			"event_label":  "Crédito editado",
			"source":       source,
			"created_at":   formatDateWithSettings(createdAt),
			"created_by":   createdBy,
			"changes":      changes,
			"change_count": len(changes),
			"impact":       impact,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

type creditEditReportFilters struct {
	DateFrom     string
	DateTo       string
	Username     string
	Status       string
	Kind         string
	Customer     string
	CreditSaleID int
	Limit        int
}

type productLoanReportFilters struct {
	DateFrom      string
	DateTo        string
	Status        string
	Overdue       string
	Customer      string
	Product       string
	ManagedBy     string
	ProductLoanID int
	Limit         int
}

type cashLoanReportFilters struct {
	DateFrom     string
	DateTo       string
	Username     string
	Status       string
	Customer     string
	CreditSaleID int
	Limit        int
}

func parseProductLoanReportFilters(r *http.Request, defaultLimit int) (productLoanReportFilters, string) {
	productLoanIDRaw := strings.TrimSpace(r.URL.Query().Get("product_loan_id"))
	productLoanID := 0
	if productLoanIDRaw != "" {
		if parsed, err := strconv.Atoi(productLoanIDRaw); err == nil && parsed > 0 {
			productLoanID = parsed
		}
	}
	return productLoanReportFilters{
		DateFrom:      strings.TrimSpace(r.URL.Query().Get("date_from")),
		DateTo:        strings.TrimSpace(r.URL.Query().Get("date_to")),
		Status:        strings.TrimSpace(r.URL.Query().Get("status")),
		Overdue:       strings.TrimSpace(r.URL.Query().Get("overdue")),
		Customer:      strings.TrimSpace(r.URL.Query().Get("customer")),
		Product:       strings.TrimSpace(r.URL.Query().Get("product")),
		ManagedBy:     strings.TrimSpace(r.URL.Query().Get("managed_by")),
		ProductLoanID: productLoanID,
		Limit:         defaultLimit,
	}, productLoanIDRaw
}

func normalizeProductLoanStatusFilter(value string) string {
	switch normalizeProductLoanStatus(value) {
	case productLoanStatusActive:
		if strings.TrimSpace(strings.ToLower(value)) == string(productLoanStatusActive) {
			return string(productLoanStatusActive)
		}
	case productLoanStatusReturned:
		return string(productLoanStatusReturned)
	case productLoanStatusPaid:
		return string(productLoanStatusPaid)
	case productLoanStatusCancelled:
		return string(productLoanStatusCancelled)
	}
	return ""
}

func isProductLoanOverdue(status productLoanStatus, dueAt string) bool {
	if normalizeProductLoanStatus(string(status)) != productLoanStatusActive {
		return false
	}
	parsed, ok := parseFlexibleTime(dueAt)
	if !ok {
		return false
	}
	now := time.Now().In(appTimeLocation)
	dueDate := time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, appTimeLocation)
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, appTimeLocation)
	return dueDate.Before(today)
}

func productLoanEventLabel(eventType string) string {
	switch strings.TrimSpace(strings.ToLower(eventType)) {
	case "product_loan_created":
		return "Préstamo creado"
	case "product_loan_closed":
		return "Préstamo cerrado"
	default:
		return strings.TrimSpace(eventType)
	}
}

func listProductLoansReport(db *sql.DB, currentUser *User, tenantID int, filters productLoanReportFilters) ([]productLoanReportItem, error) {
	if currentUser == nil || !isAdminRole(currentUser.Role) {
		return nil, requestError{Status: http.StatusForbidden, Message: "Solo administrador puede consultar préstamos físicos."}
	}
	tenantID = normalizeTenantID(tenantID)
	if filters.Limit <= 0 {
		filters.Limit = 150
	}
	if filters.Limit > 500 {
		filters.Limit = 500
	}
	filters.DateFrom = strings.TrimSpace(filters.DateFrom)
	filters.DateTo = strings.TrimSpace(filters.DateTo)
	filters.Status = normalizeProductLoanStatusFilter(filters.Status)
	filters.Overdue = strings.TrimSpace(strings.ToLower(filters.Overdue))
	filters.Customer = strings.TrimSpace(filters.Customer)
	filters.Product = strings.TrimSpace(filters.Product)
	filters.ManagedBy = strings.TrimSpace(filters.ManagedBy)

	query := `
		SELECT
			pl.id,
			COALESCE(pl.product_id, ''),
			COALESCE(NULLIF(p.nombre, ''), pl.product_id),
			COALESCE(pl.quantity, 0),
			COALESCE(pl.customer_id, 0),
			COALESCE(c.name, pl.borrower_name, ''),
			COALESCE(c.document_type, pl.borrower_document_type, ''),
			COALESCE(c.document_number, pl.borrower_document_number, ''),
			COALESCE(c.phone, pl.borrower_phone, ''),
			COALESCE(c.city, pl.borrower_city, ''),
			COALESCE(u.username, ''),
			COALESCE(pl.loaned_at, ''),
			COALESCE(pl.due_at, ''),
			COALESCE(pl.closed_at, ''),
			COALESCE(pl.status, 'active'),
			COALESCE(pl.notes, ''),
			COALESCE(pl.close_notes, '')
		FROM product_loans pl
		LEFT JOIN productos p ON p.sku = pl.product_id AND p.tenant_id = pl.tenant_id
		LEFT JOIN customers c ON c.id = pl.customer_id AND c.tenant_id = pl.tenant_id
		LEFT JOIN users u ON u.id = pl.created_by AND u.tenant_id = pl.tenant_id
		WHERE pl.tenant_id = ?
	`
	args := []any{tenantID}
	if filters.DateFrom != "" {
		query += ` AND ` + sqlDatePrefixExpr("pl.loaned_at") + ` >= ?`
		args = append(args, filters.DateFrom)
	}
	if filters.DateTo != "" {
		query += ` AND ` + sqlDatePrefixExpr("pl.loaned_at") + ` <= ?`
		args = append(args, filters.DateTo)
	}
	if filters.Status != "" {
		query += ` AND COALESCE(pl.status, 'active') = ?`
		args = append(args, filters.Status)
	}
	if filters.Customer != "" {
		query += ` AND (
			LOWER(COALESCE(c.name, pl.borrower_name, '')) LIKE ?
			OR LOWER(COALESCE(c.document_number, pl.borrower_document_number, '')) LIKE ?
			OR LOWER(COALESCE(c.phone, pl.borrower_phone, '')) LIKE ?
			OR LOWER(COALESCE(c.city, pl.borrower_city, '')) LIKE ?
		)`
		search := "%" + strings.ToLower(filters.Customer) + "%"
		args = append(args, search, search, search, search)
	}
	if filters.Product != "" {
		query += ` AND (
			LOWER(COALESCE(pl.product_id, '')) LIKE ?
			OR LOWER(COALESCE(p.nombre, '')) LIKE ?
		)`
		search := "%" + strings.ToLower(filters.Product) + "%"
		args = append(args, search, search)
	}
	if filters.ManagedBy != "" {
		query += ` AND LOWER(COALESCE(u.username, '')) LIKE ?`
		args = append(args, "%"+strings.ToLower(filters.ManagedBy)+"%")
	}
	if filters.ProductLoanID > 0 {
		query += ` AND pl.id = ?`
		args = append(args, filters.ProductLoanID)
	}
	query += ` ORDER BY pl.loaned_at DESC, pl.id DESC LIMIT ?`
	args = append(args, filters.Limit)

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]productLoanReportItem, 0, filters.Limit)
	loanIDs := make([]int, 0, filters.Limit)
	for rows.Next() {
		var item productLoanReportItem
		if err := rows.Scan(
			&item.ProductLoanID,
			&item.ProductID,
			&item.ProductName,
			&item.Quantity,
			&item.CustomerID,
			&item.CustomerName,
			&item.CustomerDocumentType,
			&item.CustomerDocument,
			&item.CustomerPhone,
			&item.CustomerCity,
			&item.ManagedByName,
			&item.LoanedAt,
			&item.DueAt,
			&item.ClosedAt,
			&item.Status,
			&item.Notes,
			&item.CloseNotes,
		); err != nil {
			return nil, err
		}
		status := normalizeProductLoanStatus(item.Status)
		item.Status = string(status)
		item.StatusLabel = productLoanStatusLabel(status)
		item.IsOverdue = isProductLoanOverdue(status, item.DueAt)
		if item.IsOverdue {
			item.OverdueLabel = "Vencido"
		} else {
			item.OverdueLabel = "En fecha"
		}
		item.LoanedAt = formatDateWithSettings(item.LoanedAt)
		item.DueAt = formatDateWithSettings(item.DueAt)
		item.ClosedAt = formatDateWithSettings(item.ClosedAt)
		items = append(items, item)
		loanIDs = append(loanIDs, item.ProductLoanID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(items) == 0 {
		return items, nil
	}
	unitMap, err := loadProductLoanUnitIDs(db, tenantID, loanIDs)
	if err != nil {
		return nil, err
	}
	filtered := make([]productLoanReportItem, 0, len(items))
	for _, item := range items {
		item.UnitIDs = unitMap[item.ProductLoanID]
		item.UnitIDsText = strings.Join(item.UnitIDs, ", ")
		if item.UnitIDsText == "" {
			item.UnitIDsText = "-"
		}
		if filters.Overdue == "yes" && !item.IsOverdue {
			continue
		}
		if filters.Overdue == "no" && item.IsOverdue {
			continue
		}
		filtered = append(filtered, item)
	}
	return filtered, nil
}

func parseCashLoanReportFilters(r *http.Request, defaultLimit int) (cashLoanReportFilters, string) {
	creditSaleIDRaw := strings.TrimSpace(r.URL.Query().Get("credit_sale_id"))
	creditSaleID := 0
	if creditSaleIDRaw != "" {
		if parsed, err := strconv.Atoi(creditSaleIDRaw); err == nil && parsed > 0 {
			creditSaleID = parsed
		}
	}
	return cashLoanReportFilters{
		DateFrom:     strings.TrimSpace(r.URL.Query().Get("date_from")),
		DateTo:       strings.TrimSpace(r.URL.Query().Get("date_to")),
		Username:     strings.TrimSpace(r.URL.Query().Get("username")),
		Status:       strings.TrimSpace(r.URL.Query().Get("status")),
		Customer:     strings.TrimSpace(r.URL.Query().Get("customer")),
		CreditSaleID: creditSaleID,
		Limit:        defaultLimit,
	}, creditSaleIDRaw
}

func cashLoanStatusLabel(status creditStatus) string {
	label := creditStatusLabel(status)
	label = strings.Replace(label, "Crédito", "Préstamo", 1)
	return label
}

func listCashLoansReport(db *sql.DB, currentUser *User, tenantID int, filters cashLoanReportFilters) ([]cashLoanReportItem, error) {
	if currentUser == nil || !isAdminRole(currentUser.Role) {
		return nil, requestError{Status: http.StatusForbidden, Message: "Solo administrador puede consultar préstamos de dinero."}
	}
	tenantID = normalizeTenantID(tenantID)
	if filters.Limit <= 0 {
		filters.Limit = 150
	}
	if filters.Limit > 500 {
		filters.Limit = 500
	}
	filters.DateFrom = strings.TrimSpace(filters.DateFrom)
	filters.DateTo = strings.TrimSpace(filters.DateTo)
	filters.Username = strings.TrimSpace(filters.Username)
	filters.Status = normalizeCreditStatusFilter(filters.Status)
	filters.Customer = strings.TrimSpace(filters.Customer)

	query := `
		SELECT
			cs.id,
			cs.created_at,
			COALESCE(cs.customer_id, 0),
			COALESCE(c.name, cs.debtor_name, ''),
			COALESCE(c.document_type, cs.debtor_document_type, ''),
			COALESCE(c.document_number, cs.debtor_document_number, ''),
			COALESCE(c.phone, cs.debtor_phone, ''),
			COALESCE(c.city, ''),
			COALESCE(u.username, ''),
			COALESCE(cs.installments_total, 0),
			COALESCE(cs.installments_paid, 0),
			COALESCE(cs.total_value, 0),
			COALESCE(cs.interest_percent, 0),
			COALESCE(cs.installment_value, 0),
			COALESCE((
				SELECT SUM(ci.amount_paid)
				FROM credit_installments ci
				WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id
			), 0),
			COALESCE((
				SELECT COUNT(*)
				FROM credit_installments ci
				WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id AND COALESCE(ci.payment_type, 'cuota') = 'cuota'
			), COALESCE(cs.installments_paid, 0)),
			COALESCE(cs.notes, ''),
			COALESCE((
				SELECT ci.amount_paid
				FROM credit_installments ci
				WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id
				ORDER BY ci.created_at DESC, ci.id DESC
				LIMIT 1
			), 0),
			COALESCE((
				SELECT ci.created_at
				FROM credit_installments ci
				WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id
				ORDER BY ci.created_at DESC, ci.id DESC
				LIMIT 1
			), ''),
			COALESCE((
				SELECT COALESCE(ci.payment_type, 'cuota')
				FROM credit_installments ci
				WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id
				ORDER BY ci.created_at DESC, ci.id DESC
				LIMIT 1
			), ''),
			COALESCE(cs.status, '')
		FROM credit_sales cs
		LEFT JOIN customers c ON c.id = cs.customer_id AND c.tenant_id = cs.tenant_id
		LEFT JOIN users u ON u.id = cs.created_by AND u.tenant_id = cs.tenant_id
		WHERE cs.tenant_id = ? AND COALESCE(cs.kind, ?) = ?
	`
	args := []any{tenantID, string(creditSaleKindCash), string(creditSaleKindCash)}
	if filters.DateFrom != "" {
		query += ` AND ` + sqlDatePrefixExpr("cs.created_at") + ` >= ?`
		args = append(args, filters.DateFrom)
	}
	if filters.DateTo != "" {
		query += ` AND ` + sqlDatePrefixExpr("cs.created_at") + ` <= ?`
		args = append(args, filters.DateTo)
	}
	if filters.Username != "" {
		query += ` AND LOWER(COALESCE(u.username, '')) LIKE ?`
		args = append(args, "%"+strings.ToLower(filters.Username)+"%")
	}
	if filters.Customer != "" {
		query += ` AND (
			LOWER(COALESCE(c.name, cs.debtor_name, '')) LIKE ?
			OR LOWER(COALESCE(c.document_number, cs.debtor_document_number, '')) LIKE ?
			OR LOWER(COALESCE(c.phone, cs.debtor_phone, '')) LIKE ?
			OR LOWER(COALESCE(c.city, '')) LIKE ?
		)`
		search := "%" + strings.ToLower(filters.Customer) + "%"
		args = append(args, search, search, search, search)
	}
	if filters.CreditSaleID > 0 {
		query += ` AND cs.id = ?`
		args = append(args, filters.CreditSaleID)
	}
	query += ` ORDER BY cs.created_at DESC, cs.id DESC LIMIT ?`
	args = append(args, filters.Limit)

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]cashLoanReportItem, 0, filters.Limit)
	for rows.Next() {
		var (
			item                cashLoanReportItem
			statusRaw           string
			paidInstallmentsCnt int
		)
		if err := rows.Scan(
			&item.CreditSaleID,
			&item.CreatedAt,
			&item.CustomerID,
			&item.CustomerName,
			&item.CustomerDocumentType,
			&item.CustomerDocument,
			&item.CustomerPhone,
			&item.CustomerCity,
			&item.ManagedByName,
			&item.InstallmentsTotal,
			&item.InstallmentsPaid,
			&item.TotalValue,
			&item.InterestPercent,
			&item.InstallmentValue,
			&item.TotalPaid,
			&paidInstallmentsCnt,
			&item.Notes,
			&item.LastPaymentAmount,
			&item.LastPaymentAt,
			&item.LastPaymentType,
			&statusRaw,
		); err != nil {
			return nil, err
		}
		legacyTotalPaid := math.Round((float64(item.InstallmentsPaid)*item.InstallmentValue)*100) / 100
		if item.TotalPaid < legacyTotalPaid {
			item.TotalPaid = legacyTotalPaid
		}
		item.InstallmentsPending = max(item.InstallmentsTotal-paidInstallmentsCnt, 0)
		item.DebtTotal = creditDebtTotal(item.InstallmentsTotal, item.InstallmentValue)
		item.CurrentDebt = creditCurrentDebt(item.DebtTotal, item.TotalPaid)
		status := effectiveCreditStatus(statusRaw, item.CurrentDebt, item.DebtTotal)
		if filters.Status != "" && string(status) != filters.Status {
			continue
		}
		item.Status = string(status)
		item.StatusLabel = cashLoanStatusLabel(status)
		item.KindLabel = creditKindLabel(creditSaleKindCash)
		item.CreatedAt = formatDateWithSettings(item.CreatedAt)
		item.LastPaymentAt = formatDateWithSettings(item.LastPaymentAt)
		item.DetailURL = ""
		if item.CustomerID > 0 {
			item.DetailURL = fmt.Sprintf("/clientes/%d", item.CustomerID)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func loadProductLoanUnitIDs(db *sql.DB, tenantID int, loanIDs []int) (map[int][]string, error) {
	result := make(map[int][]string, len(loanIDs))
	if len(loanIDs) == 0 {
		return result, nil
	}
	placeholders := make([]string, 0, len(loanIDs))
	args := make([]any, 0, len(loanIDs)+1)
	args = append(args, normalizeTenantID(tenantID))
	for _, loanID := range loanIDs {
		placeholders = append(placeholders, "?")
		args = append(args, loanID)
	}
	rows, err := db.Query(`
		SELECT product_loan_id, unit_id
		FROM product_loan_units
		WHERE tenant_id = ? AND product_loan_id IN (`+strings.Join(placeholders, ",")+`)
		ORDER BY product_loan_id ASC, id ASC
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var loanID int
		var unitID string
		if err := rows.Scan(&loanID, &unitID); err != nil {
			return nil, err
		}
		result[loanID] = append(result[loanID], unitID)
	}
	return result, rows.Err()
}

func productLoanDetailForUser(db *sql.DB, currentUser *User, tenantID, productLoanID int) (productLoanReportItem, []productLoanTimelineItem, error) {
	if currentUser == nil || !isAdminRole(currentUser.Role) {
		return productLoanReportItem{}, nil, requestError{Status: http.StatusForbidden, Message: "Solo administrador puede consultar préstamos físicos."}
	}
	tenantID = normalizeTenantID(tenantID)
	var item productLoanReportItem
	err := db.QueryRow(`
		SELECT
			pl.id,
			COALESCE(pl.product_id, ''),
			COALESCE(NULLIF(p.nombre, ''), pl.product_id),
			COALESCE(pl.quantity, 0),
			COALESCE(pl.customer_id, 0),
			COALESCE(c.name, pl.borrower_name, ''),
			COALESCE(c.document_type, pl.borrower_document_type, ''),
			COALESCE(c.document_number, pl.borrower_document_number, ''),
			COALESCE(c.phone, pl.borrower_phone, ''),
			COALESCE(c.city, pl.borrower_city, ''),
			COALESCE(u.username, ''),
			COALESCE(pl.loaned_at, ''),
			COALESCE(pl.due_at, ''),
			COALESCE(pl.closed_at, ''),
			COALESCE(pl.status, 'active'),
			COALESCE(pl.notes, ''),
			COALESCE(pl.close_notes, '')
		FROM product_loans pl
		LEFT JOIN productos p ON p.sku = pl.product_id AND p.tenant_id = pl.tenant_id
		LEFT JOIN customers c ON c.id = pl.customer_id AND c.tenant_id = pl.tenant_id
		LEFT JOIN users u ON u.id = pl.created_by AND u.tenant_id = pl.tenant_id
		WHERE pl.tenant_id = ? AND pl.id = ?
		LIMIT 1
	`, tenantID, productLoanID).Scan(
		&item.ProductLoanID,
		&item.ProductID,
		&item.ProductName,
		&item.Quantity,
		&item.CustomerID,
		&item.CustomerName,
		&item.CustomerDocumentType,
		&item.CustomerDocument,
		&item.CustomerPhone,
		&item.CustomerCity,
		&item.ManagedByName,
		&item.LoanedAt,
		&item.DueAt,
		&item.ClosedAt,
		&item.Status,
		&item.Notes,
		&item.CloseNotes,
	)
	if err != nil {
		return productLoanReportItem{}, nil, err
	}
	status := normalizeProductLoanStatus(item.Status)
	item.Status = string(status)
	item.StatusLabel = productLoanStatusLabel(status)
	item.IsOverdue = isProductLoanOverdue(status, item.DueAt)
	if item.IsOverdue {
		item.OverdueLabel = "Vencido"
	} else {
		item.OverdueLabel = "En fecha"
	}
	item.LoanedAt = formatDateWithSettings(item.LoanedAt)
	item.DueAt = formatDateWithSettings(item.DueAt)
	item.ClosedAt = formatDateWithSettings(item.ClosedAt)

	unitMap, err := loadProductLoanUnitIDs(db, tenantID, []int{productLoanID})
	if err != nil {
		return productLoanReportItem{}, nil, err
	}
	item.UnitIDs = unitMap[productLoanID]
	item.UnitIDsText = strings.Join(item.UnitIDs, ", ")
	if item.UnitIDsText == "" {
		item.UnitIDsText = "-"
	}

	rows, err := db.Query(`
		SELECT
			a.created_at,
			a.event_type,
			COALESCE(u.username, ''),
			a.source,
			COALESCE(a.payload_json, '{}')
		FROM audit_events a
		LEFT JOIN users u ON u.id = a.user_id AND u.tenant_id = a.tenant_id
		WHERE a.tenant_id = ? AND a.entity_type = 'product_loan' AND a.entity_id = ?
		ORDER BY a.created_at DESC, a.id DESC
		LIMIT 40
	`, tenantID, strconv.Itoa(productLoanID))
	if err != nil {
		return productLoanReportItem{}, nil, err
	}
	defer rows.Close()

	timeline := make([]productLoanTimelineItem, 0, 16)
	for rows.Next() {
		var (
			itemTimeline productLoanTimelineItem
			payloadRaw   string
		)
		if err := rows.Scan(&itemTimeline.CreatedAt, &itemTimeline.EventType, &itemTimeline.Username, &itemTimeline.Source, &payloadRaw); err != nil {
			return productLoanReportItem{}, nil, err
		}
		itemTimeline.CreatedAt = formatDateWithSettings(itemTimeline.CreatedAt)
		itemTimeline.Label = productLoanEventLabel(itemTimeline.EventType)
		if strings.TrimSpace(payloadRaw) != "" {
			payload := map[string]any{}
			if err := json.Unmarshal([]byte(payloadRaw), &payload); err == nil {
				if notes, ok := payload["notes"].(string); ok {
					itemTimeline.Notes = strings.TrimSpace(notes)
				}
			}
		}
		timeline = append(timeline, itemTimeline)
	}
	if err := rows.Err(); err != nil {
		return productLoanReportItem{}, nil, err
	}

	return item, timeline, nil
}

func listEditedCreditsReport(db *sql.DB, currentUser *User, tenantID int, filters creditEditReportFilters) ([]creditEditReportItem, error) {
	if currentUser == nil || !isAdminRole(currentUser.Role) {
		return nil, requestError{Status: http.StatusForbidden, Message: "Solo administrador puede consultar créditos editados."}
	}
	tenantID = normalizeTenantID(tenantID)
	if filters.Limit <= 0 {
		filters.Limit = 100
	}
	if filters.Limit > 500 {
		filters.Limit = 500
	}
	filters.DateFrom = strings.TrimSpace(filters.DateFrom)
	filters.DateTo = strings.TrimSpace(filters.DateTo)
	filters.Username = strings.TrimSpace(filters.Username)
	filters.Status = normalizeCreditStatusFilter(filters.Status)
	filters.Kind = normalizeCreditKindFilter(filters.Kind)
	filters.Customer = strings.TrimSpace(filters.Customer)

	query := `
		SELECT
			a.id,
			a.created_at,
			a.source,
			COALESCE(a.payload_json, '{}'),
			COALESCE(u.username, ''),
			a.tenant_id,
			COALESCE(t.slug, ''),
			COALESCE(t.name, ''),
			COALESCE(cs.id, 0),
			COALESCE(cs.kind, ?),
			COALESCE(cs.product_id, ''),
			CASE
				WHEN COALESCE(cs.kind, ?) = ? THEN 'Préstamo de dinero'
				ELSE COALESCE(NULLIF(p.nombre, ''), cs.product_id)
			END,
			COALESCE(c.id, 0),
			COALESCE(c.name, cs.debtor_name, ''),
			COALESCE(c.document_number, cs.debtor_document_number, ''),
			COALESCE(c.phone, cs.debtor_phone, ''),
			COALESCE(cs.status, '')
		FROM audit_events a
		LEFT JOIN users u ON u.id = a.user_id AND u.tenant_id = a.tenant_id
		LEFT JOIN tenants t ON t.id = a.tenant_id
		LEFT JOIN credit_sales cs ON cs.id = CAST(a.entity_id AS INTEGER) AND cs.tenant_id = a.tenant_id
		LEFT JOIN productos p ON p.sku = cs.product_id AND p.tenant_id = cs.tenant_id
		LEFT JOIN customers c ON c.id = cs.customer_id AND c.tenant_id = cs.tenant_id
		WHERE a.tenant_id = ? AND a.entity_type = 'credit_sale' AND a.event_type = 'credit_sale_updated'
	`
	args := []any{string(creditSaleKindProduct), string(creditSaleKindProduct), string(creditSaleKindCash), tenantID}
	if filters.DateFrom != "" {
		query += ` AND ` + sqlDatePrefixExpr("a.created_at") + ` >= ?`
		args = append(args, filters.DateFrom)
	}
	if filters.DateTo != "" {
		query += ` AND ` + sqlDatePrefixExpr("a.created_at") + ` <= ?`
		args = append(args, filters.DateTo)
	}
	if filters.Username != "" {
		query += ` AND LOWER(COALESCE(u.username, '')) LIKE ?`
		args = append(args, "%"+strings.ToLower(filters.Username)+"%")
	}
	if filters.Kind != "" {
		query += ` AND COALESCE(cs.kind, ?) = ?`
		args = append(args, string(creditSaleKindProduct), filters.Kind)
	}
	if filters.Customer != "" {
		query += ` AND (
			LOWER(COALESCE(c.name, cs.debtor_name, '')) LIKE ?
			OR LOWER(COALESCE(c.document_number, cs.debtor_document_number, '')) LIKE ?
			OR LOWER(COALESCE(c.phone, cs.debtor_phone, '')) LIKE ?
		)`
		search := "%" + strings.ToLower(filters.Customer) + "%"
		args = append(args, search, search, search)
	}
	if filters.CreditSaleID > 0 {
		query += ` AND CAST(a.entity_id AS INTEGER) = ?`
		args = append(args, filters.CreditSaleID)
	}
	query += ` ORDER BY a.created_at DESC, a.id DESC LIMIT ?`
	args = append(args, filters.Limit)

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]creditEditReportItem, 0, filters.Limit)
	for rows.Next() {
		var (
			item       creditEditReportItem
			payloadRaw string
			kindRaw    string
			statusRaw  string
		)
		if err := rows.Scan(
			&item.AuditID,
			&item.CreatedAt,
			&item.Source,
			&payloadRaw,
			&item.Username,
			&item.TenantID,
			&item.TenantSlug,
			&item.TenantName,
			&item.CreditSaleID,
			&kindRaw,
			&item.ProductID,
			&item.ProductName,
			&item.CustomerID,
			&item.CustomerName,
			&item.CustomerDocument,
			&item.CustomerPhone,
			&statusRaw,
		); err != nil {
			return nil, err
		}
		item.CreatedAt = formatDateWithSettings(item.CreatedAt)
		kind := normalizeCreditSaleKind(kindRaw)
		item.Kind = string(kind)
		item.KindLabel = creditKindLabel(kind)

		payload := map[string]any{}
		if strings.TrimSpace(payloadRaw) != "" {
			_ = json.Unmarshal([]byte(payloadRaw), &payload)
		}

		if rawChanges, ok := payload["changes"].([]any); ok {
			item.Changes = make([]creditEditReportChange, 0, len(rawChanges))
			item.ChangedFields = make([]string, 0, len(rawChanges))
			for _, raw := range rawChanges {
				changeMap, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				field := fmt.Sprintf("%v", changeMap["field"])
				label := fmt.Sprintf("%v", changeMap["label"])
				change := creditEditReportChange{
					Field:      field,
					Label:      label,
					Before:     changeMap["before"],
					After:      changeMap["after"],
					BeforeText: formatCreditAuditValue(field, changeMap["before"]),
					AfterText:  formatCreditAuditValue(field, changeMap["after"]),
				}
				item.Changes = append(item.Changes, change)
				if field != "" {
					item.ChangedFields = append(item.ChangedFields, field)
				}
			}
		}
		item.ChangeCount = len(item.Changes)
		item.ChangedFieldsText = strings.Join(item.ChangedFields, ", ")
		if rawChangeCount, ok := payload["change_count"].(float64); ok && item.ChangeCount == 0 {
			item.ChangeCount = int(rawChangeCount)
		}

		if rawImpact, ok := payload["impact"].(map[string]any); ok {
			if v, ok := rawImpact["debt_total_before"].(float64); ok {
				item.DebtTotalBefore = roundedMoney(v)
			}
			if v, ok := rawImpact["debt_total_after"].(float64); ok {
				item.DebtTotalAfter = roundedMoney(v)
			}
			if v, ok := rawImpact["total_paid_before"].(float64); ok {
				item.TotalPaidBefore = roundedMoney(v)
			}
			if v, ok := rawImpact["total_paid_after"].(float64); ok {
				item.TotalPaidAfter = roundedMoney(v)
			}
			if v, ok := rawImpact["current_debt_before"].(float64); ok {
				item.CurrentDebtBefore = roundedMoney(v)
			}
			if v, ok := rawImpact["current_debt_after"].(float64); ok {
				item.CurrentDebtAfter = roundedMoney(v)
			}
			item.CurrentDebtDelta = roundedMoney(item.CurrentDebtAfter - item.CurrentDebtBefore)
			if v, ok := rawImpact["installments_due_after"].(float64); ok {
				item.InstallmentsDueNow = int(v)
			}
			if v := strings.TrimSpace(fmt.Sprintf("%v", rawImpact["status_before"])); v != "" && v != "<nil>" {
				item.StatusBefore = v
			}
			if v := strings.TrimSpace(fmt.Sprintf("%v", rawImpact["status_after"])); v != "" && v != "<nil>" {
				item.StatusAfter = v
			}
			if v := strings.TrimSpace(fmt.Sprintf("%v", rawImpact["status_label_before"])); v != "" && v != "<nil>" {
				item.StatusLabelBefore = v
			}
			if v := strings.TrimSpace(fmt.Sprintf("%v", rawImpact["status_label_after"])); v != "" && v != "<nil>" {
				item.StatusLabelAfter = v
			}
		}

		status := effectiveCreditStatus(statusRaw, item.CurrentDebtAfter, item.DebtTotalAfter)
		item.Status = string(status)
		item.StatusLabel = creditStatusLabel(status)
		if item.StatusAfter == "" {
			item.StatusAfter = item.Status
		}
		if item.StatusLabelAfter == "" {
			item.StatusLabelAfter = item.StatusLabel
		}
		if item.StatusBefore == "" {
			item.StatusBefore = item.StatusAfter
		}
		if item.StatusLabelBefore == "" {
			item.StatusLabelBefore = creditStatusLabel(normalizeEditableCreditStatus(item.StatusBefore))
		}
		if filters.Status != "" && item.Status != filters.Status && item.StatusAfter != filters.Status {
			continue
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func creditEditReportItemAPI(item creditEditReportItem) map[string]any {
	changes := make([]map[string]any, 0, len(item.Changes))
	for _, change := range item.Changes {
		changes = append(changes, map[string]any{
			"field":       change.Field,
			"label":       change.Label,
			"before":      change.Before,
			"after":       change.After,
			"before_text": change.BeforeText,
			"after_text":  change.AfterText,
		})
	}
	return map[string]any{
		"audit_id":             item.AuditID,
		"credit_sale_id":       item.CreditSaleID,
		"created_at":           item.CreatedAt,
		"source":               item.Source,
		"username":             item.Username,
		"tenant_id":            item.TenantID,
		"tenant_slug":          item.TenantSlug,
		"tenant_name":          item.TenantName,
		"kind":                 item.Kind,
		"kind_label":           item.KindLabel,
		"product_id":           item.ProductID,
		"product_name":         item.ProductName,
		"customer_id":          item.CustomerID,
		"customer_name":        item.CustomerName,
		"customer_document":    item.CustomerDocument,
		"customer_phone":       item.CustomerPhone,
		"status":               item.Status,
		"status_label":         item.StatusLabel,
		"status_before":        item.StatusBefore,
		"status_after":         item.StatusAfter,
		"status_label_before":  item.StatusLabelBefore,
		"status_label_after":   item.StatusLabelAfter,
		"changed_fields":       item.ChangedFields,
		"changed_fields_text":  item.ChangedFieldsText,
		"change_count":         item.ChangeCount,
		"changes":              changes,
		"debt_total_before":    item.DebtTotalBefore,
		"debt_total_after":     item.DebtTotalAfter,
		"total_paid_before":    item.TotalPaidBefore,
		"total_paid_after":     item.TotalPaidAfter,
		"current_debt_before":  item.CurrentDebtBefore,
		"current_debt_after":   item.CurrentDebtAfter,
		"current_debt_delta":   item.CurrentDebtDelta,
		"installments_due_now": item.InstallmentsDueNow,
	}
}

func generateNextProductSKU(db *sql.DB) (string, error) {
	rows, err := db.Query(`SELECT sku FROM productos WHERE sku LIKE 'P-%'`)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	maxNum := 0
	for rows.Next() {
		var sku string
		if err := rows.Scan(&sku); err != nil {
			return "", err
		}
		if !strings.HasPrefix(sku, "P-") {
			continue
		}
		n, err := strconv.Atoi(strings.TrimPrefix(sku, "P-"))
		if err != nil {
			continue
		}
		if n > maxNum {
			maxNum = n
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	for next := maxNum + 1; ; next++ {
		candidate := fmt.Sprintf("P-%03d", next)
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM productos WHERE sku = ?`, candidate).Scan(&count); err != nil {
			return "", err
		}
		if count == 0 {
			return candidate, nil
		}
	}
}

func buildLineSuggestions(products []productOption, current string) []string {
	seen := make(map[string]struct{})
	lines := make([]string, 0)
	add := func(raw string) {
		line := strings.TrimSpace(raw)
		if line == "" {
			return
		}
		key := strings.ToLower(line)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		lines = append(lines, line)
	}
	for _, p := range products {
		add(p.Line)
	}
	add(current)
	sort.Slice(lines, func(i, j int) bool {
		return strings.ToLower(lines[i]) < strings.ToLower(lines[j])
	})
	return lines
}

type cambioFormData struct {
	Title               string
	Subtitle            string
	ProductoID          string
	Productos           []productOption
	Unidades            []unitOption
	PersonaCambio       string
	Notas               string
	Salientes           []string
	SalientesMap        map[string]bool
	IncomingMode        string
	IncomingExistingID  string
	IncomingExistingQty int
	IncomingNewSKU      string
	IncomingNewName     string
	IncomingNewLine     string
	IncomingNewQty      int
	Errors              map[string]string
	CurrentUser         *User
}

type cambioConfirmData struct {
	Title               string
	Subtitle            string
	ProductoID          string
	ProductoNombre      string
	PersonaCambio       string
	Notas               string
	Salientes           []string
	Entrantes           []string
	IncomingMode        string
	IncomingExistingID  string
	IncomingExistingQty int
	IncomingNewSKU      string
	IncomingNewName     string
	IncomingNewLine     string
	IncomingNewQty      int
	CurrentUser         *User
}

type estadoCount struct {
	Estado   string
	Cantidad int
	Link     string
}

type periodTotal struct {
	Label   string
	Total   string
	Range   string
	Value   float64
	Percent float64
}

type metodoPagoTotal struct {
	Metodo   string  `json:"metodo"`
	Cantidad int     `json:"cantidad"`
	Total    string  `json:"total"`
	Value    float64 `json:"value"`
}

type timelinePoint struct {
	Fecha    string  `json:"fecha"`
	Cantidad int     `json:"cantidad"`
	Total    string  `json:"total"`
	Value    float64 `json:"value"`
	Index    int     `json:"index"`
	Percent  float64 `json:"percent"`
}

type dashboardSaleDetail struct {
	ID         int    `json:"id"`
	Fecha      string `json:"fecha"`
	Producto   string `json:"producto"`
	Cantidad   int    `json:"cantidad"`
	Total      string `json:"total"`
	MetodoPago string `json:"metodo_pago"`
}

type dashboardUserTimelineSeries struct {
	UserLabel string          `json:"user_label"`
	Total     string          `json:"total"`
	Value     float64         `json:"value"`
	Color     string          `json:"color"`
	Points    []timelinePoint `json:"points"`
}

type dashboardCategoryTotal struct {
	Key   string  `json:"key"`
	Label string  `json:"label"`
	Count int     `json:"count"`
	Total string  `json:"total"`
	Value float64 `json:"value"`
	Color string  `json:"color"`
}

type pieSlice struct {
	Metodo  string  `json:"metodo"`
	Total   string  `json:"total"`
	Percent float64 `json:"percent"`
	Offset  float64 `json:"offset"`
	Gap     float64 `json:"gap"`
	Color   string  `json:"color"`
}

type dashboardData struct {
	Title           string
	Subtitle        string
	EstadoConteos   []estadoCount
	MetodosPago     []metodoPagoTotal
	PieSlices       []pieSlice
	PieTotal        string
	MaxTimeline     float64
	MaxTimelineText string
	TimelinePoints  string
	Timeline        []timelinePoint
	UserTimeline    []dashboardUserTimelineSeries
	CategoryTotals  []dashboardCategoryTotal
	Sales           []dashboardSaleDetail
	CurrentUser     *User
	CanLoan         bool
	CanCredit       bool
	RangeStart      string
	RangeEnd        string
	RangeTotal      string
	RangeCount      int
}

type dashboardDataResponse struct {
	Ok bool `json:"ok"`

	RangeStart string `json:"range_start"`
	RangeEnd   string `json:"range_end"`
	RangeTotal string `json:"range_total"`
	RangeCount int    `json:"range_count"`

	MetodosPago     []metodoPagoTotal             `json:"metodos_pago"`
	PieSlices       []pieSlice                    `json:"pie_slices"`
	PieTotal        string                        `json:"pie_total"`
	MaxTimeline     float64                       `json:"max_timeline"`
	MaxTimelineText string                        `json:"max_timeline_text"`
	Timeline        []timelinePoint               `json:"timeline"`
	UserTimeline    []dashboardUserTimelineSeries `json:"user_timeline"`
	CategoryTotals  []dashboardCategoryTotal      `json:"category_totals"`
	Sales           []dashboardSaleDetail         `json:"sales"`
}

func buildDashboardSalesData(db *sql.DB, user *User, startStr, endStr string, startDate, endDate time.Time) (dashboardDataResponse, error) {
	resp := dashboardDataResponse{
		Ok:         true,
		RangeStart: startStr,
		RangeEnd:   endStr,
	}
	visibilitySQL, visibilityArgs := productVisibilityPredicate("p", user)
	salesDateExpr := sqlDatePrefixExpr("v.fecha")
	userSeriesColors := []string{"#2c6bed", "#e85d3c", "#22a88b", "#7d4cf6", "#f5a524", "#0ea5c9"}
	categoryColors := map[string]string{
		"venta":   "#2c6bed",
		"credito": "#22a88b",
		"retoma":  "#f5a524",
	}

	var rangeTotal float64
	var rangeCount int
	rangeArgs := append([]any{startStr, endStr}, visibilityArgs...)
	if err := db.QueryRow(`
		SELECT
			COALESCE(SUM(precio_final * cantidad), 0),
			COALESCE(COUNT(*), 0)
		FROM ventas v
		LEFT JOIN productos p ON p.sku = v.producto_id
		WHERE `+salesDateExpr+` BETWEEN ? AND ? AND `+visibilitySQL, rangeArgs...).Scan(&rangeTotal, &rangeCount); err != nil {
		return dashboardDataResponse{}, err
	}
	resp.RangeTotal = formatCurrency(rangeTotal)
	resp.RangeCount = rangeCount

	metodoArgs := append([]any{startStr, endStr}, visibilityArgs...)
	metodoRows, err := db.Query(`
		SELECT metodo_pago, COUNT(*), SUM(precio_final * cantidad)
		FROM ventas v
		LEFT JOIN productos p ON p.sku = v.producto_id
		WHERE `+salesDateExpr+` BETWEEN ? AND ? AND `+visibilitySQL+`
		GROUP BY metodo_pago
		ORDER BY SUM(precio_final * cantidad) DESC`, metodoArgs...)
	if err != nil {
		return dashboardDataResponse{}, err
	}
	defer metodoRows.Close()

	metodosPago := []metodoPagoTotal{}
	totalPago := 0.0
	for metodoRows.Next() {
		var metodo string
		var cantidad int
		var total float64
		if err := metodoRows.Scan(&metodo, &cantidad, &total); err != nil {
			return dashboardDataResponse{}, err
		}
		metodosPago = append(metodosPago, metodoPagoTotal{
			Metodo:   metodo,
			Cantidad: cantidad,
			Total:    formatCurrency(total),
			Value:    total,
		})
		totalPago += total
	}
	if err := metodoRows.Err(); err != nil {
		return dashboardDataResponse{}, err
	}
	resp.MetodosPago = metodosPago
	resp.PieTotal = formatCurrency(totalPago)

	pieColors := []string{"#2c6bed", "#7d4cf6", "#22a88b", "#f5a524", "#e5484d", "#14b8a6"}
	pieSlices := []pieSlice{}
	offset := 25.0
	for i, metodo := range metodosPago {
		percent := 0.0
		if totalPago > 0 {
			percent = (metodo.Value / totalPago) * 100
		}
		gap := 100 - percent
		color := pieColors[i%len(pieColors)]
		pieSlices = append(pieSlices, pieSlice{
			Metodo:  metodo.Metodo,
			Total:   metodo.Total,
			Percent: percent,
			Offset:  offset,
			Gap:     gap,
			Color:   color,
		})
		offset -= percent
	}
	resp.PieSlices = pieSlices

	timeArgs := append([]any{startStr, endStr}, visibilityArgs...)
	timeRows, err := db.Query(`
		SELECT `+salesDateExpr+` as fecha, COUNT(*), SUM(precio_final * cantidad)
		FROM ventas v
		LEFT JOIN productos p ON p.sku = v.producto_id
		WHERE `+salesDateExpr+` BETWEEN ? AND ? AND `+visibilitySQL+`
		GROUP BY `+salesDateExpr+`
		ORDER BY `+salesDateExpr, timeArgs...)
	if err != nil {
		return dashboardDataResponse{}, err
	}
	defer timeRows.Close()

	timelineByDate := make(map[string]timelinePoint)
	for timeRows.Next() {
		var fecha string
		var cantidad int
		var total float64
		if err := timeRows.Scan(&fecha, &cantidad, &total); err != nil {
			return dashboardDataResponse{}, err
		}
		timelineByDate[fecha] = timelinePoint{
			Fecha:    fecha,
			Cantidad: cantidad,
			Total:    formatCurrency(total),
			Value:    total,
		}
	}
	if err := timeRows.Err(); err != nil {
		return dashboardDataResponse{}, err
	}

	timeline := []timelinePoint{}
	maxTimeline := 0.0
	index := 0
	for cursor := startDate; !cursor.After(endDate); cursor = cursor.AddDate(0, 0, 1) {
		fecha := cursor.Format("2006-01-02")
		point, ok := timelineByDate[fecha]
		if !ok {
			point = timelinePoint{
				Fecha:    fecha,
				Cantidad: 0,
				Total:    formatCurrency(0),
				Value:    0,
			}
		}
		point.Index = index
		timeline = append(timeline, point)
		if point.Value > maxTimeline {
			maxTimeline = point.Value
		}
		index++
	}

	if maxTimeline > 0 {
		for i := range timeline {
			timeline[i].Percent = (timeline[i].Value / maxTimeline) * 100
		}
	}

	resp.MaxTimeline = maxTimeline
	resp.MaxTimelineText = formatCurrency(maxTimeline)
	resp.Timeline = timeline

	type userTimelineBucket struct {
		label       string
		valueByDate map[string]float64
		total       float64
	}

	userTimelineArgs := append([]any{startStr, endStr}, visibilityArgs...)
	userTimelineRows, err := db.Query(`
		SELECT
			`+sqlDatePrefixExpr("a.created_at")+` as fecha,
			COALESCE(NULLIF(TRIM(u.username), ''), 'Sin usuario') as user_label,
			COALESCE(a.payload_json, '{}') as payload_json
		FROM audit_events a
		LEFT JOIN users u ON u.id = a.user_id
		LEFT JOIN productos p ON p.sku = a.entity_id
		WHERE a.event_type = 'sale_registered'
			AND `+sqlDatePrefixExpr("a.created_at")+` BETWEEN ? AND ?
			AND `+visibilitySQL+`
		ORDER BY user_label ASC, fecha ASC
	`, userTimelineArgs...)
	if err != nil {
		return dashboardDataResponse{}, err
	}
	defer userTimelineRows.Close()

	userBuckets := map[string]*userTimelineBucket{}
	for userTimelineRows.Next() {
		var (
			fecha      string
			userLabel  string
			payloadRaw string
		)
		if err := userTimelineRows.Scan(&fecha, &userLabel, &payloadRaw); err != nil {
			return dashboardDataResponse{}, err
		}
		total := 0.0
		if payloadRaw != "" {
			var payload map[string]any
			if err := json.Unmarshal([]byte(payloadRaw), &payload); err == nil {
				switch v := payload["total"].(type) {
				case float64:
					total = v
				case string:
					if parsed, parseErr := strconv.ParseFloat(strings.TrimSpace(v), 64); parseErr == nil {
						total = parsed
					}
				}
			}
		}
		if total <= 0 {
			continue
		}
		bucket, ok := userBuckets[userLabel]
		if !ok {
			bucket = &userTimelineBucket{
				label:       userLabel,
				valueByDate: map[string]float64{},
			}
			userBuckets[userLabel] = bucket
		}
		bucket.valueByDate[fecha] += total
		bucket.total += total
	}
	if err := userTimelineRows.Err(); err != nil {
		return dashboardDataResponse{}, err
	}

	userLabels := make([]string, 0, len(userBuckets))
	for label := range userBuckets {
		userLabels = append(userLabels, label)
	}
	sort.Strings(userLabels)
	userTimeline := make([]dashboardUserTimelineSeries, 0, len(userLabels))
	for idx, label := range userLabels {
		bucket := userBuckets[label]
		points := make([]timelinePoint, 0, len(timeline))
		for pointIdx, cursor := 0, startDate; !cursor.After(endDate); cursor, pointIdx = cursor.AddDate(0, 0, 1), pointIdx+1 {
			fecha := cursor.Format("2006-01-02")
			value := bucket.valueByDate[fecha]
			points = append(points, timelinePoint{
				Fecha:   fecha,
				Total:   formatCurrency(value),
				Value:   value,
				Index:   pointIdx,
				Percent: 0,
			})
		}
		maxUserValue := 0.0
		for _, point := range points {
			if point.Value > maxUserValue {
				maxUserValue = point.Value
			}
		}
		if maxUserValue > 0 {
			for i := range points {
				points[i].Percent = (points[i].Value / maxUserValue) * 100
			}
		}
		userTimeline = append(userTimeline, dashboardUserTimelineSeries{
			UserLabel: bucket.label,
			Total:     formatCurrency(bucket.total),
			Value:     bucket.total,
			Color:     userSeriesColors[idx%len(userSeriesColors)],
			Points:    points,
		})
	}
	resp.UserTimeline = userTimeline

	_, movementEnabledMap, err := loadMovementSettings(db)
	if err != nil {
		return dashboardDataResponse{}, err
	}
	categoryTotals := make([]dashboardCategoryTotal, 0, 3)
	categoryTotals = append(categoryTotals, dashboardCategoryTotal{
		Key:   "venta",
		Label: "Ventas",
		Count: rangeCount,
		Total: formatCurrency(rangeTotal),
		Value: rangeTotal,
		Color: categoryColors["venta"],
	})
	if movementEnabled(movementEnabledMap, "credito") {
		var creditTotal float64
		var creditCount int
		creditArgs := append([]any{startStr, endStr}, visibilityArgs...)
		if err := db.QueryRow(`
			SELECT COALESCE(SUM(cs.total_value), 0), COALESCE(COUNT(*), 0)
			FROM credit_sales cs
			LEFT JOIN productos p ON p.sku = cs.product_id
			WHERE `+sqlDatePrefixExpr("cs.created_at")+` BETWEEN ? AND ? AND `+visibilitySQL, creditArgs...).Scan(&creditTotal, &creditCount); err != nil {
			return dashboardDataResponse{}, err
		}
		categoryTotals = append(categoryTotals, dashboardCategoryTotal{
			Key:   "credito",
			Label: "Créditos",
			Count: creditCount,
			Total: formatCurrency(creditTotal),
			Value: creditTotal,
			Color: categoryColors["credito"],
		})
	}
	if movementEnabled(movementEnabledMap, "retoma") {
		var retomaTotal float64
		var retomaCount int
		retomaArgs := append([]any{startStr, endStr}, visibilityArgs...)
		if err := db.QueryRow(`
			SELECT COALESCE(SUM(r.valor_recibido), 0), COALESCE(COUNT(*), 0)
			FROM retomas r
			LEFT JOIN productos p ON p.sku = r.producto_id
			WHERE `+sqlDatePrefixExpr("r.fecha")+` BETWEEN ? AND ? AND `+visibilitySQL, retomaArgs...).Scan(&retomaTotal, &retomaCount); err != nil {
			return dashboardDataResponse{}, err
		}
		categoryTotals = append(categoryTotals, dashboardCategoryTotal{
			Key:   "retoma",
			Label: "Retomas",
			Count: retomaCount,
			Total: formatCurrency(retomaTotal),
			Value: retomaTotal,
			Color: categoryColors["retoma"],
		})
	}
	resp.CategoryTotals = categoryTotals

	saleArgs := append([]any{startStr, endStr}, visibilityArgs...)
	saleRows, err := db.Query(`
		SELECT
			v.id,
			v.fecha,
			COALESCE(p.nombre, v.producto_id),
			v.cantidad,
			v.precio_final,
			v.metodo_pago
		FROM ventas v
		LEFT JOIN productos p ON p.sku = v.producto_id
		WHERE `+salesDateExpr+` BETWEEN ? AND ? AND `+visibilitySQL+`
		ORDER BY v.fecha DESC, v.id DESC
	`, saleArgs...)
	if err != nil {
		return dashboardDataResponse{}, err
	}
	defer saleRows.Close()

	sales := make([]dashboardSaleDetail, 0, 64)
	for saleRows.Next() {
		var (
			id         int
			fechaRaw   string
			producto   string
			cantidad   int
			precioUnit float64
			metodoPago string
		)
		if err := saleRows.Scan(&id, &fechaRaw, &producto, &cantidad, &precioUnit, &metodoPago); err != nil {
			return dashboardDataResponse{}, err
		}
		fecha := fechaRaw
		if len(fechaRaw) >= 10 {
			fecha = fechaRaw[:10]
		}
		sales = append(sales, dashboardSaleDetail{
			ID:         id,
			Fecha:      formatDateWithSettings(fecha),
			Producto:   producto,
			Cantidad:   cantidad,
			Total:      formatCurrency(precioUnit * float64(cantidad)),
			MetodoPago: metodoPago,
		})
	}
	if err := saleRows.Err(); err != nil {
		return dashboardDataResponse{}, err
	}
	resp.Sales = sales

	return resp, nil
}

type User struct {
	ID       int
	Username string
	Role     string
	IsActive bool
	TenantID int
}

type managedUserInput struct {
	Username   string
	Name       string
	Email      string
	Password   string
	Role       string
	IsActive   bool
	TelegramID string
}

type managedUserRecord struct {
	ID         int
	Username   string
	Name       string
	Email      string
	Role       string
	IsActive   bool
	TenantID   int
	CreatedAt  string
	TelegramID string
}

type contextKey string

const (
	userContextKey               contextKey = "user"
	apiIntegrationNameContextKey contextKey = "api_integration_name"
	apiAuthModeContextKey        contextKey = "api_auth_mode"
	tenantContextKey             contextKey = "tenant"
)

func findProduct(products []productOption, id string) (productOption, bool) {
	for _, product := range products {
		if product.ID == id {
			return product, true
		}
	}
	return productOption{}, false
}

func buildEntranteIDs(prefix string, qty int) []string {
	ids := make([]string, 0, qty)
	for i := 1; i <= qty; i++ {
		ids = append(ids, prefix+"-"+strconv.Itoa(i))
	}
	return ids
}

func buildSalientesMap(salientes []string) map[string]bool {
	mapped := make(map[string]bool, len(salientes))
	for _, id := range salientes {
		mapped[id] = true
	}
	return mapped
}

func estadoClass(estado string) string {
	switch estado {
	case "Disponible", "available":
		return "available"
	case "Prestada", "Prestado", "loaned":
		return "loaned"
	case "Reservada", "Reservado", "reserved":
		return "reserved"
	case "Danada", "Dañada", "Dañado", "damaged":
		return "damaged"
	case "Vendida", "Vendido", "sold":
		return "sold"
	case "Cambio", "swapped":
		return "swapped"
	default:
		return "available"
	}
}

func ensureMovimientosTable(db *sql.DB) error {
	_, err := db.Exec(normalizeSchemaSQLForEngine(`
		CREATE TABLE IF NOT EXISTS movimientos (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			tenant_id INTEGER NOT NULL DEFAULT 1,
			producto_id TEXT NOT NULL,
			unidad_id TEXT NOT NULL,
			tipo TEXT NOT NULL,
			nota TEXT NOT NULL DEFAULT '',
			usuario TEXT NOT NULL DEFAULT '',
			fecha TEXT NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_movimientos_tenant_producto_fecha ON movimientos (tenant_id, producto_id, fecha);
		CREATE INDEX IF NOT EXISTS idx_movimientos_tenant_unidad_fecha ON movimientos (tenant_id, unidad_id, fecha);
	`, currentDBEngine()))
	return err
}

func logMovimientos(tx *sql.Tx, productoID string, unidadIDs []string, tipo, nota string, user *User, now string) error {
	username := ""
	tenantID := defaultTenantID
	if user != nil {
		username = user.Username
		tenantID = normalizeTenantID(user.TenantID)
	}
	stmt, err := tx.Prepare(`INSERT INTO movimientos (tenant_id, producto_id, unidad_id, tipo, nota, usuario, fecha) VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, unidadID := range unidadIDs {
		if _, err := stmt.Exec(tenantID, productoID, unidadID, tipo, nota, username, now); err != nil {
			return err
		}
	}
	return nil
}

func normalizeAuditSource(source string) string {
	switch strings.TrimSpace(strings.ToLower(source)) {
	case "api", "n8n", "agent", "web":
		return strings.TrimSpace(strings.ToLower(source))
	default:
		return "manual"
	}
}

func nullableUserID(user *User) any {
	if user == nil {
		return nil
	}
	return user.ID
}

type customerInput struct {
	CustomerID     int
	Name           string
	Phone          string
	DocumentType   string
	DocumentNumber string
	Address        string
	City           string
	Notes          string
}

type invoiceCreateInput struct {
	SaleID       int
	CreditSaleID int
	Customer     customerInput
	Notes        string
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func normalizeCreditSaleKind(value string) creditSaleKind {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case string(creditSaleKindCash):
		return creditSaleKindCash
	default:
		return creditSaleKindProduct
	}
}

func creditKindLabel(kind creditSaleKind) string {
	if kind == creditSaleKindCash {
		return "Préstamo"
	}
	return "Crédito"
}

func normalizeEditableCreditStatus(value string) creditStatus {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case string(creditStatusCompleted):
		return creditStatusCompleted
	case string(creditStatusSuspended):
		return creditStatusSuspended
	case string(creditStatusCancelled):
		return creditStatusCancelled
	default:
		return creditStatusActive
	}
}

func effectiveCreditStatus(stored string, currentDebt, debtTotal float64) creditStatus {
	if currentDebt <= 0 && debtTotal > 0 {
		return creditStatusCompleted
	}
	switch normalizeEditableCreditStatus(stored) {
	case creditStatusSuspended:
		return creditStatusSuspended
	case creditStatusCancelled:
		return creditStatusCancelled
	default:
		return creditStatusActive
	}
}

func creditStatusLabel(status creditStatus) string {
	switch status {
	case creditStatusCompleted:
		return "Crédito completado"
	case creditStatusSuspended:
		return "Crédito suspendido"
	case creditStatusCancelled:
		return "Crédito cancelado"
	default:
		return "Crédito activo"
	}
}

func creditStatusClass(status creditStatus) string {
	switch status {
	case creditStatusCompleted:
		return "credit_completed"
	case creditStatusSuspended:
		return "credit_suspended"
	case creditStatusCancelled:
		return "credit_cancelled"
	default:
		return "credit_active"
	}
}

func roundedMoney(v float64) float64 {
	return math.Round(v*100) / 100
}

func creditChangeEntry(field, label string, before, after any) map[string]any {
	return map[string]any{
		"field":  field,
		"label":  label,
		"before": before,
		"after":  after,
	}
}

func normalizeCreditStatusFilter(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	switch value {
	case string(creditStatusActive), string(creditStatusCompleted), string(creditStatusSuspended), string(creditStatusCancelled):
		return value
	default:
		return ""
	}
}

func normalizeCreditKindFilter(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	switch value {
	case string(creditSaleKindProduct), string(creditSaleKindCash):
		return value
	default:
		return ""
	}
}

func formatCreditAuditValue(field string, value any) string {
	if value == nil {
		return "Sin valor"
	}
	switch field {
	case "installment_value":
		switch typed := value.(type) {
		case float64:
			return formatCurrency(typed)
		case float32:
			return formatCurrency(float64(typed))
		case int:
			return formatCurrency(float64(typed))
		case int64:
			return formatCurrency(float64(typed))
		case json.Number:
			if parsed, err := typed.Float64(); err == nil {
				return formatCurrency(parsed)
			}
		case string:
			if parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64); err == nil {
				return formatCurrency(parsed)
			}
		}
	case "status":
		return creditStatusLabel(normalizeEditableCreditStatus(fmt.Sprintf("%v", value)))
	}
	switch typed := value.(type) {
	case string:
		if strings.TrimSpace(typed) == "" {
			return "Sin valor"
		}
		return typed
	case float64:
		if math.Mod(typed, 1) == 0 {
			return strconv.Itoa(int(typed))
		}
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		if typed {
			return "Si"
		}
		return "No"
	default:
		return fmt.Sprintf("%v", value)
	}
}

func normalizeCreditPaymentType(value string) creditPaymentType {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case string(creditPaymentTypeAbono):
		return creditPaymentTypeAbono
	default:
		return creditPaymentTypeCuota
	}
}

func normalizeProductLoanStatus(value string) productLoanStatus {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case string(productLoanStatusReturned):
		return productLoanStatusReturned
	case string(productLoanStatusPaid):
		return productLoanStatusPaid
	case string(productLoanStatusCancelled):
		return productLoanStatusCancelled
	default:
		return productLoanStatusActive
	}
}

func productLoanStatusLabel(status productLoanStatus) string {
	switch normalizeProductLoanStatus(string(status)) {
	case productLoanStatusReturned:
		return "Retornado"
	case productLoanStatusPaid:
		return "Cerrado por pago"
	case productLoanStatusCancelled:
		return "Cancelado"
	default:
		return "Prestado"
	}
}

func productLoanStatusClass(status productLoanStatus) string {
	switch normalizeProductLoanStatus(string(status)) {
	case productLoanStatusReturned:
		return "available"
	case productLoanStatusPaid:
		return "sold"
	case productLoanStatusCancelled:
		return "reserved"
	default:
		return "loaned"
	}
}

func creditDebtTotal(installmentsTotal int, installmentValue float64) float64 {
	if installmentsTotal <= 0 || installmentValue <= 0 {
		return 0
	}
	return math.Round((float64(installmentsTotal)*installmentValue)*100) / 100
}

func creditCurrentDebt(debtTotal, totalPaid float64) float64 {
	current := math.Round((debtTotal-totalPaid)*100) / 100
	if current < 0 {
		return 0
	}
	return current
}

func findCustomerByID(db *sql.DB, tenantID, customerID int) (*Customer, error) {
	if customerID <= 0 {
		return nil, sql.ErrNoRows
	}
	var item Customer
	err := db.QueryRow(`
		SELECT id, tenant_id, name, phone, document_type, document_number, address, city, notes, created_at, updated_at
		FROM customers
		WHERE tenant_id = ? AND id = ?
	`, normalizeTenantID(tenantID), customerID).Scan(
		&item.ID,
		&item.TenantID,
		&item.Name,
		&item.Phone,
		&item.DocumentType,
		&item.DocumentNumber,
		&item.Address,
		&item.City,
		&item.Notes,
		&item.CreatedAt,
		&item.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &item, nil
}

func validateCustomerInput(input customerInput) map[string]string {
	fields := map[string]string{}
	input.Name = strings.TrimSpace(input.Name)
	input.Phone = strings.TrimSpace(input.Phone)
	input.DocumentType = strings.TrimSpace(input.DocumentType)
	input.DocumentNumber = strings.TrimSpace(input.DocumentNumber)
	if input.CustomerID > 0 {
		return fields
	}
	if input.Name == "" {
		fields["customer_name"] = "El nombre del cliente es obligatorio."
	}
	switch input.DocumentType {
	case "CC", "C Extranjeria", "Pasaporte":
	default:
		fields["customer_document_type"] = "Selecciona un tipo de documento válido."
	}
	if input.DocumentNumber == "" {
		fields["customer_document_number"] = "El documento del cliente es obligatorio."
	}
	if input.Phone == "" {
		fields["customer_phone"] = "El teléfono del cliente es obligatorio."
	}
	return fields
}

func resolveCustomerForCredit(tx *sql.Tx, tenantID int, input customerInput) (*Customer, error) {
	tenantID = normalizeTenantID(tenantID)
	input.Name = strings.TrimSpace(input.Name)
	input.Phone = strings.TrimSpace(input.Phone)
	input.DocumentType = strings.TrimSpace(input.DocumentType)
	input.DocumentNumber = strings.TrimSpace(input.DocumentNumber)
	input.Address = strings.TrimSpace(input.Address)
	input.City = strings.TrimSpace(input.City)
	input.Notes = strings.TrimSpace(input.Notes)

	if input.CustomerID > 0 {
		var existing Customer
		err := tx.QueryRow(`
			SELECT id, tenant_id, name, phone, document_type, document_number, address, city, notes, created_at, updated_at
			FROM customers
			WHERE tenant_id = ? AND id = ?
		`, tenantID, input.CustomerID).Scan(
			&existing.ID,
			&existing.TenantID,
			&existing.Name,
			&existing.Phone,
			&existing.DocumentType,
			&existing.DocumentNumber,
			&existing.Address,
			&existing.City,
			&existing.Notes,
			&existing.CreatedAt,
			&existing.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		return &existing, nil
	}

	now := time.Now().Format(time.RFC3339)
	var existing Customer
	err := tx.QueryRow(`
		SELECT id, tenant_id, name, phone, document_type, document_number, address, city, notes, created_at, updated_at
		FROM customers
		WHERE tenant_id = ? AND document_type = ? AND document_number = ?
	`, tenantID, input.DocumentType, input.DocumentNumber).Scan(
		&existing.ID,
		&existing.TenantID,
		&existing.Name,
		&existing.Phone,
		&existing.DocumentType,
		&existing.DocumentNumber,
		&existing.Address,
		&existing.City,
		&existing.Notes,
		&existing.CreatedAt,
		&existing.UpdatedAt,
	)
	if err == nil {
		if _, err := tx.Exec(`
			UPDATE customers
			SET name = ?, phone = ?, address = ?, city = ?, notes = ?, updated_at = ?
			WHERE tenant_id = ? AND id = ?
		`, input.Name, input.Phone, input.Address, input.City, input.Notes, now, tenantID, existing.ID); err != nil {
			return nil, err
		}
		existing.Name = input.Name
		existing.Phone = input.Phone
		existing.Address = input.Address
		existing.City = input.City
		existing.Notes = input.Notes
		existing.UpdatedAt = now
		return &existing, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}

	customerID, err := insertAndReturnID(tx, `
		INSERT INTO customers (tenant_id, name, phone, document_type, document_number, address, city, notes, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, tenantID, input.Name, input.Phone, input.DocumentType, input.DocumentNumber, input.Address, input.City, input.Notes, now, now)
	if err != nil {
		return nil, err
	}
	return &Customer{
		ID:             int(customerID),
		TenantID:       tenantID,
		Name:           input.Name,
		Phone:          input.Phone,
		DocumentType:   input.DocumentType,
		DocumentNumber: input.DocumentNumber,
		Address:        input.Address,
		City:           input.City,
		Notes:          input.Notes,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, nil
}

func updateUnitsByIDStatus(tx *sql.Tx, tenantID int, unitIDs []string, currentStatuses []string, nextStatus string) error {
	if len(unitIDs) == 0 {
		return fmt.Errorf("no hay unidades para actualizar")
	}
	statuses := currentStatuses
	if len(statuses) == 0 {
		statuses = []string{"Disponible", "available"}
	}
	idPlaceholders := make([]string, 0, len(unitIDs))
	args := make([]any, 0, 2+len(unitIDs)+len(statuses))
	args = append(args, nextStatus, normalizeTenantID(tenantID))
	for _, id := range unitIDs {
		idPlaceholders = append(idPlaceholders, "?")
		args = append(args, id)
	}
	statusPlaceholders := make([]string, 0, len(statuses))
	for _, status := range statuses {
		statusPlaceholders = append(statusPlaceholders, "?")
		args = append(args, status)
	}
	query := fmt.Sprintf(
		"UPDATE unidades SET estado = ? WHERE tenant_id = ? AND id IN (%s) AND estado IN (%s)",
		strings.Join(idPlaceholders, ","),
		strings.Join(statusPlaceholders, ","),
	)
	result, err := tx.Exec(query, args...)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if int(affected) != len(unitIDs) {
		return fmt.Errorf("unidades actualizadas inesperadas: %d", affected)
	}
	return nil
}

func createProductLoan(db *sql.DB, currentUser *User, input productLoanCreateInput, source string, decoratePayload func(map[string]any) map[string]any) (productLoanOperationResult, error) {
	if currentUser == nil || !isStaffRole(currentUser.Role) {
		return productLoanOperationResult{}, requestError{Status: http.StatusForbidden, Message: "Solo personal autorizado puede registrar préstamos de producto."}
	}
	_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantIDFromUser(currentUser))
	if err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo cargar la configuración de movimientos."}
	}
	if !movementEnabled(movementEnabledMap, "prestamo") {
		return productLoanOperationResult{}, requestError{Status: http.StatusForbidden, Message: "El flujo de préstamo está deshabilitado en Configuración."}
	}

	input.ProductID = strings.TrimSpace(input.ProductID)
	input.Quantity = max(input.Quantity, 1)
	input.Notes = strings.TrimSpace(input.Notes)
	input.DueAt = strings.TrimSpace(input.DueAt)
	if input.ProductID == "" {
		return productLoanOperationResult{}, requestError{Status: http.StatusBadRequest, Message: "Producto inválido.", Fields: map[string]string{"product_id": "El producto es obligatorio."}}
	}
	if fields := validateCustomerInput(input.Customer); len(fields) > 0 {
		return productLoanOperationResult{}, requestError{Status: http.StatusBadRequest, Message: "Datos inválidos.", Fields: fields}
	}
	if input.DueAt != "" {
		if _, err := time.Parse("2006-01-02", input.DueAt); err != nil {
			return productLoanOperationResult{}, requestError{Status: http.StatusBadRequest, Message: "La fecha estimada de retorno no es válida.", Fields: map[string]string{"due_at": "Ingresa una fecha válida."}}
		}
	}
	allowed, err := productAccessibleByID(db, currentUser, input.ProductID)
	if err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo validar acceso al producto."}
	}
	if !allowed {
		return productLoanOperationResult{}, requestError{Status: http.StatusForbidden, Message: "No tienes acceso a este producto."}
	}

	tx, err := db.Begin()
	if err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo iniciar el préstamo."}
	}
	defer tx.Rollback()

	customer, err := resolveCustomerForCredit(tx, tenantIDFromUser(currentUser), input.Customer)
	if err != nil {
		return productLoanOperationResult{}, err
	}
	unitIDs, err := selectAndMarkUnitsByStatus(tx, tenantIDFromUser(currentUser), input.ProductID, input.Quantity, "Prestada")
	if err != nil {
		if err == errInsufficientStock {
			return productLoanOperationResult{}, requestError{Status: http.StatusBadRequest, Message: "No hay stock disponible suficiente para registrar el préstamo.", Fields: map[string]string{"quantity": "No hay stock disponible suficiente para registrar el préstamo."}}
		}
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo apartar el producto para préstamo."}
	}

	now := time.Now().Format(time.RFC3339)
	productLoanID, err := insertAndReturnID(tx, `
		INSERT INTO product_loans (
			tenant_id, product_id, customer_id, quantity, borrower_name, borrower_phone,
			borrower_document_type, borrower_document_number, borrower_address, borrower_city,
			notes, status, loaned_at, due_at, created_by
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, tenantIDFromUser(currentUser), input.ProductID, nullableIntValue(customer.ID), input.Quantity, customer.Name, customer.Phone, customer.DocumentType, customer.DocumentNumber, customer.Address, customer.City, input.Notes, string(productLoanStatusActive), now, input.DueAt, nullableUserID(currentUser))
	if err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar el préstamo."}
	}
	for _, unitID := range unitIDs {
		if _, err := tx.Exec(`
			INSERT INTO product_loan_units (tenant_id, product_loan_id, unit_id)
			VALUES (?, ?, ?)
		`, tenantIDFromUser(currentUser), productLoanID, unitID); err != nil {
			return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar el detalle del préstamo."}
		}
	}
	movementNote := input.Notes
	if movementNote == "" {
		movementNote = fmt.Sprintf("Prestado a %s", customer.Name)
	}
	if err := logMovimientos(tx, input.ProductID, unitIDs, "prestamo", movementNote, currentUser, now); err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar el movimiento del préstamo."}
	}
	auditPayload := map[string]any{
		"product_loan_id":          productLoanID,
		"product_id":               input.ProductID,
		"customer_id":              customer.ID,
		"borrower_name":            customer.Name,
		"borrower_document_type":   customer.DocumentType,
		"borrower_document_number": customer.DocumentNumber,
		"quantity":                 input.Quantity,
		"due_at":                   input.DueAt,
		"unit_ids":                 unitIDs,
		"notes":                    input.Notes,
	}
	if decoratePayload != nil {
		auditPayload = decoratePayload(auditPayload)
	}
	if err := logAuditEvent(tx, currentUser, "product_loan_created", "product_loan", strconv.FormatInt(productLoanID, 10), source, auditPayload); err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la auditoría del préstamo."}
	}
	if err := logCustomerEvent(tx, currentUser, customer.ID, "product_loan_created", "product_loan", strconv.FormatInt(productLoanID, 10), 0, map[string]any{
		"product_id": input.ProductID,
		"quantity":   input.Quantity,
		"due_at":     input.DueAt,
		"unit_ids":   unitIDs,
	}); err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la trazabilidad del cliente."}
	}
	if err := tx.Commit(); err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo confirmar el préstamo."}
	}
	return productLoanOperationResult{
		ProductLoanID: int(productLoanID),
		ProductID:     input.ProductID,
		CustomerID:    customer.ID,
		BorrowerName:  customer.Name,
		Quantity:      input.Quantity,
		Status:        productLoanStatusActive,
		LoanedAt:      now,
		DueAt:         input.DueAt,
	}, nil
}

func closeProductLoan(db *sql.DB, currentUser *User, input productLoanCloseInput, source string, decoratePayload func(map[string]any) map[string]any) (productLoanOperationResult, error) {
	if currentUser == nil || !isStaffRole(currentUser.Role) {
		return productLoanOperationResult{}, requestError{Status: http.StatusForbidden, Message: "Solo personal autorizado puede cerrar préstamos de producto."}
	}
	_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantIDFromUser(currentUser))
	if err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo cargar la configuración de movimientos."}
	}
	if !movementEnabled(movementEnabledMap, "prestamo") {
		return productLoanOperationResult{}, requestError{Status: http.StatusForbidden, Message: "El flujo de préstamo está deshabilitado en Configuración."}
	}
	status := normalizeProductLoanStatus(string(input.Status))
	if status == productLoanStatusActive {
		return productLoanOperationResult{}, requestError{Status: http.StatusBadRequest, Message: "Selecciona un cierre válido para el préstamo.", Fields: map[string]string{"status": "Debes elegir retornado, cerrado por pago o cancelado."}}
	}
	tx, err := db.Begin()
	if err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo iniciar el cierre del préstamo."}
	}
	defer tx.Rollback()

	var (
		productID     string
		customerID    int
		borrowerName  string
		quantity      int
		currentStatus string
		loanedAt      string
		dueAt         string
	)
	err = tx.QueryRow(`
		SELECT product_id, COALESCE(customer_id, 0), borrower_name, quantity, status, COALESCE(loaned_at, ''), COALESCE(due_at, '')
		FROM product_loans
		WHERE tenant_id = ? AND id = ?
		LIMIT 1
	`, tenantIDFromUser(currentUser), input.ProductLoanID).Scan(&productID, &customerID, &borrowerName, &quantity, &currentStatus, &loanedAt, &dueAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return productLoanOperationResult{}, requestError{Status: http.StatusNotFound, Message: "Préstamo no encontrado."}
		}
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo cargar el préstamo."}
	}
	if normalizeProductLoanStatus(currentStatus) != productLoanStatusActive {
		return productLoanOperationResult{}, requestError{Status: http.StatusBadRequest, Message: "Este préstamo ya está cerrado."}
	}
	allowed, err := productAccessibleByID(db, currentUser, productID)
	if err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo validar acceso al producto."}
	}
	if !allowed {
		return productLoanOperationResult{}, requestError{Status: http.StatusForbidden, Message: "No tienes acceso a este préstamo."}
	}

	rows, err := tx.Query(`
		SELECT unit_id
		FROM product_loan_units
		WHERE tenant_id = ? AND product_loan_id = ?
		ORDER BY id ASC
	`, tenantIDFromUser(currentUser), input.ProductLoanID)
	if err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo cargar el detalle del préstamo."}
	}
	unitIDs := make([]string, 0, quantity)
	for rows.Next() {
		var unitID string
		if err := rows.Scan(&unitID); err != nil {
			rows.Close()
			return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo leer las unidades prestadas."}
		}
		unitIDs = append(unitIDs, unitID)
	}
	rows.Close()
	if len(unitIDs) == 0 {
		return productLoanOperationResult{}, requestError{Status: http.StatusBadRequest, Message: "Este préstamo no tiene unidades asociadas."}
	}

	nextUnitStatus := "Disponible"
	movementType := "prestamo_retorno"
	switch status {
	case productLoanStatusPaid:
		nextUnitStatus = "Vendida"
		movementType = "prestamo_pagado"
	case productLoanStatusCancelled:
		nextUnitStatus = "Disponible"
		movementType = "prestamo_cancelado"
	}
	if err := updateUnitsByIDStatus(tx, tenantIDFromUser(currentUser), unitIDs, []string{"Prestada"}, nextUnitStatus); err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo cerrar el préstamo sobre las unidades."}
	}
	now := time.Now().Format(time.RFC3339)
	if _, err := tx.Exec(`
		UPDATE product_loans
		SET status = ?, closed_at = ?, closed_by = ?, close_notes = ?
		WHERE tenant_id = ? AND id = ?
	`, string(status), now, nullableUserID(currentUser), strings.TrimSpace(input.Notes), tenantIDFromUser(currentUser), input.ProductLoanID); err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo actualizar el préstamo."}
	}
	if err := logMovimientos(tx, productID, unitIDs, movementType, strings.TrimSpace(input.Notes), currentUser, now); err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar el cierre del préstamo."}
	}
	auditPayload := map[string]any{
		"product_loan_id": input.ProductLoanID,
		"product_id":      productID,
		"status":          string(status),
		"unit_ids":        unitIDs,
		"notes":           strings.TrimSpace(input.Notes),
	}
	if decoratePayload != nil {
		auditPayload = decoratePayload(auditPayload)
	}
	if err := logAuditEvent(tx, currentUser, "product_loan_closed", "product_loan", strconv.Itoa(input.ProductLoanID), source, auditPayload); err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la auditoría del cierre."}
	}
	if customerID > 0 {
		if err := logCustomerEvent(tx, currentUser, customerID, "product_loan_closed", "product_loan", strconv.Itoa(input.ProductLoanID), 0, map[string]any{
			"product_id": productID,
			"status":     string(status),
			"unit_ids":   unitIDs,
		}); err != nil {
			return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la trazabilidad del cliente."}
		}
	}
	if err := tx.Commit(); err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo confirmar el cierre del préstamo."}
	}
	return productLoanOperationResult{
		ProductLoanID: input.ProductLoanID,
		ProductID:     productID,
		CustomerID:    customerID,
		BorrowerName:  borrowerName,
		Quantity:      quantity,
		Status:        status,
		LoanedAt:      loanedAt,
		DueAt:         dueAt,
		ClosedAt:      now,
	}, nil
}

func logCustomerEvent(exec sqlExecer, user *User, customerID int, eventType, refType, refID string, amount float64, payload map[string]any) error {
	payloadJSON := "{}"
	if len(payload) > 0 {
		if encoded, err := json.Marshal(payload); err == nil {
			payloadJSON = string(encoded)
		}
	}
	_, err := exec.Exec(`
		INSERT INTO customer_events (tenant_id, customer_id, event_type, ref_type, ref_id, amount, payload_json, created_at, created_by)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, tenantIDFromUser(user), customerID, strings.TrimSpace(eventType), strings.TrimSpace(refType), strings.TrimSpace(refID), amount, payloadJSON, time.Now().Format(time.RFC3339), nullableUserID(user))
	return err
}

func listCustomersForTenant(db *sql.DB, tenantID int, q string, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 100
	}
	q = strings.TrimSpace(strings.ToLower(q))
	query := `
		SELECT
			c.id,
			c.name,
			c.phone,
			c.document_type,
			c.document_number,
			c.address,
			c.city,
			c.notes,
			c.created_at,
			c.updated_at,
			COALESCE(COUNT(cs.id), 0),
			COALESCE(SUM(cs.quantity), 0),
			COALESCE(SUM(COALESCE(cs.installments_total, 0) * COALESCE(cs.installment_value, 0)), 0),
			COALESCE(SUM(
				CASE
					WHEN COALESCE(pay.total_paid, 0) > (COALESCE(cs.installments_paid, 0) * COALESCE(cs.installment_value, 0))
						THEN COALESCE(pay.total_paid, 0)
					ELSE (COALESCE(cs.installments_paid, 0) * COALESCE(cs.installment_value, 0))
				END
			), 0),
			COALESCE(SUM(
				CASE
					WHEN (COALESCE(cs.installments_total, 0) * COALESCE(cs.installment_value, 0)) -
						(CASE
							WHEN COALESCE(pay.total_paid, 0) > (COALESCE(cs.installments_paid, 0) * COALESCE(cs.installment_value, 0))
								THEN COALESCE(pay.total_paid, 0)
							ELSE (COALESCE(cs.installments_paid, 0) * COALESCE(cs.installment_value, 0))
						END) > 0
					THEN 1
					ELSE 0
				END
			), 0),
			COALESCE(MAX(cs.created_at), '')
		FROM customers c
		LEFT JOIN credit_sales cs
			ON cs.tenant_id = c.tenant_id AND cs.customer_id = c.id
		LEFT JOIN (
			SELECT tenant_id, credit_sale_id, SUM(amount_paid) AS total_paid
			FROM credit_installments
			GROUP BY tenant_id, credit_sale_id
		) pay
			ON pay.tenant_id = cs.tenant_id AND pay.credit_sale_id = cs.id
		WHERE c.tenant_id = ?
	`
	args := []any{normalizeTenantID(tenantID)}
	if q != "" {
		query += ` AND (LOWER(c.name) LIKE ? OR LOWER(c.phone) LIKE ? OR LOWER(c.document_type) LIKE ? OR LOWER(c.document_number) LIKE ? OR LOWER(c.city) LIKE ?)`
		qLike := "%" + q + "%"
		args = append(args, qLike, qLike, qLike, qLike, qLike)
	}
	query += ` GROUP BY c.id, c.name, c.phone, c.document_type, c.document_number, c.address, c.city, c.notes, c.created_at, c.updated_at
	ORDER BY c.updated_at DESC, c.id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0, limit)
	for rows.Next() {
		var (
			item          Customer
			creditsCount  int
			unitsOnCredit int
			debtTotal     float64
			totalPaid     float64
			activeCredits int
			lastCreditAt  string
		)
		if err := rows.Scan(&item.ID, &item.Name, &item.Phone, &item.DocumentType, &item.DocumentNumber, &item.Address, &item.City, &item.Notes, &item.CreatedAt, &item.UpdatedAt, &creditsCount, &unitsOnCredit, &debtTotal, &totalPaid, &activeCredits, &lastCreditAt); err != nil {
			return nil, err
		}
		currentDebt := creditCurrentDebt(debtTotal, totalPaid)
		items = append(items, map[string]any{
			"id":              item.ID,
			"name":            item.Name,
			"phone":           item.Phone,
			"document_type":   item.DocumentType,
			"document_number": item.DocumentNumber,
			"address":         item.Address,
			"city":            item.City,
			"notes":           item.Notes,
			"created_at":      formatDateWithSettings(item.CreatedAt),
			"updated_at":      formatDateWithSettings(item.UpdatedAt),
			"credits_count":   creditsCount,
			"units_on_credit": unitsOnCredit,
			"debt_total":      debtTotal,
			"total_paid":      totalPaid,
			"current_debt":    currentDebt,
			"active_credits":  activeCredits,
			"last_credit_at":  formatDateWithSettings(lastCreditAt),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func agentCustomerSearchItem(item map[string]any) map[string]any {
	return map[string]any{
		"id":              item["id"],
		"name":            item["name"],
		"phone":           item["phone"],
		"document_type":   item["document_type"],
		"document_number": item["document_number"],
		"city":            item["city"],
		"credits_count":   item["credits_count"],
		"debt_total":      item["debt_total"],
		"total_paid":      item["total_paid"],
		"current_debt":    item["current_debt"],
		"active_credits":  item["active_credits"],
		"last_credit_at":  item["last_credit_at"],
	}
}

func customerDetailForTenant(db *sql.DB, tenantID, customerID int) (map[string]any, error) {
	customer, err := findCustomerByID(db, tenantID, customerID)
	if err != nil {
		return nil, err
	}
	items, err := listCustomersForTenant(db, tenantID, customer.DocumentNumber, 200)
	if err != nil {
		return nil, err
	}
	var selected map[string]any
	for _, item := range items {
		if id, ok := item["id"].(int); ok && id == customerID {
			selected = item
			break
		}
	}
	if selected == nil {
		selected = map[string]any{
			"id":              customer.ID,
			"name":            customer.Name,
			"phone":           customer.Phone,
			"document_type":   customer.DocumentType,
			"document_number": customer.DocumentNumber,
			"address":         customer.Address,
			"city":            customer.City,
			"notes":           customer.Notes,
			"created_at":      formatDateWithSettings(customer.CreatedAt),
			"updated_at":      formatDateWithSettings(customer.UpdatedAt),
		}
	}

	recentCredits := make([]map[string]any, 0, 10)
	rows, err := db.Query(`
		SELECT
			cs.id,
			cs.created_at,
			COALESCE(cs.kind, ?),
			COALESCE(cs.product_id, ''),
			CASE
				WHEN COALESCE(cs.kind, ?) = ? THEN 'Préstamo de dinero'
				ELSE COALESCE(NULLIF(p.nombre, ''), cs.product_id)
			END,
			COALESCE(cs.quantity, 0),
			COALESCE(cs.installments_total, 0),
			COALESCE(cs.installments_paid, 0),
			COALESCE(cs.installment_value, 0),
			COALESCE((
				SELECT SUM(ci.amount_paid)
				FROM credit_installments ci
				WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id
			), 0)
		FROM credit_sales cs
		LEFT JOIN productos p ON p.sku = cs.product_id AND p.tenant_id = cs.tenant_id
		WHERE cs.tenant_id = ? AND cs.customer_id = ?
		ORDER BY cs.created_at DESC, cs.id DESC
		LIMIT 10
	`, string(creditSaleKindProduct), string(creditSaleKindProduct), string(creditSaleKindCash), normalizeTenantID(tenantID), customerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			creditID          int
			createdAt         string
			kindRaw           string
			productID         string
			productName       string
			quantity          int
			installmentsTotal int
			installmentsPaid  int
			installmentValue  float64
			totalPaid         float64
		)
		if err := rows.Scan(&creditID, &createdAt, &kindRaw, &productID, &productName, &quantity, &installmentsTotal, &installmentsPaid, &installmentValue, &totalPaid); err != nil {
			return nil, err
		}
		kind := normalizeCreditSaleKind(kindRaw)
		legacyPaid := math.Round((float64(installmentsPaid)*installmentValue)*100) / 100
		if totalPaid < legacyPaid {
			totalPaid = legacyPaid
		}
		debtTotal := creditDebtTotal(installmentsTotal, installmentValue)
		recentCredits = append(recentCredits, map[string]any{
			"id":                 creditID,
			"created_at":         formatDateWithSettings(createdAt),
			"kind":               string(kind),
			"kind_label":         creditKindLabel(kind),
			"product_id":         productID,
			"product_name":       productName,
			"quantity":           quantity,
			"installments_total": installmentsTotal,
			"installments_paid":  installmentsPaid,
			"installment_value":  installmentValue,
			"debt_total":         debtTotal,
			"total_paid":         totalPaid,
			"current_debt":       creditCurrentDebt(debtTotal, totalPaid),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	selected["recent_credits"] = recentCredits
	return selected, nil
}

func customerEventsForTenant(db *sql.DB, tenantID, customerID, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := db.Query(`
		SELECT ce.id, ce.event_type, ce.ref_type, ce.ref_id, ce.amount, ce.payload_json, ce.created_at, COALESCE(u.username, '')
		FROM customer_events ce
		LEFT JOIN users u ON u.id = ce.created_by AND u.tenant_id = ce.tenant_id
		WHERE ce.tenant_id = ? AND ce.customer_id = ?
		ORDER BY ce.created_at DESC, ce.id DESC
		LIMIT ?
	`, normalizeTenantID(tenantID), customerID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0, limit)
	for rows.Next() {
		var (
			id         int
			eventType  string
			refType    string
			refID      string
			amount     float64
			payloadRaw string
			createdAt  string
			createdBy  string
		)
		if err := rows.Scan(&id, &eventType, &refType, &refID, &amount, &payloadRaw, &createdAt, &createdBy); err != nil {
			return nil, err
		}
		payload := map[string]any{}
		if strings.TrimSpace(payloadRaw) != "" {
			_ = json.Unmarshal([]byte(payloadRaw), &payload)
		}
		items = append(items, map[string]any{
			"id":         id,
			"event_type": eventType,
			"ref_type":   refType,
			"ref_id":     refID,
			"amount":     amount,
			"payload":    payload,
			"created_at": formatDateWithSettings(createdAt),
			"created_by": createdBy,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func customerListViewURL() string {
	return "/clientes"
}

func customerDetailViewURL(customerID int) string {
	if customerID <= 0 {
		return customerListViewURL()
	}
	return fmt.Sprintf("/clientes/%d", customerID)
}

func stringFromAny(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	case fmt.Stringer:
		return typed.String()
	default:
		return strings.TrimSpace(fmt.Sprint(value))
	}
}

func intFromAny(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case float32:
		return int(typed)
	case string:
		parsed, _ := strconv.Atoi(strings.TrimSpace(typed))
		return parsed
	default:
		return 0
	}
}

func floatFromAny(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	case string:
		parsed, _ := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed
	default:
		return 0
	}
}

func buildCustomerListViewItems(items []map[string]any) []customerListViewItem {
	result := make([]customerListViewItem, 0, len(items))
	for _, item := range items {
		customerID := intFromAny(item["id"])
		result = append(result, customerListViewItem{
			ID:              customerID,
			Name:            stringFromAny(item["name"]),
			Phone:           stringFromAny(item["phone"]),
			DocumentType:    stringFromAny(item["document_type"]),
			DocumentNumber:  stringFromAny(item["document_number"]),
			Address:         stringFromAny(item["address"]),
			City:            stringFromAny(item["city"]),
			Notes:           stringFromAny(item["notes"]),
			CreatedAt:       stringFromAny(item["created_at"]),
			UpdatedAt:       stringFromAny(item["updated_at"]),
			CreditsCount:    intFromAny(item["credits_count"]),
			UnitsOnCredit:   intFromAny(item["units_on_credit"]),
			ActiveCredits:   intFromAny(item["active_credits"]),
			DebtTotalText:   formatCurrency(floatFromAny(item["debt_total"])),
			TotalPaidText:   formatCurrency(floatFromAny(item["total_paid"])),
			CurrentDebtText: formatCurrency(floatFromAny(item["current_debt"])),
			LastCreditAt:    stringFromAny(item["last_credit_at"]),
			DetailURL:       customerDetailViewURL(customerID),
		})
	}
	return result
}

func listCustomerInvoicesForTenant(db *sql.DB, tenantID, customerID, limit int) ([]customerInvoiceViewItem, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := db.Query(`
		SELECT id, COALESCE(invoice_number, ''), COALESCE(source_type, 'sale'), COALESCE(status, 'issued'), COALESCE(total, 0), COALESCE(created_at, '')
		FROM invoices
		WHERE tenant_id = ? AND customer_id = ?
		ORDER BY created_at DESC, id DESC
		LIMIT ?
	`, normalizeTenantID(tenantID), customerID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]customerInvoiceViewItem, 0, limit)
	for rows.Next() {
		var item customerInvoiceViewItem
		var total float64
		if err := rows.Scan(&item.ID, &item.InvoiceNumber, &item.SourceType, &item.Status, &total, &item.CreatedAt); err != nil {
			return nil, err
		}
		item.SourceLabel = invoiceSourceLabel(item.SourceType)
		item.StatusLabel = invoiceStatusLabel(item.Status)
		item.TotalText = formatCurrency(total)
		item.CreatedAt = formatDateWithSettings(item.CreatedAt)
		item.ViewURL = invoiceViewURL(item.ID)
		items = append(items, item)
	}
	return items, rows.Err()
}

func listCustomerProductLoansForTenant(db *sql.DB, tenantID, customerID, limit int) ([]customerProductLoanViewItem, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := db.Query(`
		SELECT
			pl.id,
			COALESCE(pl.product_id, ''),
			COALESCE(NULLIF(p.nombre, ''), pl.product_id),
			COALESCE(pl.quantity, 0),
			COALESCE(pl.status, 'active'),
			COALESCE(pl.loaned_at, ''),
			COALESCE(pl.due_at, ''),
			COALESCE(pl.closed_at, '')
		FROM product_loans pl
		LEFT JOIN productos p ON p.sku = pl.product_id AND p.tenant_id = pl.tenant_id
		WHERE pl.tenant_id = ? AND pl.customer_id = ?
		ORDER BY pl.loaned_at DESC, pl.id DESC
		LIMIT ?
	`, normalizeTenantID(tenantID), customerID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]customerProductLoanViewItem, 0, limit)
	for rows.Next() {
		var item customerProductLoanViewItem
		if err := rows.Scan(&item.ID, &item.ProductID, &item.ProductName, &item.Quantity, &item.Status, &item.LoanedAt, &item.DueAt, &item.ClosedAt); err != nil {
			return nil, err
		}
		status := normalizeProductLoanStatus(item.Status)
		item.Status = string(status)
		item.StatusLabel = productLoanStatusLabel(status)
		item.IsOverdue = isProductLoanOverdue(status, item.DueAt)
		item.LoanedAt = formatDateWithSettings(item.LoanedAt)
		item.DueAt = formatDateWithSettings(item.DueAt)
		item.ClosedAt = formatDateWithSettings(item.ClosedAt)
		item.DetailURL = fmt.Sprintf("/prestamos/producto/%d", item.ID)
		items = append(items, item)
	}
	return items, rows.Err()
}

func listCustomerProductsForTenant(db *sql.DB, tenantID, customerID, limit int) ([]customerProductViewItem, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := db.Query(`
		SELECT
			product_id,
			product_name,
			COALESCE(SUM(quantity), 0),
			COALESCE(SUM(total_value), 0),
			COALESCE(MAX(created_at), ''),
			COALESCE(MAX(has_invoice), 0),
			COALESCE(MAX(has_credit), 0)
		FROM (
			SELECT
				COALESCE(ii.product_id, '') AS product_id,
				COALESCE(NULLIF(ii.description, ''), ii.product_id) AS product_name,
				COALESCE(ii.quantity, 0) AS quantity,
				COALESCE(ii.total, 0) AS total_value,
				COALESCE(i.created_at, '') AS created_at,
				1 AS has_invoice,
				0 AS has_credit
			FROM invoices i
			INNER JOIN invoice_items ii
				ON ii.tenant_id = i.tenant_id AND ii.invoice_id = i.id
			WHERE i.tenant_id = ? AND i.customer_id = ? AND COALESCE(ii.product_id, '') <> ''

			UNION ALL

			SELECT
				COALESCE(cs.product_id, '') AS product_id,
				COALESCE(NULLIF(p.nombre, ''), cs.product_id) AS product_name,
				COALESCE(cs.quantity, 0) AS quantity,
				COALESCE(cs.installments_total, 0) * COALESCE(cs.installment_value, 0) AS total_value,
				COALESCE(cs.created_at, '') AS created_at,
				0 AS has_invoice,
				1 AS has_credit
			FROM credit_sales cs
			LEFT JOIN productos p ON p.sku = cs.product_id AND p.tenant_id = cs.tenant_id
			WHERE cs.tenant_id = ? AND cs.customer_id = ? AND COALESCE(cs.kind, ?) = ?
				AND COALESCE(cs.product_id, '') <> ''
				AND NOT EXISTS (
					SELECT 1
					FROM invoices i
					WHERE i.tenant_id = cs.tenant_id AND i.credit_sale_id = cs.id
				)
		) customer_products
		GROUP BY product_id, product_name
		ORDER BY MAX(created_at) DESC, product_name ASC
		LIMIT ?
	`, normalizeTenantID(tenantID), customerID, normalizeTenantID(tenantID), customerID, string(creditSaleKindProduct), string(creditSaleKindProduct), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]customerProductViewItem, 0, limit)
	for rows.Next() {
		var (
			item       customerProductViewItem
			totalValue float64
			lastAt     string
			hasInvoice int
			hasCredit  int
		)
		if err := rows.Scan(&item.ProductID, &item.ProductName, &item.Quantity, &totalValue, &lastAt, &hasInvoice, &hasCredit); err != nil {
			return nil, err
		}
		sources := make([]string, 0, 2)
		if hasInvoice > 0 {
			sources = append(sources, "Factura")
		}
		if hasCredit > 0 {
			sources = append(sources, "Crédito")
		}
		item.TotalText = formatCurrency(totalValue)
		item.LastAt = formatDateWithSettings(lastAt)
		item.SourcesText = strings.Join(sources, " + ")
		items = append(items, item)
	}
	return items, rows.Err()
}

func customerTimelineEventLabel(eventType string) string {
	switch strings.TrimSpace(strings.ToLower(eventType)) {
	case "profile_created":
		return "Perfil creado"
	case "profile_updated":
		return "Perfil actualizado"
	case "credit_created":
		return "Crédito creado"
	case "credit_payment_recorded":
		return "Pago registrado"
	case "credit_updated":
		return "Crédito editado"
	case "invoice_created":
		return "Factura emitida"
	case "product_loan_created":
		return "Préstamo físico creado"
	case "product_loan_closed":
		return "Préstamo físico cerrado"
	default:
		return strings.TrimSpace(eventType)
	}
}

func customerTimelineDetailURL(refType, refID string) string {
	switch strings.TrimSpace(strings.ToLower(refType)) {
	case "invoice":
		id, _ := strconv.Atoi(strings.TrimSpace(refID))
		return invoiceViewURL(id)
	case "product_loan":
		return fmt.Sprintf("/prestamos/producto/%s", strings.TrimSpace(refID))
	default:
		return ""
	}
}

func customerTimelineSummary(eventType string, payload map[string]any) string {
	switch strings.TrimSpace(strings.ToLower(eventType)) {
	case "profile_created", "profile_updated":
		name := stringFromAny(payload["name"])
		city := stringFromAny(payload["city"])
		if name != "" && city != "" {
			return fmt.Sprintf("%s · %s", name, city)
		}
		return firstNonEmptyString(name, city)
	case "credit_created", "credit_updated":
		product := firstNonEmptyString(stringFromAny(payload["product_name"]), stringFromAny(payload["product"]))
		kindLabel := firstNonEmptyString(stringFromAny(payload["kind_label"]), stringFromAny(payload["kind"]))
		if product != "" && kindLabel != "" {
			return fmt.Sprintf("%s · %s", kindLabel, product)
		}
		return firstNonEmptyString(product, kindLabel)
	case "credit_payment_recorded":
		paymentType := firstNonEmptyString(stringFromAny(payload["payment_type_label"]), stringFromAny(payload["payment_type"]))
		creditID := stringFromAny(payload["credit_sale_id"])
		if paymentType != "" && creditID != "" {
			return fmt.Sprintf("%s sobre crédito #%s", paymentType, creditID)
		}
		return paymentType
	case "invoice_created":
		number := stringFromAny(payload["invoice_number"])
		sourceType := stringFromAny(payload["source_type"])
		if number != "" && sourceType != "" {
			return fmt.Sprintf("%s · %s", number, invoiceSourceLabel(sourceType))
		}
		return number
	case "product_loan_created", "product_loan_closed":
		product := firstNonEmptyString(stringFromAny(payload["product_name"]), stringFromAny(payload["product_id"]))
		status := firstNonEmptyString(stringFromAny(payload["status_label"]), stringFromAny(payload["status"]))
		if product != "" && status != "" {
			return fmt.Sprintf("%s · %s", product, status)
		}
		return firstNonEmptyString(product, status)
	default:
		return ""
	}
}

func buildCustomerTimelineViewItems(items []map[string]any) []customerTimelineViewItem {
	result := make([]customerTimelineViewItem, 0, len(items))
	for _, item := range items {
		payload, _ := item["payload"].(map[string]any)
		amount := floatFromAny(item["amount"])
		result = append(result, customerTimelineViewItem{
			EventType:  stringFromAny(item["event_type"]),
			EventLabel: customerTimelineEventLabel(stringFromAny(item["event_type"])),
			RefType:    stringFromAny(item["ref_type"]),
			RefID:      stringFromAny(item["ref_id"]),
			Summary:    customerTimelineSummary(stringFromAny(item["event_type"]), payload),
			AmountText: func() string {
				if amount <= 0 {
					return ""
				}
				return formatCurrency(amount)
			}(),
			CreatedAt: stringFromAny(item["created_at"]),
			CreatedBy: stringFromAny(item["created_by"]),
			DetailURL: customerTimelineDetailURL(stringFromAny(item["ref_type"]), stringFromAny(item["ref_id"])),
		})
	}
	return result
}

func customerDetailViewForTenant(db *sql.DB, currentUser *User, customerID int) (customerDetailViewData, error) {
	if currentUser == nil || !isStaffRole(currentUser.Role) {
		return customerDetailViewData{}, requestError{Status: http.StatusForbidden, Message: "Solo personal autorizado puede consultar clientes."}
	}
	tenantID := tenantIDFromUser(currentUser)
	base, err := customerDetailForTenant(db, tenantID, customerID)
	if err != nil {
		return customerDetailViewData{}, err
	}
	events, err := customerEventsForTenant(db, tenantID, customerID, 80)
	if err != nil {
		return customerDetailViewData{}, err
	}
	invoices, err := listCustomerInvoicesForTenant(db, tenantID, customerID, 20)
	if err != nil {
		return customerDetailViewData{}, err
	}
	loans, err := listCustomerProductLoansForTenant(db, tenantID, customerID, 20)
	if err != nil {
		return customerDetailViewData{}, err
	}
	products, err := listCustomerProductsForTenant(db, tenantID, customerID, 20)
	if err != nil {
		return customerDetailViewData{}, err
	}

	detail := customerDetailViewData{
		Summary:  buildCustomerListViewItems([]map[string]any{base})[0],
		Invoices: invoices,
		Loans:    loans,
		Products: products,
		Timeline: buildCustomerTimelineViewItems(events),
	}

	if recentCredits, ok := base["recent_credits"].([]map[string]any); ok {
		detail.Credits = make([]customerCreditViewItem, 0, len(recentCredits))
		for _, item := range recentCredits {
			detail.Credits = append(detail.Credits, customerCreditViewItem{
				ID:                intFromAny(item["id"]),
				CreatedAt:         stringFromAny(item["created_at"]),
				Kind:              stringFromAny(item["kind"]),
				KindLabel:         stringFromAny(item["kind_label"]),
				ProductID:         stringFromAny(item["product_id"]),
				ProductName:       stringFromAny(item["product_name"]),
				Quantity:          intFromAny(item["quantity"]),
				InstallmentsTotal: intFromAny(item["installments_total"]),
				InstallmentsPaid:  intFromAny(item["installments_paid"]),
				InstallmentValue:  formatCurrency(floatFromAny(item["installment_value"])),
				DebtTotalText:     formatCurrency(floatFromAny(item["debt_total"])),
				TotalPaidText:     formatCurrency(floatFromAny(item["total_paid"])),
				CurrentDebtText:   formatCurrency(floatFromAny(item["current_debt"])),
			})
		}
	}

	return detail, nil
}

func migrateCreditTablesForCashLoans(db *sql.DB) error {
	if isPostgresDB() {
		for _, stmt := range []string{
			`ALTER TABLE credit_sales ALTER COLUMN product_id DROP NOT NULL`,
			`ALTER TABLE credit_installments ALTER COLUMN product_id DROP NOT NULL`,
		} {
			if _, err := db.Exec(stmt); err != nil {
				return err
			}
		}
		if _, err := db.Exec(`UPDATE credit_sales SET product_id = NULL WHERE TRIM(COALESCE(product_id, '')) = ''`); err != nil {
			return err
		}
		if _, err := db.Exec(`UPDATE credit_installments SET product_id = NULL WHERE TRIM(COALESCE(product_id, '')) = ''`); err != nil {
			return err
		}
		return nil
	}
	creditSalesSQL, err := tableSQL(db, "credit_sales")
	if err != nil {
		return err
	}
	creditInstallmentsSQL, err := tableSQL(db, "credit_installments")
	if err != nil {
		return err
	}
	creditSalesLower := strings.ToLower(creditSalesSQL)
	creditInstallmentsLower := strings.ToLower(creditInstallmentsSQL)
	if !strings.Contains(creditSalesLower, "product_id text not null") && !strings.Contains(creditInstallmentsLower, "product_id text not null") {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DROP TABLE IF EXISTS credit_installments__cash_new`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS credit_sales__cash_new`); err != nil {
		return err
	}
	if _, err := tx.Exec(`
		CREATE TABLE credit_sales__cash_new (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			tenant_id INTEGER NOT NULL DEFAULT 1,
			customer_id INTEGER,
			kind TEXT NOT NULL DEFAULT 'product_credit',
			product_id TEXT,
			quantity INTEGER NOT NULL DEFAULT 1,
			debtor_name TEXT NOT NULL,
			debtor_document_type TEXT NOT NULL DEFAULT '',
			debtor_document_number TEXT NOT NULL DEFAULT '',
			debtor_phone TEXT NOT NULL DEFAULT '',
			installments_total INTEGER NOT NULL,
			installments_paid INTEGER NOT NULL DEFAULT 0,
			total_value REAL NOT NULL,
			interest_percent REAL NOT NULL DEFAULT 0,
			installment_value REAL NOT NULL,
			notes TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'active',
			created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			created_by INTEGER
		)
	`); err != nil {
		return err
	}
	if _, err := tx.Exec(`
		CREATE TABLE credit_installments__cash_new (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			tenant_id INTEGER NOT NULL DEFAULT 1,
			credit_sale_id INTEGER,
			product_id TEXT,
			installment_number INTEGER NOT NULL,
			amount_paid REAL NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			created_by INTEGER,
			payment_type TEXT NOT NULL DEFAULT 'cuota'
		)
	`); err != nil {
		return err
	}
	if _, err := tx.Exec(`
		INSERT INTO credit_sales__cash_new (id, tenant_id, customer_id, kind, product_id, quantity, debtor_name, debtor_document_type, debtor_document_number, debtor_phone, installments_total, installments_paid, total_value, interest_percent, installment_value, notes, created_at, created_by)
		SELECT id, tenant_id, customer_id, COALESCE(NULLIF(kind, ''), ?), NULLIF(product_id, ''), quantity, debtor_name, debtor_document_type, debtor_document_number, debtor_phone, installments_total, installments_paid, total_value, interest_percent, installment_value, notes, created_at, created_by
		FROM credit_sales
		ORDER BY id ASC
	`, string(creditSaleKindProduct)); err != nil {
		return err
	}
	if _, err := tx.Exec(`
		INSERT INTO credit_installments__cash_new (id, tenant_id, credit_sale_id, product_id, installment_number, amount_paid, created_at, created_by, payment_type)
		SELECT id, tenant_id, credit_sale_id, NULLIF(product_id, ''), installment_number, amount_paid, created_at, created_by, COALESCE(NULLIF(payment_type, ''), ?)
		FROM credit_installments
		ORDER BY id ASC
	`, string(creditPaymentTypeCuota)); err != nil {
		return err
	}
	if _, err := tx.Exec(`DROP TABLE credit_installments`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DROP TABLE credit_sales`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE credit_sales__cash_new RENAME TO credit_sales`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE credit_installments__cash_new RENAME TO credit_installments`); err != nil {
		return err
	}
	for _, stmt := range []string{
		`CREATE INDEX IF NOT EXISTS idx_credit_sales_product_id ON credit_sales(product_id, created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_credit_sales_debtor_name ON credit_sales(debtor_name)`,
		`CREATE INDEX IF NOT EXISTS idx_credit_sales_tenant_product_id ON credit_sales(tenant_id, product_id, created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_credit_sales_tenant_debtor_name ON credit_sales(tenant_id, debtor_name)`,
		`CREATE INDEX IF NOT EXISTS idx_credit_sales_tenant_customer_id ON credit_sales(tenant_id, customer_id, created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_credit_sales_tenant_kind_created ON credit_sales(tenant_id, kind, created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_credit_installments_credit_sale_id ON credit_installments(credit_sale_id, installment_number)`,
		`CREATE INDEX IF NOT EXISTS idx_credit_installments_tenant_product_id ON credit_installments(tenant_id, product_id, installment_number)`,
		`CREATE INDEX IF NOT EXISTS idx_credit_installments_tenant_credit_sale_id ON credit_installments(tenant_id, credit_sale_id, installment_number)`,
		`CREATE INDEX IF NOT EXISTS idx_credit_installments_tenant_payment_type ON credit_installments(tenant_id, payment_type, created_at)`,
	} {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func tenantIDFromUser(user *User) int {
	if user == nil {
		return defaultTenantID
	}
	return normalizeTenantID(user.TenantID)
}

func invalidateSessionToken(db *sql.DB, token string) {
	token = strings.TrimSpace(token)
	if db == nil || token == "" {
		return
	}
	_, _ = db.Exec("DELETE FROM sessions WHERE token = ?", token)
}

func logAuditEvent(exec sqlExecer, user *User, eventType, entityType, entityID, source string, payload map[string]any) error {
	payloadJSON := "{}"
	if len(payload) > 0 {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		payloadJSON = string(encoded)
	}
	var userID any = nil
	if user != nil && user.ID > 0 {
		userID = user.ID
	}
	_, err := exec.Exec(`
		INSERT INTO audit_events (tenant_id, event_type, entity_type, entity_id, user_id, source, payload_json, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, tenantIDFromUser(user), strings.TrimSpace(eventType), strings.TrimSpace(entityType), strings.TrimSpace(entityID), userID, normalizeAuditSource(source), payloadJSON, time.Now().Format(time.RFC3339))
	return err
}

func withAPIAuditMetadata(r *http.Request, payload map[string]any) map[string]any {
	if payload == nil {
		payload = map[string]any{}
	}
	if integrationName := apiIntegrationNameFromContext(r); integrationName != "" {
		payload["integration_name"] = integrationName
	}
	return payload
}

func writeAPIJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeAPIError(w http.ResponseWriter, status int, message string, fields map[string]string) {
	resp := map[string]any{
		"ok":    false,
		"error": message,
	}
	if len(fields) > 0 {
		resp["fields"] = fields
	}
	writeAPIJSON(w, status, resp)
}

type requestError struct {
	Status  int
	Message string
	Fields  map[string]string
}

func (e requestError) Error() string {
	return e.Message
}

type retomaOperationInput struct {
	ProductID      string
	Quantity       int
	ValueReceived  float64
	ReceivedState  string
	PublishToStock bool
	FinalSalePrice *float64
	Notes          string
}

type retomaOperationResult struct {
	RetomaID         int64
	ProductID        string
	ProductName      string
	Quantity         int
	ValueReceived    float64
	ReceivedState    string
	PublishedToStock bool
	UnitsCreated     int
	FinalSalePrice   *float64
	Message          string
}

type inventoryAdjustInput struct {
	ProductID      string
	TargetQuantity *int
	Notes          string
	SalePrice      *float64
	Name           *string
	RetomaEnabled  *bool
	RetomaPrice    *float64
}

type inventoryAdjustResult struct {
	ProductID        string
	PreviousQuantity int
	CurrentQuantity  int
	Delta            int
	Message          string
}

type creditSaleCreateInput struct {
	Kind              creditSaleKind
	ProductID         string
	ProductName       string
	Quantity          int
	Customer          *Customer
	InstallmentsTotal int
	TotalValue        float64
	InterestPercent   float64
	InstallmentValue  float64
	Notes             string
}

type creditSaleCreateResult struct {
	CreditSaleID     int64
	CustomerID       int
	Kind             creditSaleKind
	ProductID        string
	ProductName      string
	Quantity         int
	InstallmentValue float64
	DebtTotal        float64
	TotalPaid        float64
	CurrentDebt      float64
	Message          string
}

type creditSaleUpdateInput struct {
	InstallmentsTotal int
	InstallmentsPaid  int
	InstallmentValue  float64
	Notes             string
	Status            creditStatus
}

type creditSaleUpdateResult struct {
	CreditSaleID        int
	CustomerID          int
	Kind                creditSaleKind
	ProductID           string
	ProductName         string
	Quantity            int
	InstallmentsTotal   int
	InstallmentsPaid    int
	ActualQuotaPayments int
	TotalValue          float64
	InterestPercent     float64
	InstallmentValue    float64
	Notes               string
	Status              creditStatus
	DebtTotal           float64
	TotalPaid           float64
	CurrentDebt         float64
}

type apiCreditPayload struct {
	Kind                   string   `json:"kind"`
	ProductID              string   `json:"product_id"`
	Quantity               int      `json:"quantity"`
	CustomerID             int      `json:"customer_id"`
	CustomerName           string   `json:"customer_name"`
	CustomerPhone          string   `json:"customer_phone"`
	CustomerDocumentType   string   `json:"customer_document_type"`
	CustomerDocumentNumber string   `json:"customer_document_number"`
	CustomerAddress        string   `json:"customer_address"`
	CustomerCity           string   `json:"customer_city"`
	CustomerNotes          string   `json:"customer_notes"`
	DebtorName             string   `json:"debtor_name"`
	DebtorDocumentType     string   `json:"debtor_document_type"`
	DebtorDocumentNumber   string   `json:"debtor_document_number"`
	DebtorPhone            string   `json:"debtor_phone"`
	InstallmentsTotal      int      `json:"installments_total"`
	TotalValue             float64  `json:"total_value"`
	InterestPercent        float64  `json:"interest_percent"`
	InstallmentValue       *float64 `json:"installment_value"`
	Notes                  string   `json:"notes"`
}

func registerRetoma(db *sql.DB, currentUser *User, input retomaOperationInput, source string, decoratePayload func(map[string]any) map[string]any) (retomaOperationResult, error) {
	input.ProductID = strings.TrimSpace(input.ProductID)
	input.ReceivedState = strings.TrimSpace(input.ReceivedState)
	input.Notes = strings.TrimSpace(input.Notes)
	tenantID := tenantIDFromUser(currentUser)

	_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantID)
	if err != nil {
		return retomaOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo cargar la configuración de movimientos."}
	}
	if !movementEnabled(movementEnabledMap, "retoma") {
		return retomaOperationResult{}, requestError{Status: http.StatusForbidden, Message: "La retoma está deshabilitada en Configuración."}
	}

	fields := map[string]string{}
	if input.ProductID == "" {
		fields["product_id"] = "Selecciona un producto válido."
	}
	if input.Quantity <= 0 {
		fields["quantity"] = "La cantidad debe ser mayor a 0."
	}
	if input.ValueReceived < 0 {
		fields["value_received"] = "El valor recibido debe ser mayor o igual a 0."
	}
	validStates := map[string]struct{}{
		"Nuevo":          {},
		"Usado":          {},
		"Dañado":         {},
		"Para repuestos": {},
		"Otro":           {},
	}
	if _, ok := validStates[input.ReceivedState]; !ok {
		fields["received_state"] = "Selecciona un estado recibido válido."
	}
	if input.FinalSalePrice != nil && *input.FinalSalePrice < 0 {
		fields["final_sale_price"] = "El precio final de venta debe ser mayor o igual a 0."
	}
	if len(fields) > 0 {
		return retomaOperationResult{}, requestError{Status: http.StatusBadRequest, Message: "Datos inválidos.", Fields: fields}
	}

	allowed, err := productAccessibleByID(db, currentUser, input.ProductID)
	if err != nil {
		return retomaOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo validar acceso al producto."}
	}
	if !allowed {
		return retomaOperationResult{}, requestError{Status: http.StatusForbidden, Message: "No tienes acceso a este producto."}
	}

	var (
		productName      string
		retomaEnabled    int
		defaultRetomaRaw sql.NullFloat64
	)
	if err := db.QueryRow(`
		SELECT nombre, COALESCE(retoma_enabled, 0), retoma_price
		FROM productos
		WHERE tenant_id = ? AND sku = ?
	`, tenantID, input.ProductID).Scan(&productName, &retomaEnabled, &defaultRetomaRaw); err != nil {
		if err == sql.ErrNoRows {
			return retomaOperationResult{}, requestError{Status: http.StatusNotFound, Message: "Producto no encontrado."}
		}
		return retomaOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo cargar el producto."}
	}
	if retomaEnabled != 1 {
		return retomaOperationResult{}, requestError{Status: http.StatusForbidden, Message: "Este producto no tiene retoma habilitada."}
	}

	tx, err := db.Begin()
	if err != nil {
		return retomaOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo iniciar la transacción."}
	}
	defer tx.Rollback()

	now := time.Now().Format(time.RFC3339)
	precioPublicado := sql.NullFloat64{}
	if input.PublishToStock && input.FinalSalePrice != nil {
		precioPublicado = sql.NullFloat64{Float64: *input.FinalSalePrice, Valid: true}
	}
	retomaID, err := insertAndReturnID(tx, `
		INSERT INTO retomas (tenant_id, producto_id, cantidad, valor_recibido, estado_recibido, publicado_stock, precio_publicado, notas, fecha)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, tenantID, input.ProductID, input.Quantity, input.ValueReceived, input.ReceivedState, boolToInt(input.PublishToStock), precioPublicado, input.Notes, now)
	if err != nil {
		return retomaOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la retoma."}
	}

	unitIDs := make([]string, 0, input.Quantity)
	baseID := time.Now().UnixNano()
	for i := 0; i < input.Quantity; i++ {
		unitIDs = append(unitIDs, fmt.Sprintf("RETOMA-%s-%d-%d", input.ProductID, baseID, i+1))
	}
	movementNote := fmt.Sprintf("Estado recibido: %s | Valor recibido: %s", input.ReceivedState, formatCurrency(input.ValueReceived))
	if input.Notes != "" {
		movementNote += " | " + input.Notes
	}
	if err := logMovimientos(tx, input.ProductID, unitIDs, "retoma", movementNote, currentUser, now); err != nil {
		return retomaOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar el movimiento de retoma."}
	}

	stockCreatedIDs := make([]string, 0, input.Quantity)
	if input.PublishToStock {
		if precioPublicado.Valid {
			if _, err := tx.Exec(`UPDATE productos SET precio_venta = ? WHERE tenant_id = ? AND sku = ?`, precioPublicado.Float64, tenantID, input.ProductID); err != nil {
				return retomaOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo actualizar el precio final del producto."}
			}
		}
		baseID = time.Now().UnixNano()
		for i := 0; i < input.Quantity; i++ {
			unitID := fmt.Sprintf("U-%s-RET-%d-%d", input.ProductID, baseID, i+1)
			if _, err := tx.Exec(
				`INSERT INTO unidades (id, tenant_id, producto_id, estado, creado_en, caducidad) VALUES (?, ?, ?, ?, ?, ?)`,
				unitID, tenantID, input.ProductID, "Disponible", now, nil,
			); err != nil {
				return retomaOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo publicar la retoma al stock."}
			}
			stockCreatedIDs = append(stockCreatedIDs, unitID)
		}
		stockNote := fmt.Sprintf("Retoma publicada a stock | Estado recibido: %s", input.ReceivedState)
		if precioPublicado.Valid {
			stockNote += " | Precio final: " + formatCurrency(precioPublicado.Float64)
		}
		if input.Notes != "" {
			stockNote += " | " + input.Notes
		}
		if err := logMovimientos(tx, input.ProductID, stockCreatedIDs, "retoma_stock", stockNote, currentUser, now); err != nil {
			return retomaOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar el ingreso a stock de la retoma."}
		}
	}

	auditPayload := map[string]any{
		"retoma_id":            retomaID,
		"product_id":           input.ProductID,
		"product_name":         productName,
		"quantity":             input.Quantity,
		"value_received":       input.ValueReceived,
		"estado_recibido":      input.ReceivedState,
		"published_to_stock":   input.PublishToStock,
		"units_created":        len(stockCreatedIDs),
		"notas":                input.Notes,
		"default_retoma_price": defaultRetomaRaw,
	}
	if precioPublicado.Valid {
		auditPayload["final_sale_price"] = precioPublicado.Float64
	} else {
		auditPayload["final_sale_price"] = nil
	}
	if decoratePayload != nil {
		auditPayload = decoratePayload(auditPayload)
	}
	if err := logAuditEvent(tx, currentUser, "retoma_registered", "retoma", input.ProductID, source, auditPayload); err != nil {
		return retomaOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la auditoría de la retoma."}
	}

	if err := tx.Commit(); err != nil {
		return retomaOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo confirmar la retoma."}
	}

	var finalSalePrice *float64
	if precioPublicado.Valid {
		value := precioPublicado.Float64
		finalSalePrice = &value
	}
	message := "Retoma registrada correctamente."
	if input.PublishToStock {
		message = "Retoma registrada y publicada a stock correctamente."
	}
	return retomaOperationResult{
		RetomaID:         retomaID,
		ProductID:        input.ProductID,
		ProductName:      productName,
		Quantity:         input.Quantity,
		ValueReceived:    input.ValueReceived,
		ReceivedState:    input.ReceivedState,
		PublishedToStock: input.PublishToStock,
		UnitsCreated:     len(stockCreatedIDs),
		FinalSalePrice:   finalSalePrice,
		Message:          message,
	}, nil
}

func adjustInventoryProduct(db *sql.DB, currentUser *User, input inventoryAdjustInput, source string, decoratePayload func(map[string]any) map[string]any) (inventoryAdjustResult, error) {
	input.ProductID = strings.TrimSpace(input.ProductID)
	input.Notes = strings.TrimSpace(input.Notes)
	tenantID := tenantIDFromUser(currentUser)
	if input.Name != nil {
		trimmed := strings.TrimSpace(*input.Name)
		input.Name = &trimmed
	}

	fields := map[string]string{}
	if input.ProductID == "" {
		fields["product_id"] = "Selecciona un producto válido."
	}
	if input.TargetQuantity == nil && input.SalePrice == nil && input.Name == nil && input.RetomaEnabled == nil {
		fields["changes"] = "Debes enviar al menos un cambio para actualizar inventario."
	}
	if input.TargetQuantity != nil && *input.TargetQuantity < 0 {
		fields["target_quantity"] = "La cantidad objetivo debe ser mayor o igual a 0."
	}
	if input.SalePrice != nil && *input.SalePrice < 0 {
		fields["sale_price"] = "El precio de venta debe ser mayor o igual a 0."
	}
	if input.Name != nil && *input.Name == "" {
		fields["name"] = "El nombre no puede estar vacío."
	}
	if input.RetomaPrice != nil && *input.RetomaPrice < 0 {
		fields["retoma_price"] = "El valor de retoma debe ser mayor o igual a 0."
	}
	if input.RetomaEnabled == nil && input.RetomaPrice != nil {
		fields["retoma_enabled"] = "Debes indicar retoma_enabled para actualizar la configuración de retoma."
	}
	if input.RetomaEnabled != nil && *input.RetomaEnabled && input.RetomaPrice == nil {
		fields["retoma_price"] = "El valor de retoma es obligatorio cuando retoma_enabled es true."
	}
	if len(fields) > 0 {
		return inventoryAdjustResult{}, requestError{Status: http.StatusBadRequest, Message: "Datos inválidos.", Fields: fields}
	}

	allowed, err := productAccessibleByID(db, currentUser, input.ProductID)
	if err != nil {
		return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo validar acceso al producto."}
	}
	if !allowed {
		return inventoryAdjustResult{}, requestError{Status: http.StatusForbidden, Message: "No tienes acceso a este producto."}
	}

	var currentSalePrice float64
	if err := db.QueryRow(`SELECT COALESCE(precio_venta, 0) FROM productos WHERE tenant_id = ? AND sku = ?`, tenantID, input.ProductID).Scan(&currentSalePrice); err != nil {
		if err == sql.ErrNoRows {
			return inventoryAdjustResult{}, requestError{Status: http.StatusNotFound, Message: "Producto no encontrado."}
		}
		return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo cargar el producto."}
	}
	priceForValidation := currentSalePrice
	if input.SalePrice != nil {
		priceForValidation = *input.SalePrice
	}
	if input.RetomaEnabled != nil && *input.RetomaEnabled && input.RetomaPrice != nil && priceForValidation > 0 && *input.RetomaPrice > priceForValidation {
		fields["retoma_price"] = "El valor de retoma no debe superar el valor de venta."
		return inventoryAdjustResult{}, requestError{Status: http.StatusBadRequest, Message: "Datos inválidos.", Fields: fields}
	}

	tx, err := db.Begin()
	if err != nil {
		return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo iniciar la transacción."}
	}
	defer tx.Rollback()

	rows, err := tx.Query(`
		SELECT id
		FROM unidades
		WHERE tenant_id = ? AND producto_id = ? AND estado IN ('Disponible', 'available')
		ORDER BY creado_en DESC, id DESC
	`, tenantID, input.ProductID)
	if err != nil {
		return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo consultar el stock actual."}
	}
	availableIDs := make([]string, 0, 64)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo leer el stock actual."}
		}
		availableIDs = append(availableIDs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo procesar el stock actual."}
	}
	rows.Close()

	current := len(availableIDs)
	target := current
	if input.TargetQuantity != nil {
		target = *input.TargetQuantity
	}
	delta := target - current
	now := time.Now().Format(time.RFC3339)
	if delta > 0 {
		createdIDs := make([]string, 0, delta)
		baseID := time.Now().UnixNano()
		for i := 0; i < delta; i++ {
			unitID := fmt.Sprintf("U-%s-AJ-%d-%d", input.ProductID, baseID, i)
			if _, err := tx.Exec(
				`INSERT INTO unidades (id, tenant_id, producto_id, estado, creado_en, caducidad) VALUES (?, ?, ?, ?, ?, ?)`,
				unitID, tenantID, input.ProductID, "Disponible", now, nil,
			); err != nil {
				return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo incrementar el stock."}
			}
			createdIDs = append(createdIDs, unitID)
		}
		logNote := input.Notes
		if logNote == "" {
			logNote = fmt.Sprintf("Ajuste manual de stock: %d -> %d", current, target)
		}
		if err := logMovimientos(tx, input.ProductID, createdIDs, "ajuste_stock_entrada", logNote, currentUser, now); err != nil {
			return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar el ajuste."}
		}
	} else if delta < 0 {
		removeCount := -delta
		if removeCount > len(availableIDs) {
			return inventoryAdjustResult{}, requestError{Status: http.StatusBadRequest, Message: "No hay stock suficiente para reducir a ese valor.", Fields: map[string]string{"target_quantity": "No hay stock suficiente para reducir a ese valor."}}
		}
		removeIDs := availableIDs[:removeCount]
		placeholders := make([]string, len(removeIDs))
		args := make([]any, 0, len(removeIDs)+1)
		for i, id := range removeIDs {
			placeholders[i] = "?"
			args = append(args, id)
		}
		args = append(args, input.ProductID)
		query := fmt.Sprintf(
			"DELETE FROM unidades WHERE tenant_id = ? AND id IN (%s) AND producto_id = ? AND estado IN ('Disponible', 'available')",
			strings.Join(placeholders, ","),
		)
		args = append([]any{tenantID}, args...)
		res, err := tx.Exec(query, args...)
		if err != nil {
			return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo reducir el stock."}
		}
		affected, err := res.RowsAffected()
		if err != nil || int(affected) != removeCount {
			return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo confirmar el ajuste de stock."}
		}
		logNote := input.Notes
		if logNote == "" {
			logNote = fmt.Sprintf("Ajuste manual de stock: %d -> %d", current, target)
		}
		if err := logMovimientos(tx, input.ProductID, removeIDs, "ajuste_stock_salida", logNote, currentUser, now); err != nil {
			return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar el ajuste."}
		}
	}

	updatedFields := map[string]any{}
	if input.SalePrice != nil {
		res, err := tx.Exec(`UPDATE productos SET precio_venta = ? WHERE tenant_id = ? AND sku = ?`, *input.SalePrice, tenantID, input.ProductID)
		if err != nil {
			return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo actualizar el precio de venta."}
		}
		affected, err := res.RowsAffected()
		if err != nil || affected == 0 {
			return inventoryAdjustResult{}, requestError{Status: http.StatusBadRequest, Message: "Producto inválido para actualizar precio."}
		}
		updatedFields["sale_price"] = *input.SalePrice
	}
	if input.Name != nil {
		res, err := tx.Exec(`UPDATE productos SET nombre = ? WHERE tenant_id = ? AND sku = ?`, *input.Name, tenantID, input.ProductID)
		if err != nil {
			return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo actualizar el nombre del producto."}
		}
		affected, err := res.RowsAffected()
		if err != nil || affected == 0 {
			return inventoryAdjustResult{}, requestError{Status: http.StatusBadRequest, Message: "Producto inválido para actualizar nombre."}
		}
		updatedFields["name"] = *input.Name
	}
	if input.RetomaEnabled != nil {
		var newRetomaPrice sql.NullFloat64
		if *input.RetomaEnabled && input.RetomaPrice != nil {
			newRetomaPrice = sql.NullFloat64{Float64: *input.RetomaPrice, Valid: true}
		}
		res, err := tx.Exec(`UPDATE productos SET retoma_enabled = ?, retoma_price = ? WHERE tenant_id = ? AND sku = ?`, boolToInt(*input.RetomaEnabled), newRetomaPrice, tenantID, input.ProductID)
		if err != nil {
			return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo actualizar la configuración de retoma."}
		}
		affected, err := res.RowsAffected()
		if err != nil || affected == 0 {
			return inventoryAdjustResult{}, requestError{Status: http.StatusBadRequest, Message: "Producto inválido para actualizar retoma."}
		}
		updatedFields["retoma_enabled"] = *input.RetomaEnabled
		if newRetomaPrice.Valid {
			updatedFields["retoma_price"] = newRetomaPrice.Float64
		} else {
			updatedFields["retoma_price"] = nil
		}
	}

	if delta != 0 {
		auditPayload := map[string]any{
			"product_id":        input.ProductID,
			"previous_quantity": current,
			"target_quantity":   target,
			"current_quantity":  target,
			"delta":             delta,
			"notes":             input.Notes,
		}
		if decoratePayload != nil {
			auditPayload = decoratePayload(auditPayload)
		}
		if err := logAuditEvent(tx, currentUser, "inventory_adjusted", "product", input.ProductID, source, auditPayload); err != nil {
			return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la auditoría del ajuste de inventario."}
		}
	}
	if len(updatedFields) > 0 {
		updatedFields["product_id"] = input.ProductID
		if decoratePayload != nil {
			updatedFields = decoratePayload(updatedFields)
		}
		if err := logAuditEvent(tx, currentUser, "product_updated", "product", input.ProductID, source, updatedFields); err != nil {
			return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la auditoría del producto."}
		}
	}

	if err := tx.Commit(); err != nil {
		return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo confirmar la transacción."}
	}

	message := "Inventario actualizado correctamente."
	switch {
	case delta != 0 && input.SalePrice != nil:
		message = "Stock y precio de venta actualizados correctamente."
	case delta == 0 && input.SalePrice != nil:
		message = "Precio de venta actualizado correctamente."
	}
	if input.Name != nil && delta == 0 && input.SalePrice == nil {
		message = "Nombre del producto actualizado correctamente."
	} else if input.Name != nil && delta == 0 && input.SalePrice != nil {
		message = "Nombre y precio de venta actualizados correctamente."
	} else if input.Name != nil && delta != 0 && input.SalePrice == nil {
		message = "Stock y nombre del producto actualizados correctamente."
	} else if input.Name != nil && delta != 0 && input.SalePrice != nil {
		message = "Stock, nombre y precio de venta actualizados correctamente."
	}
	if input.RetomaEnabled != nil && delta == 0 && input.SalePrice == nil && input.Name == nil {
		message = "Configuración de retoma actualizada correctamente."
	} else if input.RetomaEnabled != nil && delta == 0 && input.SalePrice != nil && input.Name == nil {
		message = "Precio de venta y retoma actualizados correctamente."
	} else if input.RetomaEnabled != nil && delta == 0 && input.Name != nil && input.SalePrice == nil {
		message = "Nombre y configuración de retoma actualizados correctamente."
	} else if input.RetomaEnabled != nil && delta == 0 && input.Name != nil && input.SalePrice != nil {
		message = "Nombre, precio de venta y retoma actualizados correctamente."
	} else if input.RetomaEnabled != nil && delta != 0 && input.SalePrice == nil && input.Name == nil {
		message = "Stock y configuración de retoma actualizados correctamente."
	} else if input.RetomaEnabled != nil && delta != 0 && input.SalePrice != nil && input.Name == nil {
		message = "Stock, precio de venta y retoma actualizados correctamente."
	} else if input.RetomaEnabled != nil && delta != 0 && input.Name != nil && input.SalePrice == nil {
		message = "Stock, nombre y configuración de retoma actualizados correctamente."
	} else if input.RetomaEnabled != nil && delta != 0 && input.Name != nil && input.SalePrice != nil {
		message = "Stock, nombre y precio de venta y retoma actualizados correctamente."
	} else if delta == 0 && input.SalePrice == nil && input.Name == nil && input.RetomaEnabled == nil {
		message = "Stock sin cambios."
	}

	return inventoryAdjustResult{
		ProductID:        input.ProductID,
		PreviousQuantity: current,
		CurrentQuantity:  target,
		Delta:            delta,
		Message:          message,
	}, nil
}

type creditInstallmentResult struct {
	CreditSaleID      int
	CustomerID        int
	Kind              creditSaleKind
	ProductID         string
	ProductName       string
	DebtorName        string
	InstallmentsTotal int
	InstallmentsPaid  int
	TotalValue        float64
	DebtTotal         float64
	TotalPaid         float64
	CurrentDebt       float64
	InterestPercent   float64
	InstallmentValue  float64
	AmountPaid        float64
	InstallmentNumber int
	PaymentType       creditPaymentType
}

func createCreditSale(tx *sql.Tx, currentUser *User, input creditSaleCreateInput, source string, decoratePayload func(map[string]any) map[string]any) (creditSaleCreateResult, error) {
	tenantID := tenantIDFromUser(currentUser)
	input.Kind = normalizeCreditSaleKind(string(input.Kind))
	input.ProductID = strings.TrimSpace(input.ProductID)
	input.ProductName = strings.TrimSpace(input.ProductName)
	input.Notes = strings.TrimSpace(input.Notes)
	if input.Quantity <= 0 {
		input.Quantity = 1
	}
	if input.Customer == nil || input.Customer.ID <= 0 {
		return creditSaleCreateResult{}, requestError{Status: http.StatusBadRequest, Message: "Cliente inválido para el crédito."}
	}
	if input.Kind == creditSaleKindCash {
		input.ProductID = ""
		if input.ProductName == "" {
			input.ProductName = "Préstamo de dinero"
		}
	} else if input.ProductName == "" {
		input.ProductName = input.ProductID
	}

	debtTotal := creditDebtTotal(input.InstallmentsTotal, input.InstallmentValue)
	now := time.Now().Format(time.RFC3339)
	summaryPrefix := "VENTA A CREDITO"
	if input.Kind == creditSaleKindCash {
		summaryPrefix = "PRESTAMO DE DINERO"
	}
	storedNotes := fmt.Sprintf("%s | Cliente: %s | Cuotas: %d | Interes: %.2f%% | Valor cuota: %.2f", summaryPrefix, input.Customer.Name, input.InstallmentsTotal, input.InterestPercent, input.InstallmentValue)
	if input.Notes != "" {
		storedNotes += " | " + input.Notes
	}
	var productIDValue any = input.ProductID
	if input.ProductID == "" {
		productIDValue = nil
	}

	creditSaleID, err := insertAndReturnID(tx, `
		INSERT INTO credit_sales (tenant_id, customer_id, kind, product_id, quantity, debtor_name, debtor_document_type, debtor_document_number, debtor_phone, installments_total, installments_paid, total_value, interest_percent, installment_value, notes, status, created_at, created_by)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)
	`, tenantID, input.Customer.ID, string(input.Kind), productIDValue, input.Quantity, input.Customer.Name, input.Customer.DocumentType, input.Customer.DocumentNumber, input.Customer.Phone, input.InstallmentsTotal, input.TotalValue, input.InterestPercent, input.InstallmentValue, storedNotes, string(creditStatusActive), now, nullableUserID(currentUser))
	if err != nil {
		return creditSaleCreateResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar el crédito."}
	}

	if err := logCustomerEvent(tx, currentUser, input.Customer.ID, "credit_created", "credit_sale", strconv.FormatInt(creditSaleID, 10), debtTotal, map[string]any{
		"kind":               string(input.Kind),
		"kind_label":         creditKindLabel(input.Kind),
		"product_id":         input.ProductID,
		"product_name":       input.ProductName,
		"quantity":           input.Quantity,
		"installments_total": input.InstallmentsTotal,
		"installment_value":  input.InstallmentValue,
		"current_debt":       debtTotal,
	}); err != nil {
		return creditSaleCreateResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la trazabilidad del cliente."}
	}

	auditPayload := map[string]any{
		"credit_sale_id":           creditSaleID,
		"customer_id":              input.Customer.ID,
		"customer_name":            input.Customer.Name,
		"customer_phone":           input.Customer.Phone,
		"customer_document_type":   input.Customer.DocumentType,
		"customer_document_number": input.Customer.DocumentNumber,
		"customer_address":         input.Customer.Address,
		"customer_city":            input.Customer.City,
		"kind":                     string(input.Kind),
		"kind_label":               creditKindLabel(input.Kind),
		"product_id":               input.ProductID,
		"product_name":             input.ProductName,
		"debtor_name":              input.Customer.Name,
		"debtor_document_type":     input.Customer.DocumentType,
		"debtor_document_number":   input.Customer.DocumentNumber,
		"debtor_phone":             input.Customer.Phone,
		"installments_total":       input.InstallmentsTotal,
		"installments_paid":        0,
		"total_value":              input.TotalValue,
		"debt_total":               debtTotal,
		"total_paid":               0,
		"current_debt":             debtTotal,
		"interest_percent":         input.InterestPercent,
		"installment_value":        input.InstallmentValue,
		"quantity":                 input.Quantity,
	}
	if decoratePayload != nil {
		auditPayload = decoratePayload(auditPayload)
	}
	if err := logAuditEvent(tx, currentUser, "credit_sale_created", "credit_sale", strconv.FormatInt(creditSaleID, 10), source, auditPayload); err != nil {
		return creditSaleCreateResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la auditoría del crédito."}
	}

	message := "Venta a crédito registrada correctamente."
	if input.Kind == creditSaleKindCash {
		message = "Préstamo registrado correctamente."
	}
	return creditSaleCreateResult{
		CreditSaleID:     creditSaleID,
		CustomerID:       input.Customer.ID,
		Kind:             input.Kind,
		ProductID:        input.ProductID,
		ProductName:      input.ProductName,
		Quantity:         input.Quantity,
		InstallmentValue: input.InstallmentValue,
		DebtTotal:        debtTotal,
		TotalPaid:        0,
		CurrentDebt:      debtTotal,
		Message:          message,
	}, nil
}

func updateCreditSale(db *sql.DB, currentUser *User, creditSaleID int, input creditSaleUpdateInput, source string, decoratePayload func(map[string]any) map[string]any) (creditSaleUpdateResult, error) {
	if currentUser == nil || !isAdminRole(currentUser.Role) {
		return creditSaleUpdateResult{}, requestError{Status: http.StatusForbidden, Message: "Solo administrador puede editar créditos."}
	}
	tenantID := tenantIDFromUser(currentUser)
	input.Notes = strings.TrimSpace(input.Notes)
	input.Status = normalizeEditableCreditStatus(string(input.Status))
	if input.InstallmentsTotal <= 0 {
		return creditSaleUpdateResult{}, requestError{Status: http.StatusBadRequest, Message: "El número de cuotas debe ser mayor a 0.", Fields: map[string]string{"installments_total": "El número de cuotas debe ser mayor a 0."}}
	}
	if input.InstallmentValue <= 0 {
		return creditSaleUpdateResult{}, requestError{Status: http.StatusBadRequest, Message: "El valor de cuota debe ser mayor a 0.", Fields: map[string]string{"installment_value": "El valor de cuota debe ser mayor a 0."}}
	}

	var result creditSaleUpdateResult
	var statusRaw string
	if err := db.QueryRow(`
		SELECT
			cs.id,
			COALESCE(cs.customer_id, 0),
			COALESCE(cs.kind, ?),
			COALESCE(cs.product_id, ''),
			CASE
				WHEN COALESCE(cs.kind, ?) = ? THEN 'Préstamo de dinero'
				ELSE COALESCE(NULLIF(p.nombre, ''), cs.product_id)
			END,
			cs.quantity,
			COALESCE(cs.installments_total, 0),
			COALESCE(cs.installments_paid, 0),
			COALESCE(cs.total_value, 0),
			COALESCE(cs.interest_percent, 0),
			COALESCE(cs.installment_value, 0),
			COALESCE(cs.notes, ''),
			COALESCE(cs.status, ''),
			COALESCE((
				SELECT SUM(ci.amount_paid)
				FROM credit_installments ci
				WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id
			), 0),
			COALESCE((
				SELECT COUNT(*)
				FROM credit_installments ci
				WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id AND COALESCE(ci.payment_type, 'cuota') = 'cuota'
			), 0)
		FROM credit_sales cs
		LEFT JOIN productos p ON p.sku = cs.product_id AND p.tenant_id = cs.tenant_id
		WHERE cs.tenant_id = ? AND cs.id = ?
		LIMIT 1
	`, string(creditSaleKindProduct), string(creditSaleKindProduct), string(creditSaleKindCash), tenantID, creditSaleID).Scan(
		&result.CreditSaleID,
		&result.CustomerID,
		&result.Kind,
		&result.ProductID,
		&result.ProductName,
		&result.Quantity,
		&result.InstallmentsTotal,
		&result.InstallmentsPaid,
		&result.TotalValue,
		&result.InterestPercent,
		&result.InstallmentValue,
		&result.Notes,
		&statusRaw,
		&result.TotalPaid,
		&result.ActualQuotaPayments,
	); err != nil {
		if err == sql.ErrNoRows {
			return creditSaleUpdateResult{}, requestError{Status: http.StatusNotFound, Message: "Crédito no encontrado."}
		}
		return creditSaleUpdateResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo cargar el crédito."}
	}

	if result.Kind == creditSaleKindProduct {
		allowed, err := productAccessibleByID(db, currentUser, result.ProductID)
		if err != nil {
			return creditSaleUpdateResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo validar acceso al producto."}
		}
		if !allowed {
			return creditSaleUpdateResult{}, requestError{Status: http.StatusForbidden, Message: "No tienes acceso a este crédito."}
		}
	}

	if input.InstallmentsPaid < result.ActualQuotaPayments {
		return creditSaleUpdateResult{}, requestError{Status: http.StatusBadRequest, Message: "No puedes dejar cuotas pagadas por debajo de los pagos ya registrados.", Fields: map[string]string{"installments_paid": "No puedes dejar cuotas pagadas por debajo de los pagos ya registrados."}}
	}
	if input.InstallmentsPaid > input.InstallmentsTotal {
		return creditSaleUpdateResult{}, requestError{Status: http.StatusBadRequest, Message: "Las cuotas pagadas no pueden superar el total de cuotas.", Fields: map[string]string{"installments_paid": "Las cuotas pagadas no pueden superar el total de cuotas."}}
	}

	previousInstallmentsTotal := result.InstallmentsTotal
	previousInstallmentsPaid := result.InstallmentsPaid
	previousInstallmentValue := roundedMoney(result.InstallmentValue)
	previousNotes := result.Notes
	previousDebtTotal := roundedMoney(creditDebtTotal(previousInstallmentsTotal, previousInstallmentValue))
	previousLegacyTotalPaid := roundedMoney(float64(previousInstallmentsPaid) * previousInstallmentValue)
	previousTotalPaid := roundedMoney(result.TotalPaid)
	if previousTotalPaid < previousLegacyTotalPaid {
		previousTotalPaid = previousLegacyTotalPaid
	}
	previousCurrentDebt := roundedMoney(creditCurrentDebt(previousDebtTotal, previousTotalPaid))
	previousStatus := effectiveCreditStatus(statusRaw, previousCurrentDebt, previousDebtTotal)

	debtTotal := creditDebtTotal(input.InstallmentsTotal, input.InstallmentValue)
	legacyTotalPaid := math.Round((float64(input.InstallmentsPaid)*input.InstallmentValue)*100) / 100
	if result.TotalPaid < legacyTotalPaid {
		result.TotalPaid = legacyTotalPaid
	}
	currentDebt := creditCurrentDebt(debtTotal, result.TotalPaid)
	if input.Status == creditStatusCompleted && currentDebt > 0 {
		return creditSaleUpdateResult{}, requestError{Status: http.StatusBadRequest, Message: "No puedes marcar como completado un crédito con deuda actual.", Fields: map[string]string{"status": "No puedes marcar como completado un crédito con deuda actual."}}
	}

	storedStatus := string(input.Status)
	if input.Status != creditStatusSuspended && input.Status != creditStatusCancelled && input.Status != creditStatusCompleted {
		storedStatus = string(creditStatusActive)
	}
	if _, err := db.Exec(`
		UPDATE credit_sales
		SET installments_total = ?, installments_paid = ?, total_value = ?, installment_value = ?, notes = ?, status = ?
		WHERE tenant_id = ? AND id = ?
	`, input.InstallmentsTotal, input.InstallmentsPaid, debtTotal, input.InstallmentValue, input.Notes, storedStatus, tenantID, creditSaleID); err != nil {
		return creditSaleUpdateResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo actualizar el crédito."}
	}

	result.InstallmentsTotal = input.InstallmentsTotal
	result.InstallmentsPaid = input.InstallmentsPaid
	result.TotalValue = debtTotal
	result.InstallmentValue = input.InstallmentValue
	result.Notes = input.Notes
	result.DebtTotal = debtTotal
	result.CurrentDebt = currentDebt
	result.Status = effectiveCreditStatus(storedStatus, currentDebt, debtTotal)
	result.TotalPaid = roundedMoney(result.TotalPaid)
	result.DebtTotal = roundedMoney(result.DebtTotal)
	result.CurrentDebt = roundedMoney(result.CurrentDebt)

	changes := make([]map[string]any, 0, 5)
	changedFields := make([]string, 0, 5)
	if previousInstallmentsTotal != result.InstallmentsTotal {
		changes = append(changes, creditChangeEntry("installments_total", "Cuotas totales", previousInstallmentsTotal, result.InstallmentsTotal))
		changedFields = append(changedFields, "installments_total")
	}
	if previousInstallmentsPaid != result.InstallmentsPaid {
		changes = append(changes, creditChangeEntry("installments_paid", "Cuotas pagadas", previousInstallmentsPaid, result.InstallmentsPaid))
		changedFields = append(changedFields, "installments_paid")
	}
	if previousInstallmentValue != roundedMoney(result.InstallmentValue) {
		changes = append(changes, creditChangeEntry("installment_value", "Valor por cuota", previousInstallmentValue, roundedMoney(result.InstallmentValue)))
		changedFields = append(changedFields, "installment_value")
	}
	if previousNotes != result.Notes {
		changes = append(changes, creditChangeEntry("notes", "Observaciones", previousNotes, result.Notes))
		changedFields = append(changedFields, "notes")
	}
	if previousStatus != result.Status {
		changes = append(changes, creditChangeEntry("status", "Estado", string(previousStatus), string(result.Status)))
		changedFields = append(changedFields, "status")
	}

	impact := map[string]any{
		"debt_total_before":      previousDebtTotal,
		"debt_total_after":       result.DebtTotal,
		"total_paid_before":      previousTotalPaid,
		"total_paid_after":       result.TotalPaid,
		"current_debt_before":    previousCurrentDebt,
		"current_debt_after":     result.CurrentDebt,
		"status_before":          string(previousStatus),
		"status_after":           string(result.Status),
		"status_label_before":    creditStatusLabel(previousStatus),
		"status_label_after":     creditStatusLabel(result.Status),
		"installments_due_after": max(result.InstallmentsTotal-result.InstallmentsPaid, 0),
	}
	if len(changes) == 0 {
		return result, nil
	}

	auditPayload := map[string]any{
		"credit_sale_id":        result.CreditSaleID,
		"customer_id":           result.CustomerID,
		"kind":                  string(result.Kind),
		"kind_label":            creditKindLabel(result.Kind),
		"product_id":            result.ProductID,
		"product_name":          result.ProductName,
		"quantity":              result.Quantity,
		"installments_total":    result.InstallmentsTotal,
		"installments_paid":     result.InstallmentsPaid,
		"actual_quota_payments": result.ActualQuotaPayments,
		"total_value":           result.TotalValue,
		"debt_total":            result.DebtTotal,
		"total_paid":            result.TotalPaid,
		"current_debt":          result.CurrentDebt,
		"interest_percent":      result.InterestPercent,
		"installment_value":     result.InstallmentValue,
		"status":                string(result.Status),
		"status_label":          creditStatusLabel(result.Status),
		"notes":                 result.Notes,
		"changes":               changes,
		"changed_fields":        changedFields,
		"change_count":          len(changes),
		"impact":                impact,
	}
	if decoratePayload != nil {
		auditPayload = decoratePayload(auditPayload)
	}
	if err := logAuditEvent(db, currentUser, "credit_sale_updated", "credit_sale", strconv.Itoa(result.CreditSaleID), source, auditPayload); err != nil {
		return creditSaleUpdateResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la auditoría del crédito."}
	}
	if result.CustomerID > 0 {
		if err := logCustomerEvent(db, currentUser, result.CustomerID, "credit_updated", "credit_sale", strconv.Itoa(result.CreditSaleID), result.CurrentDebt, map[string]any{
			"kind":                  string(result.Kind),
			"kind_label":            creditKindLabel(result.Kind),
			"installments_total":    result.InstallmentsTotal,
			"installments_paid":     result.InstallmentsPaid,
			"actual_quota_payments": result.ActualQuotaPayments,
			"installment_value":     result.InstallmentValue,
			"total_value":           result.TotalValue,
			"current_debt":          result.CurrentDebt,
			"status":                string(result.Status),
			"status_label":          creditStatusLabel(result.Status),
			"changes":               changes,
			"changed_fields":        changedFields,
			"change_count":          len(changes),
			"impact":                impact,
		}); err != nil {
			return creditSaleUpdateResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la trazabilidad del cliente."}
		}
	}

	return result, nil
}

func addCreditInstallment(db *sql.DB, creditSaleID int, amountPaidValue *float64, paymentTypeValue string, currentUser *User, source string, decoratePayload func(map[string]any) map[string]any) (creditInstallmentResult, error) {
	tenantID := tenantIDFromUser(currentUser)
	paymentType := normalizeCreditPaymentType(paymentTypeValue)
	var (
		accessProductID string
		creditKindRaw   string
	)
	if err := db.QueryRow(`SELECT COALESCE(product_id, ''), COALESCE(kind, ?) FROM credit_sales WHERE tenant_id = ? AND id = ?`, string(creditSaleKindProduct), tenantID, creditSaleID).Scan(&accessProductID, &creditKindRaw); err != nil {
		if err == sql.ErrNoRows {
			return creditInstallmentResult{}, requestError{Status: http.StatusNotFound, Message: "Crédito no encontrado."}
		}
		return creditInstallmentResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo cargar el crédito."}
	}
	creditKind := normalizeCreditSaleKind(creditKindRaw)
	if creditKind == creditSaleKindProduct {
		allowed, err := productAccessibleByID(db, currentUser, accessProductID)
		if err != nil {
			return creditInstallmentResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo validar acceso al producto."}
		}
		if !allowed {
			return creditInstallmentResult{}, requestError{Status: http.StatusForbidden, Message: "No tienes acceso a este producto."}
		}
	}

	tx, err := db.Begin()
	if err != nil {
		return creditInstallmentResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo iniciar la transacción."}
	}
	defer tx.Rollback()

	var result creditInstallmentResult
	if err := tx.QueryRow(`
		SELECT cs.id, COALESCE(cs.customer_id, 0), COALESCE(cs.kind, ?), COALESCE(cs.product_id, ''), COALESCE(p.nombre, ''), COALESCE(c.name, cs.debtor_name, ''), COALESCE(cs.installments_total, 0), COALESCE(cs.installments_paid, 0), COALESCE(cs.total_value, 0), COALESCE(cs.interest_percent, 0), COALESCE(cs.installment_value, 0),
		       COALESCE((SELECT SUM(ci.amount_paid) FROM credit_installments ci WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id), 0)
		FROM credit_sales cs
		LEFT JOIN productos p ON p.sku = cs.product_id AND p.tenant_id = cs.tenant_id
		LEFT JOIN customers c ON c.id = cs.customer_id AND c.tenant_id = cs.tenant_id
		WHERE cs.tenant_id = ? AND cs.id = ?
		LIMIT 1
	`, string(creditSaleKindProduct), tenantID, creditSaleID).Scan(&result.CreditSaleID, &result.CustomerID, &creditKindRaw, &result.ProductID, &result.ProductName, &result.DebtorName, &result.InstallmentsTotal, &result.InstallmentsPaid, &result.TotalValue, &result.InterestPercent, &result.InstallmentValue, &result.TotalPaid); err != nil {
		if err == sql.ErrNoRows {
			return creditInstallmentResult{}, requestError{Status: http.StatusNotFound, Message: "Crédito no encontrado."}
		}
		return creditInstallmentResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo cargar el crédito."}
	}
	result.Kind = normalizeCreditSaleKind(creditKindRaw)
	if result.Kind == creditSaleKindCash && result.ProductName == "" {
		result.ProductName = "Préstamo de dinero"
	}
	result.DebtTotal = creditDebtTotal(result.InstallmentsTotal, result.InstallmentValue)
	legacyTotalPaid := math.Round((float64(result.InstallmentsPaid)*result.InstallmentValue)*100) / 100
	if result.TotalPaid < legacyTotalPaid {
		result.TotalPaid = legacyTotalPaid
	}
	result.CurrentDebt = creditCurrentDebt(result.DebtTotal, result.TotalPaid)
	result.PaymentType = paymentType
	if result.InstallmentsTotal <= 0 {
		return creditInstallmentResult{}, requestError{Status: http.StatusBadRequest, Message: "El crédito no tiene cuotas configuradas."}
	}
	if result.CurrentDebt <= 0 && result.DebtTotal > 0 {
		return creditInstallmentResult{}, requestError{Status: http.StatusBadRequest, Message: "Este crédito ya está completamente pagado."}
	}
	if paymentType == creditPaymentTypeCuota && result.InstallmentsPaid >= result.InstallmentsTotal {
		return creditInstallmentResult{}, requestError{Status: http.StatusBadRequest, Message: "Este crédito ya no tiene cuotas pendientes."}
	}

	amountPaid := result.InstallmentValue
	if amountPaidValue != nil {
		if *amountPaidValue <= 0 {
			return creditInstallmentResult{}, requestError{Status: http.StatusBadRequest, Message: "El valor abonado debe ser mayor a 0."}
		}
		amountPaid = *amountPaidValue
	}
	if result.CurrentDebt > 0 && amountPaid > result.CurrentDebt {
		return creditInstallmentResult{}, requestError{Status: http.StatusBadRequest, Message: "El valor pagado supera la deuda actual."}
	}

	result.InstallmentNumber = result.InstallmentsPaid
	if paymentType == creditPaymentTypeCuota {
		result.InstallmentNumber = result.InstallmentsPaid + 1
	}
	result.AmountPaid = amountPaid
	now := time.Now().Format(time.RFC3339)
	var installmentProductID any = result.ProductID
	if result.ProductID == "" {
		installmentProductID = nil
	}
	if _, err := tx.Exec(`
		INSERT INTO credit_installments (tenant_id, credit_sale_id, product_id, installment_number, amount_paid, payment_type, created_at, created_by)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, tenantID, result.CreditSaleID, installmentProductID, result.InstallmentNumber, amountPaid, string(paymentType), now, nullableUserID(currentUser)); err != nil {
		return creditInstallmentResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar el pago."}
	}
	if paymentType == creditPaymentTypeCuota {
		if _, err := tx.Exec(`
			UPDATE credit_sales
			SET installments_paid = installments_paid + 1
			WHERE tenant_id = ? AND id = ? AND installments_paid < installments_total
		`, tenantID, result.CreditSaleID); err != nil {
			return creditInstallmentResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo actualizar el crédito."}
		}
	}
	result.TotalPaid = math.Round((result.TotalPaid+amountPaid)*100) / 100
	result.CurrentDebt = creditCurrentDebt(result.DebtTotal, result.TotalPaid)
	if paymentType == creditPaymentTypeCuota {
		result.InstallmentsPaid = result.InstallmentNumber
	}

	auditPayload := map[string]any{
		"credit_sale_id":     result.CreditSaleID,
		"customer_id":        result.CustomerID,
		"kind":               string(result.Kind),
		"kind_label":         creditKindLabel(result.Kind),
		"product_id":         result.ProductID,
		"product_name":       result.ProductName,
		"debtor_name":        result.DebtorName,
		"installments_total": result.InstallmentsTotal,
		"installments_paid":  result.InstallmentsPaid,
		"paid_installments":  result.InstallmentsPaid,
		"total_value":        result.TotalValue,
		"debt_total":         result.DebtTotal,
		"total_paid":         result.TotalPaid,
		"current_debt":       result.CurrentDebt,
		"interest_percent":   result.InterestPercent,
		"installment_value":  result.InstallmentValue,
		"amount_paid":        amountPaid,
		"installment_number": result.InstallmentNumber,
		"payment_type":       string(paymentType),
	}
	if decoratePayload != nil {
		auditPayload = decoratePayload(auditPayload)
	}
	if err := logAuditEvent(tx, currentUser, "credit_installment_added", "credit_sale", strconv.Itoa(result.CreditSaleID), source, auditPayload); err != nil {
		return creditInstallmentResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la auditoría de la cuota."}
	}
	if result.CustomerID > 0 {
		if err := logCustomerEvent(tx, currentUser, result.CustomerID, "credit_payment_recorded", "credit_sale", strconv.Itoa(result.CreditSaleID), amountPaid, map[string]any{
			"payment_type":       string(paymentType),
			"installment_number": result.InstallmentNumber,
			"total_paid":         result.TotalPaid,
			"current_debt":       result.CurrentDebt,
		}); err != nil {
			return creditInstallmentResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la trazabilidad del cliente."}
		}
	}

	if err := tx.Commit(); err != nil {
		return creditInstallmentResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo confirmar la cuota."}
	}
	return result, nil
}

func createCreditViaAPI(db *sql.DB, currentUser *User, payload apiCreditPayload, source string, defaultKind creditSaleKind, decoratePayload func(map[string]any) map[string]any) (map[string]any, error) {
	_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantIDFromUser(currentUser))
	if err != nil {
		return nil, requestError{Status: http.StatusInternalServerError, Message: "Error al cargar tipos de movimiento."}
	}
	if !movementEnabled(movementEnabledMap, "credito") {
		return nil, requestError{Status: http.StatusForbidden, Message: "El flujo de crédito está deshabilitado en Configuración."}
	}

	if strings.TrimSpace(payload.Kind) == "" {
		payload.Kind = string(defaultKind)
	}
	creditKind := normalizeCreditSaleKind(payload.Kind)
	payload.ProductID = strings.TrimSpace(payload.ProductID)
	payload.CustomerName = strings.TrimSpace(payload.CustomerName)
	payload.CustomerPhone = strings.TrimSpace(payload.CustomerPhone)
	payload.CustomerDocumentType = strings.TrimSpace(payload.CustomerDocumentType)
	payload.CustomerDocumentNumber = strings.TrimSpace(payload.CustomerDocumentNumber)
	payload.CustomerAddress = strings.TrimSpace(payload.CustomerAddress)
	payload.CustomerCity = strings.TrimSpace(payload.CustomerCity)
	payload.CustomerNotes = strings.TrimSpace(payload.CustomerNotes)
	payload.DebtorName = strings.TrimSpace(payload.DebtorName)
	payload.DebtorDocumentType = strings.TrimSpace(payload.DebtorDocumentType)
	payload.DebtorDocumentNumber = strings.TrimSpace(payload.DebtorDocumentNumber)
	payload.DebtorPhone = strings.TrimSpace(payload.DebtorPhone)
	payload.Notes = strings.TrimSpace(payload.Notes)
	if payload.Quantity <= 0 {
		payload.Quantity = 1
	}
	customerInput := customerInput{
		CustomerID: payload.CustomerID,
		Name: firstNonEmptyString(
			payload.CustomerName,
			payload.DebtorName,
		),
		Phone: firstNonEmptyString(
			payload.CustomerPhone,
			payload.DebtorPhone,
		),
		DocumentType: firstNonEmptyString(
			payload.CustomerDocumentType,
			payload.DebtorDocumentType,
		),
		DocumentNumber: firstNonEmptyString(
			payload.CustomerDocumentNumber,
			payload.DebtorDocumentNumber,
		),
		Address: payload.CustomerAddress,
		City:    payload.CustomerCity,
		Notes:   payload.CustomerNotes,
	}
	var (
		productsSnapshot []productOption
		stockByProd      map[string]int
		selectedProduct  productOption
		selectedFound    bool
	)
	if creditKind == creditSaleKindProduct {
		productsSnapshot, err = loadVisibleProductsForUser(db, currentUser)
		if err != nil {
			return nil, requestError{Status: http.StatusInternalServerError, Message: "No se pudieron cargar los productos."}
		}

		stockByProd, err = availableCountsByProduct(db, tenantIDFromUser(currentUser))
		if err != nil {
			return nil, requestError{Status: http.StatusInternalServerError, Message: "Error al consultar stock."}
		}
	}

	fields := map[string]string{}
	if strings.TrimSpace(payload.Kind) != "" && creditKind != creditSaleKindCash && strings.TrimSpace(strings.ToLower(payload.Kind)) != string(creditSaleKindProduct) {
		fields["kind"] = "Selecciona un tipo de crédito válido."
	}
	if customerFields := validateCustomerInput(customerInput); len(customerFields) > 0 {
		if msg, ok := customerFields["customer_name"]; ok {
			fields["debtor_name"] = msg
		}
		if msg, ok := customerFields["customer_document_type"]; ok {
			fields["debtor_document_type"] = msg
		}
		if msg, ok := customerFields["customer_document_number"]; ok {
			fields["debtor_document_number"] = msg
		}
		if msg, ok := customerFields["customer_phone"]; ok {
			fields["debtor_phone"] = msg
		}
	}
	if customerInput.CustomerID <= 0 && customerInput.City == "" {
		fields["customer_city"] = "La ciudad del cliente es obligatoria."
	}
	if customerInput.CustomerID > 0 {
		if _, err := findCustomerByID(db, tenantIDFromUser(currentUser), customerInput.CustomerID); err != nil {
			if err == sql.ErrNoRows {
				fields["customer_id"] = "Selecciona un cliente válido."
			} else {
				return nil, requestError{Status: http.StatusInternalServerError, Message: "No se pudo validar el cliente."}
			}
		}
	}
	if payload.InstallmentsTotal <= 0 {
		fields["installments_total"] = "La cantidad total de cuotas debe ser mayor a 0."
	}
	if payload.TotalValue <= 0 {
		fields["total_value"] = "El valor total debe ser mayor a 0."
	}
	if payload.InterestPercent < 0 {
		fields["interest_percent"] = "El porcentaje de interés debe ser un número mayor o igual a 0."
	}
	if creditKind == creditSaleKindProduct {
		if payload.ProductID == "" {
			fields["product_id"] = "Selecciona un producto válido."
		}
		if payload.Quantity <= 0 {
			fields["quantity"] = "La cantidad debe ser un número positivo."
		}
		if allowed, err := productAccessibleByID(db, currentUser, payload.ProductID); err != nil {
			return nil, requestError{Status: http.StatusInternalServerError, Message: "No se pudo validar acceso al producto."}
		} else if !allowed {
			fields["product_id"] = "No tienes acceso a este producto."
		}
		selectedProduct, selectedFound = findProduct(productsSnapshot, payload.ProductID)
		if !selectedFound {
			fields["product_id"] = "Selecciona un producto válido."
		}
		if payload.ProductID != "" && payload.Quantity > 0 {
			if available := stockByProd[payload.ProductID]; available > 0 && payload.Quantity > available {
				fields["quantity"] = "No hay stock disponible suficiente para completar la venta."
			}
		}
	}
	installmentValue := 0.0
	if payload.TotalValue > 0 && payload.InstallmentsTotal > 0 {
		financedTotal := payload.TotalValue + (payload.TotalValue * payload.InterestPercent / 100)
		installmentValue = math.Round((financedTotal/float64(payload.InstallmentsTotal))*100) / 100
	}
	if payload.InstallmentValue != nil {
		if *payload.InstallmentValue <= 0 {
			fields["installment_value"] = "El valor por cuota debe ser mayor a 0."
		} else {
			installmentValue = *payload.InstallmentValue
		}
	}
	if installmentValue <= 0 {
		fields["installment_value"] = "El valor por cuota debe ser mayor a 0."
	}
	if len(fields) > 0 {
		return nil, requestError{Status: http.StatusBadRequest, Message: "Datos inválidos.", Fields: fields}
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, requestError{Status: http.StatusInternalServerError, Message: "Error al procesar la venta a crédito."}
	}
	defer tx.Rollback()

	soldUnitIDs := []string{}
	now := time.Now().Format(time.RFC3339)
	if creditKind == creditSaleKindProduct {
		soldUnitIDs, err = selectAndMarkUnitsSold(tx, tenantIDFromUser(currentUser), payload.ProductID, payload.Quantity)
		if err != nil {
			if err == errInsufficientStock {
				return nil, requestError{Status: http.StatusBadRequest, Message: "No hay stock disponible suficiente para completar la venta.", Fields: map[string]string{"quantity": "No hay stock disponible suficiente para completar la venta."}}
			}
			return nil, requestError{Status: http.StatusInternalServerError, Message: "Error al actualizar inventario."}
		}
		creditSummary := fmt.Sprintf("VENTA A CREDITO | Cuotas: %d | Interes: %.2f%% | Valor cuota: %.2f", payload.InstallmentsTotal, payload.InterestPercent, installmentValue)
		if payload.Notes != "" {
			creditSummary += " | " + payload.Notes
		}
		if err := logMovimientos(tx, payload.ProductID, soldUnitIDs, "venta_credito", creditSummary, currentUser, now); err != nil {
			return nil, requestError{Status: http.StatusInternalServerError, Message: "Error al registrar movimiento de venta."}
		}
	}
	customer, err := resolveCustomerForCredit(tx, tenantIDFromUser(currentUser), customerInput)
	if err != nil {
		return nil, requestError{Status: http.StatusInternalServerError, Message: "Error al resolver el cliente del crédito."}
	}
	productName := "Préstamo de dinero"
	if creditKind == creditSaleKindProduct && selectedFound {
		productName = selectedProduct.Name
	}
	createdCredit, err := createCreditSale(tx, currentUser, creditSaleCreateInput{
		Kind:              creditKind,
		ProductID:         payload.ProductID,
		ProductName:       productName,
		Quantity:          payload.Quantity,
		Customer:          customer,
		InstallmentsTotal: payload.InstallmentsTotal,
		TotalValue:        payload.TotalValue,
		InterestPercent:   payload.InterestPercent,
		InstallmentValue:  installmentValue,
		Notes:             payload.Notes,
	}, source, decoratePayload)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, requestError{Status: http.StatusInternalServerError, Message: "Error al confirmar la venta a crédito."}
	}

	return map[string]any{
		"ok":                true,
		"credit_sale_id":    createdCredit.CreditSaleID,
		"customer_id":       customer.ID,
		"kind":              string(createdCredit.Kind),
		"kind_label":        creditKindLabel(createdCredit.Kind),
		"product_id":        createdCredit.ProductID,
		"product_name":      createdCredit.ProductName,
		"quantity":          createdCredit.Quantity,
		"installment_value": createdCredit.InstallmentValue,
		"debt_total":        createdCredit.DebtTotal,
		"total_paid":        createdCredit.TotalPaid,
		"current_debt":      createdCredit.CurrentDebt,
		"message":           createdCredit.Message,
	}, nil
}

func loadInventoryCountsForProducts(db *sql.DB, tenantID int, productIDs []string) (map[string]productInventoryCounts, error) {
	counts := make(map[string]productInventoryCounts, len(productIDs))
	if len(productIDs) == 0 {
		return counts, nil
	}

	placeholders := make([]string, len(productIDs))
	args := make([]any, 0, len(productIDs))
	for i, productID := range productIDs {
		placeholders[i] = "?"
		args = append(args, productID)
	}

	rows, err := db.Query(`
		SELECT producto_id, estado, COUNT(*)
		FROM unidades
		WHERE tenant_id = ? AND producto_id IN (`+strings.Join(placeholders, ",")+`)
		GROUP BY producto_id, estado
	`, append([]any{normalizeTenantID(tenantID)}, args...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			productID string
			estado    string
			count     int
		)
		if err := rows.Scan(&productID, &estado, &count); err != nil {
			return nil, err
		}
		current := counts[productID]
		switch estado {
		case "Disponible", "available":
			current.Available = count
		case "Reservada", "reserved":
			current.Reserved = count
		case "Cambio", "swapped":
			current.Swapped = count
		case "Danada", "Dañada", "damaged":
			current.Damaged = count
		}
		counts[productID] = current
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return counts, nil
}

func agentProductItem(product productOption, counts productInventoryCounts, includeOwner bool) map[string]any {
	var retomaPrice any = nil
	if product.HasRetomaPrice {
		retomaPrice = product.RetomaPrice
	}

	item := map[string]any{
		"id":             product.ID,
		"name":           product.Name,
		"line":           product.Line,
		"location":       product.Location,
		"sale_price":     product.SalePrice,
		"retoma_enabled": product.RetomaEnabled,
		"retoma_price":   retomaPrice,
		"available":      counts.Available,
		"status": func() string {
			if counts.Available > 0 {
				return "available"
			}
			return "out_of_stock"
		}(),
	}
	if includeOwner && product.HasOwner {
		item["owner_user_id"] = product.OwnerUserID
	}
	return item
}

func findVisibleProduct(products []productOption, productID string) (productOption, bool) {
	productID = strings.TrimSpace(productID)
	for _, product := range products {
		if strings.EqualFold(product.ID, productID) {
			return product, true
		}
	}
	return productOption{}, false
}

func selectAndMarkUnitsSold(tx *sql.Tx, tenantID int, productID string, qty int) ([]string, error) {
	return selectAndMarkUnitsByStatus(tx, tenantID, productID, qty, "Vendida")
}

func selectAndMarkUnitsByStatus(tx *sql.Tx, tenantID int, productID string, qty int, nextStatus string) ([]string, error) {
	if qty <= 0 {
		return nil, fmt.Errorf("cantidad inválida")
	}

	rows, err := tx.Query(`
		SELECT id
		FROM unidades
		WHERE tenant_id = ? AND producto_id = ? AND estado IN ('Disponible', 'available')
		ORDER BY creado_en, id
		LIMIT ?`, normalizeTenantID(tenantID), productID, qty)
	if err != nil {
		return nil, fmt.Errorf("query unidades: %w", err)
	}
	defer rows.Close()

	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan unidad: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows unidades: %w", err)
	}

	if len(ids) < qty {
		return nil, errInsufficientStock
	}

	placeholders := make([]string, len(ids))
	args := make([]interface{}, 0, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args = append(args, id)
	}

	query := fmt.Sprintf("UPDATE unidades SET estado = ? WHERE tenant_id = ? AND id IN (%s) AND estado IN ('Disponible', 'available')", strings.Join(placeholders, ","))
	updateArgs := make([]interface{}, 0, len(args)+1)
	updateArgs = append(updateArgs, nextStatus)
	updateArgs = append(updateArgs, normalizeTenantID(tenantID))
	updateArgs = append(updateArgs, args...)
	result, err := tx.Exec(query, updateArgs...)
	if err != nil {
		return nil, fmt.Errorf("update unidades: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("rows affected: %w", err)
	}
	if int(affected) != qty {
		return nil, fmt.Errorf("unidades actualizadas inesperadas: %d", affected)
	}

	return ids, nil
}

func availableUnitsByProduct(db *sql.DB, tenantID int, productID string) ([]unitOption, error) {
	rows, err := db.Query(`
		SELECT id
		FROM unidades
		WHERE tenant_id = ? AND producto_id = ? AND estado IN ('Disponible', 'available')
		ORDER BY creado_en, id`, normalizeTenantID(tenantID), productID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	units := []unitOption{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		units = append(units, unitOption{ID: id})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return units, nil
}

func availableCountsByProduct(db *sql.DB, tenantID int) (map[string]int, error) {
	rows, err := db.Query(`
		SELECT producto_id, COUNT(*)
		FROM unidades
		WHERE tenant_id = ? AND estado IN ('Disponible', 'available')
		GROUP BY producto_id`, normalizeTenantID(tenantID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]int{}
	for rows.Next() {
		var id string
		var count int
		if err := rows.Scan(&id, &count); err != nil {
			return nil, err
		}
		out[id] = count
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func currentBusinessSettings() BusinessSettings {
	businessSettingsMu.RLock()
	defer businessSettingsMu.RUnlock()
	return businessSettings
}

func businessSettingsFromTemplateData(data any) (BusinessSettings, bool) {
	if data == nil {
		return BusinessSettings{}, false
	}
	value := reflect.ValueOf(data)
	if !value.IsValid() {
		return BusinessSettings{}, false
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return BusinessSettings{}, false
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return BusinessSettings{}, false
	}
	field := value.FieldByName("Settings")
	if !field.IsValid() || !field.CanInterface() {
		return BusinessSettings{}, false
	}
	settings, ok := field.Interface().(BusinessSettings)
	if !ok {
		return BusinessSettings{}, false
	}
	return normalizeBusinessSettings(settings), true
}

func currentUserFromTemplateData(data any) (*User, bool) {
	if data == nil {
		return nil, false
	}
	value := reflect.ValueOf(data)
	if !value.IsValid() {
		return nil, false
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil, false
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return nil, false
	}
	field := value.FieldByName("CurrentUser")
	if !field.IsValid() || field.Kind() != reflect.Pointer || field.IsNil() || !field.CanInterface() {
		return nil, false
	}
	user, ok := field.Interface().(*User)
	return user, ok && user != nil
}

func boolFieldFromTemplateData(data any, fieldName string) bool {
	if data == nil || strings.TrimSpace(fieldName) == "" {
		return false
	}
	value := reflect.ValueOf(data)
	if !value.IsValid() {
		return false
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return false
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return false
	}
	field := value.FieldByName(fieldName)
	if !field.IsValid() || field.Kind() != reflect.Bool {
		return false
	}
	return field.Bool()
}

func movementEnabledFromTemplateData(db *sql.DB, data any, fieldName, movementType string) bool {
	if boolFieldFromTemplateData(data, fieldName) {
		return true
	}
	user, ok := currentUserFromTemplateData(data)
	if !ok || user == nil {
		return false
	}
	_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantIDFromUser(user))
	if err != nil {
		log.Printf("template movement settings tenant_id=%d: %v", tenantIDFromUser(user), err)
		return false
	}
	return movementEnabled(movementEnabledMap, movementType)
}

func setCurrentBusinessSettings(settings BusinessSettings) {
	businessSettingsMu.Lock()
	businessSettings = normalizeBusinessSettings(settings)
	businessSettingsMu.Unlock()
}

func normalizeBusinessSettings(settings BusinessSettings) BusinessSettings {
	defaults := defaultBusinessSettings()
	settings.BusinessName = strings.TrimSpace(settings.BusinessName)
	if settings.BusinessName == "" {
		settings.BusinessName = defaults.BusinessName
	}
	settings.LogoPath = strings.TrimSpace(settings.LogoPath)
	if settings.LogoPath == "" {
		settings.LogoPath = defaults.LogoPath
	}
	settings.PrimaryColor = normalizeHexColor(settings.PrimaryColor, defaults.PrimaryColor)
	settings.Currency = normalizeCurrency(settings.Currency)
	settings.DateFormat = normalizeDateFormat(settings.DateFormat)
	settings.LabelPaperWidth = normalizePaperWidth(settings.LabelPaperWidth, defaults.LabelPaperWidth)
	settings.InvoicePaperWidth = normalizePaperWidth(settings.InvoicePaperWidth, defaults.InvoicePaperWidth)
	settings.TicketPaperWidth = normalizePaperWidth(settings.TicketPaperWidth, defaults.TicketPaperWidth)
	return settings
}

func effectiveBusinessLogoPath(settings BusinessSettings, data any) string {
	defaultLogoPath := strings.TrimSpace(defaultBusinessSettings().LogoPath)
	globalLogoPath := strings.TrimSpace(currentBusinessSettings().LogoPath)
	logoPath := strings.TrimSpace(settings.LogoPath)

	if logoPath == "" {
		if globalLogoPath != "" {
			return globalLogoPath
		}
		return defaultLogoPath
	}

	// For non-default tenants without custom branding (legacy/default logo),
	// prefer current global branding configured by platform admin.
	if logoPath == defaultLogoPath {
		if user, ok := currentUserFromTemplateData(data); ok && tenantIDFromUser(user) != defaultTenantID {
			if globalLogoPath != "" {
				return globalLogoPath
			}
		}
	}

	return logoPath
}

func normalizeCurrency(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "USD", "EUR", "COP":
		return strings.ToUpper(strings.TrimSpace(raw))
	default:
		return defaultBusinessSettings().Currency
	}
}

func normalizeDateFormat(raw string) string {
	switch strings.TrimSpace(raw) {
	case "2006-01-02", "02/01/2006", "01/02/2006", "02-01-2006":
		return strings.TrimSpace(raw)
	default:
		return defaultBusinessSettings().DateFormat
	}
}

func normalizePaperWidth(raw, fallback string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "80", "80mm", "80x50":
		return "80mm"
	case "57", "57mm", "57x30", "50x30":
		return "57mm"
	case "58", "58mm", "58x40", "60x40":
		return "58mm"
	default:
		return normalizePaperWidth(fallback, defaultBusinessSettings().LabelPaperWidth)
	}
}

func normalizeHexColor(raw, fallback string) string {
	value := strings.TrimSpace(strings.TrimPrefix(raw, "#"))
	if len(value) != 6 {
		return fallback
	}
	for _, ch := range value {
		switch {
		case ch >= '0' && ch <= '9':
		case ch >= 'a' && ch <= 'f':
		case ch >= 'A' && ch <= 'F':
		default:
			return fallback
		}
	}
	return "#" + strings.ToLower(value)
}

func shadeHexColor(hex string, delta int) string {
	hex = normalizeHexColor(hex, defaultBusinessSettings().PrimaryColor)
	parse := func(part string) int {
		v, err := strconv.ParseInt(part, 16, 0)
		if err != nil {
			return 0
		}
		return int(v)
	}
	clamp := func(v int) int {
		if v < 0 {
			return 0
		}
		if v > 255 {
			return 255
		}
		return v
	}
	r := clamp(parse(hex[1:3]) + delta)
	g := clamp(parse(hex[3:5]) + delta)
	b := clamp(parse(hex[5:7]) + delta)
	return fmt.Sprintf("#%02x%02x%02x", r, g, b)
}

func parseDateFlexible(raw string) (time.Time, bool) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return time.Time{}, false
	}
	layouts := []string{
		time.RFC3339,
		"2006-01-02",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05-07:00",
	}
	for _, layout := range layouts {
		var (
			t   time.Time
			err error
		)
		if strings.Contains(layout, "-07:00") || layout == time.RFC3339 {
			t, err = time.Parse(layout, value)
		} else {
			t, err = time.ParseInLocation(layout, value, appTimeLocation)
		}
		if err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func formatDateWithSettings(raw string) string {
	if raw == "" {
		return ""
	}
	settings := currentBusinessSettings()
	if t, ok := parseDateFlexible(raw); ok {
		return t.In(appTimeLocation).Format(settings.DateFormat)
	}
	return raw
}

func loadBusinessSettings(db *sql.DB) (BusinessSettings, error) {
	return loadBusinessSettingsForTenant(db, defaultTenantID)
}

func loadBusinessSettingsForTenant(db *sql.DB, tenantID int) (BusinessSettings, error) {
	settings := defaultBusinessSettings()
	cols, err := tableColumns(db, "business_settings")
	if err != nil {
		return BusinessSettings{}, err
	}
	labelExpr := "'58mm'"
	invoiceExpr := "'58mm'"
	ticketExpr := "'58mm'"
	if cols["label_paper_width"] {
		labelExpr = "label_paper_width"
	}
	if cols["invoice_paper_width"] {
		invoiceExpr = "invoice_paper_width"
	}
	if cols["ticket_paper_width"] {
		ticketExpr = "ticket_paper_width"
	}
	query := fmt.Sprintf(`
		SELECT id, business_name, logo_path, primary_color, currency, date_format, %s AS label_paper_width, %s AS invoice_paper_width, %s AS ticket_paper_width, updated_at
		FROM business_settings
		WHERE tenant_id = ?
		ORDER BY id ASC
		LIMIT 1
	`, labelExpr, invoiceExpr, ticketExpr)
	row := db.QueryRow(query, normalizeTenantID(tenantID))
	var updatedAt sql.NullString
	err = row.Scan(&settings.ID, &settings.BusinessName, &settings.LogoPath, &settings.PrimaryColor, &settings.Currency, &settings.DateFormat, &settings.LabelPaperWidth, &settings.InvoicePaperWidth, &settings.TicketPaperWidth, &updatedAt)
	if err == sql.ErrNoRows {
		return normalizeBusinessSettings(settings), nil
	}
	if err != nil {
		return BusinessSettings{}, err
	}
	settings.UpdatedAt = updatedAt.String
	return normalizeBusinessSettings(settings), nil
}

func saveBusinessSettings(db *sql.DB, settings BusinessSettings) (BusinessSettings, error) {
	return saveBusinessSettingsForTenant(db, defaultTenantID, settings)
}

func saveBusinessSettingsForTenant(db *sql.DB, tenantID int, settings BusinessSettings) (BusinessSettings, error) {
	settings = normalizeBusinessSettings(settings)
	settings.UpdatedAt = time.Now().Format(time.RFC3339)
	cols, err := tableColumns(db, "business_settings")
	if err != nil {
		return BusinessSettings{}, err
	}
	insertCols := []string{"tenant_id", "business_name", "logo_path", "primary_color", "currency", "date_format", "updated_at"}
	args := []any{normalizeTenantID(tenantID), settings.BusinessName, settings.LogoPath, settings.PrimaryColor, settings.Currency, settings.DateFormat, settings.UpdatedAt}
	updateCols := []string{
		"business_name = excluded.business_name",
		"logo_path = excluded.logo_path",
		"primary_color = excluded.primary_color",
		"currency = excluded.currency",
		"date_format = excluded.date_format",
		"updated_at = excluded.updated_at",
	}
	if cols["label_paper_width"] {
		insertCols = append(insertCols, "label_paper_width")
		args = append(args, settings.LabelPaperWidth)
		updateCols = append(updateCols, "label_paper_width = excluded.label_paper_width")
	}
	if cols["invoice_paper_width"] {
		insertCols = append(insertCols, "invoice_paper_width")
		args = append(args, settings.InvoicePaperWidth)
		updateCols = append(updateCols, "invoice_paper_width = excluded.invoice_paper_width")
	}
	if cols["ticket_paper_width"] {
		insertCols = append(insertCols, "ticket_paper_width")
		args = append(args, settings.TicketPaperWidth)
		updateCols = append(updateCols, "ticket_paper_width = excluded.ticket_paper_width")
	}
	placeholders := make([]string, 0, len(insertCols))
	for range insertCols {
		placeholders = append(placeholders, "?")
	}
	query := fmt.Sprintf(`
		INSERT INTO business_settings (%s)
		VALUES (%s)
		ON CONFLICT(tenant_id) DO UPDATE SET
			%s
	`, strings.Join(insertCols, ", "), strings.Join(placeholders, ", "), strings.Join(updateCols, ", "))
	if _, err := db.Exec(query, args...); err != nil {
		return BusinessSettings{}, err
	}
	if err := db.QueryRow(`SELECT id FROM business_settings WHERE tenant_id = ?`, normalizeTenantID(tenantID)).Scan(&settings.ID); err != nil {
		return BusinessSettings{}, err
	}
	return settings, nil
}

func loadBusinessLines(db *sql.DB, activeOnly bool) ([]BusinessLine, error) {
	return loadBusinessLinesForTenant(db, defaultTenantID, activeOnly)
}

func loadBusinessLinesForTenant(db *sql.DB, tenantID int, activeOnly bool) ([]BusinessLine, error) {
	query := `
		SELECT id, name, active, created_at, updated_at
		FROM business_lines
	`
	args := []any{normalizeTenantID(tenantID)}
	query += ` WHERE tenant_id = ?`
	if activeOnly {
		query += ` AND active = 1`
	}
	query += ` ORDER BY LOWER(name), id`
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	lines := make([]BusinessLine, 0)
	for rows.Next() {
		var line BusinessLine
		var active int
		if err := rows.Scan(&line.ID, &line.Name, &active, &line.CreatedAt, &line.UpdatedAt); err != nil {
			return nil, err
		}
		line.Active = active == 1
		line.CreatedAt = formatDateWithSettings(line.CreatedAt)
		line.UpdatedAt = formatDateWithSettings(line.UpdatedAt)
		lines = append(lines, line)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

func businessLineNames(lines []BusinessLine) []string {
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		name := strings.TrimSpace(line.Name)
		if name == "" {
			continue
		}
		out = append(out, name)
	}
	return out
}

func ensureLineOption(options []string, current string) []string {
	current = strings.TrimSpace(current)
	if current == "" {
		return options
	}
	for _, option := range options {
		if strings.EqualFold(strings.TrimSpace(option), current) {
			return options
		}
	}
	return append(options, current)
}

func defaultPaymentMethodNames() []string {
	return []string{"Efectivo", "Transferencia", "Nequi", "Daviplata"}
}

func defaultMovementTypes() []string {
	return []string{"venta", "cambio", "retoma", "prestamo", "credito"}
}

func loadPaymentMethods(db *sql.DB, activeOnly bool) ([]PaymentMethod, error) {
	return loadPaymentMethodsForTenant(db, defaultTenantID, activeOnly)
}

func loadPaymentMethodsForTenant(db *sql.DB, tenantID int, activeOnly bool) ([]PaymentMethod, error) {
	query := `
		SELECT id, name, active, sort_order, created_at, updated_at
		FROM payment_methods
	`
	args := []any{normalizeTenantID(tenantID)}
	query += ` WHERE tenant_id = ?`
	if activeOnly {
		query += ` AND active = 1`
	}
	query += ` ORDER BY sort_order ASC, LOWER(name) ASC, id ASC`
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	methods := make([]PaymentMethod, 0)
	for rows.Next() {
		var method PaymentMethod
		var active int
		if err := rows.Scan(&method.ID, &method.Name, &active, &method.SortOrder, &method.CreatedAt, &method.UpdatedAt); err != nil {
			return nil, err
		}
		method.Active = active == 1
		method.CreatedAt = formatDateWithSettings(method.CreatedAt)
		method.UpdatedAt = formatDateWithSettings(method.UpdatedAt)
		methods = append(methods, method)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return methods, nil
}

func loadAPIKeys(db *sql.DB) ([]APIKey, error) {
	return loadAPIKeysForTenant(db, defaultTenantID)
}

func loadAPIKeysForTenant(db *sql.DB, tenantID int) ([]APIKey, error) {
	tenant, err := resolveTenantByID(db, tenantID)
	if err != nil {
		return nil, err
	}
	canonicalInitialName := strings.ToLower(initialAPIKeyNameForTenant(tenant))
	rows, err := db.Query(`
		SELECT id, name, COALESCE(NULLIF(tenant_id, 0), ?), active, created_at, updated_at
		FROM api_keys
		WHERE COALESCE(NULLIF(tenant_id, 0), ?) = ?
		ORDER BY active DESC, updated_at DESC, id DESC
	`, defaultTenantID, defaultTenantID, normalizeTenantID(tenantID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]APIKey, 0, 16)
	for rows.Next() {
		var item APIKey
		var active int
		if err := rows.Scan(&item.ID, &item.Name, &item.TenantID, &active, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		item.Active = active == 1
		item.TenantID = normalizeTenantID(item.TenantID)
		item.IsInitial = strings.ToLower(strings.TrimSpace(item.Name)) == canonicalInitialName || strings.HasSuffix(strings.ToLower(strings.TrimSpace(item.Name)), "-inicial")
		item.CreatedAt = formatDateWithSettings(item.CreatedAt)
		item.UpdatedAt = formatDateWithSettings(item.UpdatedAt)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func paymentMethodNames(methods []PaymentMethod) []string {
	out := make([]string, 0, len(methods))
	for _, method := range methods {
		name := strings.TrimSpace(method.Name)
		if name == "" {
			continue
		}
		out = append(out, name)
	}
	return out
}

func seedPaymentMethodsIfMissing(db *sql.DB) error {
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM payment_methods WHERE tenant_id = ?`, defaultTenantID).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	now := time.Now().Format(time.RFC3339)
	for idx, name := range defaultPaymentMethodNames() {
		if _, err := db.Exec(`
			INSERT INTO payment_methods (tenant_id, name, active, sort_order, created_at, updated_at)
			VALUES (?, ?, 1, ?, ?, ?)
		`, defaultTenantID, name, idx+1, now, now); err != nil {
			return err
		}
	}
	return nil
}

func seedMovementSettingsIfMissing(db *sql.DB) error {
	now := time.Now().Format(time.RFC3339)
	for _, movementType := range defaultMovementTypes() {
		if _, err := db.Exec(`
			INSERT INTO movement_settings (tenant_id, movement_type, enabled, updated_at)
			VALUES (?, ?, 1, ?)
			ON CONFLICT(tenant_id, movement_type) DO NOTHING
		`, defaultTenantID, movementType, now); err != nil {
			return err
		}
	}
	return nil
}

func loadMovementSettings(db *sql.DB) ([]MovementSetting, map[string]bool, error) {
	return loadMovementSettingsForTenant(db, defaultTenantID)
}

func loadMovementSettingsForTenant(db *sql.DB, tenantID int) ([]MovementSetting, map[string]bool, error) {
	rows, err := db.Query(`
		SELECT id, movement_type, enabled, updated_at
		FROM movement_settings
		WHERE tenant_id = ?
		ORDER BY CASE movement_type
			WHEN 'venta' THEN 1
			WHEN 'cambio' THEN 2
			WHEN 'retoma' THEN 3
			WHEN 'prestamo' THEN 4
			WHEN 'credito' THEN 5
			ELSE 99
		END, id
	`, normalizeTenantID(tenantID))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	settings := make([]MovementSetting, 0)
	enabledMap := make(map[string]bool)
	for rows.Next() {
		var item MovementSetting
		var enabled int
		if err := rows.Scan(&item.ID, &item.MovementType, &enabled, &item.UpdatedAt); err != nil {
			return nil, nil, err
		}
		item.Enabled = enabled == 1
		item.UpdatedAt = formatDateWithSettings(item.UpdatedAt)
		settings = append(settings, item)
		enabledMap[item.MovementType] = item.Enabled
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	for _, movementType := range defaultMovementTypes() {
		if _, ok := enabledMap[movementType]; !ok {
			enabledMap[movementType] = true
		}
	}
	return settings, enabledMap, nil
}

func movementEnabled(enabledMap map[string]bool, movementType string) bool {
	enabled, ok := enabledMap[movementType]
	if !ok {
		return true
	}
	return enabled
}

func ensureUploadDirs() error {
	return os.MkdirAll(filepath.Join("static", "uploads", "branding"), 0o755)
}

func saveBusinessLogo(file io.Reader, originalName string) (string, error) {
	if err := ensureUploadDirs(); err != nil {
		return "", err
	}
	ext := strings.ToLower(filepath.Ext(originalName))
	switch ext {
	case ".png", ".jpg", ".jpeg", ".svg", ".webp":
	default:
		return "", fmt.Errorf("formato de logo no soportado")
	}
	fileName := fmt.Sprintf("logo-%d%s", time.Now().UnixNano(), ext)
	relPath := filepath.Join("uploads", "branding", fileName)
	fullPath := filepath.Join("static", relPath)
	dst, err := os.Create(fullPath)
	if err != nil {
		return "", err
	}
	defer dst.Close()
	if _, err := io.Copy(dst, file); err != nil {
		return "", err
	}
	return "/static/" + filepath.ToSlash(relPath), nil
}

func formatCurrency(value float64) string {
	rounded := int64(math.Round(value))
	settings := currentBusinessSettings()
	switch settings.Currency {
	case "USD":
		return "USD " + formatIntDots(rounded)
	case "EUR":
		return "EUR " + formatIntDots(rounded)
	default:
		return "$" + formatIntDots(rounded)
	}
}

func saleReceiptViewURL(saleID int) string {
	return fmt.Sprintf("/venta/comprobante?sale_id=%d", saleID)
}

func saleReceiptDownloadURL(saleID int) string {
	return fmt.Sprintf("/venta/comprobante?sale_id=%d&download=1", saleID)
}

func saleThermalTicketViewURL(saleID int) string {
	return fmt.Sprintf("/venta/ticket?sale_id=%d", saleID)
}

func saleReceiptViewURLWithBuyer(saleID int, buyerName, buyerDocument string) string {
	values := url.Values{}
	values.Set("sale_id", strconv.Itoa(saleID))
	values.Set("buyer_name", strings.TrimSpace(buyerName))
	values.Set("buyer_document", strings.TrimSpace(buyerDocument))
	return "/venta/comprobante?" + values.Encode()
}

func saleReceiptDownloadURLWithBuyer(saleID int, buyerName, buyerDocument string) string {
	values := url.Values{}
	values.Set("sale_id", strconv.Itoa(saleID))
	values.Set("download", "1")
	values.Set("buyer_name", strings.TrimSpace(buyerName))
	values.Set("buyer_document", strings.TrimSpace(buyerDocument))
	return "/venta/comprobante?" + values.Encode()
}

func saleThermalTicketViewURLWithBuyer(saleID int, buyerName, buyerDocument string) string {
	values := url.Values{}
	values.Set("sale_id", strconv.Itoa(saleID))
	values.Set("buyer_name", strings.TrimSpace(buyerName))
	values.Set("buyer_document", strings.TrimSpace(buyerDocument))
	return "/venta/ticket?" + values.Encode()
}

func invoiceViewURL(invoiceID int) string {
	return fmt.Sprintf("/facturas/%d", invoiceID)
}

func invoiceNewFromSaleURL(saleID int) string {
	return fmt.Sprintf("/facturas/nueva?sale_id=%d", saleID)
}

func invoiceNewFromCreditURL(creditSaleID int) string {
	return fmt.Sprintf("/facturas/nueva?credit_sale_id=%d", creditSaleID)
}

func productLabelPrintURL(productIDs []string, size string) string {
	if len(productIDs) == 0 {
		return ""
	}
	values := url.Values{}
	for _, productID := range productIDs {
		productID = strings.TrimSpace(productID)
		if productID == "" {
			continue
		}
		values.Add("id", productID)
	}
	size = strings.TrimSpace(strings.ToLower(size))
	switch size {
	case "80", "80mm", "80x50":
		values.Set("size", "80mm")
	case "57", "57mm", "57x30", "50x30":
		values.Set("size", "57mm")
	case "58", "58mm", "58x40", "60x40":
		values.Set("size", "58mm")
	}
	if len(values["id"]) == 0 {
		return ""
	}
	return "/productos/etiquetas?" + values.Encode()
}

func thermalPaperDimensions(size string) (normalized string, widthMM, dpi int, paperClass string) {
	switch strings.TrimSpace(strings.ToLower(size)) {
	case "80", "80mm", "80x50":
		return "80mm", 80, 203, "wide"
	case "57", "57mm", "57x30", "50x30":
		return "57mm", 57, 203, "compact"
	default:
		return "58mm", 58, 203, "standard"
	}
}

func labelSizeDimensions(size string) (normalized string, widthMM, heightMM int) {
	switch strings.TrimSpace(strings.ToLower(size)) {
	case "80", "80mm", "80x50":
		return "80mm", 80, 50
	case "57", "57mm", "57x30", "50x30":
		return "57mm", 57, 30
	default:
		return "58mm", 58, 40
	}
}

func barcodeDataURI(value string, width, height int) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("barcode value empty")
	}
	code, err := code128.Encode(value)
	if err != nil {
		return "", err
	}
	scaled, err := barcode.Scale(code, width, height)
	if err != nil {
		return "", err
	}
	var buffer bytes.Buffer
	if err := png.Encode(&buffer, scaled); err != nil {
		return "", err
	}
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(buffer.Bytes()), nil
}

func productLabelItemsForUser(db *sql.DB, currentUser *User, productIDs []string, size string) ([]productLabelItem, int, int, error) {
	if currentUser == nil || !isStaffRole(currentUser.Role) {
		return nil, 0, 0, requestError{Status: http.StatusForbidden, Message: "Solo personal autorizado puede imprimir etiquetas."}
	}
	normalizedSize, widthMM, heightMM := labelSizeDimensions(size)
	_ = normalizedSize
	seen := map[string]struct{}{}
	items := make([]productLabelItem, 0, len(productIDs))
	barcodeWidth := 280
	barcodeHeight := 72
	switch {
	case widthMM >= 80:
		barcodeWidth = 360
		barcodeHeight = 92
	case widthMM <= 57:
		barcodeWidth = 220
		barcodeHeight = 54
	}

	for _, rawID := range productIDs {
		productID := strings.TrimSpace(rawID)
		if productID == "" {
			continue
		}
		if _, exists := seen[productID]; exists {
			continue
		}
		seen[productID] = struct{}{}

		allowed, err := productAccessibleByID(db, currentUser, productID)
		if err != nil {
			return nil, 0, 0, err
		}
		if !allowed {
			continue
		}

		var (
			name      string
			salePrice float64
		)
		err = db.QueryRow(`
			SELECT COALESCE(nombre, sku), COALESCE(precio_venta, 0)
			FROM productos
			WHERE tenant_id = ? AND sku = ?
			LIMIT 1
		`, tenantIDFromUser(currentUser), productID).Scan(&name, &salePrice)
		if err != nil {
			if err == sql.ErrNoRows {
				continue
			}
			return nil, 0, 0, err
		}

		barcodeURI, err := barcodeDataURI(productID, barcodeWidth, barcodeHeight)
		if err != nil {
			return nil, 0, 0, err
		}
		items = append(items, productLabelItem{
			ID:             productID,
			Name:           name,
			Price:          formatCurrency(salePrice),
			BarcodeDataURI: template.URL(barcodeURI),
		})
	}
	if len(items) == 0 {
		return nil, widthMM, heightMM, requestError{Status: http.StatusNotFound, Message: "No hay productos disponibles para imprimir etiquetas."}
	}
	return items, widthMM, heightMM, nil
}

func saleReceiptNumber(saleID int, saleDate string) string {
	compactDate := strings.ReplaceAll(strings.TrimSpace(saleDate), "-", "")
	if compactDate == "" {
		compactDate = time.Now().In(appTimeLocation).Format("20060102")
	}
	return fmt.Sprintf("CV-%s-%06d", compactDate, saleID)
}

func loadSaleReceiptData(db *sql.DB, currentUser *User, saleID int) (saleReceiptData, error) {
	tenantID := tenantIDFromUser(currentUser)
	var (
		createdAtRaw  string
		productID     string
		productName   string
		quantity      int
		unitPrice     float64
		paymentMethod string
		channel       string
		soldBy        string
		notes         string
	)

	err := db.QueryRow(`
		SELECT
			v.fecha,
			v.producto_id,
			COALESCE(p.nombre, v.producto_id),
			v.cantidad,
			v.precio_final,
			COALESCE(v.metodo_pago, ''),
			COALESCE(v.channel, ''),
			COALESCE(v.sold_by, ''),
			COALESCE(v.notas, '')
		FROM ventas v
		LEFT JOIN productos p ON p.sku = v.producto_id AND p.tenant_id = v.tenant_id
		WHERE v.tenant_id = ? AND v.id = ?
		LIMIT 1
	`, tenantID, saleID).Scan(&createdAtRaw, &productID, &productName, &quantity, &unitPrice, &paymentMethod, &channel, &soldBy, &notes)
	if err != nil {
		if err == sql.ErrNoRows {
			return saleReceiptData{}, requestError{Status: http.StatusNotFound, Message: "Venta no encontrada."}
		}
		return saleReceiptData{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo cargar la venta."}
	}

	allowed, err := productAccessibleByID(db, currentUser, productID)
	if err != nil {
		return saleReceiptData{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo validar acceso a la venta."}
	}
	if !allowed {
		return saleReceiptData{}, requestError{Status: http.StatusForbidden, Message: "No tienes acceso a esta venta."}
	}

	saleDate := createdAtRaw
	saleTime := ""
	saleDateTime := createdAtRaw
	if parsed, ok := parseFlexibleTime(createdAtRaw); ok {
		saleDate = formatDateWithSettings(parsed.Format("2006-01-02"))
		saleTime = parsed.In(appTimeLocation).Format("15:04")
		saleDateTime = parsed.In(appTimeLocation).Format("2006-01-02 15:04")
	} else if len(createdAtRaw) >= 10 {
		saleDate = formatDateWithSettings(createdAtRaw[:10])
	}
	settings, err := loadBusinessSettingsForTenant(db, tenantID)
	if err != nil {
		settings = currentBusinessSettings()
	}
	_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantID)
	if err != nil {
		return saleReceiptData{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudieron cargar los movimientos disponibles."}
	}

	return saleReceiptData{
		Title:            "Comprobante de venta",
		Subtitle:         "Comprobante simple generado desde una venta existente.",
		SaleID:           saleID,
		ReceiptNumber:    saleReceiptNumber(saleID, createdAtRaw[:min(10, len(createdAtRaw))]),
		SaleDate:         saleDate,
		SaleTime:         saleTime,
		SaleDateTime:     saleDateTime,
		OperationType:    "Venta",
		ProductoID:       productID,
		ProductoNom:      productName,
		Cantidad:         quantity,
		PrecioUnitario:   formatCurrency(unitPrice),
		Total:            formatCurrency(unitPrice * float64(quantity)),
		MetodoPago:       paymentMethod,
		SoldBy:           soldBy,
		Channel:          channel,
		Notas:            notes,
		DownloadURL:      saleReceiptDownloadURL(saleID),
		ThermalURL:       saleThermalTicketViewURL(saleID),
		InvoiceCreateURL: invoiceNewFromSaleURL(saleID),
		CanLoan:          movementEnabled(movementEnabledMap, "prestamo"),
		CanCredit:        movementEnabled(movementEnabledMap, "credito"),
		CurrentUser:      currentUser,
		Settings:         settings,
	}, nil
}

func invoiceStatusLabel(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "cancelled":
		return "Factura anulada"
	default:
		return "Factura emitida"
	}
}

func invoiceSourceLabel(sourceType string) string {
	switch strings.TrimSpace(strings.ToLower(sourceType)) {
	case "credit":
		return "Crédito"
	default:
		return "Venta"
	}
}

func invoiceNumber(invoiceID int64, createdAt time.Time) string {
	if invoiceID <= 0 {
		return ""
	}
	return fmt.Sprintf("FAC-%s-%06d", createdAt.In(appTimeLocation).Format("20060102"), invoiceID)
}

func loadSaleInvoiceSource(db *sql.DB, currentUser *User, saleID int) (invoiceSourceSnapshot, error) {
	if saleID <= 0 {
		return invoiceSourceSnapshot{}, requestError{Status: http.StatusBadRequest, Message: "Venta inválida."}
	}
	data, err := loadSaleReceiptData(db, currentUser, saleID)
	if err != nil {
		return invoiceSourceSnapshot{}, err
	}
	unitPrice := parseCurrencyToFloat(data.PrecioUnitario)
	lineTotal := parseCurrencyToFloat(data.Total)
	return invoiceSourceSnapshot{
		SourceType:  "sale",
		SourceLabel: "Venta",
		SaleID:      saleID,
		Item: invoiceItemData{
			ProductID:     data.ProductoID,
			Description:   data.ProductoNom,
			Quantity:      data.Cantidad,
			UnitPrice:     unitPrice,
			UnitPriceText: data.PrecioUnitario,
			LineTotal:     lineTotal,
			LineTotalText: data.Total,
		},
	}, nil
}

func loadCreditInvoiceSource(db *sql.DB, currentUser *User, creditSaleID int) (invoiceSourceSnapshot, error) {
	if creditSaleID <= 0 {
		return invoiceSourceSnapshot{}, requestError{Status: http.StatusBadRequest, Message: "Crédito inválido."}
	}
	item, err := creditDetailForUser(db, currentUser, creditSaleID)
	if err != nil {
		if err == sql.ErrNoRows {
			return invoiceSourceSnapshot{}, requestError{Status: http.StatusNotFound, Message: "Crédito no encontrado."}
		}
		return invoiceSourceSnapshot{}, err
	}
	customerID, _ := item["customer_id"].(int)
	if customerID == 0 {
		if value, ok := item["customer_id"].(float64); ok {
			customerID = int(value)
		}
	}
	var customer *Customer
	if customerID > 0 {
		customer, _ = findCustomerByID(db, tenantIDFromUser(currentUser), customerID)
	}
	quantity, _ := item["quantity"].(int)
	if quantity == 0 {
		if value, ok := item["quantity"].(float64); ok {
			quantity = int(value)
		}
	}
	totalValue, _ := item["total_value"].(float64)
	productID, _ := item["product_id"].(string)
	productName, _ := item["product"].(string)
	kind, _ := item["kind"].(string)
	if strings.TrimSpace(productName) == "" && strings.TrimSpace(kind) == string(creditSaleKindCash) {
		productName = "Préstamo de dinero"
	}
	if quantity <= 0 {
		quantity = 1
	}
	unitPrice := totalValue / float64(quantity)
	return invoiceSourceSnapshot{
		SourceType:   "credit",
		SourceLabel:  "Crédito",
		CreditSaleID: creditSaleID,
		Customer:     customer,
		Item: invoiceItemData{
			ProductID:     productID,
			Description:   productName,
			Quantity:      quantity,
			UnitPrice:     unitPrice,
			UnitPriceText: formatCurrency(unitPrice),
			LineTotal:     totalValue,
			LineTotalText: formatCurrency(totalValue),
		},
	}, nil
}

func resolveCustomerForInvoice(tx *sql.Tx, currentUser *User, source invoiceSourceSnapshot, input customerInput) (*Customer, error) {
	tenantID := tenantIDFromUser(currentUser)
	input.Name = strings.TrimSpace(input.Name)
	input.Phone = strings.TrimSpace(input.Phone)
	input.DocumentType = strings.TrimSpace(input.DocumentType)
	input.DocumentNumber = strings.TrimSpace(input.DocumentNumber)
	input.Address = strings.TrimSpace(input.Address)
	input.City = strings.TrimSpace(input.City)
	input.Notes = strings.TrimSpace(input.Notes)

	if input.CustomerID > 0 || input.Name != "" || input.DocumentNumber != "" || input.Phone != "" {
		if fields := validateCustomerInput(input); len(fields) > 0 {
			return nil, requestError{Status: http.StatusBadRequest, Message: "Datos inválidos.", Fields: fields}
		}
		customer, err := resolveCustomerForCredit(tx, tenantID, input)
		if err != nil {
			return nil, err
		}
		return customer, nil
	}

	if source.Customer != nil {
		return source.Customer, nil
	}

	return nil, requestError{Status: http.StatusBadRequest, Message: "Debes indicar el cliente para emitir la factura.", Fields: map[string]string{
		"customer_name": "El cliente es obligatorio para la factura.",
	}}
}

func findExistingInvoiceForReference(exec sqlQueryExecer, tenantID, saleID, creditSaleID int) (int, string, error) {
	var (
		invoiceID     int
		invoiceNumber string
	)
	switch {
	case saleID > 0:
		err := exec.QueryRow(`SELECT id, invoice_number FROM invoices WHERE tenant_id = ? AND sale_id = ? ORDER BY id DESC LIMIT 1`, normalizeTenantID(tenantID), saleID).Scan(&invoiceID, &invoiceNumber)
		if err == sql.ErrNoRows {
			return 0, "", nil
		}
		return invoiceID, invoiceNumber, err
	case creditSaleID > 0:
		err := exec.QueryRow(`SELECT id, invoice_number FROM invoices WHERE tenant_id = ? AND credit_sale_id = ? ORDER BY id DESC LIMIT 1`, normalizeTenantID(tenantID), creditSaleID).Scan(&invoiceID, &invoiceNumber)
		if err == sql.ErrNoRows {
			return 0, "", nil
		}
		return invoiceID, invoiceNumber, err
	default:
		return 0, "", nil
	}
}

func loadInvoiceViewDataForUser(db *sql.DB, currentUser *User, invoiceID int) (invoiceViewData, error) {
	tenantID := tenantIDFromUser(currentUser)
	var data invoiceViewData
	var (
		subtotal float64
		total    float64
	)
	err := db.QueryRow(`
		SELECT
			id,
			invoice_number,
			source_type,
			COALESCE(sale_id, 0),
			COALESCE(credit_sale_id, 0),
			COALESCE(customer_id, 0),
			COALESCE(customer_name, ''),
			COALESCE(customer_phone, ''),
			COALESCE(customer_document_type, ''),
			COALESCE(customer_document_number, ''),
			COALESCE(customer_address, ''),
			COALESCE(customer_city, ''),
			COALESCE(notes, ''),
			COALESCE(subtotal, 0),
			COALESCE(total, 0),
			COALESCE(status, 'issued'),
			COALESCE(created_at, '')
		FROM invoices
		WHERE tenant_id = ? AND id = ?
		LIMIT 1
	`, tenantID, invoiceID).Scan(
		&data.InvoiceID,
		&data.InvoiceNumber,
		&data.SourceType,
		&data.SaleID,
		&data.CreditSaleID,
		&data.CustomerID,
		&data.CustomerName,
		&data.CustomerPhone,
		&data.CustomerDocumentType,
		&data.CustomerDocumentNumber,
		&data.CustomerAddress,
		&data.CustomerCity,
		&data.Notes,
		&subtotal,
		&total,
		&data.Status,
		&data.CreatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return invoiceViewData{}, requestError{Status: http.StatusNotFound, Message: "Factura no encontrada."}
		}
		return invoiceViewData{}, err
	}
	data.SourceLabel = invoiceSourceLabel(data.SourceType)
	data.StatusLabel = invoiceStatusLabel(data.Status)
	data.SubtotalText = formatCurrency(subtotal)
	data.TotalText = formatCurrency(total)

	switch data.SourceType {
	case "credit":
		allowed, err := creditAccessibleByID(db, currentUser, data.CreditSaleID)
		if err != nil {
			return invoiceViewData{}, err
		}
		if !allowed {
			return invoiceViewData{}, requestError{Status: http.StatusForbidden, Message: "No tienes acceso a esta factura."}
		}
	default:
		var productID string
		if err := db.QueryRow(`SELECT producto_id FROM ventas WHERE tenant_id = ? AND id = ? LIMIT 1`, tenantID, data.SaleID).Scan(&productID); err != nil {
			if err == sql.ErrNoRows {
				return invoiceViewData{}, requestError{Status: http.StatusNotFound, Message: "Factura no encontrada."}
			}
			return invoiceViewData{}, err
		}
		allowed, err := productAccessibleByID(db, currentUser, productID)
		if err != nil {
			return invoiceViewData{}, err
		}
		if !allowed {
			return invoiceViewData{}, requestError{Status: http.StatusForbidden, Message: "No tienes acceso a esta factura."}
		}
	}

	rows, err := db.Query(`
		SELECT COALESCE(product_id, ''), description, quantity, COALESCE(unit_price, 0), COALESCE(total, 0)
		FROM invoice_items
		WHERE tenant_id = ? AND invoice_id = ?
		ORDER BY id ASC
	`, tenantID, invoiceID)
	if err != nil {
		return invoiceViewData{}, err
	}
	defer rows.Close()

	items := make([]invoiceItemData, 0)
	subtotal = 0
	total = 0
	for rows.Next() {
		var item invoiceItemData
		if err := rows.Scan(&item.ProductID, &item.Description, &item.Quantity, &item.UnitPrice, &item.LineTotal); err != nil {
			return invoiceViewData{}, err
		}
		item.UnitPriceText = formatCurrency(item.UnitPrice)
		item.LineTotalText = formatCurrency(item.LineTotal)
		subtotal += item.LineTotal
		total += item.LineTotal
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return invoiceViewData{}, err
	}

	data.Items = items
	data.SubtotalText = formatCurrency(subtotal)
	data.TotalText = formatCurrency(total)
	if parsed, ok := parseFlexibleTime(data.CreatedAt); ok {
		data.CreatedAt = parsed.In(appTimeLocation).Format("2006-01-02 15:04")
	}
	settings, err := loadBusinessSettingsForTenant(db, tenantID)
	if err != nil {
		settings = currentBusinessSettings()
	}
	_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantID)
	if err != nil {
		return invoiceViewData{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudieron cargar los movimientos disponibles."}
	}
	data.Settings = settings
	data.CanLoan = movementEnabled(movementEnabledMap, "prestamo")
	data.CanCredit = movementEnabled(movementEnabledMap, "credito")
	data.CurrentUser = currentUser
	return data, nil
}

func invoiceDetailForUser(db *sql.DB, currentUser *User, invoiceID int) (map[string]any, error) {
	data, err := loadInvoiceViewDataForUser(db, currentUser, invoiceID)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(data.Items))
	for _, item := range data.Items {
		items = append(items, map[string]any{
			"product_id":  item.ProductID,
			"description": item.Description,
			"quantity":    item.Quantity,
			"unit_price":  item.UnitPrice,
			"total":       item.LineTotal,
		})
	}
	return map[string]any{
		"id":                       data.InvoiceID,
		"invoice_number":           data.InvoiceNumber,
		"source_type":              data.SourceType,
		"source_label":             data.SourceLabel,
		"sale_id":                  data.SaleID,
		"credit_sale_id":           data.CreditSaleID,
		"customer_id":              data.CustomerID,
		"customer_name":            data.CustomerName,
		"customer_phone":           data.CustomerPhone,
		"customer_document_type":   data.CustomerDocumentType,
		"customer_document_number": data.CustomerDocumentNumber,
		"customer_address":         data.CustomerAddress,
		"customer_city":            data.CustomerCity,
		"notes":                    data.Notes,
		"subtotal":                 parseCurrencyToFloat(data.SubtotalText),
		"total":                    parseCurrencyToFloat(data.TotalText),
		"status":                   data.Status,
		"status_label":             data.StatusLabel,
		"created_at":               data.CreatedAt,
		"view_url":                 invoiceViewURL(data.InvoiceID),
		"items":                    items,
	}, nil
}

func listInvoicesForUser(db *sql.DB, currentUser *User, q, fromStr, toStr string, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 200 {
		limit = 200
	}
	tenantID := tenantIDFromUser(currentUser)
	q = strings.TrimSpace(strings.ToLower(q))
	args := []any{tenantID}
	query := `
		SELECT id, invoice_number, source_type, COALESCE(sale_id, 0), COALESCE(credit_sale_id, 0), COALESCE(customer_name, ''), COALESCE(customer_document_number, ''), COALESCE(total, 0), COALESCE(status, 'issued'), COALESCE(created_at, '')
		FROM invoices
		WHERE tenant_id = ?
	`
	if q != "" {
		query += ` AND (LOWER(invoice_number) LIKE ? OR LOWER(customer_name) LIKE ? OR LOWER(customer_document_number) LIKE ?)`
		like := "%" + q + "%"
		args = append(args, like, like, like)
	}
	if fromStr != "" {
		query += ` AND ` + sqlDatePrefixExpr("created_at") + ` >= ?`
		args = append(args, fromStr)
	}
	if toStr != "" {
		query += ` AND ` + sqlDatePrefixExpr("created_at") + ` <= ?`
		args = append(args, toStr)
	}
	query += ` ORDER BY created_at DESC, id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var (
			invoiceID        int
			invoiceNumber    string
			sourceType       string
			saleID           int
			creditSaleID     int
			customerName     string
			customerDocument string
			total            float64
			status           string
			createdAt        string
		)
		if err := rows.Scan(&invoiceID, &invoiceNumber, &sourceType, &saleID, &creditSaleID, &customerName, &customerDocument, &total, &status, &createdAt); err != nil {
			return nil, err
		}
		switch sourceType {
		case "credit":
			allowed, err := creditAccessibleByID(db, currentUser, creditSaleID)
			if err != nil {
				return nil, err
			}
			if !allowed {
				continue
			}
		default:
			var productID string
			if err := db.QueryRow(`SELECT producto_id FROM ventas WHERE tenant_id = ? AND id = ? LIMIT 1`, tenantID, saleID).Scan(&productID); err != nil {
				if err == sql.ErrNoRows {
					continue
				}
				return nil, err
			}
			allowed, err := productAccessibleByID(db, currentUser, productID)
			if err != nil {
				return nil, err
			}
			if !allowed {
				continue
			}
		}
		if parsed, ok := parseFlexibleTime(createdAt); ok {
			createdAt = parsed.In(appTimeLocation).Format("2006-01-02 15:04")
		}
		items = append(items, map[string]any{
			"id":                invoiceID,
			"invoice_number":    invoiceNumber,
			"source_type":       sourceType,
			"source_label":      invoiceSourceLabel(sourceType),
			"sale_id":           saleID,
			"credit_sale_id":    creditSaleID,
			"customer_name":     customerName,
			"customer_document": customerDocument,
			"total":             total,
			"status":            status,
			"status_label":      invoiceStatusLabel(status),
			"created_at":        createdAt,
			"view_url":          invoiceViewURL(invoiceID),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func parseCurrencyToFloat(value string) float64 {
	value = strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(value, "$", ""), ".", ""))
	if value == "" {
		return 0
	}
	parsed, _ := strconv.ParseFloat(value, 64)
	return parsed
}

func createInvoiceDocument(db *sql.DB, currentUser *User, input invoiceCreateInput, source string, decoratePayload func(map[string]any) map[string]any) (map[string]any, bool, error) {
	if currentUser == nil || !isStaffRole(currentUser.Role) {
		return nil, false, requestError{Status: http.StatusForbidden, Message: "Solo personal autorizado puede emitir facturas."}
	}
	tenantID := tenantIDFromUser(currentUser)
	input.Notes = strings.TrimSpace(input.Notes)
	if (input.SaleID > 0 && input.CreditSaleID > 0) || (input.SaleID <= 0 && input.CreditSaleID <= 0) {
		return nil, false, requestError{Status: http.StatusBadRequest, Message: "Debes indicar sale_id o credit_sale_id.", Fields: map[string]string{
			"source": "Debes indicar una venta o un crédito.",
		}}
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()

	existingID, _, err := findExistingInvoiceForReference(tx, tenantID, input.SaleID, input.CreditSaleID)
	if err != nil {
		return nil, false, err
	}
	if existingID > 0 {
		item, err := invoiceDetailForUser(db, currentUser, existingID)
		return item, false, err
	}

	var sourceSnapshot invoiceSourceSnapshot
	if input.SaleID > 0 {
		sourceSnapshot, err = loadSaleInvoiceSource(db, currentUser, input.SaleID)
	} else {
		sourceSnapshot, err = loadCreditInvoiceSource(db, currentUser, input.CreditSaleID)
	}
	if err != nil {
		return nil, false, err
	}

	customer, err := resolveCustomerForInvoice(tx, currentUser, sourceSnapshot, input.Customer)
	if err != nil {
		return nil, false, err
	}

	now := time.Now().Format(time.RFC3339)
	invoiceID, err := insertAndReturnID(tx, `
		INSERT INTO invoices (
			tenant_id, invoice_number, source_type, sale_id, credit_sale_id, customer_id,
			customer_name, customer_phone, customer_document_type, customer_document_number,
			customer_address, customer_city, notes, subtotal, total, status, created_by, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'issued', ?, ?)
	`, tenantID, "", sourceSnapshot.SourceType, nullableIntValue(input.SaleID), nullableIntValue(input.CreditSaleID), nullableIntValue(customer.ID), customer.Name, customer.Phone, customer.DocumentType, customer.DocumentNumber, customer.Address, customer.City, input.Notes, sourceSnapshot.Item.LineTotal, sourceSnapshot.Item.LineTotal, nullableUserID(currentUser), now)
	if err != nil {
		return nil, false, err
	}
	number := invoiceNumber(invoiceID, time.Now())
	if _, err := tx.Exec(`UPDATE invoices SET invoice_number = ? WHERE tenant_id = ? AND id = ?`, number, tenantID, invoiceID); err != nil {
		return nil, false, err
	}
	if _, err := tx.Exec(`
		INSERT INTO invoice_items (tenant_id, invoice_id, product_id, description, quantity, unit_price, total)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, tenantID, invoiceID, sourceSnapshot.Item.ProductID, sourceSnapshot.Item.Description, sourceSnapshot.Item.Quantity, sourceSnapshot.Item.UnitPrice, sourceSnapshot.Item.LineTotal); err != nil {
		return nil, false, err
	}
	auditPayload := map[string]any{
		"invoice_id":        invoiceID,
		"invoice_number":    number,
		"source_type":       sourceSnapshot.SourceType,
		"sale_id":           input.SaleID,
		"credit_sale_id":    input.CreditSaleID,
		"customer_id":       customer.ID,
		"customer_name":     customer.Name,
		"customer_document": customer.DocumentNumber,
		"total":             sourceSnapshot.Item.LineTotal,
	}
	if decoratePayload != nil {
		auditPayload = decoratePayload(auditPayload)
	}
	if err := logAuditEvent(tx, currentUser, "invoice_created", "invoice", strconv.FormatInt(invoiceID, 10), source, auditPayload); err != nil {
		return nil, false, err
	}
	if customer.ID > 0 {
		if err := logCustomerEvent(tx, currentUser, customer.ID, "invoice_created", "invoice", strconv.FormatInt(invoiceID, 10), sourceSnapshot.Item.LineTotal, map[string]any{
			"invoice_number": number,
			"source_type":    sourceSnapshot.SourceType,
			"sale_id":        input.SaleID,
			"credit_sale_id": input.CreditSaleID,
			"total":          sourceSnapshot.Item.LineTotal,
		}); err != nil {
			return nil, false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	item, err := invoiceDetailForUser(db, currentUser, int(invoiceID))
	if err != nil {
		return nil, false, err
	}
	return item, true, nil
}

func loadInvoiceFormData(db *sql.DB, currentUser *User, input invoiceCreateInput, flash, errText string) (invoiceFormData, error) {
	var (
		sourceSnapshot invoiceSourceSnapshot
		err            error
	)
	switch {
	case input.SaleID > 0:
		sourceSnapshot, err = loadSaleInvoiceSource(db, currentUser, input.SaleID)
	case input.CreditSaleID > 0:
		sourceSnapshot, err = loadCreditInvoiceSource(db, currentUser, input.CreditSaleID)
	default:
		return invoiceFormData{}, requestError{Status: http.StatusBadRequest, Message: "Debes indicar una venta o un crédito para generar la factura."}
	}
	if err != nil {
		return invoiceFormData{}, err
	}

	data := invoiceFormData{
		Title:         "Generar factura",
		Subtitle:      "Factura operativa simple vinculada a una venta o a un crédito existente.",
		Flash:         strings.TrimSpace(flash),
		Error:         strings.TrimSpace(errText),
		SourceType:    sourceSnapshot.SourceType,
		SourceLabel:   sourceSnapshot.SourceLabel,
		SaleID:        input.SaleID,
		CreditSaleID:  input.CreditSaleID,
		ProductName:   sourceSnapshot.Item.Description,
		Quantity:      sourceSnapshot.Item.Quantity,
		UnitPriceText: sourceSnapshot.Item.UnitPriceText,
		TotalText:     sourceSnapshot.Item.LineTotalText,
		Notes:         input.Notes,
		CurrentUser:   currentUser,
	}
	_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantIDFromUser(currentUser))
	if err != nil {
		return invoiceFormData{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudieron cargar los movimientos disponibles."}
	}
	data.CanLoan = movementEnabled(movementEnabledMap, "prestamo")
	data.CanCredit = movementEnabled(movementEnabledMap, "credito")

	customer := sourceSnapshot.Customer
	if input.Customer.CustomerID > 0 ||
		input.Customer.Name != "" ||
		input.Customer.Phone != "" ||
		input.Customer.DocumentType != "" ||
		input.Customer.DocumentNumber != "" ||
		input.Customer.Address != "" ||
		input.Customer.City != "" {
		data.CustomerID = input.Customer.CustomerID
		data.CustomerName = input.Customer.Name
		data.CustomerPhone = input.Customer.Phone
		data.CustomerDocumentType = input.Customer.DocumentType
		data.CustomerDocumentNumber = input.Customer.DocumentNumber
		data.CustomerAddress = input.Customer.Address
		data.CustomerCity = input.Customer.City
	} else if customer != nil {
		data.CustomerID = customer.ID
		data.CustomerName = customer.Name
		data.CustomerPhone = customer.Phone
		data.CustomerDocumentType = customer.DocumentType
		data.CustomerDocumentNumber = customer.DocumentNumber
		data.CustomerAddress = customer.Address
		data.CustomerCity = customer.City
	}
	return data, nil
}

type apiInvoicePayload struct {
	SaleID                 int    `json:"sale_id"`
	CreditSaleID           int    `json:"credit_sale_id"`
	CustomerID             int    `json:"customer_id"`
	CustomerName           string `json:"customer_name"`
	CustomerPhone          string `json:"customer_phone"`
	CustomerDocumentType   string `json:"customer_document_type"`
	CustomerDocumentNumber string `json:"customer_document_number"`
	CustomerAddress        string `json:"customer_address"`
	CustomerCity           string `json:"customer_city"`
	CustomerNotes          string `json:"customer_notes"`
	DebtorName             string `json:"debtor_name"`
	DebtorPhone            string `json:"debtor_phone"`
	DebtorDocumentType     string `json:"debtor_document_type"`
	DebtorDocumentNumber   string `json:"debtor_document_number"`
	Notes                  string `json:"notes"`
}

func (p apiInvoicePayload) customerInput() customerInput {
	return customerInput{
		CustomerID:     p.CustomerID,
		Name:           firstNonEmptyString(p.CustomerName, p.DebtorName),
		Phone:          firstNonEmptyString(p.CustomerPhone, p.DebtorPhone),
		DocumentType:   firstNonEmptyString(p.CustomerDocumentType, p.DebtorDocumentType),
		DocumentNumber: firstNonEmptyString(p.CustomerDocumentNumber, p.DebtorDocumentNumber),
		Address:        strings.TrimSpace(p.CustomerAddress),
		City:           strings.TrimSpace(p.CustomerCity),
		Notes:          strings.TrimSpace(p.CustomerNotes),
	}
}

func nullableIntValue(value int) any {
	if value <= 0 {
		return nil
	}
	return value
}

func parseIntOrZero(value string) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || parsed <= 0 {
		return 0
	}
	return parsed
}

// formatIntDots formats an integer with '.' as thousands separator (e.g. 1234567 -> "1.234.567").
// This matches common Spanish formatting and improves readability in UI.
func formatIntDots(n int64) string {
	if n == 0 {
		return "0"
	}
	sign := ""
	if n < 0 {
		sign = "-"
		n = -n
	}

	s := strconv.FormatInt(n, 10)
	// Insert '.' every 3 digits from the right.
	out := make([]byte, 0, len(s)+len(s)/3)
	rem := len(s) % 3
	if rem == 0 {
		rem = 3
	}
	out = append(out, s[:rem]...)
	for i := rem; i < len(s); i += 3 {
		out = append(out, '.')
		out = append(out, s[i:i+3]...)
	}
	return sign + string(out)
}

func parseDateOrDefault(value string, fallback time.Time) time.Time {
	if value == "" {
		return fallback
	}
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil {
		return fallback
	}
	return parsed
}

// parseCOPInteger parses a currency-like string into an integer COP value.
// It accepts plain numbers and formatted inputs (e.g. "1.234.567", "$1,234,567").
func parseCOPInteger(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty")
	}
	digits := strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, raw)
	if digits == "" {
		return 0, fmt.Errorf("invalid")
	}
	v, err := strconv.Atoi(digits)
	if err != nil {
		return 0, err
	}
	return v, nil
}

func parseFlexibleTime(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false
	}
	// Common formats used in this app/SQLite:
	// - RFC3339 for movimiento/unidad timestamps
	// - "YYYY-MM-DD HH:MM:SS" for SQLite CURRENT_TIMESTAMP
	// - "YYYY-MM-DD" for date-only values
	layouts := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		var (
			t   time.Time
			err error
		)
		if strings.Contains(layout, "-07:00") || layout == time.RFC3339 {
			t, err = time.Parse(layout, value)
		} else {
			t, err = time.ParseInLocation(layout, value, appTimeLocation)
		}
		if err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// monthsBetween returns the number of full months elapsed from start to end.
func monthsBetween(start, end time.Time) int {
	start = time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC)
	end = time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, time.UTC)
	if end.Before(start) {
		start, end = end, start
	}
	months := int(end.Year()-start.Year())*12 + int(end.Month()-start.Month())
	// If we haven't reached the "day of month" yet, subtract a month.
	if end.Day() < start.Day() {
		months--
	}
	if months < 0 {
		return 0
	}
	return months
}

func statusLabel(estado string) string {
	labels := map[string]string{
		"Prestada":   "Prestado",
		"Prestado":   "Prestado",
		"loaned":     "Prestado",
		"available":  "Disponible",
		"sold":       "Vendido",
		"swapped":    "Cambio",
		"Disponible": "Disponible",
		"Vendida":    "Vendido",
		"Vendido":    "Vendido",
		"Cambio":     "Cambio",
	}
	if label, ok := labels[estado]; ok {
		return label
	}
	return estado
}

func buildTimelinePoints(timeline []timelinePoint, width, height, padding float64) string {
	if len(timeline) == 0 {
		return ""
	}
	if len(timeline) == 1 {
		x := padding
		y := height - padding - (timeline[0].Percent/100)*(height-2*padding)
		return fmt.Sprintf("%.1f,%.1f", x, y)
	}
	step := (width - 2*padding) / float64(len(timeline)-1)
	points := make([]string, 0, len(timeline))
	for i, point := range timeline {
		x := padding + step*float64(i)
		y := height - padding - (point.Percent/100)*(height-2*padding)
		points = append(points, fmt.Sprintf("%.1f,%.1f", x, y))
	}
	return strings.Join(points, " ")
}

func generateToken() (string, error) {
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(tokenBytes), nil
}

func setSessionCookie(w http.ResponseWriter, token string, expiresAt time.Time, secure bool) {
	http.SetCookie(w, &http.Cookie{
		Name:     "session_token",
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   secure,
		Expires:  expiresAt,
	})
}

func clearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     "session_token",
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
}

func hashAPIToken(token string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return hex.EncodeToString(sum[:])
}

func normalizeTenantID(tenantID int) int {
	if tenantID <= 0 {
		return defaultTenantID
	}
	return tenantID
}

func defaultTenant() Tenant {
	return Tenant{
		ID:     defaultTenantID,
		Slug:   defaultTenantSlug,
		Name:   defaultTenantName,
		Active: true,
	}
}

func isPlatformAdmin(user *User) bool {
	return user != nil && user.Role == rolePlatformAdmin
}

func isAdminRole(role string) bool {
	return role == roleAdmin || role == rolePlatformAdmin
}

func isStaffRole(role string) bool {
	return isAdminRole(role) || role == roleEmployee
}

func isValidManagedRole(role string, allowPlatform bool) bool {
	switch role {
	case roleEmployee, roleAdmin:
		return true
	case rolePlatformAdmin:
		return allowPlatform
	default:
		return false
	}
}

func canManageTenants(user *User) bool {
	return isPlatformAdmin(user)
}

func normalizeTenantSlug(value string) string {
	replacer := strings.NewReplacer(
		"á", "a",
		"é", "e",
		"í", "i",
		"ó", "o",
		"ú", "u",
		"Á", "a",
		"É", "e",
		"Í", "i",
		"Ó", "o",
		"Ú", "u",
	)
	value = strings.TrimSpace(strings.ToLower(replacer.Replace(value)))

	var builder strings.Builder
	lastWasDash := false
	for _, r := range value {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			builder.WriteRune(r)
			lastWasDash = false
		case r == ' ' || r == '-' || r == '_' || r == '.':
			if builder.Len() == 0 || lastWasDash {
				continue
			}
			builder.WriteByte('-')
			lastWasDash = true
		}
	}

	return strings.Trim(builder.String(), "-")
}

func listTenants(db *sql.DB) ([]Tenant, error) {
	rows, err := db.Query(`
		SELECT
			t.id,
			t.slug,
			t.name,
			t.active,
			t.created_at,
			t.updated_at,
			COALESCE((
				SELECT u.username
				FROM users u
				WHERE COALESCE(NULLIF(u.tenant_id, 0), ?) = t.id
					AND u.role = 'admin'
				ORDER BY u.id ASC
				LIMIT 1
			), ''),
			COALESCE((
				SELECT k.name
				FROM api_keys k
				WHERE COALESCE(NULLIF(k.tenant_id, 0), ?) = t.id
				ORDER BY
					CASE
						WHEN k.name LIKE '%-inicial' THEN 0
						ELSE 1
					END,
					k.id ASC
				LIMIT 1
			), '')
		FROM tenants t
		ORDER BY id ASC
	`, defaultTenantID, defaultTenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tenants := make([]Tenant, 0, 16)
	for rows.Next() {
		var item Tenant
		var active int
		if err := rows.Scan(
			&item.ID,
			&item.Slug,
			&item.Name,
			&active,
			&item.CreatedAt,
			&item.UpdatedAt,
			&item.InitialAdminUsername,
			&item.InitialAPIKeyName,
		); err != nil {
			return nil, err
		}
		item.Active = active == 1
		item.IsDefault = normalizeTenantID(item.ID) == defaultTenantID
		item.CreatedAt = formatDateWithSettings(item.CreatedAt)
		item.UpdatedAt = formatDateWithSettings(item.UpdatedAt)
		tenants = append(tenants, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tenants, nil
}

func loadTenantForManagement(db *sql.DB, tenantID int) (*Tenant, error) {
	tenantID = normalizeTenantID(tenantID)

	var (
		item   Tenant
		active int
	)
	err := db.QueryRow(`
		SELECT id, slug, name, active, created_at, updated_at
		FROM tenants
		WHERE id = ?
	`, tenantID).Scan(&item.ID, &item.Slug, &item.Name, &active, &item.CreatedAt, &item.UpdatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, requestError{Status: http.StatusNotFound, Message: "La empresa no existe."}
		}
		return nil, err
	}
	item.Active = active == 1
	item.IsDefault = tenantID == defaultTenantID
	return &item, nil
}

func updateTenantBasics(db *sql.DB, currentUser *User, tenantID int, name, slug string) (*Tenant, error) {
	if !canManageTenants(currentUser) {
		return nil, requestError{Status: http.StatusForbidden, Message: "No tienes permisos para editar empresas."}
	}

	currentTenant, err := loadTenantForManagement(db, tenantID)
	if err != nil {
		return nil, err
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return nil, requestError{Status: http.StatusBadRequest, Message: "El nombre de la empresa es obligatorio."}
	}

	slug = strings.TrimSpace(slug)
	if currentTenant.IsDefault {
		if slug != "" && slug != currentTenant.Slug {
			return nil, requestError{Status: http.StatusBadRequest, Message: "El slug del tenant base no se puede editar en esta fase."}
		}
		slug = currentTenant.Slug
	} else {
		slug = normalizeTenantSlug(slug)
		if slug == "" {
			slug = normalizeTenantSlug(name)
		}
		if slug == "" {
			return nil, requestError{Status: http.StatusBadRequest, Message: "El slug de la empresa es obligatorio."}
		}
	}

	if currentTenant.Name == name && currentTenant.Slug == slug {
		return currentTenant, nil
	}

	now := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		UPDATE tenants
		SET name = ?, slug = ?, updated_at = ?
		WHERE id = ?
	`, name, slug, now, currentTenant.ID); err != nil {
		errText := strings.ToLower(err.Error())
		if strings.Contains(errText, "tenants_slug") || strings.Contains(errText, "unique constraint failed: tenants.slug") {
			return nil, requestError{Status: http.StatusBadRequest, Message: "Ya existe una empresa con ese slug."}
		}
		return nil, err
	}

	payload := map[string]any{
		"previous_name": currentTenant.Name,
		"new_name":      name,
	}
	if currentTenant.Slug != slug {
		payload["previous_slug"] = currentTenant.Slug
		payload["new_slug"] = slug
	}
	if err := logAuditEvent(db, currentUser, "tenant_updated", "tenant", strconv.Itoa(currentTenant.ID), "manual", payload); err != nil {
		return nil, err
	}

	return loadTenantForManagement(db, currentTenant.ID)
}

func setTenantActiveState(db *sql.DB, currentUser *User, tenantID int, active bool) (*Tenant, error) {
	if !canManageTenants(currentUser) {
		return nil, requestError{Status: http.StatusForbidden, Message: "No tienes permisos para administrar empresas."}
	}

	currentTenant, err := loadTenantForManagement(db, tenantID)
	if err != nil {
		return nil, err
	}
	if currentTenant.IsDefault {
		return nil, requestError{Status: http.StatusBadRequest, Message: "El tenant base no se puede activar ni inactivar en esta fase."}
	}
	if currentTenant.Active == active {
		return currentTenant, nil
	}

	now := time.Now().Format(time.RFC3339)
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`
		UPDATE tenants
		SET active = ?, updated_at = ?
		WHERE id = ?
	`, boolToInt(active), now, currentTenant.ID); err != nil {
		return nil, err
	}
	if !active {
		if _, err := tx.Exec(`DELETE FROM sessions WHERE tenant_id = ?`, currentTenant.ID); err != nil {
			return nil, err
		}
	}

	eventType := "tenant_activated"
	if !active {
		eventType = "tenant_deactivated"
	}
	if err := logAuditEvent(tx, currentUser, eventType, "tenant", strconv.Itoa(currentTenant.ID), "manual", map[string]any{
		"tenant_name": currentTenant.Name,
		"tenant_slug": currentTenant.Slug,
		"active":      active,
	}); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return loadTenantForManagement(db, currentTenant.ID)
}

func createAPIKeyRecord(exec sqlExecer, tenantID int, name, token, now string) error {
	_, err := exec.Exec(`
		INSERT INTO api_keys (name, token_hash, tenant_id, active, created_at, updated_at)
		VALUES (?, ?, ?, 1, ?, ?)
	`, strings.TrimSpace(name), hashAPIToken(token), normalizeTenantID(tenantID), now, now)
	return err
}

func isReservedInitialAPIKeyName(name string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(name)), "-inicial")
}

func isInitialTenantAPIKey(tenant *Tenant, keyName string) bool {
	normalized := strings.ToLower(strings.TrimSpace(keyName))
	if normalized == "" {
		return false
	}
	return normalized == strings.ToLower(initialAPIKeyNameForTenant(tenant)) || strings.HasSuffix(normalized, "-inicial")
}

func loadAPIKeyForTenant(db *sql.DB, tenantID, keyID int) (*APIKey, error) {
	tenantID = normalizeTenantID(tenantID)
	var (
		item   APIKey
		active int
	)
	err := db.QueryRow(`
		SELECT id, name, COALESCE(NULLIF(tenant_id, 0), ?), active, created_at, updated_at
		FROM api_keys
		WHERE id = ? AND COALESCE(NULLIF(tenant_id, 0), ?) = ?
		LIMIT 1
	`, defaultTenantID, keyID, defaultTenantID, tenantID).Scan(&item.ID, &item.Name, &item.TenantID, &active, &item.CreatedAt, &item.UpdatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, requestError{Status: http.StatusNotFound, Message: "API key no encontrada."}
		}
		return nil, err
	}
	item.Active = active == 1
	item.TenantID = normalizeTenantID(item.TenantID)
	return &item, nil
}

func updateTenantAPIKey(db *sql.DB, currentUser *User, keyID int, name string, active bool) error {
	tenantID := tenantIDFromUser(currentUser)
	key, err := loadAPIKeyForTenant(db, tenantID, keyID)
	if err != nil {
		return err
	}
	tenant, err := resolveTenantByID(db, tenantID)
	if err != nil {
		return err
	}
	if isInitialTenantAPIKey(tenant, key.Name) {
		if strings.TrimSpace(name) != key.Name || active != key.Active {
			return requestError{Status: http.StatusBadRequest, Message: "La API key inicial se gestiona desde Empresas / tenants. Usa regenerar API key inicial."}
		}
		return nil
	}

	result, err := db.Exec(`
		UPDATE api_keys
		SET name = ?, active = ?, updated_at = ?
		WHERE id = ? AND COALESCE(NULLIF(tenant_id, 0), ?) = ?
	`, strings.TrimSpace(name), boolToInt(active), time.Now().Format(time.RFC3339), keyID, defaultTenantID, tenantID)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			return requestError{Status: http.StatusBadRequest, Message: "Ya existe una API key con ese nombre."}
		}
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected == 0 {
		return requestError{Status: http.StatusNotFound, Message: "API key no encontrada."}
	}
	return nil
}

func initialAPIKeyNameForTenant(tenant *Tenant) string {
	if tenant == nil {
		return "tenant-inicial"
	}
	slug := normalizeTenantSlug(tenant.Slug)
	if slug == "" {
		slug = normalizeTenantSlug(tenant.Name)
	}
	if slug == "" {
		slug = fmt.Sprintf("tenant-%d", normalizeTenantID(tenant.ID))
	}
	return slug + "-inicial"
}

func rotateTenantInitialAPIKey(db *sql.DB, currentUser *User, tenantID int) (string, string, error) {
	if !canManageTenants(currentUser) {
		return "", "", requestError{Status: http.StatusForbidden, Message: "No tienes permisos para regenerar la API key inicial."}
	}

	tenant, err := resolveTenantByID(db, tenantID)
	if err != nil {
		return "", "", err
	}

	initialName := initialAPIKeyNameForTenant(tenant)
	token, err := generateToken()
	if err != nil {
		return "", "", err
	}
	now := time.Now().Format(time.RFC3339)

	tx, err := db.Begin()
	if err != nil {
		return "", "", err
	}
	defer tx.Rollback()

	var (
		existingID   int
		existingName string
		found        bool
	)
	err = tx.QueryRow(`
		SELECT id, name
		FROM api_keys
		WHERE COALESCE(NULLIF(tenant_id, 0), ?) = ? AND name = ?
		ORDER BY id ASC
		LIMIT 1
	`, defaultTenantID, tenant.ID, initialName).Scan(&existingID, &existingName)
	if err == sql.ErrNoRows {
		err = tx.QueryRow(`
			SELECT id, name
			FROM api_keys
			WHERE COALESCE(NULLIF(tenant_id, 0), ?) = ? AND name LIKE ?
			ORDER BY id ASC
			LIMIT 1
		`, defaultTenantID, tenant.ID, "%-inicial").Scan(&existingID, &existingName)
	}
	if err != nil && err != sql.ErrNoRows {
		return "", "", err
	}
	found = err == nil

	if found {
		if _, err := tx.Exec(`
			UPDATE api_keys
			SET name = ?, token_hash = ?, active = 1, updated_at = ?
			WHERE id = ?
		`, initialName, hashAPIToken(token), now, existingID); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "unique") {
				return "", "", requestError{Status: http.StatusBadRequest, Message: "No se pudo regenerar la API key inicial por conflicto de nombre."}
			}
			return "", "", err
		}
	} else {
		if err := createAPIKeyRecord(tx, tenant.ID, initialName, token, now); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "unique") {
				return "", "", requestError{Status: http.StatusBadRequest, Message: "No se pudo crear la API key inicial por conflicto de nombre."}
			}
			return "", "", err
		}
	}

	if err := logAuditEvent(tx, currentUser, "tenant_initial_api_key_rotated", "tenant", strconv.Itoa(tenant.ID), "manual", map[string]any{
		"tenant_name":           tenant.Name,
		"tenant_slug":           tenant.Slug,
		"api_key_name":          initialName,
		"reused_existing_key":   found,
		"previous_api_key_name": existingName,
	}); err != nil {
		return "", "", err
	}

	if err := tx.Commit(); err != nil {
		return "", "", err
	}
	return initialName, token, nil
}

func createTenantWithSeed(db *sql.DB, currentUser *User, usersCols map[string]bool, name, slug, adminUsername, adminPassword string) (*tenantProvisionResult, error) {
	if !canManageTenants(currentUser) {
		return nil, requestError{Status: http.StatusForbidden, Message: "No tienes permisos para crear empresas."}
	}

	name = strings.TrimSpace(name)
	slug = normalizeTenantSlug(slug)
	adminUsername = strings.TrimSpace(adminUsername)
	if slug == "" {
		slug = normalizeTenantSlug(name)
	}
	if name == "" {
		return nil, requestError{Status: http.StatusBadRequest, Message: "El nombre de la empresa es obligatorio."}
	}
	if slug == "" {
		return nil, requestError{Status: http.StatusBadRequest, Message: "No se pudo generar un slug válido para la empresa."}
	}
	if adminUsername == "" {
		return nil, requestError{Status: http.StatusBadRequest, Message: "El usuario admin inicial es obligatorio."}
	}
	if adminPassword == "" {
		return nil, requestError{Status: http.StatusBadRequest, Message: "La contraseña inicial del admin es obligatoria."}
	}

	sourceTenantID := normalizeTenantID(currentUser.TenantID)
	sourceSettings, err := loadBusinessSettingsForTenant(db, sourceTenantID)
	if err != nil {
		return nil, err
	}
	sourceLines, err := loadBusinessLinesForTenant(db, sourceTenantID, false)
	if err != nil {
		return nil, err
	}
	sourcePaymentMethods, err := loadPaymentMethodsForTenant(db, sourceTenantID, false)
	if err != nil {
		return nil, err
	}
	sourceMovementSettings, _, err := loadMovementSettingsForTenant(db, sourceTenantID)
	if err != nil {
		return nil, err
	}

	initialAPIKeyName := initialAPIKeyNameForTenant(&Tenant{Slug: slug, Name: name})
	initialAPIToken, err := generateToken()
	if err != nil {
		return nil, err
	}

	now := time.Now().Format(time.RFC3339)
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var existingUsernameCount int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM users WHERE username = ?`, adminUsername).Scan(&existingUsernameCount); err != nil {
		return nil, err
	}
	if existingUsernameCount > 0 {
		return nil, requestError{Status: http.StatusBadRequest, Message: "El usuario admin inicial ya existe."}
	}
	var existingAPIKeyNameCount int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM api_keys WHERE name = ?`, initialAPIKeyName).Scan(&existingAPIKeyNameCount); err != nil {
		return nil, err
	}
	if existingAPIKeyNameCount > 0 {
		return nil, requestError{Status: http.StatusBadRequest, Message: "Ya existe una API key inicial con ese nombre."}
	}

	tenantID, err := insertAndReturnID(tx, `
		INSERT INTO tenants (slug, name, active, created_at, updated_at)
		VALUES (?, ?, 1, ?, ?)
	`, slug, name, now, now)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "tenants_slug") || strings.Contains(strings.ToLower(err.Error()), "unique constraint failed: tenants.slug") {
			return nil, requestError{Status: http.StatusBadRequest, Message: "Ya existe una empresa con ese slug."}
		}
		return nil, err
	}

	if _, err := tx.Exec(`
		INSERT INTO business_settings (tenant_id, business_name, logo_path, primary_color, currency, date_format, label_paper_width, invoice_paper_width, ticket_paper_width, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, tenantID, name, sourceSettings.LogoPath, sourceSettings.PrimaryColor, sourceSettings.Currency, sourceSettings.DateFormat, sourceSettings.LabelPaperWidth, sourceSettings.InvoicePaperWidth, sourceSettings.TicketPaperWidth, now); err != nil {
		return nil, err
	}

	for _, line := range sourceLines {
		if _, err := tx.Exec(`
			INSERT INTO business_lines (tenant_id, name, active, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?)
		`, tenantID, line.Name, boolToInt(line.Active), now, now); err != nil {
			return nil, err
		}
	}

	if len(sourcePaymentMethods) == 0 {
		for idx, methodName := range defaultPaymentMethodNames() {
			if _, err := tx.Exec(`
				INSERT INTO payment_methods (tenant_id, name, active, sort_order, created_at, updated_at)
				VALUES (?, ?, 1, ?, ?, ?)
			`, tenantID, methodName, idx+1, now, now); err != nil {
				return nil, err
			}
		}
	} else {
		for idx, method := range sourcePaymentMethods {
			sortOrder := method.SortOrder
			if sortOrder <= 0 {
				sortOrder = idx + 1
			}
			if _, err := tx.Exec(`
				INSERT INTO payment_methods (tenant_id, name, active, sort_order, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?)
			`, tenantID, method.Name, boolToInt(method.Active), sortOrder, now, now); err != nil {
				return nil, err
			}
		}
	}

	if len(sourceMovementSettings) == 0 {
		for _, movementType := range defaultMovementTypes() {
			if _, err := tx.Exec(`
				INSERT INTO movement_settings (tenant_id, movement_type, enabled, updated_at)
				VALUES (?, ?, 1, ?)
			`, tenantID, movementType, now); err != nil {
				return nil, err
			}
		}
	} else {
		for _, setting := range sourceMovementSettings {
			if _, err := tx.Exec(`
				INSERT INTO movement_settings (tenant_id, movement_type, enabled, updated_at)
				VALUES (?, ?, ?, ?)
			`, tenantID, setting.MovementType, boolToInt(setting.Enabled), now); err != nil {
				return nil, err
			}
		}
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(adminPassword), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}
	userCols := []string{"username", "password_hash", "role"}
	userArgs := []any{adminUsername, string(hashedPassword), roleAdmin}
	if usersCols["name"] {
		userCols = append(userCols, "name")
		userArgs = append(userArgs, adminUsername)
	}
	if usersCols["email"] {
		email := adminUsername
		if !strings.Contains(email, "@") {
			email = adminUsername + "@local"
		}
		userCols = append(userCols, "email")
		userArgs = append(userArgs, email)
	}
	if usersCols["password_salt"] {
		userCols = append(userCols, "password_salt")
		userArgs = append(userArgs, "bcrypt")
	}
	if usersCols["created_at"] {
		userCols = append(userCols, "created_at")
		userArgs = append(userArgs, now)
	}
	if usersCols["tenant_id"] {
		userCols = append(userCols, "tenant_id")
		userArgs = append(userArgs, int(tenantID))
	}
	if usersCols["is_active"] {
		userCols = append(userCols, "is_active")
		userArgs = append(userArgs, 1)
	}
	if usersCols["active"] {
		userCols = append(userCols, "active")
		userArgs = append(userArgs, 1)
	}
	placeholders := make([]string, len(userCols))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	if _, err := tx.Exec(
		fmt.Sprintf("INSERT INTO users (%s) VALUES (%s)", strings.Join(userCols, ", "), strings.Join(placeholders, ", ")),
		userArgs...,
	); err != nil {
		return nil, requestError{Status: http.StatusBadRequest, Message: userCreateErrorText(err)}
	}

	if err := createAPIKeyRecord(tx, int(tenantID), initialAPIKeyName, initialAPIToken, now); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			return nil, requestError{Status: http.StatusBadRequest, Message: "No se pudo crear la API key inicial por conflicto de nombre."}
		}
		return nil, err
	}

	if err := logAuditEvent(tx, currentUser, "tenant_created", "tenant", strconv.FormatInt(tenantID, 10), "manual", map[string]any{
		"tenant_name":     name,
		"tenant_slug":     slug,
		"admin_username":  adminUsername,
		"initial_api_key": initialAPIKeyName,
	}); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	tenant, err := resolveTenantByID(db, int(tenantID))
	if err != nil {
		return nil, err
	}
	return &tenantProvisionResult{
		Tenant:            tenant,
		InitialAPIKeyName: initialAPIKeyName,
		InitialAPIToken:   initialAPIToken,
		InitialAdminUser:  adminUsername,
	}, nil
}

func tenantIDFromRequest(r *http.Request) int {
	if tenant := tenantFromContext(r); tenant != nil {
		return normalizeTenantID(tenant.ID)
	}
	return tenantIDFromUser(userFromContext(r))
}

func userFromContext(r *http.Request) *User {
	if user, ok := r.Context().Value(userContextKey).(*User); ok {
		return user
	}
	return nil
}

func tenantFromContext(r *http.Request) *Tenant {
	if tenant, ok := r.Context().Value(tenantContextKey).(*Tenant); ok {
		return tenant
	}
	return nil
}

func apiIntegrationNameFromContext(r *http.Request) string {
	if name, ok := r.Context().Value(apiIntegrationNameContextKey).(string); ok {
		return strings.TrimSpace(name)
	}
	return ""
}

func apiAuthModeFromContext(r *http.Request) string {
	if mode, ok := r.Context().Value(apiAuthModeContextKey).(string); ok {
		return strings.TrimSpace(mode)
	}
	return ""
}

func resolveTenantByID(db *sql.DB, tenantID int) (*Tenant, error) {
	tenantID = normalizeTenantID(tenantID)

	var (
		tenant Tenant
		active int
	)
	err := db.QueryRow(`
		SELECT id, slug, name, active, created_at, updated_at
		FROM tenants
		WHERE id = ?
	`, tenantID).Scan(&tenant.ID, &tenant.Slug, &tenant.Name, &active, &tenant.CreatedAt, &tenant.UpdatedAt)
	if err != nil {
		if err == sql.ErrNoRows && tenantID == defaultTenantID {
			fallback := defaultTenant()
			return &fallback, nil
		}
		return nil, err
	}
	tenant.Active = active == 1
	return &tenant, nil
}

func tableSQL(db *sql.DB, table string) (string, error) {
	if isPostgresDB() {
		return "", sql.ErrNoRows
	}
	var sqlText sql.NullString
	if err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&sqlText); err != nil {
		return "", err
	}
	return sqlText.String, nil
}

func migrateBusinessSettingsForTenancy(db *sql.DB) error {
	cols, err := tableColumns(db, "business_settings")
	if err != nil {
		return err
	}
	sqlText, err := tableSQL(db, "business_settings")
	if err != nil {
		return err
	}
	if cols["tenant_id"] && !strings.Contains(strings.ToLower(sqlText), "check (id = 1)") {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DROP TABLE IF EXISTS business_settings__tenant_new`); err != nil {
		return err
	}
	if _, err := tx.Exec(`
		CREATE TABLE business_settings__tenant_new (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			tenant_id INTEGER NOT NULL UNIQUE,
			business_name TEXT NOT NULL,
			logo_path TEXT NOT NULL DEFAULT '',
			primary_color TEXT NOT NULL DEFAULT '#0ea5c9',
			currency TEXT NOT NULL DEFAULT 'COP',
			date_format TEXT NOT NULL DEFAULT '2006-01-02',
			label_paper_width TEXT NOT NULL DEFAULT '58mm',
			invoice_paper_width TEXT NOT NULL DEFAULT '58mm',
			ticket_paper_width TEXT NOT NULL DEFAULT '58mm',
			updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`); err != nil {
		return err
	}

	hasPrintCols := cols["label_paper_width"] && cols["invoice_paper_width"] && cols["ticket_paper_width"]
	if cols["tenant_id"] {
		selectPrintCols := "'58mm', '58mm', '58mm'"
		if hasPrintCols {
			selectPrintCols = `
				COALESCE(NULLIF(label_paper_width, ''), '58mm'),
				COALESCE(NULLIF(invoice_paper_width, ''), '58mm'),
				COALESCE(NULLIF(ticket_paper_width, ''), '58mm')`
		}
		if _, err := tx.Exec(`
			INSERT OR REPLACE INTO business_settings__tenant_new
				(tenant_id, business_name, logo_path, primary_color, currency, date_format, label_paper_width, invoice_paper_width, ticket_paper_width, updated_at)
			SELECT COALESCE(NULLIF(tenant_id, 0), ?), business_name, logo_path, primary_color, currency, date_format, `+selectPrintCols+`, updated_at
			FROM business_settings
			ORDER BY id ASC
		`, defaultTenantID); err != nil {
			return err
		}
	} else {
		if _, err := tx.Exec(`
			INSERT OR REPLACE INTO business_settings__tenant_new
				(tenant_id, business_name, logo_path, primary_color, currency, date_format, label_paper_width, invoice_paper_width, ticket_paper_width, updated_at)
			SELECT ?, business_name, logo_path, primary_color, currency, date_format, '58mm', '58mm', '58mm', updated_at
			FROM business_settings
			ORDER BY id ASC
		`, defaultTenantID); err != nil {
			return err
		}
	}

	if _, err := tx.Exec(`DROP TABLE business_settings`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE business_settings__tenant_new RENAME TO business_settings`); err != nil {
		return err
	}
	if _, err := tx.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_business_settings_tenant_id ON business_settings(tenant_id)`); err != nil {
		return err
	}
	return tx.Commit()
}

func migrateTenantScopedLookupTable(db *sql.DB, table string, createSQL, copySQL string, indexes []string) error {
	cols, err := tableColumns(db, table)
	if err != nil {
		return err
	}
	if cols["tenant_id"] {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	tempTable := table + "__tenant_new"
	if _, err := tx.Exec(`DROP TABLE IF EXISTS ` + tempTable); err != nil {
		return err
	}
	if _, err := tx.Exec(createSQL); err != nil {
		return err
	}
	if _, err := tx.Exec(copySQL, defaultTenantID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DROP TABLE ` + table); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE ` + tempTable + ` RENAME TO ` + table); err != nil {
		return err
	}
	for _, stmt := range indexes {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func userFromRequest(db *sql.DB, r *http.Request) (*User, error) {
	cookie, err := r.Cookie("session_token")
	if err != nil {
		return nil, err
	}

	var (
		user            User
		isActive        int
		sessionTenantID int
		userTenantID    int
		expiresRaw      string
	)
	query := `
		SELECT u.id, u.username, u.role, u.is_active,
		       COALESCE(NULLIF(s.tenant_id, 0), ?),
		       COALESCE(NULLIF(u.tenant_id, 0), ?),
		       s.expires_at
		FROM sessions s
		JOIN users u ON u.id = s.user_id
		WHERE s.token = ?`
	if err := db.QueryRow(query, defaultTenantID, defaultTenantID, cookie.Value).Scan(&user.ID, &user.Username, &user.Role, &isActive, &sessionTenantID, &userTenantID, &expiresRaw); err != nil {
		return nil, err
	}
	expiresAt, err := time.Parse(time.RFC3339, expiresRaw)
	if err != nil {
		return nil, err
	}
	if time.Now().After(expiresAt) {
		invalidateSessionToken(db, cookie.Value)
		return nil, sql.ErrNoRows
	}
	sessionTenantID = normalizeTenantID(sessionTenantID)
	userTenantID = normalizeTenantID(userTenantID)
	if sessionTenantID != userTenantID {
		invalidateSessionToken(db, cookie.Value)
		return nil, sql.ErrNoRows
	}
	user.IsActive = isActive == 1
	user.TenantID = userTenantID
	if !user.IsActive {
		invalidateSessionToken(db, cookie.Value)
		return nil, sql.ErrNoRows
	}
	return &user, nil
}

func bearerTokenFromRequest(r *http.Request) string {
	authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
	if authHeader == "" {
		return ""
	}
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(strings.TrimSpace(parts[0]), "Bearer") {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

func integrationPrincipalForTenant(tenantID int, integrationName string) *User {
	tenantID = normalizeTenantID(tenantID)
	integrationName = strings.TrimSpace(integrationName)
	if integrationName == "" {
		integrationName = fmt.Sprintf("tenant-%d", tenantID)
	}
	// Keep API auth tenant-scoped without depending on any mutable human admin account.
	return &User{
		Username: "api:" + integrationName,
		Role:     roleAdmin,
		IsActive: true,
		TenantID: tenantID,
	}
}

func apiAuthFromRequest(db *sql.DB, r *http.Request) (*User, string, string, error) {
	token := bearerTokenFromRequest(r)
	if token != "" {
		var integrationName string
		var active int
		var tenantID int
		err := db.QueryRow(`
			SELECT name, active, COALESCE(NULLIF(tenant_id, 0), ?)
			FROM api_keys
			WHERE token_hash = ?
		`, defaultTenantID, hashAPIToken(token)).Scan(&integrationName, &active, &tenantID)
		if err != nil {
			return nil, "", "", err
		}
		if active != 1 {
			return nil, "", "", sql.ErrNoRows
		}

		integrationName = strings.TrimSpace(integrationName)
		return integrationPrincipalForTenant(tenantID, integrationName), integrationName, "api_key", nil
	}

	if user, err := userFromRequest(db, r); err == nil && user != nil {
		return user, "", "session", nil
	}

	return nil, "", "", sql.ErrNoRows
}

func apiBusinessSettingsForRequest(db *sql.DB, r *http.Request) (BusinessSettings, error) {
	return loadBusinessSettingsForTenant(db, tenantIDFromRequest(r))
}

func apiAssignableUsersForRequest(db *sql.DB, r *http.Request) ([]assignableUser, error) {
	return loadAssignableUsersForTenant(db, tenantIDFromRequest(r))
}

func handleAPISales(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		switch r.Method {
		case http.MethodGet:
			q := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("q")))
			fromStr := strings.TrimSpace(r.URL.Query().Get("from"))
			toStr := strings.TrimSpace(r.URL.Query().Get("to"))
			fields := map[string]string{}
			if fromStr != "" {
				if _, err := time.Parse("2006-01-02", fromStr); err != nil {
					fields["from"] = "Fecha inválida. Usa formato YYYY-MM-DD."
				}
			}
			if toStr != "" {
				if _, err := time.Parse("2006-01-02", toStr); err != nil {
					fields["to"] = "Fecha inválida. Usa formato YYYY-MM-DD."
				}
			}
			if len(fields) > 0 {
				writeAPIError(w, http.StatusBadRequest, "Datos inválidos.", fields)
				return
			}

			items, err := listSalesForUser(db, currentUser, q, fromStr, toStr, 100)
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar las ventas.", nil)
				return
			}
			writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "items": items, "count": len(items)})

		case http.MethodPost:
			_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantIDFromUser(currentUser))
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "Error al cargar tipos de movimiento.", nil)
				return
			}
			if !movementEnabled(movementEnabledMap, "venta") {
				writeAPIError(w, http.StatusForbidden, "La venta está deshabilitada en Configuración.", nil)
				return
			}
			var payload struct {
				ProductID     string   `json:"product_id"`
				Quantity      *int     `json:"quantity"`
				PaymentMethod string   `json:"payment_method"`
				UnitPrice     *float64 `json:"unit_price"`
				Total         *float64 `json:"total"`
				SalePrice     *float64 `json:"sale_price"`
				Channel       string   `json:"channel"`
				SoldBy        string   `json:"sold_by"`
				Notes         string   `json:"notes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				writeAPIError(w, http.StatusBadRequest, "JSON inválido.", nil)
				return
			}
			payload.ProductID = strings.TrimSpace(payload.ProductID)
			payload.PaymentMethod = strings.TrimSpace(payload.PaymentMethod)
			payload.Channel = strings.TrimSpace(payload.Channel)
			payload.SoldBy = strings.TrimSpace(payload.SoldBy)
			payload.Notes = strings.TrimSpace(payload.Notes)

			quantity := 1
			if payload.Quantity != nil {
				quantity = *payload.Quantity
			}

			activePaymentMethods, err := loadPaymentMethodsForTenant(db, tenantIDFromUser(currentUser), true)
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar los métodos de pago.", nil)
				return
			}
			paymentMethodOptions := paymentMethodNames(activePaymentMethods)

			fields := map[string]string{}
			if payload.ProductID == "" {
				fields["product_id"] = "Selecciona un producto válido."
			}
			if quantity <= 0 {
				fields["quantity"] = "La cantidad debe ser un número positivo."
			}

			var (
				productName      string
				productSalePrice float64
			)
			if payload.ProductID != "" {
				if err := db.QueryRow(`SELECT nombre, COALESCE(precio_venta, 0) FROM productos WHERE tenant_id = ? AND (sku = ? OR id = ?) LIMIT 1`, tenantIDFromUser(currentUser), payload.ProductID, payload.ProductID).Scan(&productName, &productSalePrice); err != nil {
					if err == sql.ErrNoRows {
						fields["product_id"] = "Selecciona un producto válido."
					} else {
						writeAPIError(w, http.StatusInternalServerError, "No se pudo cargar el producto.", nil)
						return
					}
				}
			}

			if allowed, err := productAccessibleByID(db, currentUser, payload.ProductID); err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudo validar acceso al producto.", nil)
				return
			} else if !allowed && fields["product_id"] == "" {
				fields["product_id"] = "No tienes acceso a este producto."
			}

			stockByProd, err := availableCountsByProduct(db, tenantIDFromUser(currentUser))
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "Error al consultar stock.", nil)
				return
			}
			if payload.ProductID != "" && quantity > 0 {
				if available := stockByProd[payload.ProductID]; available > 0 && quantity > available {
					fields["quantity"] = "No hay stock disponible suficiente para completar la venta."
				}
			}

			paymentMethod := payload.PaymentMethod
			if paymentMethod == "" && len(paymentMethodOptions) > 0 {
				paymentMethod = paymentMethodOptions[0]
			}
			validMethod := false
			for _, method := range paymentMethodOptions {
				if paymentMethod == method {
					validMethod = true
					break
				}
			}
			if !validMethod {
				fields["payment_method"] = "Selecciona un método de pago válido."
			}

			salePrice := productSalePrice
			switch {
			case payload.Total != nil && *payload.Total > 0 && quantity > 0:
				salePrice = *payload.Total / float64(quantity)
			case payload.SalePrice != nil:
				salePrice = *payload.SalePrice
			case payload.UnitPrice != nil:
				salePrice = *payload.UnitPrice
			}
			if salePrice <= 0 {
				fields["sale_price"] = "Ingresa sale_price o configura un precio de venta válido para el producto."
			}

			if len(fields) > 0 {
				writeAPIError(w, http.StatusBadRequest, "Datos inválidos.", fields)
				return
			}

			tx, err := db.Begin()
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "Error al procesar la venta.", nil)
				return
			}
			defer tx.Rollback()

			soldUnitIDs, err := selectAndMarkUnitsSold(tx, tenantIDFromUser(currentUser), payload.ProductID, quantity)
			if err != nil {
				if err == errInsufficientStock {
					writeAPIError(w, http.StatusBadRequest, "No hay stock disponible suficiente para completar la venta.", map[string]string{"quantity": "No hay stock disponible suficiente para completar la venta."})
					return
				}
				writeAPIError(w, http.StatusInternalServerError, "Error al actualizar inventario.", nil)
				return
			}

			now := time.Now().Format(time.RFC3339)
			if err := logMovimientos(tx, payload.ProductID, soldUnitIDs, "venta", payload.Notes, currentUser, now); err != nil {
				writeAPIError(w, http.StatusInternalServerError, "Error al registrar movimiento de venta.", nil)
				return
			}
			saleID, err := insertAndReturnID(tx, `INSERT INTO ventas (tenant_id, producto_id, cantidad, precio_final, metodo_pago, channel, sold_by, notas, fecha) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, tenantIDFromUser(currentUser), payload.ProductID, quantity, salePrice, paymentMethod, payload.Channel, payload.SoldBy, payload.Notes, now)
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "Error al registrar la venta.", nil)
				return
			}
			if err := logAuditEvent(tx, currentUser, "sale_registered", "sale", payload.ProductID, "api", withAPIAuditMetadata(r, map[string]any{
				"sale_id":     saleID,
				"producto_id": payload.ProductID,
				"producto":    productName,
				"cantidad":    quantity,
				"sale_price":  salePrice,
				"metodo_pago": paymentMethod,
				"channel":     payload.Channel,
				"sold_by":     payload.SoldBy,
				"notes":       payload.Notes,
				"total":       salePrice * float64(quantity),
			})); err != nil {
				writeAPIError(w, http.StatusInternalServerError, "Error al registrar la auditoría de la venta.", nil)
				return
			}
			if err := tx.Commit(); err != nil {
				writeAPIError(w, http.StatusInternalServerError, "Error al confirmar la venta.", nil)
				return
			}
			writeAPIJSON(w, http.StatusCreated, map[string]any{
				"ok":           true,
				"sale_id":      saleID,
				"product_id":   payload.ProductID,
				"product_name": productName,
				"quantity":     quantity,
				"sale_price":   salePrice,
				"message":      "Venta registrada correctamente.",
			})

		default:
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
		}
	}
}

func handleAPICredits(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		switch r.Method {
		case http.MethodGet:
			q := strings.TrimSpace(r.URL.Query().Get("q"))
			items, err := listCreditsForUser(db, currentUser, q, 100)
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar los créditos.", nil)
				return
			}
			writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "items": items, "count": len(items)})
		case http.MethodPost:
			var payload apiCreditPayload
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				writeAPIError(w, http.StatusBadRequest, "JSON inválido.", nil)
				return
			}
			resp, err := createCreditViaAPI(db, currentUser, payload, "api", creditSaleKindProduct, func(item map[string]any) map[string]any {
				return withAPIAuditMetadata(r, item)
			})
			if err != nil {
				var reqErr requestError
				if errors.As(err, &reqErr) {
					writeAPIError(w, reqErr.Status, reqErr.Message, reqErr.Fields)
					return
				}
				writeAPIError(w, http.StatusInternalServerError, "Error al registrar el crédito.", nil)
				return
			}
			writeAPIJSON(w, http.StatusCreated, resp)
		default:
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
		}
	}
}

func handleAPICreditRoutes(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		path := strings.TrimPrefix(r.URL.Path, "/api/credits/")
		path = strings.Trim(path, "/")
		if path == "" {
			writeAPIError(w, http.StatusNotFound, "Crédito no encontrado.", nil)
			return
		}
		parts := strings.Split(path, "/")
		creditSaleID, err := strconv.Atoi(parts[0])
		if err != nil || creditSaleID <= 0 {
			writeAPIError(w, http.StatusBadRequest, "Crédito inválido.", map[string]string{"credit_sale_id": "Crédito inválido."})
			return
		}

		if len(parts) > 1 {
			if len(parts) == 2 && parts[1] == "history" {
				if r.Method != http.MethodGet {
					writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
					return
				}
				limit := 20
				if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
					if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= 200 {
						limit = parsed
					}
				}
				items, err := creditEditHistoryForUser(db, currentUser, creditSaleID, limit)
				if err != nil {
					if err == sql.ErrNoRows {
						writeAPIError(w, http.StatusNotFound, "Crédito no encontrado.", nil)
						return
					}
					writeAPIError(w, http.StatusInternalServerError, "No se pudo cargar el historial del crédito.", nil)
					return
				}
				writeAPIJSON(w, http.StatusOK, map[string]any{
					"ok":             true,
					"credit_sale_id": creditSaleID,
					"count":          len(items),
					"items":          items,
				})
				return
			}
			writeAPIError(w, http.StatusNotFound, "Ruta de crédito no encontrada.", nil)
			return
		}

		switch r.Method {
		case http.MethodGet:
			item, err := creditDetailForUser(db, currentUser, creditSaleID)
			if err != nil {
				if err == sql.ErrNoRows {
					writeAPIError(w, http.StatusNotFound, "Crédito no encontrado.", nil)
					return
				}
				writeAPIError(w, http.StatusInternalServerError, "No se pudo cargar el crédito.", nil)
				return
			}
			writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "credit": item})
		case http.MethodPut, http.MethodPatch:
			var payload struct {
				InstallmentsTotal int     `json:"installments_total"`
				InstallmentsPaid  int     `json:"installments_paid"`
				InstallmentValue  float64 `json:"installment_value"`
				Notes             string  `json:"notes"`
				Status            string  `json:"status"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				writeAPIError(w, http.StatusBadRequest, "JSON inválido.", nil)
				return
			}
			result, err := updateCreditSale(db, currentUser, creditSaleID, creditSaleUpdateInput{
				InstallmentsTotal: payload.InstallmentsTotal,
				InstallmentsPaid:  payload.InstallmentsPaid,
				InstallmentValue:  payload.InstallmentValue,
				Notes:             payload.Notes,
				Status:            normalizeEditableCreditStatus(payload.Status),
			}, "api", func(item map[string]any) map[string]any {
				return withAPIAuditMetadata(r, item)
			})
			if err != nil {
				var reqErr requestError
				if errors.As(err, &reqErr) {
					writeAPIError(w, reqErr.Status, reqErr.Message, reqErr.Fields)
					return
				}
				writeAPIError(w, http.StatusInternalServerError, "No se pudo actualizar el crédito.", nil)
				return
			}
			item, err := creditDetailForUser(db, currentUser, result.CreditSaleID)
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "Crédito actualizado, pero no se pudo cargar el detalle.", nil)
				return
			}
			writeAPIJSON(w, http.StatusOK, map[string]any{
				"ok":      true,
				"credit":  item,
				"message": "Crédito actualizado correctamente.",
			})
		default:
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
		}
	}
}

func handleAPICreditsEditedReport(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		currentUser := userFromContext(r)
		if currentUser == nil || !isAdminRole(currentUser.Role) {
			writeAPIError(w, http.StatusForbidden, "Solo administrador puede consultar créditos editados.", nil)
			return
		}
		limit := 100
		if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= 500 {
				limit = parsed
			}
		}
		creditSaleID := 0
		if raw := strings.TrimSpace(r.URL.Query().Get("credit_sale_id")); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
				creditSaleID = parsed
			}
		}
		items, err := listEditedCreditsReport(db, currentUser, tenantIDFromRequest(r), creditEditReportFilters{
			DateFrom:     r.URL.Query().Get("date_from"),
			DateTo:       r.URL.Query().Get("date_to"),
			Username:     r.URL.Query().Get("username"),
			Status:       r.URL.Query().Get("status"),
			Kind:         r.URL.Query().Get("kind"),
			Customer:     r.URL.Query().Get("customer"),
			CreditSaleID: creditSaleID,
			Limit:        limit,
		})
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				writeAPIError(w, reqErr.Status, reqErr.Message, reqErr.Fields)
				return
			}
			writeAPIError(w, http.StatusInternalServerError, "No se pudo cargar el reporte de créditos editados.", nil)
			return
		}
		payloadItems := make([]map[string]any, 0, len(items))
		for _, item := range items {
			payloadItems = append(payloadItems, creditEditReportItemAPI(item))
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{
			"ok":    true,
			"count": len(payloadItems),
			"items": payloadItems,
		})
	}
}

func handleAPIAgentCredits(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}

		currentUser := userFromContext(r)
		var payload apiCreditPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeAPIError(w, http.StatusBadRequest, "JSON inválido.", nil)
			return
		}

		resp, err := createCreditViaAPI(db, currentUser, payload, "agent", creditSaleKindCash, func(item map[string]any) map[string]any {
			return withAPIAuditMetadata(r, item)
		})
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				writeAPIError(w, reqErr.Status, reqErr.Message, reqErr.Fields)
				return
			}
			writeAPIError(w, http.StatusInternalServerError, "Error al registrar el crédito.", nil)
			return
		}
		writeAPIJSON(w, http.StatusCreated, resp)
	}
}

func handleAPIInvoices(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		switch r.Method {
		case http.MethodGet:
			limit := 100
			if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
				if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= 200 {
					limit = parsed
				}
			}
			items, err := listInvoicesForUser(db, currentUser, r.URL.Query().Get("q"), r.URL.Query().Get("date_from"), r.URL.Query().Get("date_to"), limit)
			if err != nil {
				var reqErr requestError
				if errors.As(err, &reqErr) {
					writeAPIError(w, reqErr.Status, reqErr.Message, reqErr.Fields)
					return
				}
				writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar las facturas.", nil)
				return
			}
			writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "items": items, "count": len(items)})
		case http.MethodPost:
			var payload apiInvoicePayload
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				writeAPIError(w, http.StatusBadRequest, "JSON inválido.", nil)
				return
			}
			item, created, err := createInvoiceDocument(db, currentUser, invoiceCreateInput{
				SaleID:       payload.SaleID,
				CreditSaleID: payload.CreditSaleID,
				Customer:     payload.customerInput(),
				Notes:        payload.Notes,
			}, "api", func(data map[string]any) map[string]any {
				return withAPIAuditMetadata(r, data)
			})
			if err != nil {
				var reqErr requestError
				if errors.As(err, &reqErr) {
					writeAPIError(w, reqErr.Status, reqErr.Message, reqErr.Fields)
					return
				}
				writeAPIError(w, http.StatusInternalServerError, "No se pudo generar la factura.", nil)
				return
			}
			status := http.StatusOK
			message := "Factura existente reutilizada."
			if created {
				status = http.StatusCreated
				message = "Factura generada correctamente."
			}
			writeAPIJSON(w, status, map[string]any{
				"ok":      true,
				"invoice": item,
				"message": message,
				"created": created,
			})
		default:
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
		}
	}
}

func handleAPIInvoiceRoutes(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		currentUser := userFromContext(r)
		path := strings.TrimPrefix(r.URL.Path, "/api/invoices/")
		path = strings.Trim(path, "/")
		if path == "" {
			writeAPIError(w, http.StatusNotFound, "Factura no encontrada.", nil)
			return
		}
		invoiceID, err := strconv.Atoi(path)
		if err != nil || invoiceID <= 0 {
			writeAPIError(w, http.StatusBadRequest, "Factura inválida.", map[string]string{"invoice_id": "Factura inválida."})
			return
		}
		item, err := invoiceDetailForUser(db, currentUser, invoiceID)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				writeAPIError(w, reqErr.Status, reqErr.Message, reqErr.Fields)
				return
			}
			if err == sql.ErrNoRows {
				writeAPIError(w, http.StatusNotFound, "Factura no encontrada.", nil)
				return
			}
			writeAPIError(w, http.StatusInternalServerError, "No se pudo cargar la factura.", nil)
			return
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "invoice": item})
	}
}

func handleAPIAgentInvoices(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		currentUser := userFromContext(r)
		var payload apiInvoicePayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeAPIError(w, http.StatusBadRequest, "JSON inválido.", nil)
			return
		}
		item, created, err := createInvoiceDocument(db, currentUser, invoiceCreateInput{
			SaleID:       payload.SaleID,
			CreditSaleID: payload.CreditSaleID,
			Customer:     payload.customerInput(),
			Notes:        payload.Notes,
		}, "agent", func(data map[string]any) map[string]any {
			return withAPIAuditMetadata(r, data)
		})
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				writeAPIError(w, reqErr.Status, reqErr.Message, reqErr.Fields)
				return
			}
			writeAPIError(w, http.StatusInternalServerError, "No se pudo generar la factura.", nil)
			return
		}
		status := http.StatusOK
		message := "Factura existente reutilizada."
		if created {
			status = http.StatusCreated
			message = "Factura generada correctamente."
		}
		writeAPIJSON(w, status, map[string]any{
			"ok":      true,
			"invoice": item,
			"message": message,
			"created": created,
		})
	}
}

func handleAPISwaps(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		currentUser := userFromContext(r)
		_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantIDFromUser(currentUser))
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "Error al cargar tipos de movimiento.", nil)
			return
		}
		if !movementEnabled(movementEnabledMap, "cambio") {
			writeAPIError(w, http.StatusForbidden, "El cambio está deshabilitado en Configuración.", nil)
			return
		}
		var payload struct {
			ProductID           string `json:"product_id"`
			Quantity            int    `json:"quantity"`
			PersonaCambio       string `json:"persona_del_cambio"`
			Notes               string `json:"notes"`
			IncomingMode        string `json:"incoming_mode"`
			IncomingExistingID  string `json:"incoming_existing_id"`
			IncomingExistingQty int    `json:"incoming_existing_qty"`
			IncomingNewSKU      string `json:"incoming_new_sku"`
			IncomingNewName     string `json:"incoming_new_name"`
			IncomingNewLine     string `json:"incoming_new_line"`
			IncomingNewQty      int    `json:"incoming_new_qty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeAPIError(w, http.StatusBadRequest, "JSON inválido.", nil)
			return
		}
		payload.ProductID = strings.TrimSpace(payload.ProductID)
		payload.PersonaCambio = strings.TrimSpace(payload.PersonaCambio)
		payload.Notes = strings.TrimSpace(payload.Notes)
		payload.IncomingMode = strings.TrimSpace(payload.IncomingMode)
		payload.IncomingExistingID = strings.TrimSpace(payload.IncomingExistingID)
		payload.IncomingNewSKU = strings.TrimSpace(payload.IncomingNewSKU)
		payload.IncomingNewName = strings.TrimSpace(payload.IncomingNewName)
		payload.IncomingNewLine = strings.TrimSpace(payload.IncomingNewLine)

		productsSnapshot, err := loadVisibleProductsForUser(db, currentUser)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar los productos.", nil)
			return
		}
		if len(productsSnapshot) == 0 {
			writeAPIError(w, http.StatusForbidden, "No tienes productos disponibles para cambio.", nil)
			return
		}
		fields := map[string]string{}
		if allowed, err := productAccessibleByID(db, currentUser, payload.ProductID); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo validar acceso al producto.", nil)
			return
		} else if !allowed {
			fields["product_id"] = "No tienes acceso a este producto."
		}
		selectedProduct, ok := findProduct(productsSnapshot, payload.ProductID)
		if !ok {
			fields["product_id"] = "Selecciona un producto válido."
		}
		if payload.PersonaCambio == "" {
			fields["persona_del_cambio"] = "Ingresa la persona responsable del cambio."
		}
		if payload.Quantity <= 0 {
			fields["quantity"] = "Ingresa una cantidad válida para la salida."
		}
		if payload.IncomingMode != "existing" && payload.IncomingMode != "new" {
			fields["incoming_mode"] = "Selecciona el tipo de entrada."
		}
		if payload.IncomingMode == "existing" {
			if payload.IncomingExistingID == "" {
				fields["incoming_existing_id"] = "Selecciona el producto entrante."
			} else if _, ok := findProduct(productsSnapshot, payload.IncomingExistingID); !ok {
				fields["incoming_existing_id"] = "Selecciona un producto entrante válido."
			}
			if payload.IncomingExistingQty <= 0 {
				fields["incoming_existing_qty"] = "Ingresa una cantidad válida para la entrada."
			}
		} else if payload.IncomingMode == "new" {
			if payload.IncomingNewSKU == "" {
				fields["incoming_new_sku"] = "Ingresa el SKU del producto nuevo."
			}
			if payload.IncomingNewName == "" {
				fields["incoming_new_name"] = "Ingresa el nombre del producto nuevo."
			}
			if payload.IncomingNewQty <= 0 {
				fields["incoming_new_qty"] = "Ingresa una cantidad válida para la entrada."
			}
		}
		if len(fields) > 0 {
			writeAPIError(w, http.StatusBadRequest, "Datos inválidos.", fields)
			return
		}
		tx, err := db.Begin()
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "Error al iniciar el cambio.", nil)
			return
		}
		defer tx.Rollback()
		salientesMarcadas, err := selectAndMarkUnitsByStatus(tx, tenantIDFromUser(currentUser), payload.ProductID, payload.Quantity, "Cambio")
		if err != nil {
			if err == errInsufficientStock {
				writeAPIError(w, http.StatusBadRequest, "No hay stock disponible suficiente para completar el cambio.", map[string]string{"quantity": "No hay stock disponible suficiente para completar el cambio."})
				return
			}
			writeAPIError(w, http.StatusInternalServerError, "Error al actualizar unidades salientes.", nil)
			return
		}
		now := time.Now().Format(time.RFC3339)
		notaMovimiento := strings.TrimSpace(fmt.Sprintf("%s %s", payload.PersonaCambio, payload.Notes))
		if err := logMovimientos(tx, payload.ProductID, salientesMarcadas, "cambio_salida", notaMovimiento, currentUser, now); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "Error al registrar movimiento del cambio.", nil)
			return
		}
		incomingProductID := payload.IncomingExistingID
		incomingQty := payload.IncomingExistingQty
		if payload.IncomingMode == "new" {
			incomingProductID = payload.IncomingNewSKU
			incomingQty = payload.IncomingNewQty
		}
		for i := 0; i < incomingQty; i++ {
			unitID := fmt.Sprintf("U-%d-%d", time.Now().UnixNano(), i+1)
			if _, err := tx.Exec(`INSERT INTO unidades (id, tenant_id, producto_id, estado, creado_en, caducidad) VALUES (?, ?, ?, ?, ?, ?)`, unitID, normalizeTenantID(tenantIDFromUser(currentUser)), incomingProductID, "Disponible", now, nil); err != nil {
				writeAPIError(w, http.StatusInternalServerError, "Error al registrar unidades entrantes.", nil)
				return
			}
		}
		if err := logAuditEvent(tx, currentUser, "change_registered", "change", payload.ProductID, "api", withAPIAuditMetadata(r, map[string]any{
			"producto_saliente_id": payload.ProductID,
			"producto_saliente":    selectedProduct.Name,
			"producto_entrante_id": incomingProductID,
			"cantidad_saliente":    payload.Quantity,
			"cantidad_entrante":    incomingQty,
			"modo_entrada":         payload.IncomingMode,
		})); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "Error al registrar la auditoría del cambio.", nil)
			return
		}
		if err := tx.Commit(); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "Error al confirmar el cambio.", nil)
			return
		}
		writeAPIJSON(w, http.StatusCreated, map[string]any{"ok": true, "product_id": payload.ProductID, "incoming_product_id": incomingProductID, "quantity": payload.Quantity, "incoming_quantity": incomingQty, "message": "Cambio registrado correctamente."})
	}
}

func handleAPIRetomas(db *sql.DB, syncProductPrice func(string, float64)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		switch r.Method {
		case http.MethodGet:
			q := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("q")))
			items, err := listRetomasForUser(db, currentUser, q, 100)
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar las retomas.", nil)
				return
			}
			writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "items": items, "count": len(items)})
		case http.MethodPost:
			if currentUser == nil || !isStaffRole(currentUser.Role) {
				writeAPIError(w, http.StatusForbidden, "Solo personal autorizado puede registrar retomas.", nil)
				return
			}
			var payload struct {
				ProductID      string   `json:"product_id"`
				Quantity       int      `json:"quantity"`
				ValueReceived  float64  `json:"value_received"`
				ReceivedState  string   `json:"received_state"`
				PublishToStock bool     `json:"publish_to_stock"`
				FinalSalePrice *float64 `json:"final_sale_price"`
				Notes          string   `json:"notes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				writeAPIError(w, http.StatusBadRequest, "JSON inválido.", nil)
				return
			}
			result, err := registerRetoma(db, currentUser, retomaOperationInput{
				ProductID:      payload.ProductID,
				Quantity:       payload.Quantity,
				ValueReceived:  payload.ValueReceived,
				ReceivedState:  payload.ReceivedState,
				PublishToStock: payload.PublishToStock,
				FinalSalePrice: payload.FinalSalePrice,
				Notes:          payload.Notes,
			}, "api", func(item map[string]any) map[string]any {
				return withAPIAuditMetadata(r, item)
			})
			if err != nil {
				var reqErr requestError
				if errors.As(err, &reqErr) {
					writeAPIError(w, reqErr.Status, reqErr.Message, reqErr.Fields)
					return
				}
				writeAPIError(w, http.StatusInternalServerError, "No se pudo registrar la retoma.", nil)
				return
			}
			if result.FinalSalePrice != nil && syncProductPrice != nil {
				syncProductPrice(result.ProductID, *result.FinalSalePrice)
			}
			writeAPIJSON(w, http.StatusCreated, map[string]any{
				"ok":                 true,
				"retoma_id":          result.RetomaID,
				"product_id":         result.ProductID,
				"product_name":       result.ProductName,
				"quantity":           result.Quantity,
				"value_received":     result.ValueReceived,
				"received_state":     result.ReceivedState,
				"published_to_stock": result.PublishedToStock,
				"units_created":      result.UnitsCreated,
				"message":            result.Message,
			})
		default:
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
		}
	}
}

func handleAPICreditInstallments(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		currentUser := userFromContext(r)
		_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantIDFromUser(currentUser))
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "Error al cargar tipos de movimiento.", nil)
			return
		}
		if !movementEnabled(movementEnabledMap, "credito") {
			writeAPIError(w, http.StatusForbidden, "El flujo de crédito está deshabilitado en Configuración.", nil)
			return
		}
		var payload struct {
			CreditSaleID int      `json:"credit_sale_id"`
			AmountPaid   *float64 `json:"amount_paid"`
			PaymentType  string   `json:"payment_type"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeAPIError(w, http.StatusBadRequest, "JSON inválido.", nil)
			return
		}
		if payload.CreditSaleID <= 0 {
			writeAPIError(w, http.StatusBadRequest, "Crédito inválido.", map[string]string{"credit_sale_id": "Crédito inválido."})
			return
		}
		result, err := addCreditInstallment(db, payload.CreditSaleID, payload.AmountPaid, payload.PaymentType, currentUser, "api", func(item map[string]any) map[string]any {
			return withAPIAuditMetadata(r, item)
		})
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				writeAPIError(w, reqErr.Status, reqErr.Message, nil)
				return
			}
			writeAPIError(w, http.StatusInternalServerError, "No se pudo registrar la cuota.", nil)
			return
		}
		message := fmt.Sprintf("Cuota %d registrada correctamente.", result.InstallmentNumber)
		if result.PaymentType == creditPaymentTypeAbono {
			message = "Abono registrado correctamente."
		}
		writeAPIJSON(w, http.StatusCreated, map[string]any{
			"ok":                 true,
			"credit_sale_id":     result.CreditSaleID,
			"kind":               string(result.Kind),
			"kind_label":         creditKindLabel(result.Kind),
			"product_id":         result.ProductID,
			"product_name":       result.ProductName,
			"amount_paid":        result.AmountPaid,
			"installment_number": result.InstallmentNumber,
			"payment_type":       string(result.PaymentType),
			"paid_installments":  result.InstallmentsPaid,
			"total_paid":         result.TotalPaid,
			"current_debt":       result.CurrentDebt,
			"message":            message,
		})
	}
}

func handleAPICustomers(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		if currentUser == nil || !isStaffRole(currentUser.Role) {
			writeAPIError(w, http.StatusForbidden, "Solo personal autorizado puede operar clientes.", nil)
			return
		}
		switch r.Method {
		case http.MethodGet:
			q := strings.TrimSpace(r.URL.Query().Get("q"))
			limit := 100
			if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
				if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= 500 {
					limit = parsed
				}
			}
			items, err := listCustomersForTenant(db, tenantIDFromUser(currentUser), q, limit)
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar los clientes.", nil)
				return
			}
			writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "items": items, "count": len(items)})
		case http.MethodPost:
			var payload struct {
				CustomerID     int    `json:"customer_id"`
				Name           string `json:"customer_name"`
				Phone          string `json:"customer_phone"`
				DocumentType   string `json:"customer_document_type"`
				DocumentNumber string `json:"customer_document_number"`
				Address        string `json:"customer_address"`
				City           string `json:"customer_city"`
				Notes          string `json:"customer_notes"`
				DebtorName     string `json:"debtor_name"`
				DebtorPhone    string `json:"debtor_phone"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				writeAPIError(w, http.StatusBadRequest, "JSON inválido.", nil)
				return
			}
			input := customerInput{
				CustomerID:     payload.CustomerID,
				Name:           firstNonEmptyString(payload.Name, payload.DebtorName),
				Phone:          firstNonEmptyString(payload.Phone, payload.DebtorPhone),
				DocumentType:   strings.TrimSpace(payload.DocumentType),
				DocumentNumber: strings.TrimSpace(payload.DocumentNumber),
				Address:        strings.TrimSpace(payload.Address),
				City:           strings.TrimSpace(payload.City),
				Notes:          strings.TrimSpace(payload.Notes),
			}
			fields := validateCustomerInput(input)
			if input.CustomerID <= 0 && input.City == "" {
				fields["customer_city"] = "La ciudad del cliente es obligatoria."
			}
			if input.CustomerID > 0 {
				if _, err := findCustomerByID(db, tenantIDFromUser(currentUser), input.CustomerID); err != nil {
					if err == sql.ErrNoRows {
						fields["customer_id"] = "Selecciona un cliente válido."
					} else {
						writeAPIError(w, http.StatusInternalServerError, "No se pudo validar el cliente.", nil)
						return
					}
				}
			}
			if len(fields) > 0 {
				writeAPIError(w, http.StatusBadRequest, "Datos inválidos.", fields)
				return
			}

			tx, err := db.Begin()
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudo iniciar la operación del cliente.", nil)
				return
			}
			defer tx.Rollback()

			tenantID := tenantIDFromUser(currentUser)
			previous, _ := findCustomerByID(db, tenantID, input.CustomerID)
			existingByDocumentID := 0
			if input.CustomerID <= 0 && input.DocumentType != "" && input.DocumentNumber != "" {
				if err := tx.QueryRow(`
					SELECT id
					FROM customers
					WHERE tenant_id = ? AND document_type = ? AND document_number = ?
				`, tenantID, input.DocumentType, input.DocumentNumber).Scan(&existingByDocumentID); err != nil && err != sql.ErrNoRows {
					writeAPIError(w, http.StatusInternalServerError, "No se pudo validar duplicación del cliente.", nil)
					return
				}
			}
			customer, err := resolveCustomerForCredit(tx, tenantID, input)
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudo guardar el cliente.", nil)
				return
			}
			created := previous == nil && input.CustomerID == 0 && existingByDocumentID == 0
			reused := !created
			eventType := "customer_updated"
			customerEventType := "profile_updated"
			message := "Cliente actualizado correctamente."
			if created {
				eventType = "customer_created"
				customerEventType = "profile_created"
				message = "Cliente creado correctamente."
			}
			if err := logCustomerEvent(tx, currentUser, customer.ID, customerEventType, "customer", strconv.Itoa(customer.ID), 0, map[string]any{
				"name":            customer.Name,
				"phone":           customer.Phone,
				"document_type":   customer.DocumentType,
				"document_number": customer.DocumentNumber,
				"city":            customer.City,
			}); err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudo registrar la trazabilidad del cliente.", nil)
				return
			}
			if err := logAuditEvent(tx, currentUser, eventType, "customer", strconv.Itoa(customer.ID), "api", withAPIAuditMetadata(r, map[string]any{
				"customer_id":      customer.ID,
				"customer_name":    customer.Name,
				"customer_phone":   customer.Phone,
				"document_type":    customer.DocumentType,
				"document_number":  customer.DocumentNumber,
				"customer_address": customer.Address,
				"customer_city":    customer.City,
				"reused":           reused,
			})); err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudo registrar la auditoría del cliente.", nil)
				return
			}
			if err := tx.Commit(); err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudo confirmar el cliente.", nil)
				return
			}

			item, err := customerDetailForTenant(db, tenantID, customer.ID)
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "Cliente guardado, pero no se pudo cargar el detalle.", nil)
				return
			}
			status := http.StatusCreated
			if !created {
				status = http.StatusOK
			}
			writeAPIJSON(w, status, map[string]any{
				"ok":       true,
				"created":  created,
				"reused":   reused,
				"customer": item,
				"message":  message,
			})
		default:
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
		}
	}
}

func handleAPICustomerRoutes(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		if currentUser == nil || !isStaffRole(currentUser.Role) {
			writeAPIError(w, http.StatusForbidden, "Solo personal autorizado puede operar clientes.", nil)
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/api/customers/")
		path = strings.Trim(path, "/")
		if path == "" {
			writeAPIError(w, http.StatusNotFound, "Cliente no encontrado.", nil)
			return
		}
		parts := strings.Split(path, "/")
		customerID, err := strconv.Atoi(parts[0])
		if err != nil || customerID <= 0 {
			writeAPIError(w, http.StatusBadRequest, "Cliente inválido.", nil)
			return
		}
		if _, err := findCustomerByID(db, tenantIDFromUser(currentUser), customerID); err != nil {
			if err == sql.ErrNoRows {
				writeAPIError(w, http.StatusNotFound, "Cliente no encontrado.", nil)
				return
			}
			writeAPIError(w, http.StatusInternalServerError, "No se pudo cargar el cliente.", nil)
			return
		}

		if len(parts) == 1 {
			if r.Method != http.MethodGet {
				writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
				return
			}
			item, err := customerDetailForTenant(db, tenantIDFromUser(currentUser), customerID)
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudo cargar el detalle del cliente.", nil)
				return
			}
			writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "customer": item})
			return
		}

		if len(parts) == 2 && parts[1] == "events" {
			if r.Method != http.MethodGet {
				writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
				return
			}
			limit := 50
			if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
				if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= 500 {
					limit = parsed
				}
			}
			items, err := customerEventsForTenant(db, tenantIDFromUser(currentUser), customerID, limit)
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar los eventos del cliente.", nil)
				return
			}
			writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "customer_id": customerID, "items": items, "count": len(items)})
			return
		}

		writeAPIError(w, http.StatusNotFound, "Ruta de cliente no encontrada.", nil)
	}
}

func handleAPIAgentCustomerSearch(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}

		currentUser := userFromContext(r)
		if currentUser == nil || !isStaffRole(currentUser.Role) {
			writeAPIError(w, http.StatusForbidden, "Solo personal autorizado puede consultar clientes.", nil)
			return
		}

		q := strings.TrimSpace(r.URL.Query().Get("q"))
		limit := 20
		if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= 100 {
				limit = parsed
			}
		}

		items, err := listCustomersForTenant(db, tenantIDFromUser(currentUser), q, limit)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar los clientes.", nil)
			return
		}

		compactItems := make([]map[string]any, 0, len(items))
		for _, item := range items {
			compactItems = append(compactItems, agentCustomerSearchItem(item))
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "items": compactItems, "count": len(compactItems)})
	}
}

func managedUserAPIItem(record managedUserRecord) map[string]any {
	return map[string]any{
		"id":          record.ID,
		"username":    record.Username,
		"name":        record.Name,
		"email":       record.Email,
		"role":        record.Role,
		"is_active":   record.IsActive,
		"tenant_id":   record.TenantID,
		"created_at":  record.CreatedAt,
		"telegram_id": record.TelegramID,
	}
}

func handleAPIUsers(db *sql.DB, usersCols map[string]bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		if currentUser == nil || !isAdminRole(currentUser.Role) {
			writeAPIError(w, http.StatusForbidden, "Solo administrador puede operar usuarios.", nil)
			return
		}

		switch r.Method {
		case http.MethodGet:
			items, err := listManagedUsersForTenant(db, currentUser, tenantIDFromRequest(r), usersCols)
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar los usuarios.", nil)
				return
			}
			payloadItems := make([]map[string]any, 0, len(items))
			for _, item := range items {
				payloadItems = append(payloadItems, managedUserAPIItem(item))
			}
			writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "items": payloadItems, "count": len(payloadItems)})
		case http.MethodPost:
			var payload struct {
				Username   string `json:"username"`
				Name       string `json:"name"`
				Email      string `json:"email"`
				Password   string `json:"password"`
				Role       string `json:"role"`
				IsActive   *bool  `json:"is_active"`
				TelegramID string `json:"telegram_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				writeAPIError(w, http.StatusBadRequest, "JSON inválido.", nil)
				return
			}
			isActive := true
			if payload.IsActive != nil {
				isActive = *payload.IsActive
			}
			record, err := createManagedUser(db, currentUser, tenantIDFromRequest(r), usersCols, managedUserInput{
				Username:   payload.Username,
				Name:       payload.Name,
				Email:      payload.Email,
				Password:   payload.Password,
				Role:       payload.Role,
				IsActive:   isActive,
				TelegramID: payload.TelegramID,
			}, "api", func(item map[string]any) map[string]any {
				return withAPIAuditMetadata(r, item)
			})
			if err != nil {
				var reqErr requestError
				if errors.As(err, &reqErr) {
					writeAPIError(w, reqErr.Status, reqErr.Message, reqErr.Fields)
					return
				}
				writeAPIError(w, http.StatusInternalServerError, "No se pudo crear el usuario.", nil)
				return
			}
			writeAPIJSON(w, http.StatusCreated, map[string]any{
				"ok":      true,
				"user":    managedUserAPIItem(record),
				"message": "Usuario creado correctamente.",
			})
		default:
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
		}
	}
}

func handleAPIUserRoutes(db *sql.DB, usersCols map[string]bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		if currentUser == nil || !isAdminRole(currentUser.Role) {
			writeAPIError(w, http.StatusForbidden, "Solo administrador puede operar usuarios.", nil)
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/api/users/")
		path = strings.Trim(path, "/")
		if path == "" {
			writeAPIError(w, http.StatusNotFound, "Usuario no encontrado.", nil)
			return
		}
		parts := strings.Split(path, "/")
		userID, err := strconv.Atoi(parts[0])
		if err != nil || userID <= 0 {
			writeAPIError(w, http.StatusBadRequest, "Usuario inválido.", nil)
			return
		}
		record, err := managedUserByIDForTenant(db, currentUser, tenantIDFromRequest(r), userID, usersCols)
		if err != nil {
			if err == sql.ErrNoRows {
				writeAPIError(w, http.StatusNotFound, "Usuario no encontrado.", nil)
				return
			}
			writeAPIError(w, http.StatusInternalServerError, "No se pudo cargar el usuario.", nil)
			return
		}

		if len(parts) == 1 {
			switch r.Method {
			case http.MethodGet:
				writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "user": managedUserAPIItem(record)})
			case http.MethodPut, http.MethodPatch:
				var payload struct {
					Username   *string `json:"username"`
					Name       *string `json:"name"`
					Email      *string `json:"email"`
					Role       *string `json:"role"`
					IsActive   *bool   `json:"is_active"`
					TelegramID *string `json:"telegram_id"`
				}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					writeAPIError(w, http.StatusBadRequest, "JSON inválido.", nil)
					return
				}
				input := managedUserInput{
					Username:   record.Username,
					Name:       record.Name,
					Email:      record.Email,
					Role:       record.Role,
					IsActive:   record.IsActive,
					TelegramID: record.TelegramID,
				}
				if payload.Username != nil {
					input.Username = strings.TrimSpace(*payload.Username)
				}
				if payload.Name != nil {
					input.Name = strings.TrimSpace(*payload.Name)
				}
				if payload.Email != nil {
					input.Email = strings.TrimSpace(*payload.Email)
				}
				if payload.Role != nil {
					input.Role = strings.TrimSpace(*payload.Role)
				}
				if payload.IsActive != nil {
					input.IsActive = *payload.IsActive
				}
				if payload.TelegramID != nil {
					input.TelegramID = strings.TrimSpace(*payload.TelegramID)
				}

				updatedRecord, err := updateManagedUser(db, currentUser, tenantIDFromRequest(r), userID, usersCols, input, "api", func(item map[string]any) map[string]any {
					return withAPIAuditMetadata(r, item)
				})
				if err != nil {
					var reqErr requestError
					if errors.As(err, &reqErr) {
						writeAPIError(w, reqErr.Status, reqErr.Message, reqErr.Fields)
						return
					}
					writeAPIError(w, http.StatusInternalServerError, "No se pudo actualizar el usuario.", nil)
					return
				}
				writeAPIJSON(w, http.StatusOK, map[string]any{
					"ok":      true,
					"user":    managedUserAPIItem(updatedRecord),
					"message": "Usuario actualizado correctamente.",
				})
			default:
				writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			}
			return
		}

		if len(parts) == 2 && parts[1] == "password" {
			if r.Method != http.MethodPost {
				writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
				return
			}
			if !canManagePlatformUser(currentUser, record.Role) {
				writeAPIError(w, http.StatusForbidden, "Solo un platform admin puede cambiar la contraseña de ese usuario.", nil)
				return
			}
			var payload struct {
				Password string `json:"password"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				writeAPIError(w, http.StatusBadRequest, "JSON inválido.", nil)
				return
			}
			payload.Password = strings.TrimSpace(payload.Password)
			if payload.Password == "" {
				writeAPIError(w, http.StatusBadRequest, "Contraseña obligatoria.", map[string]string{"password": "Contraseña obligatoria."})
				return
			}
			hashed, err := bcrypt.GenerateFromPassword([]byte(payload.Password), bcrypt.DefaultCost)
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudo procesar la contraseña.", nil)
				return
			}
			setCols := []string{"password_hash = ?"}
			args := []any{string(hashed)}
			if usersCols["password_salt"] {
				setCols = append(setCols, "password_salt = ?")
				args = append(args, "bcrypt")
			}
			args = append(args, record.ID, record.TenantID)
			if _, err := db.Exec(fmt.Sprintf("UPDATE users SET %s WHERE id = ? AND COALESCE(NULLIF(tenant_id, 0), 1) = ?", strings.Join(setCols, ", ")), args...); err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudo actualizar la contraseña.", nil)
				return
			}
			_, _ = db.Exec(`DELETE FROM sessions WHERE user_id = ?`, record.ID)
			if err := logAuditEvent(db, currentUser, "user_password_updated", "user", strconv.Itoa(record.ID), "api", withAPIAuditMetadata(r, map[string]any{
				"user_id":   record.ID,
				"username":  record.Username,
				"tenant_id": record.TenantID,
			})); err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudo registrar la auditoría del usuario.", nil)
				return
			}
			writeAPIJSON(w, http.StatusOK, map[string]any{
				"ok":      true,
				"user_id": record.ID,
				"message": "Contraseña actualizada correctamente.",
			})
			return
		}

		if len(parts) == 2 && parts[1] == "toggle" {
			if r.Method != http.MethodPost {
				writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
				return
			}
			var payload struct {
				IsActive *bool `json:"is_active"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil && !errors.Is(err, io.EOF) {
				writeAPIError(w, http.StatusBadRequest, "JSON inválido.", nil)
				return
			}
			nextState := !record.IsActive
			if payload.IsActive != nil {
				nextState = *payload.IsActive
			}
			updatedRecord, err := updateManagedUser(db, currentUser, tenantIDFromRequest(r), userID, usersCols, managedUserInput{
				Username:   record.Username,
				Name:       record.Name,
				Email:      record.Email,
				Role:       record.Role,
				IsActive:   nextState,
				TelegramID: record.TelegramID,
			}, "api", func(item map[string]any) map[string]any {
				return withAPIAuditMetadata(r, item)
			})
			if err != nil {
				var reqErr requestError
				if errors.As(err, &reqErr) {
					writeAPIError(w, reqErr.Status, reqErr.Message, reqErr.Fields)
					return
				}
				writeAPIError(w, http.StatusInternalServerError, "No se pudo actualizar el estado del usuario.", nil)
				return
			}
			message := "Usuario activado correctamente."
			if !updatedRecord.IsActive {
				message = "Usuario inactivado correctamente."
			}
			writeAPIJSON(w, http.StatusOK, map[string]any{
				"ok":      true,
				"user":    managedUserAPIItem(updatedRecord),
				"message": message,
			})
			return
		}

		writeAPIError(w, http.StatusNotFound, "Ruta de usuario no encontrada.", nil)
	}
}

func setAPIContextHeaders(w http.ResponseWriter, r *http.Request) {
	if tenant := tenantFromContext(r); tenant != nil {
		w.Header().Set("X-Stocki-Tenant-ID", strconv.Itoa(normalizeTenantID(tenant.ID)))
		w.Header().Set("X-Stocki-Tenant-Slug", strings.TrimSpace(tenant.Slug))
	}
	if authMode := apiAuthModeFromContext(r); authMode != "" {
		w.Header().Set("X-Stocki-Auth-Mode", authMode)
	}
	if integrationName := apiIntegrationNameFromContext(r); integrationName != "" {
		w.Header().Set("X-Stocki-Integration-Name", integrationName)
	}
}

func authMiddleware(db *sql.DB, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Allow unauthenticated access to healthcheck and static assets.
		// Static assets are safe to serve publicly and needed for the login page too.
		if r.URL.Path == "/login" || r.URL.Path == "/health" || strings.HasPrefix(r.URL.Path, "/static/") {
			next.ServeHTTP(w, r)
			return
		}

		if strings.HasPrefix(r.URL.Path, "/api/") {
			user, integrationName, authMode, err := apiAuthFromRequest(db, r)
			if err != nil {
				writeAPIError(w, http.StatusUnauthorized, "Autenticación requerida para la API.", nil)
				return
			}
			ctx := context.WithValue(r.Context(), userContextKey, user)
			tenant, err := resolveTenantByID(db, user.TenantID)
			if err != nil || !tenant.Active {
				writeAPIError(w, http.StatusUnauthorized, "Tenant inválido o inactivo.", nil)
				return
			}
			ctx = context.WithValue(ctx, tenantContextKey, tenant)
			if integrationName != "" {
				ctx = context.WithValue(ctx, apiIntegrationNameContextKey, integrationName)
			}
			if authMode != "" {
				ctx = context.WithValue(ctx, apiAuthModeContextKey, authMode)
			}
			reqWithCtx := r.WithContext(ctx)
			setAPIContextHeaders(w, reqWithCtx)
			next.ServeHTTP(w, reqWithCtx)
			return
		}

		user, err := userFromRequest(db, r)
		if err != nil {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		ctx := context.WithValue(r.Context(), userContextKey, user)
		tenant, err := resolveTenantByID(db, user.TenantID)
		if err != nil || !tenant.Active {
			clearSessionCookie(w)
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		ctx = context.WithValue(ctx, tenantContextKey, tenant)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func adminOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := userFromContext(r)
		if user == nil || !isAdminRole(user.Role) {
			http.Error(w, "Acceso restringido a administradores.", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

func platformAdminOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := userFromContext(r)
		if !isPlatformAdmin(user) {
			http.Error(w, "Acceso restringido a administración de plataforma.", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

func canManagePlatformUser(currentUser *User, targetRole string) bool {
	if targetRole != rolePlatformAdmin {
		return true
	}
	return isPlatformAdmin(currentUser)
}

func redirectWithMessage(w http.ResponseWriter, r *http.Request, path, message, errMsg string) {
	params := url.Values{}
	if message != "" {
		params.Set("mensaje", message)
	}
	if errMsg != "" {
		params.Set("error", errMsg)
	}
	target := path
	if encoded := params.Encode(); encoded != "" {
		target = target + "?" + encoded
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}

func userCreateErrorText(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "UNIQUE constraint failed: users.username"):
		return "El usuario ya existe."
	case strings.Contains(msg, `duplicate key value violates unique constraint`) && strings.Contains(strings.ToLower(msg), "users_username"):
		return "El usuario ya existe."
	case strings.Contains(msg, "CHECK constraint failed"):
		return "Datos inválidos (revisa el rol)."
	case strings.Contains(msg, "database is locked"):
		return "La base de datos está ocupada. Intenta de nuevo."
	default:
		return "No se pudo crear el usuario."
	}
}

type managedUserScanner interface {
	Scan(dest ...any) error
}

func managedUserSelectColumns(usersCols map[string]bool) []string {
	cols := []string{"id", "username"}
	if usersCols["name"] {
		cols = append(cols, "COALESCE(NULLIF(name, ''), username) AS name")
	} else {
		cols = append(cols, "username AS name")
	}
	if usersCols["email"] {
		cols = append(cols, "COALESCE(NULLIF(email, ''), CASE WHEN username LIKE '%@%' THEN username ELSE username || '@local' END) AS email")
	} else {
		cols = append(cols, "CASE WHEN username LIKE '%@%' THEN username ELSE username || '@local' END AS email")
	}
	if usersCols["telegram_id"] {
		cols = append(cols, "COALESCE(telegram_id, '') AS telegram_id")
	} else {
		cols = append(cols, "'' AS telegram_id")
	}
	cols = append(cols, "role", "COALESCE(NULLIF(tenant_id, 0), 1) AS tenant_id")
	if usersCols["is_active"] {
		cols = append(cols, "is_active")
	} else if usersCols["active"] {
		cols = append(cols, "active AS is_active")
	} else {
		cols = append(cols, "1 AS is_active")
	}
	if usersCols["created_at"] {
		cols = append(cols, "created_at")
	} else {
		cols = append(cols, "'' AS created_at")
	}
	return cols
}

func scanManagedUserRecord(scanner managedUserScanner) (managedUserRecord, error) {
	var (
		record   managedUserRecord
		isActive int
	)
	if err := scanner.Scan(
		&record.ID,
		&record.Username,
		&record.Name,
		&record.Email,
		&record.TelegramID,
		&record.Role,
		&record.TenantID,
		&isActive,
		&record.CreatedAt,
	); err != nil {
		return managedUserRecord{}, err
	}
	record.IsActive = isActive == 1
	record.TenantID = normalizeTenantID(record.TenantID)
	if strings.TrimSpace(record.Name) == "" {
		record.Name = record.Username
	}
	if strings.TrimSpace(record.Email) == "" {
		if strings.Contains(record.Username, "@") {
			record.Email = record.Username
		} else if record.Username != "" {
			record.Email = record.Username + "@local"
		}
	}
	record.CreatedAt = formatDateWithSettings(record.CreatedAt)
	return record, nil
}

func normalizeManagedUserInput(input managedUserInput, usersCols map[string]bool) managedUserInput {
	input.Username = strings.TrimSpace(input.Username)
	input.Name = strings.TrimSpace(input.Name)
	input.Email = strings.TrimSpace(input.Email)
	input.Password = strings.TrimSpace(input.Password)
	input.Role = strings.TrimSpace(input.Role)
	input.TelegramID = strings.TrimSpace(input.TelegramID)
	if usersCols["name"] && input.Name == "" {
		input.Name = input.Username
	}
	if usersCols["email"] && input.Email == "" {
		if strings.Contains(input.Username, "@") {
			input.Email = input.Username
		} else if input.Username != "" {
			input.Email = input.Username + "@local"
		}
	}
	return input
}

func listManagedUsersForTenant(db *sql.DB, currentUser *User, tenantID int, usersCols map[string]bool) ([]managedUserRecord, error) {
	tenantID = normalizeTenantID(tenantID)
	query := fmt.Sprintf("SELECT %s FROM users WHERE COALESCE(NULLIF(tenant_id, 0), 1) = ?", strings.Join(managedUserSelectColumns(usersCols), ", "))
	args := []any{tenantID}
	if !isPlatformAdmin(currentUser) {
		query += " AND role <> ?"
		args = append(args, rolePlatformAdmin)
	}
	query += " ORDER BY id"

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]managedUserRecord, 0)
	for rows.Next() {
		record, err := scanManagedUserRecord(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func managedUserByIDForTenant(db *sql.DB, currentUser *User, tenantID, userID int, usersCols map[string]bool) (managedUserRecord, error) {
	tenantID = normalizeTenantID(tenantID)
	query := fmt.Sprintf("SELECT %s FROM users WHERE id = ? AND COALESCE(NULLIF(tenant_id, 0), 1) = ?", strings.Join(managedUserSelectColumns(usersCols), ", "))
	args := []any{userID, tenantID}
	if !isPlatformAdmin(currentUser) {
		query += " AND role <> ?"
		args = append(args, rolePlatformAdmin)
	}
	return scanManagedUserRecord(db.QueryRow(query, args...))
}

func ensureTenantRetainsActiveAdmin(db *sql.DB, tenantID, targetUserID int) error {
	tenantID = normalizeTenantID(tenantID)
	var otherActiveAdmins int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM users
		WHERE COALESCE(NULLIF(tenant_id, 0), 1) = ?
		  AND role IN (?, ?)
		  AND is_active = 1
		  AND id != ?
	`, tenantID, rolePlatformAdmin, roleAdmin, targetUserID).Scan(&otherActiveAdmins); err != nil {
		return err
	}
	if otherActiveAdmins == 0 {
		return requestError{Status: http.StatusBadRequest, Message: "Debe existir al menos un admin activo."}
	}
	return nil
}

func createManagedUser(db *sql.DB, currentUser *User, tenantID int, usersCols map[string]bool, input managedUserInput, source string, decoratePayload func(map[string]any) map[string]any) (managedUserRecord, error) {
	tenantID = normalizeTenantID(tenantID)
	input = normalizeManagedUserInput(input, usersCols)

	if input.Username == "" {
		return managedUserRecord{}, requestError{Status: http.StatusBadRequest, Message: "Usuario obligatorio."}
	}
	if input.Password == "" {
		return managedUserRecord{}, requestError{Status: http.StatusBadRequest, Message: "Contraseña obligatoria."}
	}
	if !isValidManagedRole(input.Role, isPlatformAdmin(currentUser)) {
		return managedUserRecord{}, requestError{Status: http.StatusBadRequest, Message: "Rol inválido."}
	}
	if !isPlatformAdmin(currentUser) && input.Role == rolePlatformAdmin {
		return managedUserRecord{}, requestError{Status: http.StatusForbidden, Message: "Solo un platform admin puede crear ese usuario."}
	}
	if usersCols["email"] && input.Email != "" {
		var emailExists int
		if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE email = ?`, input.Email).Scan(&emailExists); err != nil {
			return managedUserRecord{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo validar el email del usuario."}
		}
		if emailExists > 0 {
			return managedUserRecord{}, requestError{Status: http.StatusBadRequest, Message: "El email ya existe."}
		}
	}

	var usernameExists int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE username = ?`, input.Username).Scan(&usernameExists); err != nil {
		return managedUserRecord{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo validar el usuario."}
	}
	if usernameExists > 0 {
		return managedUserRecord{}, requestError{Status: http.StatusBadRequest, Message: "El usuario ya existe."}
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(input.Password), bcrypt.DefaultCost)
	if err != nil {
		return managedUserRecord{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo procesar la contraseña."}
	}

	activeInt := 0
	if input.IsActive {
		activeInt = 1
	}
	targetTenantID := tenantID
	if input.Role == rolePlatformAdmin {
		targetTenantID = defaultTenantID
	}

	cols := []string{"username", "password_hash", "role", "tenant_id"}
	args := []any{input.Username, string(hashedPassword), input.Role, targetTenantID}
	if usersCols["name"] {
		cols = append(cols, "name")
		args = append(args, input.Name)
	}
	if usersCols["email"] {
		cols = append(cols, "email")
		args = append(args, input.Email)
	}
	if usersCols["password_salt"] {
		cols = append(cols, "password_salt")
		args = append(args, "bcrypt")
	}
	if usersCols["telegram_id"] {
		cols = append(cols, "telegram_id")
		args = append(args, input.TelegramID)
	}
	if usersCols["created_at"] {
		cols = append(cols, "created_at")
		args = append(args, time.Now().Format(time.RFC3339))
	}
	if usersCols["is_active"] {
		cols = append(cols, "is_active")
		args = append(args, activeInt)
	}
	if usersCols["active"] {
		cols = append(cols, "active")
		args = append(args, activeInt)
	}

	placeholders := make([]string, len(cols))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	createdUserID, err := insertAndReturnID(
		db,
		fmt.Sprintf("INSERT INTO users (%s) VALUES (%s)", strings.Join(cols, ", "), strings.Join(placeholders, ", ")),
		args...,
	)
	if err != nil {
		return managedUserRecord{}, requestError{Status: http.StatusBadRequest, Message: userCreateErrorText(err)}
	}

	record, err := managedUserByIDForTenant(db, currentUser, targetTenantID, int(createdUserID), usersCols)
	if err != nil {
		return managedUserRecord{}, requestError{Status: http.StatusInternalServerError, Message: "Usuario creado, pero no se pudo cargar el detalle."}
	}

	auditPayload := map[string]any{
		"user_id":     record.ID,
		"username":    record.Username,
		"role":        record.Role,
		"is_active":   record.IsActive,
		"tenant_id":   record.TenantID,
		"telegram_id": record.TelegramID,
	}
	if decoratePayload != nil {
		auditPayload = decoratePayload(auditPayload)
	}
	if err := logAuditEvent(db, currentUser, "user_created", "user", strconv.Itoa(record.ID), source, auditPayload); err != nil {
		return managedUserRecord{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la auditoría del usuario."}
	}

	return record, nil
}

func updateManagedUser(db *sql.DB, currentUser *User, tenantID, userID int, usersCols map[string]bool, input managedUserInput, source string, decoratePayload func(map[string]any) map[string]any) (managedUserRecord, error) {
	tenantID = normalizeTenantID(tenantID)
	currentRecord, err := managedUserByIDForTenant(db, currentUser, tenantID, userID, usersCols)
	if err != nil {
		if err == sql.ErrNoRows {
			return managedUserRecord{}, requestError{Status: http.StatusNotFound, Message: "Usuario no encontrado."}
		}
		return managedUserRecord{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo cargar el usuario."}
	}

	input = normalizeManagedUserInput(input, usersCols)
	if input.Username == "" {
		return managedUserRecord{}, requestError{Status: http.StatusBadRequest, Message: "Usuario obligatorio."}
	}
	if !isValidManagedRole(input.Role, isPlatformAdmin(currentUser)) {
		return managedUserRecord{}, requestError{Status: http.StatusBadRequest, Message: "Rol inválido."}
	}
	if !canManagePlatformUser(currentUser, currentRecord.Role) {
		return managedUserRecord{}, requestError{Status: http.StatusForbidden, Message: "Solo un platform admin puede editar ese usuario."}
	}

	if usersCols["email"] && input.Email != "" {
		var emailExists int
		if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE email = ? AND id != ?`, input.Email, currentRecord.ID).Scan(&emailExists); err != nil {
			return managedUserRecord{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo validar el email del usuario."}
		}
		if emailExists > 0 {
			return managedUserRecord{}, requestError{Status: http.StatusBadRequest, Message: "El email ya existe."}
		}
	}

	var usernameExists int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE username = ? AND id != ?`, input.Username, currentRecord.ID).Scan(&usernameExists); err != nil {
		return managedUserRecord{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo validar el usuario."}
	}
	if usernameExists > 0 {
		return managedUserRecord{}, requestError{Status: http.StatusBadRequest, Message: "El usuario ya existe."}
	}

	if isAdminRole(currentRecord.Role) && currentRecord.IsActive && (!isAdminRole(input.Role) || !input.IsActive) {
		if err := ensureTenantRetainsActiveAdmin(db, currentRecord.TenantID, currentRecord.ID); err != nil {
			return managedUserRecord{}, err
		}
	}

	setCols := []string{"username = ?", "role = ?"}
	args := []any{input.Username, input.Role}
	if usersCols["name"] {
		setCols = append(setCols, "name = ?")
		args = append(args, input.Name)
	}
	if usersCols["email"] {
		setCols = append(setCols, "email = ?")
		args = append(args, input.Email)
	}
	if usersCols["telegram_id"] {
		setCols = append(setCols, "telegram_id = ?")
		args = append(args, input.TelegramID)
	}
	activeInt := 0
	if input.IsActive {
		activeInt = 1
	}
	if usersCols["is_active"] {
		setCols = append(setCols, "is_active = ?")
		args = append(args, activeInt)
	}
	if usersCols["active"] {
		setCols = append(setCols, "active = ?")
		args = append(args, activeInt)
	}
	args = append(args, currentRecord.ID, currentRecord.TenantID)

	if _, err := db.Exec(
		fmt.Sprintf("UPDATE users SET %s WHERE id = ? AND COALESCE(NULLIF(tenant_id, 0), 1) = ?", strings.Join(setCols, ", ")),
		args...,
	); err != nil {
		return managedUserRecord{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo actualizar el usuario."}
	}
	if !input.IsActive {
		_, _ = db.Exec(`DELETE FROM sessions WHERE user_id = ?`, currentRecord.ID)
	}

	updatedRecord, err := managedUserByIDForTenant(db, currentUser, currentRecord.TenantID, currentRecord.ID, usersCols)
	if err != nil {
		return managedUserRecord{}, requestError{Status: http.StatusInternalServerError, Message: "Usuario actualizado, pero no se pudo cargar el detalle."}
	}

	auditPayload := map[string]any{
		"user_id":           updatedRecord.ID,
		"username":          updatedRecord.Username,
		"role":              updatedRecord.Role,
		"is_active":         updatedRecord.IsActive,
		"tenant_id":         updatedRecord.TenantID,
		"telegram_id":       updatedRecord.TelegramID,
		"previous_role":     currentRecord.Role,
		"previous_active":   currentRecord.IsActive,
		"previous_telegram": currentRecord.TelegramID,
	}
	if decoratePayload != nil {
		auditPayload = decoratePayload(auditPayload)
	}
	if err := logAuditEvent(db, currentUser, "user_updated", "user", strconv.Itoa(updatedRecord.ID), source, auditPayload); err != nil {
		return managedUserRecord{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la auditoría del usuario."}
	}

	return updatedRecord, nil
}

func tableColumns(db *sql.DB, table string) (map[string]bool, error) {
	for i, r := range table {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' || (i > 0 && r >= '0' && r <= '9') {
			continue
		}
		return nil, fmt.Errorf("invalid table name: %q", table)
	}

	var (
		rows *sql.Rows
		err  error
	)
	if isPostgresDB() {
		rows, err = db.Query(`
			SELECT column_name
			FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = ?
		`, table)
	} else {
		// SQLite PRAGMA table_info does not reliably accept a bound parameter for table name,
		// so we build the statement after validating the identifier.
		rows, err = db.Query(fmt.Sprintf("SELECT name FROM pragma_table_info('%s')", table))
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		cols[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cols, nil
}

func tableExists(db *sql.DB, table string) (bool, error) {
	var exists int
	if isPostgresDB() {
		if err := db.QueryRow(`
			SELECT COUNT(*)
			FROM information_schema.tables
			WHERE table_schema = current_schema() AND table_name = ?
		`, table).Scan(&exists); err != nil {
			return false, err
		}
		return exists > 0, nil
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&exists); err != nil {
		return false, err
	}
	return exists > 0, nil
}

func ensureLegacyOperationalColumns(db *sql.DB) error {
	usersCols, err := tableColumns(db, "users")
	if err != nil {
		return err
	}
	userColumns := []struct {
		name       string
		definition string
	}{
		{name: "name", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "email", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "tenant_id", definition: "INTEGER NOT NULL DEFAULT 1"},
		{name: "telegram_id", definition: "TEXT NOT NULL DEFAULT ''"},
	}
	for _, column := range userColumns {
		if !usersCols[column.name] {
			if _, err := db.Exec("ALTER TABLE users ADD COLUMN " + column.name + " " + column.definition); err != nil {
				return err
			}
		}
	}
	if _, err := db.Exec("UPDATE users SET tenant_id = ? WHERE tenant_id IS NULL OR tenant_id <= 0", defaultTenantID); err != nil {
		return err
	}
	if _, err := db.Exec("UPDATE users SET name = COALESCE(NULLIF(name, ''), username) WHERE name IS NULL OR TRIM(name) = ''"); err != nil {
		return err
	}
	if _, err := db.Exec("UPDATE users SET email = COALESCE(NULLIF(email, ''), CASE WHEN username LIKE '%@%' THEN username ELSE username || '@local' END) WHERE email IS NULL OR TRIM(email) = ''"); err != nil {
		return err
	}
	if _, err := db.Exec("UPDATE users SET telegram_id = COALESCE(telegram_id, '') WHERE telegram_id IS NULL"); err != nil {
		return err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_users_tenant_id ON users(tenant_id)"); err != nil {
		return err
	}

	productosCols, err := tableColumns(db, "productos")
	if err != nil {
		return err
	}
	if !productosCols["id"] {
		if _, err := db.Exec("ALTER TABLE productos ADD COLUMN id TEXT"); err != nil {
			return err
		}
	}
	if _, err := db.Exec("UPDATE productos SET id = sku WHERE id IS NULL OR id = ''"); err != nil {
		return err
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_productos_id_unique ON productos(id)"); err != nil {
		return err
	}
	productosCols, err = tableColumns(db, "productos")
	if err != nil {
		return err
	}
	productColumns := []struct {
		name       string
		definition string
	}{
		{name: "fecha_ingreso", definition: "TEXT"},
		{name: "retoma_enabled", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "retoma_price", definition: "REAL"},
		{name: "location", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "credit_enabled", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "debtor_name", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "installments_total", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "installments_paid", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "total_value", definition: "REAL NOT NULL DEFAULT 0"},
		{name: "installment_value", definition: "REAL NOT NULL DEFAULT 0"},
		{name: "owner_user_id", definition: "INTEGER"},
	}
	for _, column := range productColumns {
		if !productosCols[column.name] {
			if _, err := db.Exec("ALTER TABLE productos ADD COLUMN " + column.name + " " + column.definition); err != nil {
				return err
			}
		}
	}
	if _, err := db.Exec("UPDATE productos SET fecha_ingreso = CURRENT_TIMESTAMP WHERE fecha_ingreso IS NULL OR fecha_ingreso = ''"); err != nil {
		return err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_productos_debtor_name ON productos(debtor_name)"); err != nil {
		return err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_productos_owner_user_id ON productos(owner_user_id)"); err != nil {
		return err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_productos_tenant_location ON productos(tenant_id, location)"); err != nil {
		return err
	}

	creditSalesCols, err := tableColumns(db, "credit_sales")
	if err != nil {
		return err
	}
	creditSalesColumns := []struct {
		name       string
		definition string
	}{
		{name: "quantity", definition: "INTEGER NOT NULL DEFAULT 1"},
		{name: "debtor_document_type", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "debtor_document_number", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "debtor_phone", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "interest_percent", definition: "REAL NOT NULL DEFAULT 0"},
		{name: "notes", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "created_by", definition: "INTEGER"},
	}
	for _, column := range creditSalesColumns {
		if !creditSalesCols[column.name] {
			if _, err := db.Exec("ALTER TABLE credit_sales ADD COLUMN " + column.name + " " + column.definition); err != nil {
				return err
			}
		}
	}

	creditInstallmentsCols, err := tableColumns(db, "credit_installments")
	if err != nil {
		return err
	}
	if !creditInstallmentsCols["credit_sale_id"] {
		if _, err := db.Exec("ALTER TABLE credit_installments ADD COLUMN credit_sale_id INTEGER"); err != nil {
			return err
		}
	}
	if !creditInstallmentsCols["amount_paid"] {
		if _, err := db.Exec("ALTER TABLE credit_installments ADD COLUMN amount_paid REAL NOT NULL DEFAULT 0"); err != nil {
			return err
		}
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_credit_sales_product_id ON credit_sales(product_id, created_at)"); err != nil {
		return err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_credit_sales_debtor_name ON credit_sales(debtor_name)"); err != nil {
		return err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_credit_installments_credit_sale_id ON credit_installments(credit_sale_id, installment_number)"); err != nil {
		return err
	}

	retomasCols, err := tableColumns(db, "retomas")
	if err != nil {
		return err
	}
	if !retomasCols["publicado_stock"] {
		if _, err := db.Exec("ALTER TABLE retomas ADD COLUMN publicado_stock INTEGER NOT NULL DEFAULT 0"); err != nil {
			return err
		}
	}
	if !retomasCols["precio_publicado"] {
		if _, err := db.Exec("ALTER TABLE retomas ADD COLUMN precio_publicado REAL"); err != nil {
			return err
		}
	}

	ventasCols, err := tableColumns(db, "ventas")
	if err != nil {
		return err
	}
	for _, column := range []struct {
		name       string
		definition string
	}{
		{name: "notas", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "channel", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "sold_by", definition: "TEXT NOT NULL DEFAULT ''"},
	} {
		if !ventasCols[column.name] {
			if _, err := db.Exec("ALTER TABLE ventas ADD COLUMN " + column.name + " " + column.definition); err != nil {
				return err
			}
		}
	}

	unidadesCols, err := tableColumns(db, "unidades")
	if err != nil {
		return err
	}
	if !unidadesCols["caducidad"] {
		if _, err := db.Exec("ALTER TABLE unidades ADD COLUMN caducidad TEXT"); err != nil {
			return err
		}
	}
	return nil
}

func ensureCustomerCRMBase(db *sql.DB) error {
	schema := normalizeSchemaSQLForEngine(`
	CREATE TABLE IF NOT EXISTS customers (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		name TEXT NOT NULL,
		phone TEXT NOT NULL DEFAULT '',
		document_type TEXT NOT NULL DEFAULT '',
		document_number TEXT NOT NULL DEFAULT '',
		address TEXT NOT NULL DEFAULT '',
		city TEXT NOT NULL DEFAULT '',
		notes TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_customers_tenant_document ON customers (tenant_id, document_type, document_number);
	CREATE INDEX IF NOT EXISTS idx_customers_tenant_name ON customers (tenant_id, name);
	CREATE INDEX IF NOT EXISTS idx_customers_tenant_city ON customers (tenant_id, city);

	CREATE TABLE IF NOT EXISTS customer_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		customer_id INTEGER NOT NULL,
		event_type TEXT NOT NULL,
		ref_type TEXT NOT NULL DEFAULT '',
		ref_id TEXT NOT NULL DEFAULT '',
		amount REAL NOT NULL DEFAULT 0,
		payload_json TEXT NOT NULL DEFAULT '{}',
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		created_by INTEGER
	);
	CREATE INDEX IF NOT EXISTS idx_customer_events_tenant_customer_created ON customer_events (tenant_id, customer_id, created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_customer_events_tenant_event_type ON customer_events (tenant_id, event_type);

	CREATE TABLE IF NOT EXISTS invoices (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		invoice_number TEXT NOT NULL,
		source_type TEXT NOT NULL,
		sale_id INTEGER,
		credit_sale_id INTEGER,
		customer_id INTEGER,
		customer_name TEXT NOT NULL DEFAULT '',
		customer_phone TEXT NOT NULL DEFAULT '',
		customer_document_type TEXT NOT NULL DEFAULT '',
		customer_document_number TEXT NOT NULL DEFAULT '',
		customer_address TEXT NOT NULL DEFAULT '',
		customer_city TEXT NOT NULL DEFAULT '',
		notes TEXT NOT NULL DEFAULT '',
		subtotal REAL NOT NULL DEFAULT 0,
		total REAL NOT NULL DEFAULT 0,
		status TEXT NOT NULL DEFAULT 'issued',
		created_by INTEGER,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_invoices_tenant_number ON invoices (tenant_id, invoice_number);
	CREATE INDEX IF NOT EXISTS idx_invoices_tenant_created ON invoices (tenant_id, created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_invoices_tenant_sale ON invoices (tenant_id, sale_id);
	CREATE INDEX IF NOT EXISTS idx_invoices_tenant_credit ON invoices (tenant_id, credit_sale_id);
	CREATE INDEX IF NOT EXISTS idx_invoices_tenant_customer ON invoices (tenant_id, customer_id);
	CREATE UNIQUE INDEX IF NOT EXISTS uq_invoices_tenant_sale ON invoices (tenant_id, sale_id);
	CREATE UNIQUE INDEX IF NOT EXISTS uq_invoices_tenant_credit ON invoices (tenant_id, credit_sale_id);

	CREATE TABLE IF NOT EXISTS invoice_items (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		invoice_id INTEGER NOT NULL,
		product_id TEXT NOT NULL DEFAULT '',
		description TEXT NOT NULL,
		quantity INTEGER NOT NULL DEFAULT 1,
		unit_price REAL NOT NULL DEFAULT 0,
		total REAL NOT NULL DEFAULT 0
	);
	CREATE INDEX IF NOT EXISTS idx_invoice_items_tenant_invoice ON invoice_items (tenant_id, invoice_id);
	`, currentDBEngine())
	if _, err := db.Exec(schema); err != nil {
		return err
	}

	customerCRMColumns := []struct {
		table      string
		name       string
		definition string
	}{
		{table: "credit_sales", name: "customer_id", definition: "INTEGER"},
		{table: "credit_sales", name: "kind", definition: "TEXT NOT NULL DEFAULT 'product_credit'"},
		{table: "credit_sales", name: "status", definition: "TEXT NOT NULL DEFAULT 'active'"},
		{table: "credit_installments", name: "payment_type", definition: "TEXT NOT NULL DEFAULT 'cuota'"},
	}
	for _, column := range customerCRMColumns {
		cols, err := tableColumns(db, column.table)
		if err != nil {
			return err
		}
		if !cols[column.name] {
			if _, err := db.Exec("ALTER TABLE " + column.table + " ADD COLUMN " + column.name + " " + column.definition); err != nil {
				return err
			}
		}
	}
	if err := migrateCreditTablesForCashLoans(db); err != nil {
		return err
	}

	if _, err := db.Exec("UPDATE customers SET tenant_id = ? WHERE tenant_id IS NULL OR tenant_id <= 0", defaultTenantID); err != nil {
		return err
	}
	if _, err := db.Exec("UPDATE customer_events SET tenant_id = COALESCE((SELECT COALESCE(NULLIF(customers.tenant_id, 0), ?) FROM customers WHERE customers.id = customer_events.customer_id), ?) WHERE tenant_id IS NULL OR tenant_id <= 0", defaultTenantID, defaultTenantID); err != nil {
		return err
	}
	if _, err := db.Exec("UPDATE credit_sales SET kind = 'product_credit' WHERE kind IS NULL OR TRIM(kind) = ''"); err != nil {
		return err
	}
	if _, err := db.Exec("UPDATE credit_sales SET status = 'active' WHERE status IS NULL OR TRIM(status) = ''"); err != nil {
		return err
	}
	if _, err := db.Exec("UPDATE credit_installments SET payment_type = 'cuota' WHERE payment_type IS NULL OR TRIM(payment_type) = ''"); err != nil {
		return err
	}

	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_credit_sales_tenant_customer_id ON credit_sales(tenant_id, customer_id, created_at)"); err != nil {
		return err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_credit_sales_tenant_kind_created ON credit_sales(tenant_id, kind, created_at)"); err != nil {
		return err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_credit_installments_tenant_payment_type ON credit_installments(tenant_id, payment_type, created_at)"); err != nil {
		return err
	}

	return nil
}

func ensureProductLoanBase(db *sql.DB) error {
	schema := normalizeSchemaSQLForEngine(`
	CREATE TABLE IF NOT EXISTS product_loans (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		product_id TEXT NOT NULL,
		customer_id INTEGER,
		quantity INTEGER NOT NULL DEFAULT 1,
		borrower_name TEXT NOT NULL,
		borrower_phone TEXT NOT NULL DEFAULT '',
		borrower_document_type TEXT NOT NULL DEFAULT '',
		borrower_document_number TEXT NOT NULL DEFAULT '',
		borrower_address TEXT NOT NULL DEFAULT '',
		borrower_city TEXT NOT NULL DEFAULT '',
		notes TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL DEFAULT 'active',
		loaned_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		due_at TEXT NOT NULL DEFAULT '',
		closed_at TEXT NOT NULL DEFAULT '',
		created_by INTEGER,
		closed_by INTEGER,
		close_notes TEXT NOT NULL DEFAULT ''
	);
	CREATE INDEX IF NOT EXISTS idx_product_loans_tenant_status_loaned ON product_loans (tenant_id, status, loaned_at DESC);
	CREATE INDEX IF NOT EXISTS idx_product_loans_tenant_product_status ON product_loans (tenant_id, product_id, status);
	CREATE INDEX IF NOT EXISTS idx_product_loans_tenant_customer_status ON product_loans (tenant_id, customer_id, status);

	CREATE TABLE IF NOT EXISTS product_loan_units (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		product_loan_id INTEGER NOT NULL,
		unit_id TEXT NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_product_loan_units_tenant_loan ON product_loan_units (tenant_id, product_loan_id);
	CREATE INDEX IF NOT EXISTS idx_product_loan_units_tenant_unit ON product_loan_units (tenant_id, unit_id);
	`, currentDBEngine())
	if _, err := db.Exec(schema); err != nil {
		return err
	}
	return nil
}

func initDB(path string, paymentMethods []string) (*sql.DB, error) {
	return initDBWithConfig(databaseConfig{
		Engine: dbEngineSQLite,
		DSN:    path,
		Label:  "SQLite",
	}, paymentMethods)
}

func initDBWithConfig(cfg databaseConfig, paymentMethods []string) (*sql.DB, error) {
	cfg.Engine = dbEngine(strings.TrimSpace(strings.ToLower(string(cfg.Engine))))
	if cfg.Engine != dbEnginePostgres {
		cfg.Engine = dbEngineSQLite
	}
	activeDBEngine = cfg.Engine
	switch cfg.Engine {
	case dbEnginePostgres:
		return initPostgresDB(cfg.DSN, paymentMethods)
	default:
		return initSQLiteDB(cfg.DSN, paymentMethods)
	}
}

func initSQLiteDB(path string, paymentMethods []string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return nil, err
	}
	// Keep FK enforcement disabled during migrations/seeding to avoid startup failures
	// on legacy schemas; re-enable once we've aligned the schema.
	if _, err := db.Exec("PRAGMA foreign_keys=OFF"); err != nil {
		return nil, err
	}

	schema := `
	CREATE TABLE IF NOT EXISTS tenants (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		slug TEXT NOT NULL UNIQUE,
		name TEXT NOT NULL,
		active INTEGER NOT NULL DEFAULT 1,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_tenants_slug ON tenants (slug);

	CREATE TABLE IF NOT EXISTS productos (
		sku TEXT PRIMARY KEY,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		id TEXT,
		linea TEXT NOT NULL,
		nombre TEXT NOT NULL,
		location TEXT NOT NULL DEFAULT '',
		credit_enabled INTEGER NOT NULL DEFAULT 0,
		debtor_name TEXT NOT NULL DEFAULT '',
		installments_total INTEGER NOT NULL DEFAULT 0,
		installments_paid INTEGER NOT NULL DEFAULT 0,
		total_value REAL NOT NULL DEFAULT 0,
		installment_value REAL NOT NULL DEFAULT 0,
		owner_user_id INTEGER,
		precio_base REAL NOT NULL DEFAULT 0,
		precio_venta REAL NOT NULL DEFAULT 0,
		retoma_enabled INTEGER NOT NULL DEFAULT 0,
		retoma_price REAL,
		precio_consultora REAL NOT NULL DEFAULT 0,
		descuento REAL NOT NULL DEFAULT 0,
		anotaciones TEXT NOT NULL DEFAULT '',
		aplica_caducidad INTEGER NOT NULL DEFAULT 0,
		fecha_ingreso TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
	);
	CREATE INDEX IF NOT EXISTS idx_productos_linea ON productos (linea);
	CREATE INDEX IF NOT EXISTS idx_productos_tenant_id ON productos (tenant_id);

	CREATE TABLE IF NOT EXISTS ventas (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		producto_id TEXT NOT NULL,
		cantidad INTEGER NOT NULL,
		precio_final REAL NOT NULL,
		metodo_pago TEXT NOT NULL,
		channel TEXT NOT NULL DEFAULT '',
		sold_by TEXT NOT NULL DEFAULT '',
		notas TEXT NOT NULL DEFAULT '',
		fecha TEXT NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_ventas_tenant_fecha ON ventas (tenant_id, fecha);
	CREATE INDEX IF NOT EXISTS idx_ventas_tenant_metodo ON ventas (tenant_id, metodo_pago);

	CREATE TABLE IF NOT EXISTS retomas (
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
	CREATE INDEX IF NOT EXISTS idx_retomas_tenant_fecha ON retomas (tenant_id, fecha);
	CREATE INDEX IF NOT EXISTS idx_retomas_tenant_producto ON retomas (tenant_id, producto_id);

	CREATE TABLE IF NOT EXISTS unidades (
		id TEXT PRIMARY KEY,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		producto_id TEXT NOT NULL,
		estado TEXT NOT NULL,
		creado_en TEXT NOT NULL,
		caducidad TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_unidades_tenant_estado ON unidades (tenant_id, estado);

	CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		username TEXT NOT NULL UNIQUE,
		name TEXT NOT NULL DEFAULT '',
		email TEXT NOT NULL DEFAULT '',
		password_hash TEXT NOT NULL,
		role TEXT NOT NULL CHECK (role IN ('platform_admin', 'admin', 'empleado')),
		tenant_id INTEGER NOT NULL DEFAULT 1,
		telegram_id TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL,
		is_active INTEGER NOT NULL DEFAULT 1
	);
	CREATE INDEX IF NOT EXISTS idx_users_role ON users (role);
	CREATE INDEX IF NOT EXISTS idx_users_tenant_id ON users (tenant_id);

	CREATE TABLE IF NOT EXISTS sessions (
		token TEXT PRIMARY KEY,
		user_id INTEGER NOT NULL,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		created_at TEXT NOT NULL,
		expires_at TEXT NOT NULL,
		FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
	);
	CREATE INDEX IF NOT EXISTS idx_sessions_tenant_id ON sessions (tenant_id);

	CREATE TABLE IF NOT EXISTS api_keys (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		token_hash TEXT NOT NULL UNIQUE,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		active INTEGER NOT NULL DEFAULT 1,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys (active);
	CREATE INDEX IF NOT EXISTS idx_api_keys_tenant_id ON api_keys (tenant_id);

	CREATE TABLE IF NOT EXISTS business_settings (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL UNIQUE,
		business_name TEXT NOT NULL,
		logo_path TEXT NOT NULL DEFAULT '',
		primary_color TEXT NOT NULL DEFAULT '#0ea5c9',
		currency TEXT NOT NULL DEFAULT 'COP',
		date_format TEXT NOT NULL DEFAULT '2006-01-02',
		label_paper_width TEXT NOT NULL DEFAULT '58mm',
		invoice_paper_width TEXT NOT NULL DEFAULT '58mm',
		ticket_paper_width TEXT NOT NULL DEFAULT '58mm',
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_business_settings_tenant_id ON business_settings (tenant_id);

	CREATE TABLE IF NOT EXISTS business_lines (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		name TEXT NOT NULL,
		active INTEGER NOT NULL DEFAULT 1,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_business_lines_tenant_name ON business_lines (tenant_id, name);
	CREATE INDEX IF NOT EXISTS idx_business_lines_tenant_active_name ON business_lines (tenant_id, active, name);

	CREATE TABLE IF NOT EXISTS payment_methods (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		name TEXT NOT NULL,
		active INTEGER NOT NULL DEFAULT 1,
		sort_order INTEGER NOT NULL DEFAULT 0,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_payment_methods_tenant_name ON payment_methods (tenant_id, name);
	CREATE INDEX IF NOT EXISTS idx_payment_methods_tenant_sort ON payment_methods (tenant_id, sort_order, id);

	CREATE TABLE IF NOT EXISTS movement_settings (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		movement_type TEXT NOT NULL,
		enabled INTEGER NOT NULL DEFAULT 1,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_movement_settings_tenant_type ON movement_settings (tenant_id, movement_type);
	CREATE INDEX IF NOT EXISTS idx_movement_settings_tenant_enabled ON movement_settings (tenant_id, enabled);

	CREATE TABLE IF NOT EXISTS audit_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		event_type TEXT NOT NULL,
		entity_type TEXT NOT NULL,
		entity_id TEXT NOT NULL DEFAULT '',
		user_id INTEGER,
		source TEXT NOT NULL DEFAULT 'manual',
		payload_json TEXT NOT NULL DEFAULT '{}',
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_created_at ON audit_events (tenant_id, created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_event_type ON audit_events (tenant_id, event_type);
	CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_entity_type ON audit_events (tenant_id, entity_type);

	CREATE TABLE IF NOT EXISTS credit_installments (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		credit_sale_id INTEGER,
		product_id TEXT,
		installment_number INTEGER NOT NULL,
		amount_paid REAL NOT NULL DEFAULT 0,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		created_by INTEGER,
		FOREIGN KEY (product_id) REFERENCES productos (sku)
	);
	CREATE INDEX IF NOT EXISTS idx_credit_installments_tenant_product_id ON credit_installments (tenant_id, product_id, installment_number);

	CREATE TABLE IF NOT EXISTS credit_sales (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		kind TEXT NOT NULL DEFAULT 'product_credit',
		product_id TEXT,
		quantity INTEGER NOT NULL DEFAULT 1,
		debtor_name TEXT NOT NULL,
		debtor_document_type TEXT NOT NULL DEFAULT '',
		debtor_document_number TEXT NOT NULL DEFAULT '',
		debtor_phone TEXT NOT NULL DEFAULT '',
		installments_total INTEGER NOT NULL,
		installments_paid INTEGER NOT NULL DEFAULT 0,
		total_value REAL NOT NULL,
		interest_percent REAL NOT NULL DEFAULT 0,
		installment_value REAL NOT NULL,
		notes TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL DEFAULT 'active',
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		created_by INTEGER,
		FOREIGN KEY (product_id) REFERENCES productos (sku)
	);
	CREATE INDEX IF NOT EXISTS idx_credit_sales_tenant_product_id ON credit_sales (tenant_id, product_id, created_at);
	CREATE INDEX IF NOT EXISTS idx_credit_sales_tenant_debtor_name ON credit_sales (tenant_id, debtor_name);
	`

	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}

	if err := ensureMovimientosTable(db); err != nil {
		return nil, err
	}

	if _, err := db.Exec(`
		INSERT INTO tenants (id, slug, name, active, created_at, updated_at)
		VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO UPDATE SET
			slug = excluded.slug,
			name = excluded.name,
			active = 1,
			updated_at = CURRENT_TIMESTAMP
	`, defaultTenantID, defaultTenantSlug, defaultTenantName); err != nil {
		return nil, err
	}

	if err := migrateBusinessSettingsForTenancy(db); err != nil {
		return nil, err
	}
	if err := migrateTenantScopedLookupTable(db,
		"business_lines",
		`CREATE TABLE business_lines__tenant_new (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			tenant_id INTEGER NOT NULL DEFAULT 1,
			name TEXT NOT NULL,
			active INTEGER NOT NULL DEFAULT 1,
			created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`INSERT INTO business_lines__tenant_new (tenant_id, name, active, created_at, updated_at)
		 SELECT ?, name, active, created_at, updated_at
		 FROM business_lines
		 ORDER BY id ASC`,
		[]string{
			`CREATE UNIQUE INDEX IF NOT EXISTS idx_business_lines_tenant_name ON business_lines(tenant_id, name)`,
			`CREATE INDEX IF NOT EXISTS idx_business_lines_tenant_active_name ON business_lines(tenant_id, active, name)`,
		},
	); err != nil {
		return nil, err
	}
	if err := migrateTenantScopedLookupTable(db,
		"payment_methods",
		`CREATE TABLE payment_methods__tenant_new (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			tenant_id INTEGER NOT NULL DEFAULT 1,
			name TEXT NOT NULL,
			active INTEGER NOT NULL DEFAULT 1,
			sort_order INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`INSERT INTO payment_methods__tenant_new (tenant_id, name, active, sort_order, created_at, updated_at)
		 SELECT ?, name, active, sort_order, created_at, updated_at
		 FROM payment_methods
		 ORDER BY id ASC`,
		[]string{
			`CREATE UNIQUE INDEX IF NOT EXISTS idx_payment_methods_tenant_name ON payment_methods(tenant_id, name)`,
			`CREATE INDEX IF NOT EXISTS idx_payment_methods_tenant_sort ON payment_methods(tenant_id, sort_order, id)`,
		},
	); err != nil {
		return nil, err
	}
	if err := migrateTenantScopedLookupTable(db,
		"movement_settings",
		`CREATE TABLE movement_settings__tenant_new (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			tenant_id INTEGER NOT NULL DEFAULT 1,
			movement_type TEXT NOT NULL,
			enabled INTEGER NOT NULL DEFAULT 1,
			updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`INSERT INTO movement_settings__tenant_new (tenant_id, movement_type, enabled, updated_at)
		 SELECT ?, movement_type, enabled, updated_at
		 FROM movement_settings
		 ORDER BY id ASC`,
		[]string{
			`CREATE UNIQUE INDEX IF NOT EXISTS idx_movement_settings_tenant_type ON movement_settings(tenant_id, movement_type)`,
			`CREATE INDEX IF NOT EXISTS idx_movement_settings_tenant_enabled ON movement_settings(tenant_id, enabled)`,
		},
	); err != nil {
		return nil, err
	}

	// Legacy DB fix: precio_venta_historial has FK REFERENCES productos(id),
	// but older productos tables may not have the "id" column.
	var productosHasID int
	if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('productos') WHERE name = 'id'").Scan(&productosHasID); err != nil {
		return nil, err
	}
	if productosHasID == 0 {
		if _, err := db.Exec("ALTER TABLE productos ADD COLUMN id TEXT"); err != nil {
			return nil, err
		}
	}
	// Backfill id for existing rows and ensure uniqueness so FKs can reference it.
	if _, err := db.Exec("UPDATE productos SET id = sku WHERE id IS NULL OR id = ''"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_productos_id_unique ON productos(id)"); err != nil {
		return nil, err
	}

	productosCols, err := tableColumns(db, "productos")
	if err != nil {
		return nil, err
	}
	// Ensure legacy product columns exist for both SQLite and Postgres upgrades.
	if !productosCols["fecha_ingreso"] {
		if _, err := db.Exec("ALTER TABLE productos ADD COLUMN fecha_ingreso TEXT"); err != nil {
			return nil, err
		}
	}
	if !productosCols["retoma_enabled"] {
		if _, err := db.Exec("ALTER TABLE productos ADD COLUMN retoma_enabled INTEGER NOT NULL DEFAULT 0"); err != nil {
			return nil, err
		}
	}
	if !productosCols["retoma_price"] {
		if _, err := db.Exec("ALTER TABLE productos ADD COLUMN retoma_price REAL"); err != nil {
			return nil, err
		}
	}
	if !productosCols["location"] {
		if _, err := db.Exec("ALTER TABLE productos ADD COLUMN location TEXT NOT NULL DEFAULT ''"); err != nil {
			return nil, err
		}
	}
	creditProductColumns := []struct {
		name       string
		definition string
	}{
		{name: "credit_enabled", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "debtor_name", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "installments_total", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "installments_paid", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "total_value", definition: "REAL NOT NULL DEFAULT 0"},
		{name: "installment_value", definition: "REAL NOT NULL DEFAULT 0"},
	}
	for _, column := range creditProductColumns {
		var hasColumn int
		if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('productos') WHERE name = ?", column.name).Scan(&hasColumn); err != nil {
			return nil, err
		}
		if hasColumn == 0 {
			if _, err := db.Exec("ALTER TABLE productos ADD COLUMN " + column.name + " " + column.definition); err != nil {
				return nil, err
			}
		}
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_productos_debtor_name ON productos(debtor_name)"); err != nil {
		return nil, err
	}
	creditSalesColumns := []struct {
		name       string
		definition string
	}{
		{name: "quantity", definition: "INTEGER NOT NULL DEFAULT 1"},
		{name: "debtor_document_type", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "debtor_document_number", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "debtor_phone", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "interest_percent", definition: "REAL NOT NULL DEFAULT 0"},
		{name: "notes", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "created_by", definition: "INTEGER"},
	}
	for _, column := range creditSalesColumns {
		var hasColumn int
		if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('credit_sales') WHERE name = ?", column.name).Scan(&hasColumn); err != nil {
			return nil, err
		}
		if hasColumn == 0 {
			if _, err := db.Exec("ALTER TABLE credit_sales ADD COLUMN " + column.name + " " + column.definition); err != nil {
				return nil, err
			}
		}
	}
	var creditInstallmentsHasCreditSaleID int
	if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('credit_installments') WHERE name = 'credit_sale_id'").Scan(&creditInstallmentsHasCreditSaleID); err != nil {
		return nil, err
	}
	if creditInstallmentsHasCreditSaleID == 0 {
		if _, err := db.Exec("ALTER TABLE credit_installments ADD COLUMN credit_sale_id INTEGER"); err != nil {
			return nil, err
		}
	}
	var creditInstallmentsHasAmountPaid int
	if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('credit_installments') WHERE name = 'amount_paid'").Scan(&creditInstallmentsHasAmountPaid); err != nil {
		return nil, err
	}
	if creditInstallmentsHasAmountPaid == 0 {
		if _, err := db.Exec("ALTER TABLE credit_installments ADD COLUMN amount_paid REAL NOT NULL DEFAULT 0"); err != nil {
			return nil, err
		}
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_credit_sales_product_id ON credit_sales(product_id, created_at)"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_credit_sales_debtor_name ON credit_sales(debtor_name)"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_credit_installments_credit_sale_id ON credit_installments(credit_sale_id, installment_number)"); err != nil {
		return nil, err
	}
	var retomasHasPublicadoStock int
	if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('retomas') WHERE name = 'publicado_stock'").Scan(&retomasHasPublicadoStock); err != nil {
		return nil, err
	}
	if retomasHasPublicadoStock == 0 {
		if _, err := db.Exec("ALTER TABLE retomas ADD COLUMN publicado_stock INTEGER NOT NULL DEFAULT 0"); err != nil {
			return nil, err
		}
	}
	var retomasHasPrecioPublicado int
	if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('retomas') WHERE name = 'precio_publicado'").Scan(&retomasHasPrecioPublicado); err != nil {
		return nil, err
	}
	if retomasHasPrecioPublicado == 0 {
		if _, err := db.Exec("ALTER TABLE retomas ADD COLUMN precio_publicado REAL"); err != nil {
			return nil, err
		}
	}
	// Backfill missing timestamps (use CURRENT_TIMESTAMP so we always have a value).
	if _, err := db.Exec("UPDATE productos SET fecha_ingreso = CURRENT_TIMESTAMP WHERE fecha_ingreso IS NULL OR fecha_ingreso = ''"); err != nil {
		return nil, err
	}

	var productosHasOwner int
	if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('productos') WHERE name = 'owner_user_id'").Scan(&productosHasOwner); err != nil {
		return nil, err
	}
	if productosHasOwner == 0 {
		if _, err := db.Exec("ALTER TABLE productos ADD COLUMN owner_user_id INTEGER"); err != nil {
			return nil, err
		}
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_productos_owner_user_id ON productos(owner_user_id)"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_productos_tenant_location ON productos(tenant_id, location)"); err != nil {
		return nil, err
	}

	tenantColumns := []struct {
		table string
		name  string
		def   string
	}{
		{table: "productos", name: "tenant_id", def: "INTEGER NOT NULL DEFAULT 1"},
		{table: "unidades", name: "tenant_id", def: "INTEGER NOT NULL DEFAULT 1"},
		{table: "ventas", name: "tenant_id", def: "INTEGER NOT NULL DEFAULT 1"},
		{table: "retomas", name: "tenant_id", def: "INTEGER NOT NULL DEFAULT 1"},
		{table: "credit_sales", name: "tenant_id", def: "INTEGER NOT NULL DEFAULT 1"},
		{table: "credit_installments", name: "tenant_id", def: "INTEGER NOT NULL DEFAULT 1"},
		{table: "movimientos", name: "tenant_id", def: "INTEGER NOT NULL DEFAULT 1"},
		{table: "audit_events", name: "tenant_id", def: "INTEGER NOT NULL DEFAULT 1"},
		{table: "users", name: "tenant_id", def: "INTEGER NOT NULL DEFAULT 1"},
		{table: "sessions", name: "tenant_id", def: "INTEGER NOT NULL DEFAULT 1"},
		{table: "api_keys", name: "tenant_id", def: "INTEGER NOT NULL DEFAULT 1"},
		{table: "business_settings", name: "tenant_id", def: "INTEGER NOT NULL DEFAULT 1"},
		{table: "business_lines", name: "tenant_id", def: "INTEGER NOT NULL DEFAULT 1"},
		{table: "payment_methods", name: "tenant_id", def: "INTEGER NOT NULL DEFAULT 1"},
		{table: "movement_settings", name: "tenant_id", def: "INTEGER NOT NULL DEFAULT 1"},
	}
	for _, column := range tenantColumns {
		cols, err := tableColumns(db, column.table)
		if err != nil {
			return nil, err
		}
		if !cols[column.name] {
			if _, err := db.Exec("ALTER TABLE " + column.table + " ADD COLUMN " + column.name + " " + column.def); err != nil {
				return nil, err
			}
		}
	}
	if _, err := db.Exec("UPDATE users SET tenant_id = ? WHERE tenant_id IS NULL OR tenant_id <= 0", defaultTenantID); err != nil {
		return nil, err
	}
	usersCols, err := tableColumns(db, "users")
	if err != nil {
		return nil, err
	}
	businessSettingsCols, err := tableColumns(db, "business_settings")
	if err != nil {
		return nil, err
	}
	printSettingColumns := []struct {
		name    string
		def     string
		current string
	}{
		{name: "label_paper_width", def: "TEXT NOT NULL DEFAULT '58mm'", current: "58mm"},
		{name: "invoice_paper_width", def: "TEXT NOT NULL DEFAULT '58mm'", current: "58mm"},
		{name: "ticket_paper_width", def: "TEXT NOT NULL DEFAULT '58mm'", current: "58mm"},
	}
	for _, column := range printSettingColumns {
		if !businessSettingsCols[column.name] {
			if _, err := db.Exec("ALTER TABLE business_settings ADD COLUMN " + column.name + " " + column.def); err != nil {
				return nil, err
			}
		}
		if _, err := db.Exec("UPDATE business_settings SET "+column.name+" = ? WHERE "+column.name+" IS NULL OR TRIM("+column.name+") = ''", column.current); err != nil {
			return nil, err
		}
	}
	if !usersCols["name"] {
		if _, err := db.Exec("ALTER TABLE users ADD COLUMN name TEXT NOT NULL DEFAULT ''"); err != nil {
			return nil, err
		}
	}
	if !usersCols["email"] {
		if _, err := db.Exec("ALTER TABLE users ADD COLUMN email TEXT NOT NULL DEFAULT ''"); err != nil {
			return nil, err
		}
	}
	if !usersCols["telegram_id"] {
		if _, err := db.Exec("ALTER TABLE users ADD COLUMN telegram_id TEXT NOT NULL DEFAULT ''"); err != nil {
			return nil, err
		}
	}
	if _, err := db.Exec("UPDATE users SET name = COALESCE(NULLIF(name, ''), username) WHERE name IS NULL OR TRIM(name) = ''"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("UPDATE users SET email = COALESCE(NULLIF(email, ''), CASE WHEN username LIKE '%@%' THEN username ELSE username || '@local' END) WHERE email IS NULL OR TRIM(email) = ''"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("UPDATE users SET telegram_id = COALESCE(telegram_id, '') WHERE telegram_id IS NULL"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("UPDATE sessions SET tenant_id = COALESCE((SELECT COALESCE(NULLIF(users.tenant_id, 0), ?) FROM users WHERE users.id = sessions.user_id), ?) WHERE tenant_id IS NULL OR tenant_id <= 0", defaultTenantID, defaultTenantID); err != nil {
		return nil, err
	}
	if _, err := db.Exec("UPDATE api_keys SET tenant_id = ? WHERE tenant_id IS NULL OR tenant_id <= 0", defaultTenantID); err != nil {
		return nil, err
	}
	if _, err := db.Exec("UPDATE productos SET tenant_id = ? WHERE tenant_id IS NULL OR tenant_id <= 0", defaultTenantID); err != nil {
		return nil, err
	}
	if _, err := db.Exec("UPDATE unidades SET tenant_id = ? WHERE tenant_id IS NULL OR tenant_id <= 0", defaultTenantID); err != nil {
		return nil, err
	}
	if _, err := db.Exec("UPDATE ventas SET tenant_id = ? WHERE tenant_id IS NULL OR tenant_id <= 0", defaultTenantID); err != nil {
		return nil, err
	}
	if _, err := db.Exec("UPDATE retomas SET tenant_id = ? WHERE tenant_id IS NULL OR tenant_id <= 0", defaultTenantID); err != nil {
		return nil, err
	}
	if _, err := db.Exec("UPDATE credit_sales SET tenant_id = ? WHERE tenant_id IS NULL OR tenant_id <= 0", defaultTenantID); err != nil {
		return nil, err
	}
	if _, err := db.Exec("UPDATE credit_installments SET tenant_id = COALESCE((SELECT COALESCE(NULLIF(cs.tenant_id, 0), ?) FROM credit_sales cs WHERE cs.id = credit_installments.credit_sale_id), ?) WHERE tenant_id IS NULL OR tenant_id <= 0", defaultTenantID, defaultTenantID); err != nil {
		return nil, err
	}
	if _, err := db.Exec("UPDATE movimientos SET tenant_id = ? WHERE tenant_id IS NULL OR tenant_id <= 0", defaultTenantID); err != nil {
		return nil, err
	}
	if _, err := db.Exec("UPDATE audit_events SET tenant_id = ? WHERE tenant_id IS NULL OR tenant_id <= 0", defaultTenantID); err != nil {
		return nil, err
	}
	if _, err := db.Exec("UPDATE business_settings SET tenant_id = ? WHERE tenant_id IS NULL OR tenant_id <= 0", defaultTenantID); err != nil {
		return nil, err
	}
	if _, err := db.Exec("UPDATE business_lines SET tenant_id = ? WHERE tenant_id IS NULL OR tenant_id <= 0", defaultTenantID); err != nil {
		return nil, err
	}
	if _, err := db.Exec("UPDATE payment_methods SET tenant_id = ? WHERE tenant_id IS NULL OR tenant_id <= 0", defaultTenantID); err != nil {
		return nil, err
	}
	if _, err := db.Exec("UPDATE movement_settings SET tenant_id = ? WHERE tenant_id IS NULL OR tenant_id <= 0", defaultTenantID); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_productos_tenant_id ON productos(tenant_id)"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_unidades_tenant_estado ON unidades(tenant_id, estado)"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_ventas_tenant_fecha ON ventas(tenant_id, fecha)"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_ventas_tenant_metodo ON ventas(tenant_id, metodo_pago)"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_retomas_tenant_fecha ON retomas(tenant_id, fecha)"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_retomas_tenant_producto ON retomas(tenant_id, producto_id)"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_credit_sales_tenant_product_id ON credit_sales(tenant_id, product_id, created_at)"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_credit_sales_tenant_debtor_name ON credit_sales(tenant_id, debtor_name)"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_credit_installments_tenant_credit_sale_id ON credit_installments(tenant_id, credit_sale_id, installment_number)"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_movimientos_tenant_producto_fecha ON movimientos(tenant_id, producto_id, fecha)"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_movimientos_tenant_unidad_fecha ON movimientos(tenant_id, unidad_id, fecha)"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_created_at ON audit_events(tenant_id, created_at DESC)"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_event_type ON audit_events(tenant_id, event_type)"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_entity_type ON audit_events(tenant_id, entity_type)"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_users_tenant_id ON users(tenant_id)"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_sessions_tenant_id ON sessions(tenant_id)"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_api_keys_tenant_id ON api_keys(tenant_id)"); err != nil {
		return nil, err
	}

	if _, err := db.Exec("PRAGMA foreign_keys=ON"); err != nil {
		return nil, err
	}

	var notasColumn string
	if err := db.QueryRow("SELECT name FROM pragma_table_info('ventas') WHERE name = 'notas'").Scan(&notasColumn); err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if notasColumn == "" {
		if _, err := db.Exec("ALTER TABLE ventas ADD COLUMN notas TEXT NOT NULL DEFAULT ''"); err != nil {
			return nil, err
		}
	}

	var ventasChannelColumn string
	if err := db.QueryRow("SELECT name FROM pragma_table_info('ventas') WHERE name = 'channel'").Scan(&ventasChannelColumn); err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if ventasChannelColumn == "" {
		if _, err := db.Exec("ALTER TABLE ventas ADD COLUMN channel TEXT NOT NULL DEFAULT ''"); err != nil {
			return nil, err
		}
	}

	var ventasSoldByColumn string
	if err := db.QueryRow("SELECT name FROM pragma_table_info('ventas') WHERE name = 'sold_by'").Scan(&ventasSoldByColumn); err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if ventasSoldByColumn == "" {
		if _, err := db.Exec("ALTER TABLE ventas ADD COLUMN sold_by TEXT NOT NULL DEFAULT ''"); err != nil {
			return nil, err
		}
	}

	var caducidadColumn string
	if err := db.QueryRow("SELECT name FROM pragma_table_info('unidades') WHERE name = 'caducidad'").Scan(&caducidadColumn); err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if caducidadColumn == "" {
		if _, err := db.Exec("ALTER TABLE unidades ADD COLUMN caducidad TEXT"); err != nil {
			return nil, err
		}
	}

	var ventasCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM ventas").Scan(&ventasCount); err != nil {
		return nil, err
	}

	if ventasCount == 0 {
		if err := seedVentas(db, paymentMethods); err != nil {
			return nil, err
		}
	}

	var unidadesCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM unidades").Scan(&unidadesCount); err != nil {
		return nil, err
	}

	if unidadesCount == 0 {
		if err := seedUnidades(db); err != nil {
			return nil, err
		}
	}

	if err := ensureUsersRoleSupport(db); err != nil {
		return nil, err
	}
	if err := ensureCustomerCRMBase(db); err != nil {
		return nil, err
	}
	if err := ensureProductLoanBase(db); err != nil {
		return nil, err
	}
	if err := seedAdminUser(db, path); err != nil {
		return nil, err
	}
	if err := ensurePlatformAdminUser(db, adminUserNameForBootstrap()); err != nil {
		return nil, err
	}
	if err := seedPaymentMethodsIfMissing(db); err != nil {
		return nil, err
	}
	if err := seedMovementSettingsIfMissing(db); err != nil {
		return nil, err
	}

	return db, nil
}

func initPostgresDB(dsn string, paymentMethods []string) (*sql.DB, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("DB_DSN o DATABASE_URL es obligatorio cuando DB_ENGINE=postgres")
	}

	db, err := sql.Open(postgresDriverName, dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}

	schema := normalizeSchemaSQLForEngine(`
	CREATE TABLE IF NOT EXISTS tenants (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		slug TEXT NOT NULL UNIQUE,
		name TEXT NOT NULL,
		active INTEGER NOT NULL DEFAULT 1,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_tenants_slug ON tenants (slug);

	CREATE TABLE IF NOT EXISTS productos (
		sku TEXT PRIMARY KEY,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		id TEXT,
		linea TEXT NOT NULL,
		nombre TEXT NOT NULL,
		location TEXT NOT NULL DEFAULT '',
		credit_enabled INTEGER NOT NULL DEFAULT 0,
		debtor_name TEXT NOT NULL DEFAULT '',
		installments_total INTEGER NOT NULL DEFAULT 0,
		installments_paid INTEGER NOT NULL DEFAULT 0,
		total_value REAL NOT NULL DEFAULT 0,
		installment_value REAL NOT NULL DEFAULT 0,
		owner_user_id INTEGER,
		precio_base REAL NOT NULL DEFAULT 0,
		precio_venta REAL NOT NULL DEFAULT 0,
		retoma_enabled INTEGER NOT NULL DEFAULT 0,
		retoma_price REAL,
		precio_consultora REAL NOT NULL DEFAULT 0,
		descuento REAL NOT NULL DEFAULT 0,
		anotaciones TEXT NOT NULL DEFAULT '',
		aplica_caducidad INTEGER NOT NULL DEFAULT 0,
		fecha_ingreso TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
	);
	CREATE INDEX IF NOT EXISTS idx_productos_linea ON productos (linea);
	CREATE INDEX IF NOT EXISTS idx_productos_tenant_id ON productos (tenant_id);

	CREATE TABLE IF NOT EXISTS ventas (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		producto_id TEXT NOT NULL,
		cantidad INTEGER NOT NULL,
		precio_final REAL NOT NULL,
		metodo_pago TEXT NOT NULL,
		channel TEXT NOT NULL DEFAULT '',
		sold_by TEXT NOT NULL DEFAULT '',
		notas TEXT NOT NULL DEFAULT '',
		fecha TEXT NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_ventas_tenant_fecha ON ventas (tenant_id, fecha);
	CREATE INDEX IF NOT EXISTS idx_ventas_tenant_metodo ON ventas (tenant_id, metodo_pago);

	CREATE TABLE IF NOT EXISTS retomas (
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
	CREATE INDEX IF NOT EXISTS idx_retomas_tenant_fecha ON retomas (tenant_id, fecha);
	CREATE INDEX IF NOT EXISTS idx_retomas_tenant_producto ON retomas (tenant_id, producto_id);

	CREATE TABLE IF NOT EXISTS unidades (
		id TEXT PRIMARY KEY,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		producto_id TEXT NOT NULL,
		estado TEXT NOT NULL,
		creado_en TEXT NOT NULL,
		caducidad TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_unidades_tenant_estado ON unidades (tenant_id, estado);

	CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		username TEXT NOT NULL UNIQUE,
		name TEXT NOT NULL DEFAULT '',
		email TEXT NOT NULL DEFAULT '',
		password_hash TEXT NOT NULL,
		role TEXT NOT NULL CHECK (role IN ('platform_admin', 'admin', 'empleado')),
		tenant_id INTEGER NOT NULL DEFAULT 1,
		telegram_id TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL,
		is_active INTEGER NOT NULL DEFAULT 1
	);
	CREATE INDEX IF NOT EXISTS idx_users_role ON users (role);
	CREATE INDEX IF NOT EXISTS idx_users_tenant_id ON users (tenant_id);

	CREATE TABLE IF NOT EXISTS sessions (
		token TEXT PRIMARY KEY,
		user_id INTEGER NOT NULL,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		created_at TEXT NOT NULL,
		expires_at TEXT NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_sessions_tenant_id ON sessions (tenant_id);

	CREATE TABLE IF NOT EXISTS api_keys (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		token_hash TEXT NOT NULL UNIQUE,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		active INTEGER NOT NULL DEFAULT 1,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys (active);
	CREATE INDEX IF NOT EXISTS idx_api_keys_tenant_id ON api_keys (tenant_id);

	CREATE TABLE IF NOT EXISTS business_settings (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL UNIQUE,
		business_name TEXT NOT NULL,
		logo_path TEXT NOT NULL DEFAULT '',
		primary_color TEXT NOT NULL DEFAULT '#0ea5c9',
		currency TEXT NOT NULL DEFAULT 'COP',
		date_format TEXT NOT NULL DEFAULT '2006-01-02',
		label_paper_width TEXT NOT NULL DEFAULT '58mm',
		invoice_paper_width TEXT NOT NULL DEFAULT '58mm',
		ticket_paper_width TEXT NOT NULL DEFAULT '58mm',
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_business_settings_tenant_id ON business_settings (tenant_id);

	CREATE TABLE IF NOT EXISTS business_lines (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		name TEXT NOT NULL,
		active INTEGER NOT NULL DEFAULT 1,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_business_lines_tenant_name ON business_lines (tenant_id, name);
	CREATE INDEX IF NOT EXISTS idx_business_lines_tenant_active_name ON business_lines (tenant_id, active, name);

	CREATE TABLE IF NOT EXISTS payment_methods (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		name TEXT NOT NULL,
		active INTEGER NOT NULL DEFAULT 1,
		sort_order INTEGER NOT NULL DEFAULT 0,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_payment_methods_tenant_name ON payment_methods (tenant_id, name);
	CREATE INDEX IF NOT EXISTS idx_payment_methods_tenant_sort ON payment_methods (tenant_id, sort_order, id);

	CREATE TABLE IF NOT EXISTS movement_settings (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		movement_type TEXT NOT NULL,
		enabled INTEGER NOT NULL DEFAULT 1,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_movement_settings_tenant_type ON movement_settings (tenant_id, movement_type);
	CREATE INDEX IF NOT EXISTS idx_movement_settings_tenant_enabled ON movement_settings (tenant_id, enabled);

	CREATE TABLE IF NOT EXISTS audit_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		event_type TEXT NOT NULL,
		entity_type TEXT NOT NULL,
		entity_id TEXT NOT NULL DEFAULT '',
		user_id INTEGER,
		source TEXT NOT NULL DEFAULT 'manual',
		payload_json TEXT NOT NULL DEFAULT '{}',
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_created_at ON audit_events (tenant_id, created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_event_type ON audit_events (tenant_id, event_type);
	CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_entity_type ON audit_events (tenant_id, entity_type);

	CREATE TABLE IF NOT EXISTS credit_installments (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		credit_sale_id INTEGER,
		product_id TEXT,
		installment_number INTEGER NOT NULL,
		amount_paid REAL NOT NULL DEFAULT 0,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		created_by INTEGER
	);
	CREATE INDEX IF NOT EXISTS idx_credit_installments_tenant_product_id ON credit_installments (tenant_id, product_id, installment_number);

	CREATE TABLE IF NOT EXISTS credit_sales (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		kind TEXT NOT NULL DEFAULT 'product_credit',
		product_id TEXT,
		quantity INTEGER NOT NULL DEFAULT 1,
		debtor_name TEXT NOT NULL,
		debtor_document_type TEXT NOT NULL DEFAULT '',
		debtor_document_number TEXT NOT NULL DEFAULT '',
		debtor_phone TEXT NOT NULL DEFAULT '',
		installments_total INTEGER NOT NULL,
		installments_paid INTEGER NOT NULL DEFAULT 0,
		total_value REAL NOT NULL,
		interest_percent REAL NOT NULL DEFAULT 0,
		installment_value REAL NOT NULL,
		notes TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL DEFAULT 'active',
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		created_by INTEGER
	);
	CREATE INDEX IF NOT EXISTS idx_credit_sales_tenant_product_id ON credit_sales (tenant_id, product_id, created_at);
	CREATE INDEX IF NOT EXISTS idx_credit_sales_tenant_debtor_name ON credit_sales (tenant_id, debtor_name);
	`, dbEnginePostgres)

	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := ensureMovimientosTable(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := ensureLegacyOperationalColumns(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec(`
		INSERT INTO tenants (id, slug, name, active, created_at, updated_at)
		VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO UPDATE SET
			slug = excluded.slug,
			name = excluded.name,
			active = 1,
			updated_at = CURRENT_TIMESTAMP
	`, defaultTenantID, defaultTenantSlug, defaultTenantName); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec(`
		INSERT INTO business_settings (tenant_id, business_name, logo_path, primary_color, currency, date_format, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(tenant_id) DO NOTHING
	`, defaultTenantID, defaultBusinessSettings().BusinessName, defaultBusinessSettings().LogoPath, defaultBusinessSettings().PrimaryColor, defaultBusinessSettings().Currency, defaultBusinessSettings().DateFormat, time.Now().Format(time.RFC3339)); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := ensureUsersRoleSupport(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := ensureCustomerCRMBase(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := ensureProductLoanBase(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := seedAdminUser(db, dsn); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := ensurePlatformAdminUser(db, adminUserNameForBootstrap()); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := seedPaymentMethodsIfMissing(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := seedMovementSettingsIfMissing(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func seedVentas(db *sql.DB, paymentMethods []string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO ventas (producto_id, cantidad, precio_final, metodo_pago, notas, fecha)
		VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("prepare ventas: %w (rollback: %v)", err, rollbackErr)
		}
		return err
	}
	defer stmt.Close()

	baseDate := time.Now()
	products := []string{"P-001", "P-002", "P-003"}
	for i := 0; i < 14; i++ {
		date := baseDate.AddDate(0, 0, -i).Format("2006-01-02")
		entries := (i % 3) + 2
		for j := 0; j < entries; j++ {
			productoID := products[(i+j)%len(products)]
			cantidad := (j % 3) + 1
			precio := float64(18000 + (i * 1200) + (j * 800))
			metodo := paymentMethods[(i+j)%len(paymentMethods)]
			if _, err := stmt.Exec(productoID, cantidad, precio, metodo, "Venta seed", date); err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					return fmt.Errorf("insert ventas: %w (rollback: %v)", err, rollbackErr)
				}
				return err
			}
		}
	}

	return tx.Commit()
}

func seedUnidades(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO unidades (id, producto_id, estado, creado_en, caducidad)
		VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("prepare unidades: %w (rollback: %v)", err, rollbackErr)
		}
		return err
	}
	defer stmt.Close()

	statuses := []string{"Disponible", "Vendida", "Cambio"}
	products := []string{"P-001", "P-002", "P-003"}
	now := time.Now()
	for i := 1; i <= 36; i++ {
		id := fmt.Sprintf("U-%03d", i)
		productoID := products[i%len(products)]
		estado := statuses[i%len(statuses)]
		createdAt := now.AddDate(0, 0, -i).Format(time.RFC3339)
		expiryAt := now.AddDate(0, 0, 20+i).Format("2006-01-02")
		if _, err := stmt.Exec(id, productoID, estado, createdAt, expiryAt); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return fmt.Errorf("insert unidades: %w (rollback: %v)", err, rollbackErr)
			}
			return err
		}
	}

	return tx.Commit()
}

const (
	localBootstrapAdminUser = "admin"
	localBootstrapAdminPass = "SuperSecreto123"
)

func adminUserNameForBootstrap() string {
	adminUser := strings.TrimSpace(os.Getenv("ADMIN_USER"))
	if adminUser != "" {
		return adminUser
	}
	return localBootstrapAdminUser
}

func ensureUsersRoleSupport(db *sql.DB) error {
	if isPostgresDB() {
		if _, err := db.Exec(`ALTER TABLE users DROP CONSTRAINT IF EXISTS users_role_check`); err != nil {
			return err
		}
		if _, err := db.Exec(`
			ALTER TABLE users
			ADD CONSTRAINT users_role_check
			CHECK (role IN ('platform_admin', 'admin', 'empleado'))
		`); err != nil {
			return err
		}
		return nil
	}

	sqlText, err := tableSQL(db, "users")
	if err != nil {
		return err
	}
	if strings.Contains(strings.ToLower(sqlText), "platform_admin") {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DROP TABLE IF EXISTS users__role_new`); err != nil {
		return err
	}
	if _, err := tx.Exec(`
		CREATE TABLE users__role_new (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			username TEXT NOT NULL UNIQUE,
			name TEXT NOT NULL DEFAULT '',
			email TEXT NOT NULL DEFAULT '',
			password_hash TEXT NOT NULL,
			role TEXT NOT NULL CHECK (role IN ('platform_admin', 'admin', 'empleado')),
			tenant_id INTEGER NOT NULL DEFAULT 1,
			telegram_id TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			is_active INTEGER NOT NULL DEFAULT 1
		)
	`); err != nil {
		return err
	}
	if _, err := tx.Exec(`
		INSERT INTO users__role_new (id, username, name, email, password_hash, role, tenant_id, telegram_id, created_at, is_active)
		SELECT id, username, COALESCE(NULLIF(name, ''), username), COALESCE(NULLIF(email, ''), CASE WHEN username LIKE '%@%' THEN username ELSE username || '@local' END), password_hash, role, COALESCE(NULLIF(tenant_id, 0), ?), COALESCE(telegram_id, ''), created_at, is_active
		FROM users
	`, defaultTenantID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DROP TABLE users`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE users__role_new RENAME TO users`); err != nil {
		return err
	}
	if _, err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_users_role ON users (role)`); err != nil {
		return err
	}
	if _, err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_users_tenant_id ON users (tenant_id)`); err != nil {
		return err
	}
	return tx.Commit()
}

func ensurePlatformAdminUser(db *sql.DB, preferredUsername string) error {
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE role = ?`, rolePlatformAdmin).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	preferredUsername = strings.TrimSpace(preferredUsername)
	if preferredUsername != "" {
		result, err := db.Exec(`
			UPDATE users
			SET role = ?
			WHERE username = ?
		`, rolePlatformAdmin, preferredUsername)
		if err != nil {
			return err
		}
		if affected, err := result.RowsAffected(); err == nil && affected > 0 {
			return nil
		}
	}

	result, err := db.Exec(`
		UPDATE users
		SET role = ?
		WHERE id = (
			SELECT id
			FROM users
			WHERE role = ? AND COALESCE(NULLIF(tenant_id, 0), ?) = ?
			ORDER BY id
			LIMIT 1
		)
	`, rolePlatformAdmin, roleAdmin, defaultTenantID, defaultTenantID)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err == nil && affected > 0 {
		return nil
	}
	return nil
}

func seedAdminUser(db *sql.DB, dbPath string) error {
	adminUser := os.Getenv("ADMIN_USER")
	adminPass := os.Getenv("ADMIN_PASS")
	if adminUser == "" || adminPass == "" {
		var totalUsers int
		if err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(&totalUsers); err != nil {
			return err
		}
		if totalUsers == 0 && dbPath == "data.db" {
			adminUser = localBootstrapAdminUser
			adminPass = localBootstrapAdminPass
			log.Printf("ADMIN_USER/ADMIN_PASS no configurados; creando admin local por defecto user=%q", adminUser)
		} else {
			log.Print("ADMIN_USER/ADMIN_PASS no configurados, omitiendo creación automática de admin.")
			return nil
		}
	}

	var existingID int
	err := db.QueryRow("SELECT id FROM users WHERE username = ?", adminUser).Scan(&existingID)
	if err == nil {
		return nil
	}
	if err != sql.ErrNoRows {
		return err
	}

	hashed, err := bcrypt.GenerateFromPassword([]byte(adminPass), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	adminEmail := adminUser
	if !strings.Contains(adminEmail, "@") {
		adminEmail = adminUser + "@local"
	}
	_, err = db.Exec(`
		INSERT INTO users (username, name, email, password_hash, role, tenant_id, telegram_id, created_at, is_active)
		VALUES (?, ?, ?, ?, ?, ?, '', ?, 1)
	`, adminUser, adminUser, adminEmail, string(hashed), rolePlatformAdmin, defaultTenantID, time.Now().Format(time.RFC3339))
	return err
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	dbConfig := loadDatabaseConfig("data.db")

	paymentMethods := defaultPaymentMethodNames()

	db, err := initDBWithConfig(dbConfig, paymentMethods)
	if err != nil {
		log.Fatalf("Error al abrir %s: %v", dbConfig.Label, err)
	}
	defer db.Close()
	if err := ensureUploadDirs(); err != nil {
		log.Fatalf("Error al preparar uploads: %v", err)
	}
	settings, err := loadBusinessSettings(db)
	if err != nil {
		log.Fatalf("Error al cargar configuración del negocio: %v", err)
	}
	setCurrentBusinessSettings(settings)

	resolveTemplateSettings := func(data any) BusinessSettings {
		if settings, ok := businessSettingsFromTemplateData(data); ok {
			return settings
		}
		if user, ok := currentUserFromTemplateData(data); ok {
			settings, err := loadBusinessSettingsForTenant(db, tenantIDFromUser(user))
			if err == nil {
				return settings
			}
			log.Printf("branding template settings tenant_id=%d: %v", tenantIDFromUser(user), err)
		}
		return currentBusinessSettings()
	}

	tmpl := template.Must(template.New("").Funcs(template.FuncMap{
		"businessName": func(data any) string {
			return resolveTemplateSettings(data).BusinessName
		},
		"businessLogoPath": func(data any) string {
			return effectiveBusinessLogoPath(resolveTemplateSettings(data), data)
		},
		"businessPrimaryColor": func(data any) string {
			return resolveTemplateSettings(data).PrimaryColor
		},
		"businessPrimaryStrong": func(data any) string {
			return shadeHexColor(resolveTemplateSettings(data).PrimaryColor, -24)
		},
		"businessPrimarySoft": func(data any) string {
			return shadeHexColor(resolveTemplateSettings(data).PrimaryColor, 208)
		},
		"pageCanLoan": func(data any) bool {
			return movementEnabledFromTemplateData(db, data, "CanLoan", "prestamo")
		},
		"pageCanCredit": func(data any) bool {
			return movementEnabledFromTemplateData(db, data, "CanCredit", "credito")
		},
		"money": func(value float64) string {
			return formatCurrency(value)
		},
	}).ParseFiles(
		"templates/partials/app_styles.html",
		"templates/admin_users.html",
		"templates/customers.html",
		"templates/customer_detail.html",
		"templates/credit_edits_report.html",
		"templates/cash_loans_report.html",
		"templates/product_loans_report.html",
		"templates/product_loan_detail.html",
		"templates/business_settings.html",
		"templates/audit_events.html",
		"templates/dashboard.html",
		"templates/inventario.html",
		"templates/login.html",
		"templates/product_new.html",
		"templates/venta_new.html",
		"templates/venta_confirm.html",
		"templates/sale_receipt.html",
		"templates/sale_ticket_thermal.html",
		"templates/invoice_new.html",
		"templates/invoice_document.html",
		"templates/product_labels.html",
		"templates/cambio_new.html",
		"templates/cambio_confirm.html",
		"templates/csv_template.html",
		"templates/csv_export.html",
		"templates/partials/header.html",
	))
	renderTemplate := func(w http.ResponseWriter, name string, data any, renderErrMessage string) {
		var rendered bytes.Buffer
		if err := tmpl.ExecuteTemplate(&rendered, name, data); err != nil {
			http.Error(w, renderErrMessage, http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(rendered.Bytes())
	}
	activePaymentMethods, err := loadPaymentMethods(db, true)
	if err != nil {
		log.Fatalf("Error al cargar métodos de pago: %v", err)
	}
	paymentMethods = paymentMethodNames(activePaymentMethods)

	// Diagnostics to confirm which DB is being used at runtime (helps debug login issues).
	if wd, err := os.Getwd(); err == nil {
		if dbConfig.Engine == dbEngineSQLite {
			if abs, err := filepath.Abs(dbConfig.DSN); err == nil {
				log.Printf("DB_ENGINE=%s DB_PATH=%s (abs=%s) cwd=%s", dbConfig.Engine, dbConfig.DSN, abs, wd)
			} else {
				log.Printf("DB_ENGINE=%s DB_PATH=%s cwd=%s", dbConfig.Engine, dbConfig.DSN, wd)
			}
		} else {
			log.Printf("DB_ENGINE=%s DB_DSN=%s cwd=%s", dbConfig.Engine, dbConfig.DSN, wd)
		}
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(new(int)); err != nil {
		log.Printf("DB users table not queryable: %v", err)
	} else {
		var totalUsers int
		if err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(&totalUsers); err == nil {
			log.Printf("Users total=%d", totalUsers)
		}
		var adminMatches int
		if err := db.QueryRow("SELECT COUNT(*) FROM users WHERE username = 'admin'").Scan(&adminMatches); err == nil {
			log.Printf("Users username=admin matches=%d", adminMatches)
		} else {
			log.Printf("Users username=admin query failed: %v", err)
		}
	}

	var productsMu sync.RWMutex
	defaultProducts := []productOption{
		{
			ID:   "P-001",
			Name: "Proteína Balance 500g",
			Line: "Nutrición",
		},
		{
			ID:   "P-002",
			Name: "Crema Regeneradora",
			Line: "Dermocosmética",
		},
		{
			ID:   "P-003",
			Name: "Leche Pediátrica Premium",
			Line: "Pediatría",
		},
	}
	if err := seedProductosIfMissing(db, defaultProducts); err != nil {
		log.Fatalf("Error al seed de productos: %v", err)
	}
	products, err := loadProductos(db)
	if err != nil {
		log.Fatalf("Error al cargar productos: %v", err)
	}

	usersCols, err := tableColumns(db, "users")
	if err != nil {
		log.Fatalf("Error al leer esquema de users: %v", err)
	}

	currencyOptions := []string{"COP", "USD", "EUR"}
	dateFormatOptions := []string{"2006-01-02", "02/01/2006", "01/02/2006", "02-01-2006"}
	printPaperOptions := []string{"80mm", "58mm", "57mm"}

	type ventaFormData struct {
		Title                  string
		Subtitle               string
		ProductoID             string
		ProductoNom            string
		Productos              []productOption
		StockByProd            map[string]int
		Cantidad               int
		PrecioFinal            string
		ValorVentaFinal        string
		CustomerName           string
		CustomerPhone          string
		CustomerDocumentType   string
		CustomerDocumentNumber string
		CustomerAddress        string
		CustomerCity           string
		CustomerNotes          string
		MetodoPago             string
		Notas                  string
		Errors                 map[string]string
		MetodoPagos            []string
		RoutePrefix            string
		CurrentUser            *User
	}

	type ventaConfirmData struct {
		Title              string
		Subtitle           string
		SaleID             int
		ProductoID         string
		ProductoNom        string
		Cantidad           int
		PrecioFinal        string
		ValorVentaFinal    string
		MetodoPago         string
		Notas              string
		ReceiptViewURL     string
		ReceiptDownloadURL string
		ThermalTicketURL   string
		InvoiceCreateURL   string
		CurrentUser        *User
	}

	type loginPageData struct {
		Title    string
		Error    string
		Username string
	}

	type adminUserRow struct {
		ID         int
		Username   string
		Name       string
		Email      string
		TelegramID string
		Role       string
		IsActive   bool
		CreatedAt  string
	}

	type adminUsersData struct {
		Title       string
		Subtitle    string
		Flash       string
		Error       string
		Users       []adminUserRow
		CurrentUser *User
	}

	type customersPageData struct {
		Title       string
		Subtitle    string
		Flash       string
		Error       string
		Query       string
		Items       []customerListViewItem
		CurrentUser *User
	}

	type customerDetailPageData struct {
		Title       string
		Subtitle    string
		Flash       string
		Error       string
		Customer    customerDetailViewData
		CurrentUser *User
	}

	type businessSettingsPageData struct {
		Title                string
		Subtitle             string
		Flash                string
		Error                string
		VersionLabel         string
		Settings             BusinessSettings
		Lines                []BusinessLine
		PaymentMethods       []PaymentMethod
		APIKeys              []APIKey
		NewAPIKeyName        string
		CreatedAPIToken      string
		MovementSettings     []MovementSetting
		NewPaymentMethod     string
		NewLineName          string
		Tenants              []Tenant
		CanManageTenants     bool
		NewTenantName        string
		NewTenantSlug        string
		NewTenantAdmin       string
		CreatedTenantToken   string
		CreatedTenantKeyName string
		EditingLineID        int
		EditingLineName      string
		CurrencyOptions      []string
		DateFormatOptions    []string
		PrintPaperOptions    []string
		CurrentUser          *User
	}

	type auditPageData struct {
		Title       string
		Subtitle    string
		Flash       string
		Error       string
		EventType   string
		DateFrom    string
		DateTo      string
		EventTypes  []string
		Events      []AuditEvent
		CurrentUser *User
	}

	type creditEditsReportPageData struct {
		Title        string
		Subtitle     string
		Flash        string
		Error        string
		DateFrom     string
		DateTo       string
		Username     string
		Status       string
		Kind         string
		Customer     string
		CreditSaleID string
		Items        []creditEditReportItem
		CurrentUser  *User
	}

	type productLoanReportPageData struct {
		Title         string
		Subtitle      string
		Flash         string
		Error         string
		DateFrom      string
		DateTo        string
		Status        string
		Overdue       string
		Customer      string
		Product       string
		ManagedBy     string
		ProductLoanID string
		Items         []productLoanReportItem
		CurrentUser   *User
	}

	type cashLoanReportPageData struct {
		Title        string
		Subtitle     string
		Flash        string
		Error        string
		DateFrom     string
		DateTo       string
		Username     string
		Status       string
		Customer     string
		CreditSaleID string
		Items        []cashLoanReportItem
		Summary      cashLoanReportSummary
		CurrentUser  *User
	}

	type productLoanDetailPageData struct {
		Title       string
		Subtitle    string
		Flash       string
		Error       string
		Item        productLoanReportItem
		Timeline    []productLoanTimelineItem
		CurrentUser *User
	}

	type productNewData struct {
		Title             string
		Subtitle          string
		Flash             string
		LabelPrintURL     string
		SKU               string
		Nombre            string
		Linea             string
		Location          string
		OwnerUserID       string
		PrecioVenta       string
		RetomaEnabled     bool
		RetomaPrice       string
		Lineas            []string
		HasLineas         bool
		AssignableUsers   []assignableUser
		Cantidad          int
		AplicaCad         bool
		Caducidad         string
		CreditEnabled     bool
		DebtorName        string
		InstallmentsTotal string
		TotalValue        string
		InstallmentValue  string
		Errors            map[string]string
		CurrentUser       *User
	}

	settingsForUser := func(user *User) BusinessSettings {
		if user == nil {
			return currentBusinessSettings()
		}
		settings, err := loadBusinessSettingsForTenant(db, tenantIDFromUser(user))
		if err != nil {
			log.Printf("load tenant branding tenant_id=%d: %v", tenantIDFromUser(user), err)
			return currentBusinessSettings()
		}
		return settings
	}

	mux := http.NewServeMux()

	// Serve static assets from ./static at /static/.
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		tenant := tenantFromContext(r)
		tenantSlug := ""
		tenantName := ""
		if tenant != nil {
			tenantSlug = strings.TrimSpace(tenant.Slug)
			tenantName = strings.TrimSpace(tenant.Name)
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{
			"ok":        true,
			"service":   "stocki-app",
			"auth_mode": apiAuthModeFromContext(r),
			"tenant": map[string]any{
				"id":   normalizeTenantID(tenantIDFromRequest(r)),
				"slug": tenantSlug,
				"name": tenantName,
			},
		})
	})

	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			if user, err := userFromRequest(db, r); err == nil && user != nil {
				http.Redirect(w, r, "/inventario", http.StatusSeeOther)
				return
			}
			data := loginPageData{
				Title: "Iniciar sesión",
			}
			renderTemplate(w, "login.html", data, "Error al renderizar login")
			return
		}

		if r.Method != http.MethodPost {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}

		if err := r.ParseForm(); err != nil {
			http.Error(w, "No se pudo leer el formulario", http.StatusBadRequest)
			return
		}

		username := strings.TrimSpace(r.FormValue("username"))
		password := r.FormValue("password")

		var (
			user     User
			hash     string
			isActive int
		)
		err := db.QueryRow(`
					SELECT id, username, password_hash, role, is_active, COALESCE(NULLIF(tenant_id, 0), ?)
					FROM users
					WHERE username = ?
				`, defaultTenantID, username).Scan(&user.ID, &user.Username, &hash, &user.Role, &isActive, &user.TenantID)
		if err != nil || isActive != 1 {
			if err != nil {
				log.Printf("login: lookup failed username=%q err=%v", username, err)
			} else {
				log.Printf("login: user inactive username=%q", username)
			}
			data := loginPageData{
				Title:    "Iniciar sesión",
				Error:    "Credenciales inválidas.",
				Username: username,
			}
			w.WriteHeader(http.StatusUnauthorized)
			renderTemplate(w, "login.html", data, "Error al renderizar login")
			return
		}

		if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)); err != nil {
			log.Printf("login: password mismatch username=%q", username)
			data := loginPageData{
				Title:    "Iniciar sesión",
				Error:    "Credenciales inválidas.",
				Username: username,
			}
			w.WriteHeader(http.StatusUnauthorized)
			renderTemplate(w, "login.html", data, "Error al renderizar login")
			return
		}

		tenant, err := resolveTenantByID(db, user.TenantID)
		if err != nil || tenant == nil || !tenant.Active {
			log.Printf("login: tenant inactive username=%q tenant_id=%d err=%v", username, user.TenantID, err)
			data := loginPageData{
				Title:    "Iniciar sesión",
				Error:    "La empresa asociada a este usuario está inactiva.",
				Username: username,
			}
			w.WriteHeader(http.StatusUnauthorized)
			renderTemplate(w, "login.html", data, "Error al renderizar login")
			return
		}

		token, err := generateToken()
		if err != nil {
			http.Error(w, "No se pudo generar sesión", http.StatusInternalServerError)
			return
		}
		expiresAt := time.Now().Add(24 * time.Hour)
		_, err = db.Exec(`
			INSERT INTO sessions (token, user_id, tenant_id, created_at, expires_at)
			VALUES (?, ?, ?, ?, ?)
		`, token, user.ID, normalizeTenantID(user.TenantID), time.Now().Format(time.RFC3339), expiresAt.Format(time.RFC3339))
		if err != nil {
			http.Error(w, "No se pudo guardar la sesión", http.StatusInternalServerError)
			return
		}

		setSessionCookie(w, token, expiresAt, r.TLS != nil)
		http.Redirect(w, r, "/inventario", http.StatusSeeOther)
	})

	mux.HandleFunc("/logout", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodGet {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}
		if cookie, err := r.Cookie("session_token"); err == nil {
			_, _ = db.Exec("DELETE FROM sessions WHERE token = ?", cookie.Value)
		}
		clearSessionCookie(w)
		http.Redirect(w, r, "/login", http.StatusSeeOther)
	})

	mux.HandleFunc("/clientes", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		if currentUser == nil || !isStaffRole(currentUser.Role) {
			http.Error(w, "Solo personal autorizado puede consultar clientes.", http.StatusForbidden)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}
		query := strings.TrimSpace(r.URL.Query().Get("q"))
		items, err := listCustomersForTenant(db, tenantIDFromUser(currentUser), query, 150)
		if err != nil {
			http.Error(w, "Error al cargar clientes", http.StatusInternalServerError)
			return
		}
		data := customersPageData{
			Title:       "Clientes",
			Subtitle:    "Consulta operativa de clientes, historial y reutilización desde la misma base que consume la API.",
			Flash:       strings.TrimSpace(r.URL.Query().Get("mensaje")),
			Error:       strings.TrimSpace(r.URL.Query().Get("error")),
			Query:       query,
			Items:       buildCustomerListViewItems(items),
			CurrentUser: currentUser,
		}
		renderTemplate(w, "customers.html", data, "Error al renderizar clientes")
	})

	mux.HandleFunc("/clientes/", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		if currentUser == nil || !isStaffRole(currentUser.Role) {
			http.Error(w, "Solo personal autorizado puede consultar clientes.", http.StatusForbidden)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}
		customerID, err := strconv.Atoi(strings.Trim(strings.TrimPrefix(r.URL.Path, "/clientes/"), "/"))
		if err != nil || customerID <= 0 {
			http.NotFound(w, r)
			return
		}
		detail, err := customerDetailViewForTenant(db, currentUser, customerID)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				http.Error(w, reqErr.Message, reqErr.Status)
				return
			}
			if errors.Is(err, sql.ErrNoRows) {
				http.NotFound(w, r)
				return
			}
			http.Error(w, "Error al cargar detalle del cliente", http.StatusInternalServerError)
			return
		}
		data := customerDetailPageData{
			Title:       detail.Summary.Name,
			Subtitle:    "Ficha operativa del cliente, alineada con la misma información que consumen la API y los agentes.",
			Flash:       strings.TrimSpace(r.URL.Query().Get("mensaje")),
			Error:       strings.TrimSpace(r.URL.Query().Get("error")),
			Customer:    detail,
			CurrentUser: currentUser,
		}
		renderTemplate(w, "customer_detail.html", data, "Error al renderizar cliente")
	})

	mux.HandleFunc("/admin/users", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		flash := r.URL.Query().Get("mensaje")
		errText := r.URL.Query().Get("error")
		currentUser := userFromContext(r)
		managedUsers, err := listManagedUsersForTenant(db, currentUser, tenantIDFromRequest(r), usersCols)
		if err != nil {
			http.Error(w, "Error al consultar usuarios", http.StatusInternalServerError)
			return
		}

		users := make([]adminUserRow, 0, len(managedUsers))
		for _, record := range managedUsers {
			users = append(users, adminUserRow{
				ID:         record.ID,
				Username:   record.Username,
				Name:       record.Name,
				Email:      record.Email,
				TelegramID: record.TelegramID,
				Role:       record.Role,
				IsActive:   record.IsActive,
				CreatedAt:  record.CreatedAt,
			})
		}

		data := adminUsersData{
			Title:       "Roles de usuario",
			Subtitle:    "Control de accesos y roles del inventario.",
			Flash:       flash,
			Error:       errText,
			Users:       users,
			CurrentUser: currentUser,
		}
		renderTemplate(w, "admin_users.html", data, "Error al renderizar usuarios")
	}))

	mux.HandleFunc("/auditoria", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		eventType := strings.TrimSpace(r.URL.Query().Get("event_type"))
		dateFrom := strings.TrimSpace(r.URL.Query().Get("date_from"))
		dateTo := strings.TrimSpace(r.URL.Query().Get("date_to"))

		query := `
			SELECT
				a.id,
				a.event_type,
				a.entity_type,
				a.entity_id,
				a.user_id,
				COALESCE(u.username, ''),
				a.source,
				a.payload_json,
				a.created_at
			FROM audit_events a
			LEFT JOIN users u ON u.id = a.user_id
			WHERE a.tenant_id = ?
		`
		args := make([]any, 0, 4)
		args = append(args, tenantIDFromRequest(r))
		if eventType != "" {
			query += ` AND a.event_type = ?`
			args = append(args, eventType)
		}
		if dateFrom != "" {
			query += ` AND ` + sqlDatePrefixExpr("a.created_at") + ` >= ?`
			args = append(args, dateFrom)
		}
		if dateTo != "" {
			query += ` AND ` + sqlDatePrefixExpr("a.created_at") + ` <= ?`
			args = append(args, dateTo)
		}
		query += ` ORDER BY a.created_at DESC, a.id DESC LIMIT 200`

		rows, err := db.Query(query, args...)
		if err != nil {
			http.Error(w, "Error al cargar eventos de auditoría", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		events := make([]AuditEvent, 0, 64)
		eventTypeSet := map[string]struct{}{}
		for rows.Next() {
			var item AuditEvent
			var userID sql.NullInt64
			if err := rows.Scan(&item.ID, &item.EventType, &item.EntityType, &item.EntityID, &userID, &item.Username, &item.Source, &item.PayloadJSON, &item.CreatedAt); err != nil {
				http.Error(w, "Error al leer eventos de auditoría", http.StatusInternalServerError)
				return
			}
			item.HasUserID = userID.Valid
			if userID.Valid {
				item.UserID = int(userID.Int64)
			}
			item.CreatedAt = formatDateWithSettings(item.CreatedAt)
			events = append(events, item)
			eventTypeSet[item.EventType] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			http.Error(w, "Error al procesar eventos de auditoría", http.StatusInternalServerError)
			return
		}

		eventTypes := make([]string, 0, len(eventTypeSet))
		for item := range eventTypeSet {
			eventTypes = append(eventTypes, item)
		}
		sort.Strings(eventTypes)

		data := auditPageData{
			Title:       "Auditoría",
			Subtitle:    "Trazabilidad básica de acciones relevantes del sistema.",
			Flash:       r.URL.Query().Get("mensaje"),
			Error:       r.URL.Query().Get("error"),
			EventType:   eventType,
			DateFrom:    dateFrom,
			DateTo:      dateTo,
			EventTypes:  eventTypes,
			Events:      events,
			CurrentUser: userFromContext(r),
		}
		renderTemplate(w, "audit_events.html", data, "Error al renderizar auditoría")
	}))

	mux.HandleFunc("/creditos/editados", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		creditSaleIDRaw := strings.TrimSpace(r.URL.Query().Get("credit_sale_id"))
		creditSaleID := 0
		if creditSaleIDRaw != "" {
			if parsed, err := strconv.Atoi(creditSaleIDRaw); err == nil && parsed > 0 {
				creditSaleID = parsed
			}
		}
		items, err := listEditedCreditsReport(db, currentUser, tenantIDFromRequest(r), creditEditReportFilters{
			DateFrom:     strings.TrimSpace(r.URL.Query().Get("date_from")),
			DateTo:       strings.TrimSpace(r.URL.Query().Get("date_to")),
			Username:     strings.TrimSpace(r.URL.Query().Get("username")),
			Status:       strings.TrimSpace(r.URL.Query().Get("status")),
			Kind:         strings.TrimSpace(r.URL.Query().Get("kind")),
			Customer:     strings.TrimSpace(r.URL.Query().Get("customer")),
			CreditSaleID: creditSaleID,
			Limit:        150,
		})
		if err != nil {
			http.Error(w, "Error al cargar créditos editados", http.StatusInternalServerError)
			return
		}
		data := creditEditsReportPageData{
			Title:        "Creditos editados",
			Subtitle:     "Reporte operativo de cambios sobre creditos del tenant activo.",
			Flash:        r.URL.Query().Get("mensaje"),
			Error:        r.URL.Query().Get("error"),
			DateFrom:     strings.TrimSpace(r.URL.Query().Get("date_from")),
			DateTo:       strings.TrimSpace(r.URL.Query().Get("date_to")),
			Username:     strings.TrimSpace(r.URL.Query().Get("username")),
			Status:       normalizeCreditStatusFilter(strings.TrimSpace(r.URL.Query().Get("status"))),
			Kind:         normalizeCreditKindFilter(strings.TrimSpace(r.URL.Query().Get("kind"))),
			Customer:     strings.TrimSpace(r.URL.Query().Get("customer")),
			CreditSaleID: creditSaleIDRaw,
			Items:        items,
			CurrentUser:  currentUser,
		}
		renderTemplate(w, "credit_edits_report.html", data, "Error al renderizar reporte de creditos editados")
	}))

	mux.HandleFunc("/prestamos/dinero", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		cashLoanFilters, creditSaleIDRaw := parseCashLoanReportFilters(r, 150)
		items, err := listCashLoansReport(db, currentUser, tenantIDFromRequest(r), cashLoanFilters)
		if err != nil {
			http.Error(w, "Error al cargar préstamos de dinero", http.StatusInternalServerError)
			return
		}
		summary := cashLoanReportSummary{}
		for _, item := range items {
			summary.Count++
			summary.TotalValue += item.TotalValue
			summary.TotalPaid += item.TotalPaid
			summary.CurrentDebt += item.CurrentDebt
			switch item.Status {
			case string(creditStatusCompleted):
				summary.CompletedCount++
			case string(creditStatusSuspended):
				summary.SuspendedCount++
			case string(creditStatusCancelled):
				summary.CancelledCount++
			default:
				summary.ActiveCount++
			}
		}
		data := cashLoanReportPageData{
			Title:        "Préstamos de dinero",
			Subtitle:     "Reporte operativo de préstamos cash_loan del tenant activo.",
			Flash:        r.URL.Query().Get("mensaje"),
			Error:        r.URL.Query().Get("error"),
			DateFrom:     strings.TrimSpace(r.URL.Query().Get("date_from")),
			DateTo:       strings.TrimSpace(r.URL.Query().Get("date_to")),
			Username:     strings.TrimSpace(r.URL.Query().Get("username")),
			Status:       normalizeCreditStatusFilter(strings.TrimSpace(r.URL.Query().Get("status"))),
			Customer:     strings.TrimSpace(r.URL.Query().Get("customer")),
			CreditSaleID: creditSaleIDRaw,
			Items:        items,
			Summary:      summary,
			CurrentUser:  currentUser,
		}
		renderTemplate(w, "cash_loans_report.html", data, "Error al renderizar reporte de préstamos de dinero")
	}))

	mux.HandleFunc("/prestamos/producto", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		filters, productLoanIDRaw := parseProductLoanReportFilters(r, 200)
		items, err := listProductLoansReport(db, currentUser, tenantIDFromRequest(r), filters)
		if err != nil {
			http.Error(w, "Error al cargar reporte de préstamos físicos", http.StatusInternalServerError)
			return
		}
		data := productLoanReportPageData{
			Title:         "Prestamos fisicos",
			Subtitle:      "Reporte operativo de prestamos de producto del tenant activo.",
			Flash:         r.URL.Query().Get("mensaje"),
			Error:         r.URL.Query().Get("error"),
			DateFrom:      filters.DateFrom,
			DateTo:        filters.DateTo,
			Status:        normalizeProductLoanStatusFilter(filters.Status),
			Overdue:       strings.TrimSpace(strings.ToLower(filters.Overdue)),
			Customer:      filters.Customer,
			Product:       filters.Product,
			ManagedBy:     filters.ManagedBy,
			ProductLoanID: productLoanIDRaw,
			Items:         items,
			CurrentUser:   currentUser,
		}
		renderTemplate(w, "product_loans_report.html", data, "Error al renderizar reporte de préstamos físicos")
	}))

	mux.HandleFunc("/prestamos/producto/export.csv", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		filters, _ := parseProductLoanReportFilters(r, 1000)
		items, err := listProductLoansReport(db, currentUser, tenantIDFromRequest(r), filters)
		if err != nil {
			http.Error(w, "Error al exportar préstamos físicos", http.StatusInternalServerError)
			return
		}

		filename := "prestamos_fisicos.csv"
		if filters.DateFrom != "" || filters.DateTo != "" {
			from := filters.DateFrom
			if from == "" {
				from = "inicio"
			}
			to := filters.DateTo
			if to == "" {
				to = "hoy"
			}
			filename = fmt.Sprintf("prestamos_fisicos_%s_a_%s.csv", from, to)
		}

		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
		cw := csv.NewWriter(w)
		defer cw.Flush()

		_ = cw.Write([]string{
			"prestamo_id",
			"cliente",
			"documento_tipo",
			"documento_numero",
			"telefono",
			"ciudad",
			"producto_id",
			"producto",
			"cantidad",
			"gestionado_por",
			"fecha_salida",
			"retorno_esperado",
			"estado",
			"vencido",
			"unidades",
			"notas",
			"notas_cierre",
			"fecha_cierre",
		})

		for _, item := range items {
			_ = cw.Write([]string{
				strconv.Itoa(item.ProductLoanID),
				item.CustomerName,
				item.CustomerDocumentType,
				item.CustomerDocument,
				item.CustomerPhone,
				item.CustomerCity,
				item.ProductID,
				item.ProductName,
				strconv.Itoa(item.Quantity),
				item.ManagedByName,
				item.LoanedAt,
				item.DueAt,
				item.StatusLabel,
				map[bool]string{true: "si", false: "no"}[item.IsOverdue],
				item.UnitIDsText,
				item.Notes,
				item.CloseNotes,
				item.ClosedAt,
			})
		}
	}))

	mux.HandleFunc("/prestamos/producto/", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		idRaw := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/prestamos/producto/"))
		productLoanID, err := strconv.Atoi(idRaw)
		if err != nil || productLoanID <= 0 {
			http.NotFound(w, r)
			return
		}
		item, timeline, err := productLoanDetailForUser(db, currentUser, tenantIDFromRequest(r), productLoanID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				http.NotFound(w, r)
				return
			}
			http.Error(w, "Error al cargar detalle del préstamo", http.StatusInternalServerError)
			return
		}
		data := productLoanDetailPageData{
			Title:       fmt.Sprintf("Prestamo %d", productLoanID),
			Subtitle:    "Detalle operativo del préstamo físico y su trazabilidad.",
			Flash:       r.URL.Query().Get("mensaje"),
			Error:       r.URL.Query().Get("error"),
			Item:        item,
			Timeline:    timeline,
			CurrentUser: currentUser,
		}
		renderTemplate(w, "product_loan_detail.html", data, "Error al renderizar detalle del préstamo")
	}))

	renderBusinessSettingsPage := func(w http.ResponseWriter, r *http.Request, flash, errText, createdToken, newAPIKeyName, createdTenantToken, createdTenantKeyName string) {
		tenantID := tenantIDFromRequest(r)
		currentUser := userFromContext(r)
		settings := settingsForUser(currentUser)
		lines, err := loadBusinessLinesForTenant(db, tenantID, false)
		if err != nil {
			http.Error(w, "Error al cargar líneas de negocio", http.StatusInternalServerError)
			return
		}
		paymentMethodsCfg, err := loadPaymentMethodsForTenant(db, tenantID, false)
		if err != nil {
			http.Error(w, "Error al cargar canales de pago", http.StatusInternalServerError)
			return
		}
		apiKeys, err := loadAPIKeysForTenant(db, tenantID)
		if err != nil {
			http.Error(w, "Error al cargar API keys", http.StatusInternalServerError)
			return
		}
		movementSettings, _, err := loadMovementSettingsForTenant(db, tenantID)
		if err != nil {
			http.Error(w, "Error al cargar tipos de movimiento", http.StatusInternalServerError)
			return
		}
		tenants := []Tenant(nil)
		canManageTenants := canManageTenants(currentUser)
		if canManageTenants {
			tenants, err = listTenants(db)
			if err != nil {
				http.Error(w, "Error al cargar empresas", http.StatusInternalServerError)
				return
			}
		}
		editingID, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("edit_line")))
		editingName := ""
		for _, line := range lines {
			if line.ID == editingID {
				editingName = line.Name
				break
			}
		}
		data := businessSettingsPageData{
			Title:                "Configuración",
			Subtitle:             "Separa branding general del negocio y catálogos operativos desde un único panel.",
			Flash:                flash,
			Error:                errText,
			VersionLabel:         "Versión 0.9.7 25032026",
			Settings:             settings,
			Lines:                lines,
			PaymentMethods:       paymentMethodsCfg,
			APIKeys:              apiKeys,
			NewAPIKeyName:        strings.TrimSpace(newAPIKeyName),
			CreatedAPIToken:      strings.TrimSpace(createdToken),
			MovementSettings:     movementSettings,
			NewPaymentMethod:     strings.TrimSpace(r.URL.Query().Get("new_payment_method")),
			NewLineName:          strings.TrimSpace(r.URL.Query().Get("new_line")),
			Tenants:              tenants,
			CanManageTenants:     canManageTenants,
			NewTenantName:        strings.TrimSpace(r.URL.Query().Get("new_tenant_name")),
			NewTenantSlug:        strings.TrimSpace(r.URL.Query().Get("new_tenant_slug")),
			NewTenantAdmin:       strings.TrimSpace(r.URL.Query().Get("new_tenant_admin")),
			CreatedTenantToken:   strings.TrimSpace(createdTenantToken),
			CreatedTenantKeyName: strings.TrimSpace(createdTenantKeyName),
			EditingLineID:        editingID,
			EditingLineName:      editingName,
			CurrencyOptions:      currencyOptions,
			DateFormatOptions:    dateFormatOptions,
			PrintPaperOptions:    printPaperOptions,
			CurrentUser:          currentUser,
		}
		renderTemplate(w, "business_settings.html", data, "Error al renderizar configuración")
	}

	mux.HandleFunc("/configuracion", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			renderBusinessSettingsPage(w, r, r.URL.Query().Get("mensaje"), r.URL.Query().Get("error"), "", strings.TrimSpace(r.URL.Query().Get("new_api_key_name")), "", "")
			return
		}

		if r.Method != http.MethodPost {
			redirectWithMessage(w, r, "/configuracion", "", "Método no permitido.")
			return
		}

		if err := r.ParseMultipartForm(8 << 20); err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo leer el formulario.")
			return
		}

		tenantID := tenantIDFromRequest(r)
		settings, err := loadBusinessSettingsForTenant(db, tenantID)
		if err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo cargar la configuración del tenant.")
			return
		}
		settings.BusinessName = strings.TrimSpace(r.FormValue("business_name"))
		settings.PrimaryColor = normalizeHexColor(r.FormValue("primary_color"), settings.PrimaryColor)
		settings.Currency = normalizeCurrency(r.FormValue("currency"))
		settings.DateFormat = normalizeDateFormat(r.FormValue("date_format"))
		settings.LabelPaperWidth = normalizePaperWidth(r.FormValue("label_paper_width"), settings.LabelPaperWidth)
		settings.InvoicePaperWidth = normalizePaperWidth(r.FormValue("invoice_paper_width"), settings.InvoicePaperWidth)
		settings.TicketPaperWidth = normalizePaperWidth(r.FormValue("ticket_paper_width"), settings.TicketPaperWidth)

		if settings.BusinessName == "" {
			redirectWithMessage(w, r, "/configuracion", "", "El nombre del negocio es obligatorio.")
			return
		}

		file, header, err := r.FormFile("logo")
		if err != nil && err != http.ErrMissingFile {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo leer el logo.")
			return
		}
		if err == nil {
			defer file.Close()
			logoPath, saveErr := saveBusinessLogo(file, header.Filename)
			if saveErr != nil {
				redirectWithMessage(w, r, "/configuracion", "", "No se pudo guardar el logo. Usa PNG, JPG, WEBP o SVG.")
				return
			}
			settings.LogoPath = logoPath
		}

		savedSettings, err := saveBusinessSettingsForTenant(db, tenantID, settings)
		if err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo guardar la configuración.")
			return
		}
		if tenantID == defaultTenantID {
			setCurrentBusinessSettings(savedSettings)
		}
		if err := logAuditEvent(db, userFromContext(r), "business_settings_updated", "business_settings", strconv.Itoa(savedSettings.ID), "manual", map[string]any{
			"business_name":       savedSettings.BusinessName,
			"logo_path":           savedSettings.LogoPath,
			"primary_color":       savedSettings.PrimaryColor,
			"currency":            savedSettings.Currency,
			"date_format":         savedSettings.DateFormat,
			"label_paper_width":   savedSettings.LabelPaperWidth,
			"invoice_paper_width": savedSettings.InvoicePaperWidth,
			"ticket_paper_width":  savedSettings.TicketPaperWidth,
		}); err != nil {
			log.Printf("audit business settings: %v", err)
		}

		redirectWithMessage(w, r, "/configuracion", "Configuración actualizada.", "")
	}))

	mux.HandleFunc("/configuracion/tenants/create", platformAdminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			redirectWithMessage(w, r, "/configuracion", "", "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo leer el formulario.")
			return
		}
		currentUser := userFromContext(r)
		if !canManageTenants(currentUser) {
			redirectWithMessage(w, r, "/configuracion", "", "Solo un platform admin puede crear nuevas empresas en esta fase.")
			return
		}

		name := strings.TrimSpace(r.FormValue("name"))
		slug := strings.TrimSpace(r.FormValue("slug"))
		adminUsername := strings.TrimSpace(r.FormValue("admin_username"))
		adminPassword := r.FormValue("admin_password")
		adminPasswordConfirm := r.FormValue("admin_password_confirm")
		if adminUsername == "" {
			target := "/configuracion?new_tenant_name=" + url.QueryEscape(name) + "&new_tenant_slug=" + url.QueryEscape(slug) + "&new_tenant_admin=" + url.QueryEscape(adminUsername) + "&error=" + url.QueryEscape("El usuario admin inicial es obligatorio.")
			http.Redirect(w, r, target, http.StatusSeeOther)
			return
		}
		if adminPassword == "" {
			target := "/configuracion?new_tenant_name=" + url.QueryEscape(name) + "&new_tenant_slug=" + url.QueryEscape(slug) + "&new_tenant_admin=" + url.QueryEscape(adminUsername) + "&error=" + url.QueryEscape("La contraseña inicial del admin es obligatoria.")
			http.Redirect(w, r, target, http.StatusSeeOther)
			return
		}
		if adminPassword != adminPasswordConfirm {
			target := "/configuracion?new_tenant_name=" + url.QueryEscape(name) + "&new_tenant_slug=" + url.QueryEscape(slug) + "&new_tenant_admin=" + url.QueryEscape(adminUsername) + "&error=" + url.QueryEscape("La confirmación de la contraseña no coincide.")
			http.Redirect(w, r, target, http.StatusSeeOther)
			return
		}

		provisioned, err := createTenantWithSeed(db, currentUser, usersCols, name, slug, adminUsername, adminPassword)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				target := "/configuracion?new_tenant_name=" + url.QueryEscape(name) + "&new_tenant_slug=" + url.QueryEscape(slug) + "&new_tenant_admin=" + url.QueryEscape(adminUsername) + "&error=" + url.QueryEscape(reqErr.Message)
				http.Redirect(w, r, target, http.StatusSeeOther)
				return
			}
			log.Printf("create tenant: %v", err)
			target := "/configuracion?new_tenant_name=" + url.QueryEscape(name) + "&new_tenant_slug=" + url.QueryEscape(slug) + "&new_tenant_admin=" + url.QueryEscape(adminUsername) + "&error=" + url.QueryEscape("No se pudo crear la empresa.")
			http.Redirect(w, r, target, http.StatusSeeOther)
			return
		}

		renderBusinessSettingsPage(
			w,
			r,
			fmt.Sprintf("Empresa %s creada correctamente. Guarda la API key inicial ahora; no volverá a mostrarse.", provisioned.Tenant.Name),
			"",
			"",
			"",
			provisioned.InitialAPIToken,
			provisioned.InitialAPIKeyName,
		)
	}))

	mux.HandleFunc("/configuracion/tenants/update", platformAdminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			redirectWithMessage(w, r, "/configuracion", "", "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo leer el formulario.")
			return
		}

		currentUser := userFromContext(r)
		if !canManageTenants(currentUser) {
			redirectWithMessage(w, r, "/configuracion", "", "Solo un platform admin puede editar empresas en esta fase.")
			return
		}

		tenantID, err := strconv.Atoi(strings.TrimSpace(r.FormValue("tenant_id")))
		if err != nil || tenantID <= 0 {
			redirectWithMessage(w, r, "/configuracion", "", "Empresa inválida.")
			return
		}
		updatedTenant, err := updateTenantBasics(
			db,
			currentUser,
			tenantID,
			r.FormValue("name"),
			r.FormValue("slug"),
		)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				redirectWithMessage(w, r, "/configuracion", "", reqErr.Message)
				return
			}
			log.Printf("update tenant: %v", err)
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo actualizar la empresa.")
			return
		}

		redirectWithMessage(w, r, "/configuracion", fmt.Sprintf("Empresa %s actualizada correctamente.", updatedTenant.Name), "")
	}))

	mux.HandleFunc("/configuracion/tenants/toggle", platformAdminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			redirectWithMessage(w, r, "/configuracion", "", "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo leer el formulario.")
			return
		}

		currentUser := userFromContext(r)
		if !canManageTenants(currentUser) {
			redirectWithMessage(w, r, "/configuracion", "", "Solo un platform admin puede administrar empresas en esta fase.")
			return
		}

		tenantID, err := strconv.Atoi(strings.TrimSpace(r.FormValue("tenant_id")))
		if err != nil || tenantID <= 0 {
			redirectWithMessage(w, r, "/configuracion", "", "Empresa inválida.")
			return
		}
		nextState := strings.TrimSpace(r.FormValue("active")) == "1"
		updatedTenant, err := setTenantActiveState(db, currentUser, tenantID, nextState)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				redirectWithMessage(w, r, "/configuracion", "", reqErr.Message)
				return
			}
			log.Printf("toggle tenant: %v", err)
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo actualizar el estado de la empresa.")
			return
		}

		flashMessage := fmt.Sprintf("Empresa %s activada.", updatedTenant.Name)
		if !updatedTenant.Active {
			flashMessage = fmt.Sprintf("Empresa %s inactivada.", updatedTenant.Name)
		}
		redirectWithMessage(w, r, "/configuracion", flashMessage, "")
	}))

	mux.HandleFunc("/configuracion/tenants/api-key/rotate", platformAdminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			redirectWithMessage(w, r, "/configuracion", "", "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo leer el formulario.")
			return
		}

		currentUser := userFromContext(r)
		if !canManageTenants(currentUser) {
			redirectWithMessage(w, r, "/configuracion", "", "Solo un platform admin puede regenerar la API key inicial.")
			return
		}

		tenantID, err := strconv.Atoi(strings.TrimSpace(r.FormValue("tenant_id")))
		if err != nil || tenantID <= 0 {
			redirectWithMessage(w, r, "/configuracion", "", "Empresa inválida.")
			return
		}

		tenant, err := resolveTenantByID(db, tenantID)
		if err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo cargar la empresa.")
			return
		}

		keyName, token, err := rotateTenantInitialAPIKey(db, currentUser, tenantID)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				redirectWithMessage(w, r, "/configuracion", "", reqErr.Message)
				return
			}
			log.Printf("rotate tenant initial api key: %v", err)
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo regenerar la API key inicial.")
			return
		}

		renderBusinessSettingsPage(
			w,
			r,
			fmt.Sprintf("API key inicial regenerada para %s. Copia el token ahora; no volverá a mostrarse.", tenant.Name),
			"",
			"",
			"",
			token,
			keyName,
		)
	}))

	mux.HandleFunc("/configuracion/lineas/create", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			redirectWithMessage(w, r, "/configuracion", "", "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo leer el formulario.")
			return
		}
		name := strings.TrimSpace(r.FormValue("name"))
		if name == "" {
			redirectWithMessage(w, r, "/configuracion", "", "El nombre de la línea es obligatorio.")
			return
		}
		tenantID := tenantIDFromUser(userFromContext(r))
		now := time.Now().Format(time.RFC3339)
		if _, err := db.Exec(`
			INSERT INTO business_lines (tenant_id, name, active, created_at, updated_at)
			VALUES (?, ?, 1, ?, ?)
		`, tenantID, name, now, now); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "unique") {
				redirectWithMessage(w, r, "/configuracion", "", "Ya existe una línea con ese nombre.")
				return
			}
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo crear la línea.")
			return
		}
		if err := logAuditEvent(db, userFromContext(r), "business_line_created", "business_line", name, "manual", map[string]any{
			"name":   name,
			"active": true,
		}); err != nil {
			log.Printf("audit business line create: %v", err)
		}
		redirectWithMessage(w, r, "/configuracion", "Línea creada.", "")
	}))

	mux.HandleFunc("/configuracion/lineas/update", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			redirectWithMessage(w, r, "/configuracion", "", "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo leer el formulario.")
			return
		}
		lineID, err := strconv.Atoi(strings.TrimSpace(r.FormValue("id")))
		if err != nil || lineID <= 0 {
			redirectWithMessage(w, r, "/configuracion", "", "ID de línea inválido.")
			return
		}
		name := strings.TrimSpace(r.FormValue("name"))
		if name == "" {
			redirectWithMessage(w, r, "/configuracion", "", "El nombre de la línea es obligatorio.")
			return
		}
		active := 0
		if r.FormValue("active") != "" {
			active = 1
		}
		tenantID := tenantIDFromUser(userFromContext(r))
		if _, err := db.Exec(`
			UPDATE business_lines
			SET name = ?, active = ?, updated_at = ?
			WHERE id = ? AND tenant_id = ?
		`, name, active, time.Now().Format(time.RFC3339), lineID, tenantID); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "unique") {
				redirectWithMessage(w, r, "/configuracion", "", "Ya existe una línea con ese nombre.")
				return
			}
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo actualizar la línea.")
			return
		}
		if err := logAuditEvent(db, userFromContext(r), "business_line_updated", "business_line", strconv.Itoa(lineID), "manual", map[string]any{
			"name":   name,
			"active": active == 1,
		}); err != nil {
			log.Printf("audit business line update: %v", err)
		}
		redirectWithMessage(w, r, "/configuracion", "Línea actualizada.", "")
	}))

	mux.HandleFunc("/configuracion/pagos/create", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			redirectWithMessage(w, r, "/configuracion", "", "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo leer el formulario.")
			return
		}
		name := strings.TrimSpace(r.FormValue("name"))
		if name == "" {
			redirectWithMessage(w, r, "/configuracion", "", "El nombre del canal de pago es obligatorio.")
			return
		}
		var nextOrder int
		tenantID := tenantIDFromUser(userFromContext(r))
		if err := db.QueryRow(`SELECT COALESCE(MAX(sort_order), 0) + 1 FROM payment_methods WHERE tenant_id = ?`, tenantID).Scan(&nextOrder); err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo calcular el orden del canal.")
			return
		}
		now := time.Now().Format(time.RFC3339)
		if _, err := db.Exec(`
			INSERT INTO payment_methods (tenant_id, name, active, sort_order, created_at, updated_at)
			VALUES (?, ?, 1, ?, ?, ?)
		`, tenantID, name, nextOrder, now, now); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "unique") {
				redirectWithMessage(w, r, "/configuracion", "", "Ya existe un canal de pago con ese nombre.")
				return
			}
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo crear el canal de pago.")
			return
		}
		if err := logAuditEvent(db, userFromContext(r), "payment_method_created", "payment_method", name, "manual", map[string]any{
			"name":       name,
			"active":     true,
			"sort_order": nextOrder,
		}); err != nil {
			log.Printf("audit payment method create: %v", err)
		}
		redirectWithMessage(w, r, "/configuracion", "Canal de pago creado.", "")
	}))

	mux.HandleFunc("/configuracion/pagos/update", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			redirectWithMessage(w, r, "/configuracion", "", "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo leer el formulario.")
			return
		}
		methodID, err := strconv.Atoi(strings.TrimSpace(r.FormValue("id")))
		if err != nil || methodID <= 0 {
			redirectWithMessage(w, r, "/configuracion", "", "ID de canal inválido.")
			return
		}
		name := strings.TrimSpace(r.FormValue("name"))
		if name == "" {
			redirectWithMessage(w, r, "/configuracion", "", "El nombre del canal de pago es obligatorio.")
			return
		}
		sortOrder, err := strconv.Atoi(strings.TrimSpace(r.FormValue("sort_order")))
		if err != nil || sortOrder <= 0 {
			redirectWithMessage(w, r, "/configuracion", "", "El orden debe ser mayor a 0.")
			return
		}
		active := 0
		if r.FormValue("active") != "" {
			active = 1
		}
		tenantID := tenantIDFromUser(userFromContext(r))
		if _, err := db.Exec(`
			UPDATE payment_methods
			SET name = ?, active = ?, sort_order = ?, updated_at = ?
			WHERE id = ? AND tenant_id = ?
		`, name, active, sortOrder, time.Now().Format(time.RFC3339), methodID, tenantID); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "unique") {
				redirectWithMessage(w, r, "/configuracion", "", "Ya existe un canal de pago con ese nombre.")
				return
			}
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo actualizar el canal de pago.")
			return
		}
		if err := logAuditEvent(db, userFromContext(r), "payment_method_updated", "payment_method", strconv.Itoa(methodID), "manual", map[string]any{
			"name":       name,
			"active":     active == 1,
			"sort_order": sortOrder,
		}); err != nil {
			log.Printf("audit payment method update: %v", err)
		}
		redirectWithMessage(w, r, "/configuracion", "Canal de pago actualizado.", "")
	}))

	mux.HandleFunc("/configuracion/api-keys/create", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			redirectWithMessage(w, r, "/configuracion", "", "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo leer el formulario.")
			return
		}
		name := strings.TrimSpace(r.FormValue("name"))
		if name == "" {
			redirectWithMessage(w, r, "/configuracion", "", "El nombre de la API key es obligatorio.")
			return
		}
		if isReservedInitialAPIKeyName(name) {
			redirectWithMessage(w, r, "/configuracion", "", "Los nombres terminados en -inicial están reservados para la API key inicial del tenant.")
			return
		}
		token, err := generateToken()
		if err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo generar el token.")
			return
		}
		now := time.Now().Format(time.RFC3339)
		tenantID := defaultTenantID
		if user := userFromContext(r); user != nil {
			tenantID = normalizeTenantID(user.TenantID)
		}
		if _, err := db.Exec(`
			INSERT INTO api_keys (name, token_hash, tenant_id, active, created_at, updated_at)
			VALUES (?, ?, ?, 1, ?, ?)
		`, name, hashAPIToken(token), tenantID, now, now); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "unique") {
				redirectWithMessage(w, r, "/configuracion", "", "Ya existe una API key con ese nombre.")
				return
			}
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo crear la API key.")
			return
		}
		if err := logAuditEvent(db, userFromContext(r), "api_key_created", "api_key", name, "manual", map[string]any{
			"name": name,
		}); err != nil {
			log.Printf("audit api key created: %v", err)
		}
		renderBusinessSettingsPage(w, r, "API key creada. Copia el token ahora; no volverá a mostrarse.", "", token, "", "", "")
	}))

	mux.HandleFunc("/configuracion/api-keys/update", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			redirectWithMessage(w, r, "/configuracion", "", "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo leer el formulario.")
			return
		}
		keyID, err := strconv.Atoi(strings.TrimSpace(r.FormValue("id")))
		if err != nil || keyID <= 0 {
			redirectWithMessage(w, r, "/configuracion", "", "ID de API key inválido.")
			return
		}
		name := strings.TrimSpace(r.FormValue("name"))
		if name == "" {
			redirectWithMessage(w, r, "/configuracion", "", "El nombre de la API key es obligatorio.")
			return
		}
		active := r.FormValue("active") != ""
		if err := updateTenantAPIKey(db, userFromContext(r), keyID, name, active); err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				redirectWithMessage(w, r, "/configuracion", "", reqErr.Message)
				return
			}
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo actualizar la API key.")
			return
		}
		if err := logAuditEvent(db, userFromContext(r), "api_key_updated", "api_key", strconv.Itoa(keyID), "manual", map[string]any{
			"name":   name,
			"active": active,
		}); err != nil {
			log.Printf("audit api key updated: %v", err)
		}
		redirectWithMessage(w, r, "/configuracion", "API key actualizada.", "")
	}))

	mux.HandleFunc("/configuracion/movimientos/update", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			redirectWithMessage(w, r, "/configuracion", "", "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo leer el formulario.")
			return
		}
		movementType := strings.TrimSpace(strings.ToLower(r.FormValue("movement_type")))
		allowed := false
		for _, item := range defaultMovementTypes() {
			if movementType == item {
				allowed = true
				break
			}
		}
		if !allowed {
			redirectWithMessage(w, r, "/configuracion", "", "Tipo de movimiento inválido.")
			return
		}
		enabled := 0
		if r.FormValue("enabled") != "" {
			enabled = 1
		}
		tenantID := tenantIDFromUser(userFromContext(r))
		if _, err := db.Exec(`
			UPDATE movement_settings
			SET enabled = ?, updated_at = ?
			WHERE movement_type = ? AND tenant_id = ?
		`, enabled, time.Now().Format(time.RFC3339), movementType, tenantID); err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo actualizar el tipo de movimiento.")
			return
		}
		redirectWithMessage(w, r, "/configuracion", "Tipos de movimiento actualizados.", "")
	}))

	mux.HandleFunc("/admin/users/create", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			redirectWithMessage(w, r, "/admin/users", "", "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			redirectWithMessage(w, r, "/admin/users", "", "No se pudo leer el formulario.")
			return
		}

		username := strings.TrimSpace(r.FormValue("username"))
		name := strings.TrimSpace(r.FormValue("name"))
		email := strings.TrimSpace(r.FormValue("email"))
		telegramID := strings.TrimSpace(r.FormValue("telegram_id"))
		password := r.FormValue("password")
		currentUser := userFromContext(r)

		_, err := createManagedUser(db, currentUser, tenantIDFromRequest(r), usersCols, managedUserInput{
			Username:   username,
			Name:       name,
			Email:      email,
			Password:   password,
			Role:       strings.TrimSpace(r.FormValue("role")),
			IsActive:   r.FormValue("is_active") != "",
			TelegramID: telegramID,
		}, "manual", nil)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				redirectWithMessage(w, r, "/admin/users", "", reqErr.Message)
				return
			}
			log.Printf("admin/users/create: helper failed username=%q err=%v", username, err)
			redirectWithMessage(w, r, "/admin/users", "", userCreateErrorText(err))
			return
		}

		redirectWithMessage(w, r, "/admin/users", "Usuario creado.", "")
	}))

	mux.HandleFunc("/admin/users/update", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			redirectWithMessage(w, r, "/admin/users", "", "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			redirectWithMessage(w, r, "/admin/users", "", "No se pudo leer el formulario.")
			return
		}

		idValue := strings.TrimSpace(r.FormValue("id"))
		userID, err := strconv.Atoi(idValue)
		if err != nil || userID <= 0 {
			redirectWithMessage(w, r, "/admin/users", "", "ID inválido.")
			return
		}

		username := strings.TrimSpace(r.FormValue("username"))
		name := strings.TrimSpace(r.FormValue("name"))
		email := strings.TrimSpace(r.FormValue("email"))
		telegramID := strings.TrimSpace(r.FormValue("telegram_id"))
		currentUser := userFromContext(r)

		_, err = updateManagedUser(db, currentUser, tenantIDFromRequest(r), userID, usersCols, managedUserInput{
			Username:   username,
			Name:       name,
			Email:      email,
			Role:       strings.TrimSpace(r.FormValue("role")),
			IsActive:   r.FormValue("is_active") != "",
			TelegramID: telegramID,
		}, "manual", nil)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				redirectWithMessage(w, r, "/admin/users", "", reqErr.Message)
				return
			}
			log.Printf("admin/users/update: helper failed user_id=%d err=%v", userID, err)
			redirectWithMessage(w, r, "/admin/users", "", "No se pudo actualizar el usuario.")
			return
		}

		redirectWithMessage(w, r, "/admin/users", "Usuario actualizado.", "")
	}))

	mux.HandleFunc("/admin/users/password", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			redirectWithMessage(w, r, "/admin/users", "", "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			redirectWithMessage(w, r, "/admin/users", "", "No se pudo leer el formulario.")
			return
		}

		idValue := strings.TrimSpace(r.FormValue("id"))
		userID, err := strconv.Atoi(idValue)
		if err != nil || userID <= 0 {
			redirectWithMessage(w, r, "/admin/users", "", "ID inválido.")
			return
		}
		currentUser := userFromContext(r)
		targetRecord, err := managedUserByIDForTenant(db, currentUser, tenantIDFromRequest(r), userID, usersCols)
		if err != nil {
			redirectWithMessage(w, r, "/admin/users", "", "Usuario no encontrado.")
			return
		}
		if !canManagePlatformUser(currentUser, targetRecord.Role) {
			redirectWithMessage(w, r, "/admin/users", "", "Solo un platform admin puede cambiar la contraseña de ese usuario.")
			return
		}
		password := r.FormValue("password")
		if password == "" {
			redirectWithMessage(w, r, "/admin/users", "", "Contraseña obligatoria.")
			return
		}

		hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if err != nil {
			redirectWithMessage(w, r, "/admin/users", "", "No se pudo procesar la contraseña.")
			return
		}

		setCols := []string{"password_hash = ?"}
		args := []any{string(hashed)}
		if usersCols["password_salt"] {
			setCols = append(setCols, "password_salt = ?")
			args = append(args, "bcrypt")
		}
		args = append(args, userID)
		args = append(args, targetRecord.TenantID)
		if _, err := db.Exec(fmt.Sprintf("UPDATE users SET %s WHERE id = ? AND COALESCE(NULLIF(tenant_id, 0), 1) = ?", strings.Join(setCols, ", ")), args...); err != nil {
			redirectWithMessage(w, r, "/admin/users", "", "No se pudo actualizar la contraseña.")
			return
		}
		_, _ = db.Exec(`DELETE FROM sessions WHERE user_id = ?`, userID)
		redirectWithMessage(w, r, "/admin/users", "Contraseña actualizada (sesiones cerradas).", "")
	}))

	mux.HandleFunc("/admin/users/delete", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			redirectWithMessage(w, r, "/admin/users", "", "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			redirectWithMessage(w, r, "/admin/users", "", "No se pudo leer el formulario.")
			return
		}

		idValue := strings.TrimSpace(r.FormValue("id"))
		userID, err := strconv.Atoi(idValue)
		if err != nil || userID <= 0 {
			redirectWithMessage(w, r, "/admin/users", "", "ID inválido.")
			return
		}
		current := userFromContext(r)
		if current != nil && current.ID == userID {
			redirectWithMessage(w, r, "/admin/users", "", "No puedes eliminar tu propio usuario.")
			return
		}

		targetRecord, err := managedUserByIDForTenant(db, current, tenantIDFromRequest(r), userID, usersCols)
		if err != nil {
			redirectWithMessage(w, r, "/admin/users", "", "Usuario no encontrado.")
			return
		}
		if targetRecord.Role == rolePlatformAdmin && !isPlatformAdmin(current) {
			redirectWithMessage(w, r, "/admin/users", "", "Solo un platform admin puede eliminar ese usuario.")
			return
		}
		if isAdminRole(targetRecord.Role) && targetRecord.IsActive {
			if err := ensureTenantRetainsActiveAdmin(db, targetRecord.TenantID, targetRecord.ID); err != nil {
				var reqErr requestError
				if errors.As(err, &reqErr) {
					redirectWithMessage(w, r, "/admin/users", "", "No puedes eliminar el último admin activo.")
					return
				}
				redirectWithMessage(w, r, "/admin/users", "", "No se pudo validar la eliminación del usuario.")
				return
			}
		}

		_, _ = db.Exec(`DELETE FROM sessions WHERE user_id = ?`, userID)
		if _, err := db.Exec(`DELETE FROM users WHERE id = ? AND COALESCE(NULLIF(tenant_id, 0), 1) = ?`, userID, targetRecord.TenantID); err != nil {
			redirectWithMessage(w, r, "/admin/users", "", "No se pudo eliminar el usuario.")
			return
		}

		redirectWithMessage(w, r, "/admin/users", "Usuario eliminado.", "")
	}))

	mux.HandleFunc("/productos/new", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		activeLines, err := loadBusinessLinesForTenant(db, tenantIDFromRequest(r), true)
		if err != nil {
			http.Error(w, "No se pudieron cargar las líneas de negocio", http.StatusInternalServerError)
			return
		}
		nextSKU, err := generateNextProductSKU(db)
		if err != nil {
			http.Error(w, "No se pudo generar el ID", http.StatusInternalServerError)
			return
		}
		assignableUsers, err := loadAssignableUsers(db)
		if err != nil {
			http.Error(w, "No se pudieron cargar los usuarios", http.StatusInternalServerError)
			return
		}
		data := productNewData{
			Title:           "Crear producto",
			Subtitle:        "Acción reservada para administradores.",
			Flash:           strings.TrimSpace(r.URL.Query().Get("mensaje")),
			LabelPrintURL:   strings.TrimSpace(r.URL.Query().Get("label_url")),
			SKU:             nextSKU,
			Cantidad:        1,
			Location:        "",
			Lineas:          businessLineNames(activeLines),
			HasLineas:       len(activeLines) > 0,
			AssignableUsers: assignableUsers,
			CurrentUser:     userFromContext(r),
		}
		renderTemplate(w, "product_new.html", data, "Error al renderizar productos")
	}))

	mux.HandleFunc("/productos/etiquetas", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		if currentUser == nil || !isStaffRole(currentUser.Role) {
			http.Error(w, "No autorizado", http.StatusForbidden)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}

		productIDs := make([]string, 0)
		for _, productID := range r.URL.Query()["id"] {
			if trimmed := strings.TrimSpace(productID); trimmed != "" {
				productIDs = append(productIDs, trimmed)
			}
		}
		if idsRaw := strings.TrimSpace(r.URL.Query().Get("ids")); idsRaw != "" {
			for _, piece := range strings.Split(idsRaw, ",") {
				if trimmed := strings.TrimSpace(piece); trimmed != "" {
					productIDs = append(productIDs, trimmed)
				}
			}
		}
		sizeParam := r.URL.Query().Get("size")
		if strings.TrimSpace(sizeParam) == "" {
			sizeParam = r.URL.Query().Get("paper")
		}
		if strings.TrimSpace(sizeParam) == "" {
			sizeParam = settingsForUser(currentUser).LabelPaperWidth
		}
		items, widthMM, heightMM, err := productLabelItemsForUser(db, currentUser, productIDs, sizeParam)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				http.Error(w, reqErr.Message, reqErr.Status)
				return
			}
			http.Error(w, "No se pudieron generar las etiquetas.", http.StatusInternalServerError)
			return
		}
		size, _, _ := labelSizeDimensions(sizeParam)
		_, _, dpi, paperClass := thermalPaperDimensions(sizeParam)
		data := productLabelsPageData{
			Title:       "Etiquetas de producto",
			Subtitle:    "Etiquetas térmicas ajustadas para impresoras de 80, 58 y 57 mm.",
			Size:        size,
			WidthMM:     widthMM,
			HeightMM:    heightMM,
			PaperDPI:    dpi,
			PaperClass:  paperClass,
			Items:       items,
			CurrentUser: currentUser,
			Settings:    settingsForUser(currentUser),
		}
		_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantIDFromUser(currentUser))
		if err != nil {
			http.Error(w, "No se pudieron cargar los movimientos disponibles.", http.StatusInternalServerError)
			return
		}
		data.CanLoan = movementEnabled(movementEnabledMap, "prestamo")
		data.CanCredit = movementEnabled(movementEnabledMap, "credito")
		var rendered bytes.Buffer
		if err := tmpl.ExecuteTemplate(&rendered, "product_labels.html", data); err != nil {
			http.Error(w, "Error al renderizar etiquetas", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(rendered.Bytes())
	})

	mux.HandleFunc("/productos", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Redirect(w, r, "/productos/new", http.StatusSeeOther)
			return
		}

		activeLines, err := loadBusinessLinesForTenant(db, tenantIDFromRequest(r), true)
		if err != nil {
			http.Error(w, "No se pudieron cargar las líneas de negocio", http.StatusInternalServerError)
			return
		}

		if err := r.ParseForm(); err != nil {
			http.Error(w, "No se pudo leer el formulario", http.StatusBadRequest)
			return
		}

		nombre := strings.TrimSpace(r.FormValue("nombre"))
		customSKU := strings.TrimSpace(r.FormValue("id"))
		if customSKU == "" {
			customSKU = strings.TrimSpace(r.FormValue("sku"))
		}
		linea := strings.TrimSpace(r.FormValue("linea"))
		location := strings.TrimSpace(r.FormValue("location"))
		isCreditProduct := r.FormValue("credit_enabled") != ""
		ownerUserIDRaw := strings.TrimSpace(r.FormValue("owner_user_id"))
		cantidadRaw := strings.TrimSpace(r.FormValue("cantidad"))
		precioVentaRaw := strings.TrimSpace(r.FormValue("precio_venta"))
		retomaEnabled := r.FormValue("retoma_enabled") != ""
		retomaPriceRaw := strings.TrimSpace(r.FormValue("retoma_price"))
		aplicaCad := r.FormValue("aplica_caducidad") != ""
		caducidad := strings.TrimSpace(r.FormValue("fecha_caducidad"))
		debtorName := strings.TrimSpace(r.FormValue("debtor_name"))
		installmentsTotalRaw := strings.TrimSpace(r.FormValue("installments_total"))
		totalValueRaw := strings.TrimSpace(r.FormValue("total_value"))
		installmentValueRaw := strings.TrimSpace(r.FormValue("installment_value"))
		assignableUsers, err := loadAssignableUsers(db)
		if err != nil {
			http.Error(w, "No se pudieron cargar los usuarios", http.StatusInternalServerError)
			return
		}
		validOwners := make(map[string]struct{}, len(assignableUsers))
		for _, user := range assignableUsers {
			validOwners[strconv.Itoa(user.ID)] = struct{}{}
		}

		errors := map[string]string{}
		if nombre == "" {
			errors["nombre"] = "Nombre obligatorio."
		}
		if customSKU != "" {
			var existingCount int
			if err := db.QueryRow(`SELECT COUNT(*) FROM productos WHERE sku = ?`, customSKU).Scan(&existingCount); err != nil {
				http.Error(w, "No se pudo validar el ID", http.StatusInternalServerError)
				return
			}
			if existingCount > 0 {
				errors["sku"] = "Ya existe un producto con ese ID."
			}
		}
		if linea == "" {
			if len(activeLines) == 0 {
				errors["linea"] = "Primero crea una línea de negocio en Configuración."
			} else {
				errors["linea"] = "Línea obligatoria."
			}
		}
		precioVenta := 0
		if precioVentaRaw != "" {
			parsedPrice, parseErr := parseCOPInteger(precioVentaRaw)
			if parseErr != nil || parsedPrice < 0 {
				errors["precio_venta"] = "Precio de venta inválido."
			} else {
				precioVenta = parsedPrice
			}
		}
		var retomaPrice sql.NullFloat64
		if retomaEnabled {
			if retomaPriceRaw == "" {
				errors["retoma_price"] = "Valor de retoma obligatorio si habilitas retoma."
			} else if parsedRetoma, parseErr := parseCOPInteger(retomaPriceRaw); parseErr != nil || parsedRetoma < 0 {
				errors["retoma_price"] = "Valor de retoma inválido."
			} else {
				if precioVenta > 0 && parsedRetoma > precioVenta {
					errors["retoma_price"] = "El valor de retoma no debe superar el valor de venta."
				} else {
					retomaPrice = sql.NullFloat64{Float64: float64(parsedRetoma), Valid: true}
				}
			}
		} else if retomaPriceRaw != "" {
			if _, parseErr := parseCOPInteger(retomaPriceRaw); parseErr != nil {
				errors["retoma_price"] = "Valor de retoma inválido."
			}
		}
		cantidad, err := strconv.Atoi(cantidadRaw)
		if (err != nil || cantidad <= 0) && !isCreditProduct {
			errors["cantidad"] = "Cantidad debe ser entero mayor a 0."
		}
		installmentsTotal := 0
		totalValue := 0
		installmentValue := 0
		if isCreditProduct {
			cantidad = 1
			if debtorName == "" {
				errors["debtor_name"] = "Nombre del deudor obligatorio."
			}
			parsedInstallments, parseErr := strconv.Atoi(installmentsTotalRaw)
			if parseErr != nil || parsedInstallments <= 0 {
				errors["installments_total"] = "La cantidad total de cuotas debe ser mayor a 0."
			} else {
				installmentsTotal = parsedInstallments
			}
			parsedTotalValue, parseErr := parseCOPInteger(totalValueRaw)
			if parseErr != nil || parsedTotalValue <= 0 {
				errors["total_value"] = "El valor total debe ser mayor a 0."
			} else {
				totalValue = parsedTotalValue
			}
			parsedInstallmentValue, parseErr := parseCOPInteger(installmentValueRaw)
			if parseErr != nil || parsedInstallmentValue <= 0 {
				errors["installment_value"] = "El valor por cuota debe ser mayor a 0."
			} else {
				installmentValue = parsedInstallmentValue
			}
		} else {
			debtorName = ""
			installmentsTotalRaw = ""
			totalValueRaw = ""
			installmentValueRaw = ""
		}
		if aplicaCad {
			if caducidad == "" {
				errors["fecha_caducidad"] = "Fecha caducidad requerida si aplica."
			} else if _, err := time.Parse("2006-01-02", caducidad); err != nil {
				errors["fecha_caducidad"] = "Fecha caducidad debe ser YYYY-MM-DD."
			}
		} else if caducidad != "" {
			// If they provided a date, validate it anyway to avoid persisting garbage.
			if _, err := time.Parse("2006-01-02", caducidad); err != nil {
				errors["fecha_caducidad"] = "Fecha caducidad debe ser YYYY-MM-DD."
			}
		}
		var ownerUserID sql.NullInt64
		if ownerUserIDRaw != "" {
			if _, ok := validOwners[ownerUserIDRaw]; !ok {
				errors["owner_user_id"] = "Selecciona un usuario válido."
			} else if parsedOwnerID, parseErr := strconv.Atoi(ownerUserIDRaw); parseErr != nil || parsedOwnerID <= 0 {
				errors["owner_user_id"] = "Selecciona un usuario válido."
			} else {
				ownerUserID = sql.NullInt64{Int64: int64(parsedOwnerID), Valid: true}
			}
		}

		if len(errors) > 0 {
			nextSKU := customSKU
			if nextSKU == "" {
				var skuErr error
				nextSKU, skuErr = generateNextProductSKU(db)
				if skuErr != nil {
					http.Error(w, "No se pudo generar el ID", http.StatusInternalServerError)
					return
				}
			}
			w.WriteHeader(http.StatusBadRequest)
			data := productNewData{
				Title:             "Crear producto",
				Subtitle:          "Acción reservada para administradores.",
				Flash:             "",
				LabelPrintURL:     "",
				SKU:               nextSKU,
				Nombre:            nombre,
				Linea:             linea,
				Location:          location,
				OwnerUserID:       ownerUserIDRaw,
				PrecioVenta:       precioVentaRaw,
				RetomaEnabled:     retomaEnabled,
				RetomaPrice:       retomaPriceRaw,
				Lineas:            ensureLineOption(businessLineNames(activeLines), linea),
				HasLineas:         len(activeLines) > 0,
				AssignableUsers:   assignableUsers,
				Cantidad:          cantidad,
				AplicaCad:         aplicaCad,
				Caducidad:         caducidad,
				CreditEnabled:     isCreditProduct,
				DebtorName:        debtorName,
				InstallmentsTotal: installmentsTotalRaw,
				TotalValue:        totalValueRaw,
				InstallmentValue:  installmentValueRaw,
				Errors:            errors,
				CurrentUser:       userFromContext(r),
			}
			renderTemplate(w, "product_new.html", data, "Error al renderizar productos")
			return
		}

		tx, err := db.Begin()
		if err != nil {
			http.Error(w, "No se pudo iniciar la transacción", http.StatusInternalServerError)
			return
		}
		defer tx.Rollback()

		sku := customSKU
		if sku == "" {
			sku, err = generateNextProductSKU(db)
			if err != nil {
				http.Error(w, "No se pudo generar el ID", http.StatusInternalServerError)
				return
			}
		}
		now := time.Now().Format(time.RFC3339)
		if err := upsertProducto(tx, tenantIDFromRequest(r), sku, nombre, linea, now); err != nil {
			http.Error(w, "No se pudo guardar el producto", http.StatusInternalServerError)
			return
		}
		if _, err := tx.Exec(`
			UPDATE productos
			SET precio_venta = ?, retoma_enabled = ?, retoma_price = ?, credit_enabled = ?, debtor_name = ?, installments_total = ?, installments_paid = ?, total_value = ?, installment_value = ?, location = ?
			WHERE sku = ?
		`, float64(precioVenta), boolToInt(retomaEnabled), retomaPrice, boolToInt(isCreditProduct), debtorName, installmentsTotal, 0, float64(totalValue), float64(installmentValue), location, sku); err != nil {
			http.Error(w, "No se pudo guardar el precio del producto", http.StatusInternalServerError)
			return
		}
		if _, err := tx.Exec(`UPDATE productos SET owner_user_id = ? WHERE sku = ?`, ownerUserID, sku); err != nil {
			http.Error(w, "No se pudo guardar la asignación del producto", http.StatusInternalServerError)
			return
		}
		if err := logAuditEvent(tx, userFromContext(r), "product_created", "product", sku, "manual", map[string]any{
			"sku":            sku,
			"name":           nombre,
			"line":           linea,
			"retoma_enabled": retomaEnabled,
			"retoma_price":   retomaPrice,
			"owner_user_id":  ownerUserID,
			"location":       location,
			"cantidad":       cantidad,
		}); err != nil {
			http.Error(w, "No se pudo registrar la auditoría del producto", http.StatusInternalServerError)
			return
		}
		if isCreditProduct {
			if err := logAuditEvent(tx, userFromContext(r), "credit_created", "product", sku, "manual", map[string]any{
				"product_id":         sku,
				"debtor_name":        debtorName,
				"installments_total": installmentsTotal,
				"installments_paid":  0,
				"total_value":        totalValue,
				"installment_value":  installmentValue,
			}); err != nil {
				http.Error(w, "No se pudo registrar la auditoría del crédito", http.StatusInternalServerError)
				return
			}
		}
		if ownerUserID.Valid {
			if err := logAuditEvent(tx, userFromContext(r), "product_assigned", "product", sku, "manual", map[string]any{
				"sku":           sku,
				"name":          nombre,
				"owner_user_id": ownerUserID.Int64,
			}); err != nil {
				http.Error(w, "No se pudo registrar la auditoría de asignación", http.StatusInternalServerError)
				return
			}
		}

		baseID := time.Now().UnixNano()
		tenantID := normalizeTenantID(tenantIDFromRequest(r))
		for j := 0; j < cantidad; j++ {
			unitID := fmt.Sprintf("U-%s-%d", sku, baseID+int64(j))
			var cad any = nil
			if aplicaCad && caducidad != "" {
				cad = caducidad
			}
			if _, err := tx.Exec(
				`INSERT INTO unidades (id, tenant_id, producto_id, estado, creado_en, caducidad) VALUES (?, ?, ?, ?, ?, ?)`,
				unitID, tenantID, sku, "Disponible", now, cad,
			); err != nil {
				http.Error(w, "No se pudieron crear unidades", http.StatusInternalServerError)
				return
			}
		}

		if err := tx.Commit(); err != nil {
			http.Error(w, "No se pudo confirmar la transacción", http.StatusInternalServerError)
			return
		}

		// Update in-memory catalog (used by inventario/cambio screens).
		productsMu.Lock()
		found := false
		for idx := range products {
			if products[idx].ID == sku {
				products[idx].Name = nombre
				products[idx].Line = linea
				products[idx].Location = location
				products[idx].CreditEnabled = isCreditProduct
				products[idx].DebtorName = debtorName
				products[idx].InstallmentsTotal = installmentsTotal
				products[idx].InstallmentsPaid = 0
				products[idx].TotalValue = float64(totalValue)
				products[idx].InstallmentValue = float64(installmentValue)
				products[idx].SalePrice = float64(precioVenta)
				products[idx].RetomaEnabled = retomaEnabled
				products[idx].HasRetomaPrice = retomaPrice.Valid
				if retomaPrice.Valid {
					products[idx].RetomaPrice = retomaPrice.Float64
				} else {
					products[idx].RetomaPrice = 0
				}
				products[idx].HasOwner = ownerUserID.Valid
				if ownerUserID.Valid {
					products[idx].OwnerUserID = int(ownerUserID.Int64)
				} else {
					products[idx].OwnerUserID = 0
				}
				found = true
				break
			}
		}
		if !found {
			createdProduct := productOption{
				ID:                sku,
				Name:              nombre,
				Line:              linea,
				Location:          location,
				CreditEnabled:     isCreditProduct,
				DebtorName:        debtorName,
				InstallmentsTotal: installmentsTotal,
				InstallmentsPaid:  0,
				TotalValue:        float64(totalValue),
				InstallmentValue:  float64(installmentValue),
				FechaIngreso:      time.Now().Format("2006-01-02"),
				SalePrice:         float64(precioVenta),
				RetomaEnabled:     retomaEnabled,
			}
			if retomaPrice.Valid {
				createdProduct.HasRetomaPrice = true
				createdProduct.RetomaPrice = retomaPrice.Float64
			}
			if ownerUserID.Valid {
				createdProduct.HasOwner = true
				createdProduct.OwnerUserID = int(ownerUserID.Int64)
			}
			products = append(products, createdProduct)
		}
		productsMu.Unlock()

		target := "/productos/new?mensaje=" + url.QueryEscape("Producto agregado correctamente.") + "&label_url=" + url.QueryEscape(productLabelPrintURL([]string{sku}, "60x40"))
		http.Redirect(w, r, target, http.StatusSeeOther)
	}))

	mux.HandleFunc("/api/products", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			productsSnapshot, err := loadVisibleProductsForUser(db, userFromContext(r))
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar los productos.", nil)
				return
			}
			productsSnapshot = filterProductsForUser(productsSnapshot, userFromContext(r))
			items := make([]map[string]any, 0, len(productsSnapshot))
			for _, product := range productsSnapshot {
				var owner any = nil
				if product.HasOwner {
					owner = product.OwnerUserID
				}
				var retomaPrice any = nil
				if product.HasRetomaPrice {
					retomaPrice = product.RetomaPrice
				}
				items = append(items, map[string]any{
					"id":                 product.ID,
					"name":               product.Name,
					"line":               product.Line,
					"location":           product.Location,
					"credit_enabled":     product.CreditEnabled,
					"debtor_name":        product.DebtorName,
					"installments_total": product.InstallmentsTotal,
					"installments_paid":  product.InstallmentsPaid,
					"total_value":        product.TotalValue,
					"installment_value":  product.InstallmentValue,
					"fecha_ingreso":      formatDateWithSettings(product.FechaIngreso),
					"sale_price":         product.SalePrice,
					"retoma_enabled":     product.RetomaEnabled,
					"retoma_price":       retomaPrice,
					"owner_user_id":      owner,
				})
			}
			writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "items": items, "count": len(items)})
			return
		}
		if r.Method != http.MethodPost {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		currentUser := userFromContext(r)
		if currentUser == nil || !isAdminRole(currentUser.Role) {
			writeAPIError(w, http.StatusForbidden, "Solo administrador puede crear productos vía API.", nil)
			return
		}
		var payload struct {
			Name           string `json:"name"`
			Line           string `json:"line"`
			Location       string `json:"location"`
			OwnerUserID    *int   `json:"owner_user_id"`
			Quantity       int    `json:"quantity"`
			SalePrice      int    `json:"sale_price"`
			RetomaEnabled  bool   `json:"retoma_enabled"`
			RetomaPrice    *int   `json:"retoma_price"`
			AplicaCad      bool   `json:"aplica_caducidad"`
			FechaCaducidad string `json:"fecha_caducidad"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeAPIError(w, http.StatusBadRequest, "JSON inválido.", nil)
			return
		}
		payload.Name = strings.TrimSpace(payload.Name)
		payload.Line = strings.TrimSpace(payload.Line)
		payload.Location = strings.TrimSpace(payload.Location)
		payload.FechaCaducidad = strings.TrimSpace(payload.FechaCaducidad)
		activeLines, err := loadBusinessLinesForTenant(db, tenantIDFromRequest(r), true)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar las líneas de negocio.", nil)
			return
		}
		assignableUsers, err := loadAssignableUsers(db)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar los usuarios.", nil)
			return
		}
		fields := map[string]string{}
		if payload.Name == "" {
			fields["name"] = "Nombre obligatorio."
		}
		if payload.Line == "" {
			fields["line"] = "Línea obligatoria."
		} else {
			validLine := false
			for _, line := range activeLines {
				if strings.EqualFold(line.Name, payload.Line) {
					validLine = true
					break
				}
			}
			if !validLine {
				fields["line"] = "Selecciona una línea activa válida."
			}
		}
		if payload.Quantity <= 0 {
			fields["quantity"] = "Cantidad debe ser mayor a 0."
		}
		if payload.SalePrice < 0 {
			fields["sale_price"] = "Precio inválido."
		}
		var retomaPrice sql.NullFloat64
		if payload.RetomaEnabled {
			if payload.RetomaPrice == nil || *payload.RetomaPrice < 0 {
				fields["retoma_price"] = "Valor de retoma inválido."
			} else if payload.SalePrice > 0 && *payload.RetomaPrice > payload.SalePrice {
				fields["retoma_price"] = "El valor de retoma no debe superar el valor de venta."
			} else {
				retomaPrice = sql.NullFloat64{Float64: float64(*payload.RetomaPrice), Valid: true}
			}
		}
		if payload.AplicaCad {
			if payload.FechaCaducidad == "" {
				fields["fecha_caducidad"] = "Fecha caducidad requerida si aplica."
			} else if _, err := time.Parse("2006-01-02", payload.FechaCaducidad); err != nil {
				fields["fecha_caducidad"] = "Fecha caducidad debe ser YYYY-MM-DD."
			}
		}
		validOwners := map[int]struct{}{}
		for _, user := range assignableUsers {
			validOwners[user.ID] = struct{}{}
		}
		var ownerUserID sql.NullInt64
		if payload.OwnerUserID != nil {
			if _, ok := validOwners[*payload.OwnerUserID]; !ok {
				fields["owner_user_id"] = "Usuario asignado inválido."
			} else {
				ownerUserID = sql.NullInt64{Int64: int64(*payload.OwnerUserID), Valid: true}
			}
		}
		if len(fields) > 0 {
			writeAPIError(w, http.StatusBadRequest, "Datos inválidos.", fields)
			return
		}
		tx, err := db.Begin()
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo iniciar la transacción.", nil)
			return
		}
		defer tx.Rollback()
		sku, err := generateNextProductSKU(db)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo generar el ID.", nil)
			return
		}
		now := time.Now().Format(time.RFC3339)
		if err := upsertProducto(tx, tenantIDFromUser(currentUser), sku, payload.Name, payload.Line, now); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo guardar el producto.", nil)
			return
		}
		if _, err := tx.Exec(`UPDATE productos SET precio_venta = ?, retoma_enabled = ?, retoma_price = ?, owner_user_id = ?, location = ? WHERE sku = ?`, float64(payload.SalePrice), boolToInt(payload.RetomaEnabled), retomaPrice, ownerUserID, payload.Location, sku); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo guardar el producto.", nil)
			return
		}
		for j := 0; j < payload.Quantity; j++ {
			unitID := fmt.Sprintf("U-%s-%d", sku, time.Now().UnixNano()+int64(j))
			var cad any = nil
			if payload.AplicaCad && payload.FechaCaducidad != "" {
				cad = payload.FechaCaducidad
			}
			if _, err := tx.Exec(`INSERT INTO unidades (id, tenant_id, producto_id, estado, creado_en, caducidad) VALUES (?, ?, ?, ?, ?, ?)`, unitID, normalizeTenantID(tenantIDFromUser(currentUser)), sku, "Disponible", now, cad); err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudieron crear las unidades.", nil)
				return
			}
		}
		if err := logAuditEvent(tx, currentUser, "product_created", "product", sku, "api", withAPIAuditMetadata(r, map[string]any{
			"sku":            sku,
			"name":           payload.Name,
			"line":           payload.Line,
			"sale_price":     payload.SalePrice,
			"retoma_enabled": payload.RetomaEnabled,
			"retoma_price":   retomaPrice,
			"owner_user_id":  ownerUserID,
			"location":       payload.Location,
			"cantidad":       payload.Quantity,
		})); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo registrar la auditoría.", nil)
			return
		}
		if ownerUserID.Valid {
			if err := logAuditEvent(tx, currentUser, "product_assigned", "product", sku, "api", withAPIAuditMetadata(r, map[string]any{
				"sku":           sku,
				"name":          payload.Name,
				"owner_user_id": ownerUserID.Int64,
			})); err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudo registrar la auditoría.", nil)
				return
			}
		}
		if err := tx.Commit(); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo confirmar la transacción.", nil)
			return
		}
		writeAPIJSON(w, http.StatusCreated, map[string]any{"ok": true, "id": sku, "location": payload.Location, "message": "Producto creado correctamente."})
	})

	mux.HandleFunc("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		visibilitySQL, visibilityArgs := productVisibilityPredicate("p", currentUser)
		estadoRows, err := db.Query(`
			SELECT CASE WHEN estado = 'Vendida' THEN 'Vendido' ELSE estado END, COUNT(*)
			FROM unidades u
			LEFT JOIN productos p ON p.sku = u.producto_id
			WHERE `+visibilitySQL+`
			GROUP BY CASE WHEN estado = 'Vendida' THEN 'Vendido' ELSE estado END
			ORDER BY estado`, visibilityArgs...)
		if err != nil {
			http.Error(w, "Error al consultar estados", http.StatusInternalServerError)
			return
		}
		defer estadoRows.Close()

		estadoMap := map[string]int{}
		for estadoRows.Next() {
			var estado string
			var cantidad int
			if err := estadoRows.Scan(&estado, &cantidad); err != nil {
				http.Error(w, "Error al leer estados", http.StatusInternalServerError)
				return
			}
			estadoMap[estado] = cantidad
		}
		if err := estadoRows.Err(); err != nil {
			http.Error(w, "Error al procesar estados", http.StatusInternalServerError)
			return
		}

		estadoOrden := []string{"Disponible", "Cambio", "Vendido"}
		estadoConteos := make([]estadoCount, 0, len(estadoOrden))
		for _, estado := range estadoOrden {
			estadoConteos = append(estadoConteos, estadoCount{
				Estado:   estado,
				Cantidad: estadoMap[estado],
				Link:     "/inventario?estado=" + estado,
			})
		}

		now := time.Now()
		endDate := parseDateOrDefault(r.URL.Query().Get("end_date"), now)
		startDate := parseDateOrDefault(r.URL.Query().Get("start_date"), endDate.AddDate(0, 0, -6))
		if startDate.After(endDate) {
			startDate, endDate = endDate, startDate
		}
		startDate = time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, startDate.Location())
		endDate = time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 0, 0, 0, 0, endDate.Location())
		startStr := startDate.Format("2006-01-02")
		endStr := endDate.Format("2006-01-02")

		salesData, err := buildDashboardSalesData(db, currentUser, startStr, endStr, startDate, endDate)
		if err != nil {
			http.Error(w, "Error al consultar ventas", http.StatusInternalServerError)
			return
		}
		_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantIDFromUser(currentUser))
		if err != nil {
			http.Error(w, "Error al cargar tipos de movimiento", http.StatusInternalServerError)
			return
		}

		data := dashboardData{
			Title:           "Resumen de negocio",
			Subtitle:        "",
			EstadoConteos:   estadoConteos,
			MetodosPago:     salesData.MetodosPago,
			PieSlices:       salesData.PieSlices,
			PieTotal:        salesData.PieTotal,
			MaxTimeline:     salesData.MaxTimeline,
			MaxTimelineText: salesData.MaxTimelineText,
			TimelinePoints:  buildTimelinePoints(salesData.Timeline, 560, 180, 24),
			Timeline:        salesData.Timeline,
			UserTimeline:    salesData.UserTimeline,
			CategoryTotals:  salesData.CategoryTotals,
			Sales:           salesData.Sales,
			CurrentUser:     currentUser,
			CanLoan:         movementEnabled(movementEnabledMap, "prestamo"),
			CanCredit:       movementEnabled(movementEnabledMap, "credito"),
			RangeStart:      startStr,
			RangeEnd:        endStr,
			RangeTotal:      salesData.RangeTotal,
			RangeCount:      salesData.RangeCount,
		}

		var rendered bytes.Buffer
		if err := tmpl.ExecuteTemplate(&rendered, "dashboard.html", data); err != nil {
			http.Error(w, "Error al renderizar el dashboard", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(rendered.Bytes())
	})

	mux.HandleFunc("/dashboard/data", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		now := time.Now()
		endDate := parseDateOrDefault(r.URL.Query().Get("end_date"), now)
		startDate := parseDateOrDefault(r.URL.Query().Get("start_date"), endDate.AddDate(0, 0, -6))
		if startDate.After(endDate) {
			startDate, endDate = endDate, startDate
		}
		startDate = time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, startDate.Location())
		endDate = time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 0, 0, 0, 0, endDate.Location())
		startStr := startDate.Format("2006-01-02")
		endStr := endDate.Format("2006-01-02")

		data, err := buildDashboardSalesData(db, currentUser, startStr, endStr, startDate, endDate)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "No se pudo cargar datos del dashboard."})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(data)
	})

	mux.HandleFunc("/dashboard/ventas/delete", func(w http.ResponseWriter, r *http.Request) {
		writeJSONError := func(status int, message string) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": message})
		}
		if r.Method != http.MethodPost {
			writeJSONError(http.StatusMethodNotAllowed, "Método no permitido.")
			return
		}
		currentUser := userFromContext(r)
		if currentUser == nil || !isAdminRole(currentUser.Role) {
			writeJSONError(http.StatusForbidden, "Solo administrador puede eliminar ventas.")
			return
		}
		if err := r.ParseForm(); err != nil {
			writeJSONError(http.StatusBadRequest, "No se pudo leer el formulario.")
			return
		}
		idValue := strings.TrimSpace(r.FormValue("venta_id"))
		ventaID, err := strconv.Atoi(idValue)
		if err != nil || ventaID <= 0 {
			writeJSONError(http.StatusBadRequest, "ID de venta inválido.")
			return
		}
		res, err := db.Exec(`DELETE FROM ventas WHERE id = ?`, ventaID)
		if err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo eliminar la venta.")
			return
		}
		affected, err := res.RowsAffected()
		if err != nil || affected == 0 {
			writeJSONError(http.StatusNotFound, "La venta no existe o ya fue eliminada.")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "venta_id": ventaID})
	})

	mux.HandleFunc("/csv/ventas", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		now := time.Now()
		endDate := parseDateOrDefault(r.URL.Query().Get("end_date"), now)
		startDate := parseDateOrDefault(r.URL.Query().Get("start_date"), endDate.AddDate(0, 0, -6))
		if startDate.After(endDate) {
			startDate, endDate = endDate, startDate
		}
		startDate = time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, startDate.Location())
		endDate = time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 0, 0, 0, 0, endDate.Location())
		startStr := startDate.Format("2006-01-02")
		endStr := endDate.Format("2006-01-02")

		visibilitySQL, visibilityArgs := productVisibilityPredicate("p", currentUser)
		queryArgs := append([]any{startStr, endStr}, visibilityArgs...)
		salesDateExpr := sqlDatePrefixExpr("v.fecha")
		rows, err := db.Query(`
			SELECT
				v.id,
				v.fecha,
				v.producto_id,
				COALESCE(p.nombre, ''),
				v.cantidad,
				v.precio_final,
				v.metodo_pago,
				v.notas
			FROM ventas v
			LEFT JOIN productos p ON p.sku = v.producto_id
			WHERE `+salesDateExpr+` BETWEEN ? AND ? AND `+visibilitySQL+`
			ORDER BY v.fecha DESC, v.id DESC
		`, queryArgs...)
		if err != nil {
			http.Error(w, "Error al consultar ventas.", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		filename := fmt.Sprintf("ventas_%s_a_%s.csv", startStr, endStr)
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
		cw := csv.NewWriter(w)
		defer cw.Flush()

		_ = cw.Write([]string{"venta_id", "fecha", "sku", "producto", "cantidad", "precio_unitario", "total", "metodo_pago", "notas"})

		for rows.Next() {
			var (
				id         int
				fechaRaw   string
				sku        string
				nombre     string
				cantidad   int
				precioUnit float64
				metodo     string
				notas      string
			)
			if err := rows.Scan(&id, &fechaRaw, &sku, &nombre, &cantidad, &precioUnit, &metodo, &notas); err != nil {
				http.Error(w, "Error al leer ventas.", http.StatusInternalServerError)
				return
			}
			fecha := fechaRaw
			if len(fechaRaw) >= 10 {
				fecha = fechaRaw[:10]
			}
			total := precioUnit * float64(cantidad)
			_ = cw.Write([]string{
				strconv.Itoa(id),
				fecha,
				sku,
				nombre,
				strconv.Itoa(cantidad),
				fmt.Sprintf("%.2f", precioUnit),
				fmt.Sprintf("%.2f", total),
				metodo,
				notas,
			})
		}
		if err := rows.Err(); err != nil {
			http.Error(w, "Error al procesar ventas.", http.StatusInternalServerError)
			return
		}
	})

	mux.HandleFunc("/inventario", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		flash := r.URL.Query().Get("mensaje")
		receiptSaleID, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("receipt_sale_id")))
		activePaymentMethods, err := loadPaymentMethodsForTenant(db, tenantIDFromUser(currentUser), true)
		if err != nil {
			http.Error(w, "Error al cargar métodos de pago", http.StatusInternalServerError)
			return
		}
		productsSnapshot, err := loadVisibleProductsForUser(db, userFromContext(r))
		if err != nil {
			productsMu.RLock()
			productsSnapshot = make([]productOption, len(products))
			copy(productsSnapshot, products)
			productsMu.RUnlock()
		} else {
			productsMu.Lock()
			products = make([]productOption, len(productsSnapshot))
			copy(products, productsSnapshot)
			productsMu.Unlock()
		}
		productsSnapshot = filterProductsForUser(productsSnapshot, currentUser)
		editableLines := []string{}
		assignableUsers := []assignableUser{}
		if currentUser != nil && isAdminRole(currentUser.Role) {
			lines, err := loadBusinessLinesForTenant(db, tenantIDFromRequest(r), false)
			if err != nil {
				http.Error(w, "Error al cargar líneas de negocio", http.StatusInternalServerError)
				return
			}
			editableLines = businessLineNames(lines)
			assignableUsers, err = loadAssignableUsers(db)
			if err != nil {
				http.Error(w, "Error al cargar usuarios asignables", http.StatusInternalServerError)
				return
			}
		}

		inventoryProducts := make([]inventoryProduct, 0, len(productsSnapshot))
		allowedProducts := make(map[string]productOption, len(productsSnapshot))
		for _, product := range productsSnapshot {
			allowedProducts[product.ID] = product
			rows, err := db.Query(`
					SELECT id, estado, creado_en, caducidad
					FROM unidades
					WHERE tenant_id = ? AND producto_id = ?
					ORDER BY creado_en, id`, tenantIDFromUser(currentUser), product.ID)
			if err != nil {
				http.Error(w, "Error al consultar unidades", http.StatusInternalServerError)
				return
			}

			units := []inventoryUnit{}
			availableCount := 0
			loanedCount := 0
			changeCount := 0
			reservedCount := 0
			damagedCount := 0
			fifoIndex := 1
			for rows.Next() {
				var id, estado, creadoEn string
				var caducidad sql.NullString
				if err := rows.Scan(&id, &estado, &creadoEn, &caducidad); err != nil {
					rows.Close()
					http.Error(w, "Error al leer unidades", http.StatusInternalServerError)
					return
				}
				fifo := "-"
				if estado == "Disponible" || estado == "available" {
					fifo = strconv.Itoa(fifoIndex)
					fifoIndex++
					availableCount++
				} else if estado == "Prestada" || estado == "Prestado" || estado == "loaned" {
					loanedCount++
				} else if estado == "Reservada" || estado == "reserved" {
					reservedCount++
				} else if estado == "Cambio" || estado == "swapped" {
					changeCount++
				} else if estado == "Danada" || estado == "Dañada" || estado == "damaged" {
					damagedCount++
				}
				units = append(units, inventoryUnit{
					ID:          id,
					Estado:      estado,
					EstadoClass: estadoClass(estado),
					CreadoEn:    formatDateWithSettings(creadoEn),
					Caducidad:   formatDateWithSettings(caducidad.String),
					FIFO:        fifo,
				})
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				http.Error(w, "Error al procesar unidades", http.StatusInternalServerError)
				return
			}
			rows.Close()

			estadoLabel := "Disponible"
			estadoClass := "available"
			if availableCount == 0 {
				if loanedCount > 0 {
					estadoLabel = "Prestado"
					estadoClass = "loaned"
				} else if reservedCount > 0 {
					estadoLabel = "Reservado"
					estadoClass = "reserved"
				} else if changeCount > 0 {
					estadoLabel = "Cambio"
					estadoClass = "swapped"
				} else if damagedCount > 0 {
					estadoLabel = "Dañado"
					estadoClass = "damaged"
				} else {
					estadoLabel = "Vendido"
					estadoClass = "sold"
				}
			}

			// Permanence alert: if the product has been in stock for >= 6 months since fecha_ingreso,
			// flag it for UI and "Accion Caducidad 45 dias" filter.
			fechaIngresoRaw := strings.TrimSpace(product.FechaIngreso)
			if fechaIngresoRaw == "" && len(units) > 0 {
				// Fallback for legacy rows: derive from the oldest unit creation timestamp.
				fechaIngresoRaw = strings.TrimSpace(units[0].CreadoEn)
			}
			mesesEnStock := 0
			fechaIngresoISO := ""
			if t, ok := parseFlexibleTime(fechaIngresoRaw); ok {
				fechaIngresoISO = t.Format("2006-01-02")
				mesesEnStock = monthsBetween(t, time.Now())
			} else if len(fechaIngresoRaw) >= 10 {
				fechaIngresoISO = fechaIngresoRaw[:10]
			}
			alertaPermanencia := mesesEnStock >= 6

			inventoryProducts = append(inventoryProducts, inventoryProduct{
				EntryType:         "product",
				ID:                product.ID,
				Name:              product.Name,
				Line:              product.Line,
				Location:          product.Location,
				CreditEnabled:     product.CreditEnabled,
				DebtorName:        product.DebtorName,
				InstallmentsTotal: product.InstallmentsTotal,
				InstallmentsPaid:  product.InstallmentsPaid,
				TotalValue:        product.TotalValue,
				InstallmentValue:  product.InstallmentValue,
				Notes:             product.Notes,
				EstadoLabel:       estadoLabel,
				EstadoClass:       estadoClass,
				Disponible:        availableCount,
				Unidades:          units,
				DisabledSale:      availableCount == 0,
				FechaIngreso:      formatDateWithSettings(fechaIngresoISO),
				MesesEnStock:      mesesEnStock,
				AlertaPermanencia: alertaPermanencia,
				SalePrice:         product.SalePrice,
				RetomaEnabled:     product.RetomaEnabled,
				RetomaPrice:       product.RetomaPrice,
				HasRetomaPrice:    product.HasRetomaPrice,
				OwnerUserID:       product.OwnerUserID,
				HasOwner:          product.HasOwner,
			})
		}

		creditRows, err := db.Query(`
			SELECT
				cs.id,
				cs.product_id,
				cs.quantity,
				COALESCE(cs.customer_id, 0),
				COALESCE(c.name, cs.debtor_name, ''),
				COALESCE(c.document_type, cs.debtor_document_type, ''),
				COALESCE(c.document_number, cs.debtor_document_number, ''),
				COALESCE(c.phone, cs.debtor_phone, ''),
				COALESCE(c.address, ''),
				COALESCE(c.city, ''),
				COALESCE(c.notes, ''),
				COALESCE(cs.installments_total, 0),
				COALESCE(cs.installments_paid, 0),
				COALESCE((
					SELECT COUNT(*)
					FROM credit_installments ci
					WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id AND COALESCE(ci.payment_type, 'cuota') = 'cuota'
				), COALESCE(cs.installments_paid, 0)),
				COALESCE(cs.total_value, 0),
				COALESCE(cs.interest_percent, 0),
				COALESCE(cs.installment_value, 0),
				COALESCE((
					SELECT SUM(ci.amount_paid)
					FROM credit_installments ci
					WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id
				), 0),
				COALESCE((
					SELECT ci.amount_paid
					FROM credit_installments ci
					WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id
					ORDER BY ci.created_at DESC, ci.id DESC
					LIMIT 1
				), 0),
				COALESCE((
					SELECT ci.created_at
					FROM credit_installments ci
					WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id
					ORDER BY ci.created_at DESC, ci.id DESC
					LIMIT 1
				), ''),
				COALESCE((
					SELECT COALESCE(ci.payment_type, 'cuota')
					FROM credit_installments ci
					WHERE ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id
					ORDER BY ci.created_at DESC, ci.id DESC
					LIMIT 1
				), ''),
				COALESCE(cs.notes, ''),
				COALESCE(cs.status, ''),
				cs.created_at
			FROM credit_sales cs
			LEFT JOIN customers c ON c.id = cs.customer_id AND c.tenant_id = cs.tenant_id
			WHERE cs.tenant_id = ? AND COALESCE(cs.kind, ?) = ?
			ORDER BY created_at DESC, id DESC
		`, tenantIDFromUser(currentUser), string(creditSaleKindProduct), string(creditSaleKindProduct))
		if err != nil {
			http.Error(w, "Error al consultar créditos", http.StatusInternalServerError)
			return
		}
		defer creditRows.Close()
		for creditRows.Next() {
			var creditID int
			var productID string
			var quantity int
			var customerID int
			var debtorName string
			var debtorDocumentType string
			var debtorDocumentNumber string
			var debtorPhone string
			var customerAddress string
			var customerCity string
			var customerNotes string
			var installmentsTotal int
			var installmentsPaid int
			var paidInstallmentsCount int
			var totalValue float64
			var interestPercent float64
			var installmentValue float64
			var totalPaid float64
			var lastPaymentAmount float64
			var lastPaymentAt string
			var lastPaymentType string
			var notes string
			var statusRaw string
			var createdAt string
			if err := creditRows.Scan(&creditID, &productID, &quantity, &customerID, &debtorName, &debtorDocumentType, &debtorDocumentNumber, &debtorPhone, &customerAddress, &customerCity, &customerNotes, &installmentsTotal, &installmentsPaid, &paidInstallmentsCount, &totalValue, &interestPercent, &installmentValue, &totalPaid, &lastPaymentAmount, &lastPaymentAt, &lastPaymentType, &notes, &statusRaw, &createdAt); err != nil {
				http.Error(w, "Error al leer créditos", http.StatusInternalServerError)
				return
			}
			if paidInstallmentsCount < installmentsPaid {
				paidInstallmentsCount = installmentsPaid
			}
			legacyTotalPaid := math.Round((float64(installmentsPaid)*installmentValue)*100) / 100
			if totalPaid < legacyTotalPaid {
				totalPaid = legacyTotalPaid
			}
			product, ok := allowedProducts[productID]
			if !ok {
				continue
			}
			debtTotal := creditDebtTotal(installmentsTotal, installmentValue)
			currentDebt := creditCurrentDebt(debtTotal, totalPaid)
			creditStatusValue := effectiveCreditStatus(statusRaw, currentDebt, debtTotal)
			statusLabel := creditStatusLabel(creditStatusValue)
			statusClass := creditStatusClass(creditStatusValue)
			creditName := product.Name
			if quantity > 1 {
				creditName = fmt.Sprintf("%s x%d", product.Name, quantity)
			}
			inventoryProducts = append(inventoryProducts, inventoryProduct{
				EntryType:             "credit",
				CreditSaleID:          creditID,
				CustomerID:            customerID,
				BaseProductID:         productID,
				ID:                    fmt.Sprintf("CR-%d", creditID),
				Name:                  creditName,
				Line:                  "Crédito",
				CreditEnabled:         true,
				InterestPercent:       interestPercent,
				DebtorName:            debtorName,
				DebtorDocumentType:    debtorDocumentType,
				DebtorDocumentNumber:  debtorDocumentNumber,
				DebtorPhone:           debtorPhone,
				CustomerAddress:       customerAddress,
				CustomerCity:          customerCity,
				CustomerNotes:         customerNotes,
				InstallmentsTotal:     installmentsTotal,
				InstallmentsPaid:      paidInstallmentsCount,
				PaidInstallmentsCount: paidInstallmentsCount,
				TotalValue:            totalValue,
				DebtTotal:             debtTotal,
				TotalPaid:             totalPaid,
				CurrentDebt:           currentDebt,
				InstallmentValue:      installmentValue,
				LastPaymentAmount:     lastPaymentAmount,
				LastPaymentAt:         lastPaymentAt,
				LastPaymentType:       string(normalizeCreditPaymentType(lastPaymentType)),
				Notes:                 notes,
				EstadoLabel:           statusLabel,
				EstadoClass:           statusClass,
				Disponible:            0,
				Unidades:              []inventoryUnit{},
				DisabledSale:          true,
				FechaIngreso:          formatDateWithSettings(createdAt),
				SalePrice:             product.SalePrice,
				RetomaEnabled:         false,
			})
		}
		if err := creditRows.Err(); err != nil {
			http.Error(w, "Error al procesar créditos", http.StatusInternalServerError)
			return
		}
		loanRows, err := db.Query(`
			SELECT
				pl.id,
				pl.product_id,
				pl.quantity,
				COALESCE(pl.customer_id, 0),
				COALESCE(c.name, pl.borrower_name, ''),
				COALESCE(c.document_type, pl.borrower_document_type, ''),
				COALESCE(c.document_number, pl.borrower_document_number, ''),
				COALESCE(c.phone, pl.borrower_phone, ''),
				COALESCE(c.address, pl.borrower_address, ''),
				COALESCE(c.city, pl.borrower_city, ''),
				COALESCE(c.notes, ''),
				COALESCE(pl.notes, ''),
				COALESCE(pl.status, 'active'),
				COALESCE(pl.loaned_at, ''),
				COALESCE(pl.due_at, ''),
				COALESCE(pl.closed_at, ''),
				COALESCE(u.username, '')
			FROM product_loans pl
			LEFT JOIN customers c ON c.id = pl.customer_id AND c.tenant_id = pl.tenant_id
			LEFT JOIN users u ON u.id = pl.created_by AND u.tenant_id = pl.tenant_id
			WHERE pl.tenant_id = ? AND COALESCE(pl.status, 'active') = 'active'
			ORDER BY pl.loaned_at DESC, pl.id DESC
		`, tenantIDFromUser(currentUser))
		if err != nil {
			http.Error(w, "Error al consultar préstamos de producto", http.StatusInternalServerError)
			return
		}
		defer loanRows.Close()
		for loanRows.Next() {
			var (
				productLoanID          int
				productID              string
				quantity               int
				customerID             int
				borrowerName           string
				borrowerDocumentType   string
				borrowerDocumentNumber string
				borrowerPhone          string
				borrowerAddress        string
				borrowerCity           string
				customerNotes          string
				notes                  string
				statusRaw              string
				loanedAt               string
				dueAt                  string
				closedAt               string
				managedByName          string
			)
			if err := loanRows.Scan(&productLoanID, &productID, &quantity, &customerID, &borrowerName, &borrowerDocumentType, &borrowerDocumentNumber, &borrowerPhone, &borrowerAddress, &borrowerCity, &customerNotes, &notes, &statusRaw, &loanedAt, &dueAt, &closedAt, &managedByName); err != nil {
				http.Error(w, "Error al leer préstamos de producto", http.StatusInternalServerError)
				return
			}
			product, ok := allowedProducts[productID]
			if !ok {
				continue
			}
			unitRows, err := db.Query(`
				SELECT plu.unit_id
				FROM product_loan_units plu
				WHERE plu.tenant_id = ? AND plu.product_loan_id = ?
				ORDER BY plu.id ASC
			`, tenantIDFromUser(currentUser), productLoanID)
			if err != nil {
				http.Error(w, "Error al consultar unidades del préstamo", http.StatusInternalServerError)
				return
			}
			loanUnits := make([]inventoryUnit, 0, quantity)
			for unitRows.Next() {
				var unitID string
				if err := unitRows.Scan(&unitID); err != nil {
					unitRows.Close()
					http.Error(w, "Error al leer unidades del préstamo", http.StatusInternalServerError)
					return
				}
				loanUnits = append(loanUnits, inventoryUnit{
					ID:          unitID,
					Estado:      "Prestada",
					EstadoClass: "loaned",
					CreadoEn:    formatDateWithSettings(loanedAt),
					Caducidad:   "",
					FIFO:        "-",
				})
			}
			unitRows.Close()
			loanName := product.Name
			if quantity > 1 {
				loanName = fmt.Sprintf("%s x%d", product.Name, quantity)
			}
			loanStatus := normalizeProductLoanStatus(statusRaw)
			inventoryProducts = append(inventoryProducts, inventoryProduct{
				EntryType:            "loan",
				ProductLoanID:        productLoanID,
				CustomerID:           customerID,
				BaseProductID:        productID,
				ID:                   fmt.Sprintf("PR-%d", productLoanID),
				Name:                 loanName,
				Line:                 "Préstamo",
				DebtorName:           borrowerName,
				DebtorDocumentType:   borrowerDocumentType,
				DebtorDocumentNumber: borrowerDocumentNumber,
				DebtorPhone:          borrowerPhone,
				CustomerAddress:      borrowerAddress,
				CustomerCity:         borrowerCity,
				CustomerNotes:        customerNotes,
				ManagedByName:        managedByName,
				DueAt:                dueAt,
				ClosedAt:             closedAt,
				CloseStatus:          string(loanStatus),
				Notes:                notes,
				EstadoLabel:          productLoanStatusLabel(loanStatus),
				EstadoClass:          productLoanStatusClass(loanStatus),
				Disponible:           0,
				Unidades:             loanUnits,
				DisabledSale:         true,
				FechaIngreso:         formatDateWithSettings(loanedAt),
				SalePrice:            product.SalePrice,
			})
		}
		if err := loanRows.Err(); err != nil {
			http.Error(w, "Error al procesar préstamos de producto", http.StatusInternalServerError)
			return
		}
		_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantIDFromUser(currentUser))
		if err != nil {
			http.Error(w, "Error al cargar tipos de movimiento", http.StatusInternalServerError)
			return
		}
		data := inventoryPageData{
			Title:         "Seguimiento de existencias",
			Subtitle:      "",
			RoutePrefix:   "",
			Flash:         flash,
			ReceiptSaleID: receiptSaleID,
			ReceiptViewURL: func() string {
				if receiptSaleID > 0 {
					return saleReceiptViewURL(receiptSaleID)
				}
				return ""
			}(),
			ReceiptDownloadURL: func() string {
				if receiptSaleID > 0 {
					return saleReceiptDownloadURL(receiptSaleID)
				}
				return ""
			}(),
			ThermalTicketURL: func() string {
				if receiptSaleID > 0 {
					return saleThermalTicketViewURL(receiptSaleID)
				}
				return ""
			}(),
			MetodoPagos:     paymentMethodNames(activePaymentMethods),
			Products:        inventoryProducts,
			EditableLines:   editableLines,
			AssignableUsers: assignableUsers,
			CanSell:         movementEnabled(movementEnabledMap, "venta"),
			CanSwap:         movementEnabled(movementEnabledMap, "cambio"),
			CanRetoma:       movementEnabled(movementEnabledMap, "retoma"),
			CanLoan:         movementEnabled(movementEnabledMap, "prestamo"),
			CanCredit:       movementEnabled(movementEnabledMap, "credito"),
			CurrentUser:     currentUser,
		}
		renderTemplate(w, "inventario.html", data, "Error al renderizar el template")
	})

	mux.HandleFunc("/inventario/producto/editar", func(w http.ResponseWriter, r *http.Request) {
		writeJSONError := func(status int, message string) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
		}

		currentUser := userFromContext(r)
		if currentUser == nil || !isStaffRole(currentUser.Role) {
			writeJSONError(http.StatusForbidden, "Solo personal autorizado puede editar productos.")
			return
		}
		if r.Method != http.MethodPost {
			writeJSONError(http.StatusMethodNotAllowed, "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			writeJSONError(http.StatusBadRequest, "No se pudo leer el formulario.")
			return
		}

		productID := strings.TrimSpace(r.FormValue("producto_id"))
		newSKU := strings.TrimSpace(r.FormValue("id"))
		if newSKU == "" {
			newSKU = strings.TrimSpace(r.FormValue("sku"))
		}
		newName := strings.TrimSpace(r.FormValue("nombre"))
		newLine := strings.TrimSpace(r.FormValue("linea"))
		locationValue := strings.TrimSpace(r.FormValue("location"))
		ownerUserIDRaw := strings.TrimSpace(r.FormValue("owner_user_id"))
		priceValue := strings.TrimSpace(r.FormValue("precio_venta"))
		retomaEnabled := r.FormValue("retoma_enabled") != ""
		creditEnabledValue := r.FormValue("credit_enabled") != ""
		retomaPriceValue := strings.TrimSpace(r.FormValue("retoma_price"))
		notesValue := strings.TrimSpace(r.FormValue("notas"))
		debtorNameValue := strings.TrimSpace(r.FormValue("debtor_name"))
		installmentsTotalValue := strings.TrimSpace(r.FormValue("installments_total"))
		totalValueValue := strings.TrimSpace(r.FormValue("total_value"))
		installmentValueValue := strings.TrimSpace(r.FormValue("installment_value"))

		if productID == "" {
			writeJSONError(http.StatusBadRequest, "Producto inválido.")
			return
		}
		allowed, err := productAccessibleByID(db, currentUser, productID)
		if err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo validar acceso al producto.")
			return
		}
		if !allowed {
			writeJSONError(http.StatusForbidden, "No tienes acceso a este producto.")
			return
		}

		var previous struct {
			SKU               string
			Name              string
			Line              string
			Location          string
			CreditEnabled     int
			DebtorName        string
			InstallmentsTotal int
			InstallmentsPaid  int
			TotalValue        float64
			InstallmentValue  float64
			SalePrice         float64
			RetomaEnabled     int
			RetomaPrice       sql.NullFloat64
			Notes             string
			OwnerUserID       sql.NullInt64
		}
		if err := db.QueryRow(`
			SELECT sku, nombre, linea, COALESCE(location, ''), COALESCE(credit_enabled, 0), COALESCE(debtor_name, ''), COALESCE(installments_total, 0), COALESCE(installments_paid, 0), COALESCE(total_value, 0), COALESCE(installment_value, 0), COALESCE(precio_venta, 0), COALESCE(retoma_enabled, 0), retoma_price, COALESCE(anotaciones, ''), owner_user_id
			FROM productos
			WHERE sku = ? OR id = ?
			LIMIT 1
		`, productID, productID).Scan(&previous.SKU, &previous.Name, &previous.Line, &previous.Location, &previous.CreditEnabled, &previous.DebtorName, &previous.InstallmentsTotal, &previous.InstallmentsPaid, &previous.TotalValue, &previous.InstallmentValue, &previous.SalePrice, &previous.RetomaEnabled, &previous.RetomaPrice, &previous.Notes, &previous.OwnerUserID); err != nil {
			if err == sql.ErrNoRows {
				writeJSONError(http.StatusNotFound, "Producto no encontrado.")
				return
			}
			writeJSONError(http.StatusInternalServerError, "No se pudo cargar el producto.")
			return
		}

		if newSKU == "" {
			writeJSONError(http.StatusBadRequest, "El ID es obligatorio.")
			return
		}
		parsedPrice, err := parseCOPInteger(priceValue)
		if err != nil || parsedPrice < 0 {
			writeJSONError(http.StatusBadRequest, "Precio de venta inválido.")
			return
		}
		var newRetomaPrice sql.NullFloat64
		if retomaEnabled {
			if retomaPriceValue == "" {
				writeJSONError(http.StatusBadRequest, "Valor de retoma obligatorio cuando retoma está habilitada.")
				return
			}
			parsedRetomaPrice, err := parseCOPInteger(retomaPriceValue)
			if err != nil || parsedRetomaPrice < 0 {
				writeJSONError(http.StatusBadRequest, "Valor de retoma inválido.")
				return
			}
			newRetomaPrice = sql.NullFloat64{Float64: float64(parsedRetomaPrice), Valid: true}
		}

		finalName := previous.Name
		finalLine := previous.Line
		finalLocation := locationValue
		finalOwner := previous.OwnerUserID
		finalCreditEnabled := previous.CreditEnabled == 1
		finalDebtorName := previous.DebtorName
		finalInstallmentsTotal := previous.InstallmentsTotal
		finalInstallmentsPaid := previous.InstallmentsPaid
		finalTotalValue := previous.TotalValue
		finalInstallmentValue := previous.InstallmentValue
		if isAdminRole(currentUser.Role) {
			if newName == "" {
				writeJSONError(http.StatusBadRequest, "El nombre del producto es obligatorio.")
				return
			}
			finalName = newName
			if newLine == "" {
				writeJSONError(http.StatusBadRequest, "La línea es obligatoria.")
				return
			}
			lines, err := loadBusinessLinesForTenant(db, tenantIDFromRequest(r), false)
			if err != nil {
				writeJSONError(http.StatusInternalServerError, "No se pudieron cargar las líneas.")
				return
			}
			validLine := false
			for _, line := range lines {
				if strings.EqualFold(strings.TrimSpace(line.Name), newLine) {
					validLine = true
					finalLine = line.Name
					break
				}
			}
			if !validLine {
				writeJSONError(http.StatusBadRequest, "Selecciona una línea válida.")
				return
			}
			finalOwner = sql.NullInt64{}
			if ownerUserIDRaw != "" {
				assignableUsers, err := loadAssignableUsers(db)
				if err != nil {
					writeJSONError(http.StatusInternalServerError, "No se pudieron cargar los usuarios.")
					return
				}
				validOwner := false
				for _, candidate := range assignableUsers {
					if strconv.Itoa(candidate.ID) == ownerUserIDRaw {
						finalOwner = sql.NullInt64{Int64: int64(candidate.ID), Valid: true}
						validOwner = true
						break
					}
				}
				if !validOwner {
					writeJSONError(http.StatusBadRequest, "Selecciona un usuario asignado válido.")
					return
				}
			}
			finalCreditEnabled = creditEnabledValue
			if finalCreditEnabled {
				if debtorNameValue == "" {
					writeJSONError(http.StatusBadRequest, "El nombre del deudor es obligatorio.")
					return
				}
				parsedInstallmentsTotal, err := strconv.Atoi(installmentsTotalValue)
				if err != nil || parsedInstallmentsTotal <= 0 {
					writeJSONError(http.StatusBadRequest, "La cantidad total de cuotas debe ser mayor a 0.")
					return
				}
				if previous.InstallmentsPaid > parsedInstallmentsTotal {
					writeJSONError(http.StatusBadRequest, "Las cuotas pagadas actuales superan el total indicado.")
					return
				}
				parsedTotalValue, err := parseCOPInteger(totalValueValue)
				if err != nil || parsedTotalValue <= 0 {
					writeJSONError(http.StatusBadRequest, "El valor total debe ser mayor a 0.")
					return
				}
				parsedInstallmentValue, err := parseCOPInteger(installmentValueValue)
				if err != nil || parsedInstallmentValue <= 0 {
					writeJSONError(http.StatusBadRequest, "El valor por cuota debe ser mayor a 0.")
					return
				}
				finalDebtorName = debtorNameValue
				finalInstallmentsTotal = parsedInstallmentsTotal
				finalInstallmentsPaid = previous.InstallmentsPaid
				finalTotalValue = float64(parsedTotalValue)
				finalInstallmentValue = float64(parsedInstallmentValue)
			} else {
				finalDebtorName = ""
				finalInstallmentsTotal = 0
				finalInstallmentsPaid = 0
				finalTotalValue = 0
				finalInstallmentValue = 0
			}
		}

		tx, err := db.Begin()
		if err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo iniciar la transacción.")
			return
		}
		defer tx.Rollback()

		if newSKU != previous.SKU {
			var count int
			if err := tx.QueryRow(`SELECT COUNT(*) FROM productos WHERE sku = ? AND sku <> ?`, newSKU, previous.SKU).Scan(&count); err != nil {
				writeJSONError(http.StatusInternalServerError, "No se pudo validar el ID.")
				return
			}
			if count > 0 {
				writeJSONError(http.StatusBadRequest, "Ya existe otro producto con ese ID.")
				return
			}
			if _, err := tx.Exec(`UPDATE productos SET sku = ?, id = ? WHERE sku = ?`, newSKU, newSKU, previous.SKU); err != nil {
				writeJSONError(http.StatusInternalServerError, "No se pudo actualizar el ID del producto.")
				return
			}
			if _, err := tx.Exec(`UPDATE unidades SET producto_id = ? WHERE producto_id = ?`, newSKU, previous.SKU); err != nil {
				writeJSONError(http.StatusInternalServerError, "No se pudo actualizar el inventario asociado al ID.")
				return
			}
			if _, err := tx.Exec(`UPDATE credit_installments SET product_id = ? WHERE product_id = ?`, newSKU, previous.SKU); err != nil {
				writeJSONError(http.StatusInternalServerError, "No se pudo actualizar el historial de cuotas asociado al ID.")
				return
			}
		}

		if isAdminRole(currentUser.Role) {
			if _, err := tx.Exec(`
				UPDATE productos
				SET nombre = ?, linea = ?, location = ?, owner_user_id = ?, precio_venta = ?, retoma_enabled = ?, retoma_price = ?, anotaciones = ?, credit_enabled = ?, debtor_name = ?, installments_total = ?, installments_paid = ?, total_value = ?, installment_value = ?
				WHERE sku = ?
			`, finalName, finalLine, finalLocation, finalOwner, float64(parsedPrice), boolToInt(retomaEnabled), newRetomaPrice, notesValue, boolToInt(finalCreditEnabled), finalDebtorName, finalInstallmentsTotal, finalInstallmentsPaid, finalTotalValue, finalInstallmentValue, newSKU); err != nil {
				writeJSONError(http.StatusInternalServerError, "No se pudo actualizar el producto.")
				return
			}
		} else {
			if _, err := tx.Exec(`
				UPDATE productos
				SET precio_venta = ?, retoma_enabled = ?, retoma_price = ?, anotaciones = ?, location = ?
				WHERE sku = ?
			`, float64(parsedPrice), boolToInt(retomaEnabled), newRetomaPrice, notesValue, finalLocation, newSKU); err != nil {
				writeJSONError(http.StatusInternalServerError, "No se pudo actualizar el producto.")
				return
			}
		}

		payload := map[string]any{}
		if previous.SKU != newSKU {
			payload["previous_sku"] = previous.SKU
			payload["new_sku"] = newSKU
		}
		if previous.SalePrice != float64(parsedPrice) {
			payload["previous_sale_price"] = previous.SalePrice
			payload["new_sale_price"] = float64(parsedPrice)
		}
		if (previous.RetomaEnabled == 1) != retomaEnabled {
			payload["previous_retoma_enabled"] = previous.RetomaEnabled == 1
			payload["new_retoma_enabled"] = retomaEnabled
		}
		prevRetoma := any(nil)
		if previous.RetomaPrice.Valid {
			prevRetoma = previous.RetomaPrice.Float64
		}
		newRetoma := any(nil)
		if newRetomaPrice.Valid {
			newRetoma = newRetomaPrice.Float64
		}
		if previous.RetomaPrice.Valid != newRetomaPrice.Valid || (previous.RetomaPrice.Valid && newRetomaPrice.Valid && previous.RetomaPrice.Float64 != newRetomaPrice.Float64) {
			payload["previous_retoma_price"] = prevRetoma
			payload["new_retoma_price"] = newRetoma
		}
		if previous.Notes != notesValue {
			payload["previous_notes"] = previous.Notes
			payload["new_notes"] = notesValue
		}
		if previous.Location != finalLocation {
			payload["previous_location"] = previous.Location
			payload["new_location"] = finalLocation
		}
		if isAdminRole(currentUser.Role) {
			if previous.Name != finalName {
				payload["previous_name"] = previous.Name
				payload["new_name"] = finalName
			}
			if previous.Line != finalLine {
				payload["previous_line"] = previous.Line
				payload["new_line"] = finalLine
			}
			prevOwner := any(nil)
			if previous.OwnerUserID.Valid {
				prevOwner = previous.OwnerUserID.Int64
			}
			newOwner := any(nil)
			if finalOwner.Valid {
				newOwner = finalOwner.Int64
			}
			if previous.OwnerUserID.Valid != finalOwner.Valid || (previous.OwnerUserID.Valid && finalOwner.Valid && previous.OwnerUserID.Int64 != finalOwner.Int64) {
				payload["previous_owner_user_id"] = prevOwner
				payload["new_owner_user_id"] = newOwner
			}
			if (previous.CreditEnabled == 1) != finalCreditEnabled {
				payload["previous_credit_enabled"] = previous.CreditEnabled == 1
				payload["new_credit_enabled"] = finalCreditEnabled
			}
			if previous.DebtorName != finalDebtorName {
				payload["previous_debtor_name"] = previous.DebtorName
				payload["new_debtor_name"] = finalDebtorName
			}
			if previous.InstallmentsTotal != finalInstallmentsTotal {
				payload["previous_installments_total"] = previous.InstallmentsTotal
				payload["new_installments_total"] = finalInstallmentsTotal
			}
			if previous.TotalValue != finalTotalValue {
				payload["previous_total_value"] = previous.TotalValue
				payload["new_total_value"] = finalTotalValue
			}
			if previous.InstallmentValue != finalInstallmentValue {
				payload["previous_installment_value"] = previous.InstallmentValue
				payload["new_installment_value"] = finalInstallmentValue
			}
		}
		if len(payload) > 0 {
			if err := logAuditEvent(tx, currentUser, "product_updated", "product", newSKU, "web", payload); err != nil {
				writeJSONError(http.StatusInternalServerError, "No se pudo registrar la auditoría del producto.")
				return
			}
		}
		if isAdminRole(currentUser.Role) && ((previous.CreditEnabled == 1) || finalCreditEnabled) {
			creditPayload := map[string]any{
				"product_id":         newSKU,
				"debtor_name":        finalDebtorName,
				"installments_total": finalInstallmentsTotal,
				"installments_paid":  finalInstallmentsPaid,
				"total_value":        finalTotalValue,
				"installment_value":  finalInstallmentValue,
			}
			if err := logAuditEvent(tx, currentUser, "credit_updated", "product", newSKU, "web", creditPayload); err != nil {
				writeJSONError(http.StatusInternalServerError, "No se pudo registrar la auditoría del crédito.")
				return
			}
		}

		if err := tx.Commit(); err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo confirmar la edición del producto.")
			return
		}

		productsMu.Lock()
		for idx := range products {
			if products[idx].ID == previous.SKU {
				products[idx].ID = newSKU
				if isAdminRole(currentUser.Role) {
					products[idx].Name = finalName
					products[idx].Line = finalLine
					products[idx].Location = finalLocation
					products[idx].CreditEnabled = finalCreditEnabled
					products[idx].DebtorName = finalDebtorName
					products[idx].InstallmentsTotal = finalInstallmentsTotal
					products[idx].InstallmentsPaid = finalInstallmentsPaid
					products[idx].TotalValue = finalTotalValue
					products[idx].InstallmentValue = finalInstallmentValue
					products[idx].HasOwner = finalOwner.Valid
					if finalOwner.Valid {
						products[idx].OwnerUserID = int(finalOwner.Int64)
					} else {
						products[idx].OwnerUserID = 0
					}
				}
				products[idx].SalePrice = float64(parsedPrice)
				products[idx].RetomaEnabled = retomaEnabled
				products[idx].Location = finalLocation
				products[idx].HasRetomaPrice = newRetomaPrice.Valid
				products[idx].Notes = notesValue
				if newRetomaPrice.Valid {
					products[idx].RetomaPrice = newRetomaPrice.Float64
				} else {
					products[idx].RetomaPrice = 0
				}
				break
			}
		}
		productsMu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":       true,
			"producto": newSKU,
			"mensaje":  "Producto actualizado correctamente.",
		})
	})

	mux.HandleFunc("/inventario/cuota", func(w http.ResponseWriter, r *http.Request) {
		writeJSONError := func(status int, message string) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
		}

		if r.Method != http.MethodPost {
			writeJSONError(http.StatusMethodNotAllowed, "Método no permitido.")
			return
		}
		_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantIDFromRequest(r))
		if err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo cargar la configuración de movimientos.")
			return
		}
		if !movementEnabled(movementEnabledMap, "credito") {
			writeJSONError(http.StatusForbidden, "El flujo de crédito está deshabilitado en Configuración.")
			return
		}
		if err := r.ParseForm(); err != nil {
			writeJSONError(http.StatusBadRequest, "No se pudo leer el formulario.")
			return
		}
		creditSaleIDValue := strings.TrimSpace(r.FormValue("credit_sale_id"))
		if creditSaleIDValue == "" {
			writeJSONError(http.StatusBadRequest, "Crédito inválido.")
			return
		}
		creditSaleID, err := strconv.Atoi(creditSaleIDValue)
		if err != nil || creditSaleID <= 0 {
			writeJSONError(http.StatusBadRequest, "Crédito inválido.")
			return
		}
		amountPaidValue := strings.TrimSpace(r.FormValue("amount_paid"))
		paymentType := strings.TrimSpace(r.FormValue("payment_type"))
		var amountPaid *float64
		if amountPaidValue != "" {
			parsedAmountPaid, err := strconv.ParseFloat(amountPaidValue, 64)
			if err != nil {
				writeJSONError(http.StatusBadRequest, "El valor abonado debe ser mayor a 0.")
				return
			}
			amountPaid = &parsedAmountPaid
		}

		result, err := addCreditInstallment(db, creditSaleID, amountPaid, paymentType, userFromContext(r), "web", nil)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				writeJSONError(reqErr.Status, reqErr.Message)
				return
			}
			writeJSONError(http.StatusInternalServerError, "No se pudo registrar la cuota.")
			return
		}

		message := fmt.Sprintf("Cuota %d registrada correctamente.", result.InstallmentNumber)
		if result.PaymentType == creditPaymentTypeAbono {
			message = "Abono registrado correctamente."
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":                 true,
			"credit_sale_id":     result.CreditSaleID,
			"producto_id":        result.ProductID,
			"amount_paid":        result.AmountPaid,
			"installment_number": result.InstallmentNumber,
			"payment_type":       string(result.PaymentType),
			"total_paid":         result.TotalPaid,
			"current_debt":       result.CurrentDebt,
			"mensaje":            message,
		})
	})

	mux.HandleFunc("/inventario/prestamo", func(w http.ResponseWriter, r *http.Request) {
		writeJSONError := func(status int, message string) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
		}

		if r.Method != http.MethodPost {
			writeJSONError(http.StatusMethodNotAllowed, "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			writeJSONError(http.StatusBadRequest, "No se pudo leer el formulario.")
			return
		}
		productID := strings.TrimSpace(r.FormValue("producto_id"))
		qty := parseIntOrZero(r.FormValue("cantidad"))
		input := productLoanCreateInput{
			ProductID: productID,
			Quantity:  qty,
			Customer: customerInput{
				CustomerID:     parseIntOrZero(r.FormValue("customer_id")),
				Name:           strings.TrimSpace(r.FormValue("customer_name")),
				Phone:          strings.TrimSpace(r.FormValue("customer_phone")),
				DocumentType:   strings.TrimSpace(r.FormValue("customer_document_type")),
				DocumentNumber: strings.TrimSpace(r.FormValue("customer_document_number")),
				Address:        strings.TrimSpace(r.FormValue("customer_address")),
				City:           strings.TrimSpace(r.FormValue("customer_city")),
			},
			DueAt: strings.TrimSpace(r.FormValue("due_at")),
			Notes: strings.TrimSpace(r.FormValue("notes")),
		}
		result, err := createProductLoan(db, userFromContext(r), input, "web", nil)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				writeJSONError(reqErr.Status, reqErr.Message)
				return
			}
			writeJSONError(http.StatusInternalServerError, "No se pudo registrar el préstamo.")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":              true,
			"product_loan_id": result.ProductLoanID,
			"product_id":      result.ProductID,
			"quantity":        result.Quantity,
			"status":          string(result.Status),
			"mensaje":         "Préstamo registrado correctamente.",
			"redirect_url":    "/inventario?mensaje=Prestamo%20registrado%20correctamente",
		})
	})

	mux.HandleFunc("/inventario/prestamo/cerrar", func(w http.ResponseWriter, r *http.Request) {
		writeJSONError := func(status int, message string) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
		}

		if r.Method != http.MethodPost {
			writeJSONError(http.StatusMethodNotAllowed, "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			writeJSONError(http.StatusBadRequest, "No se pudo leer el formulario.")
			return
		}
		input := productLoanCloseInput{
			ProductLoanID: parseIntOrZero(r.FormValue("product_loan_id")),
			Status:        normalizeProductLoanStatus(r.FormValue("status")),
			Notes:         strings.TrimSpace(r.FormValue("notes")),
		}
		result, err := closeProductLoan(db, userFromContext(r), input, "web", nil)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				writeJSONError(reqErr.Status, reqErr.Message)
				return
			}
			writeJSONError(http.StatusInternalServerError, "No se pudo cerrar el préstamo.")
			return
		}
		message := "Préstamo retornado correctamente."
		switch result.Status {
		case productLoanStatusPaid:
			message = "Préstamo cerrado por pago correctamente."
		case productLoanStatusCancelled:
			message = "Préstamo cancelado correctamente."
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":              true,
			"product_loan_id": result.ProductLoanID,
			"product_id":      result.ProductID,
			"status":          string(result.Status),
			"mensaje":         message,
			"redirect_url":    "/inventario?mensaje=" + url.QueryEscape(message),
		})
	})

	mux.HandleFunc("/inventario/reservar", func(w http.ResponseWriter, r *http.Request) {
		writeJSONError := func(status int, message string) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
		}

		if r.Method != http.MethodPost {
			writeJSONError(http.StatusMethodNotAllowed, "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			writeJSONError(http.StatusBadRequest, "No se pudo leer el formulario.")
			return
		}
		productID := strings.TrimSpace(r.FormValue("producto_id"))
		qtyValue := strings.TrimSpace(r.FormValue("cantidad"))
		nota := strings.TrimSpace(r.FormValue("nota"))
		qty, err := strconv.Atoi(qtyValue)
		if productID == "" || err != nil || qty <= 0 {
			writeJSONError(http.StatusBadRequest, "Datos inválidos.")
			return
		}
		allowed, err := productAccessibleByID(db, userFromContext(r), productID)
		if err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo validar acceso al producto.")
			return
		}
		if !allowed {
			writeJSONError(http.StatusForbidden, "No tienes acceso a este producto.")
			return
		}

		tx, err := db.Begin()
		if err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo iniciar la transacción.")
			return
		}
		defer tx.Rollback()

		unitIDs, err := selectAndMarkUnitsByStatus(tx, tenantIDFromRequest(r), productID, qty, "Reservada")
		if err != nil {
			if err == errInsufficientStock {
				writeJSONError(http.StatusBadRequest, "No hay stock disponible suficiente para reservar.")
				return
			}
			writeJSONError(http.StatusInternalServerError, "No se pudieron reservar unidades.")
			return
		}

		now := time.Now().Format(time.RFC3339)
		if err := logMovimientos(tx, productID, unitIDs, "reservar", nota, userFromContext(r), now); err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo registrar el movimiento.")
			return
		}

		if err := tx.Commit(); err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo confirmar la transacción.")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "producto_id": productID, "cantidad": qty})
	})

	mux.HandleFunc("/inventario/dano", func(w http.ResponseWriter, r *http.Request) {
		writeJSONError := func(status int, message string) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
		}

		if r.Method != http.MethodPost {
			writeJSONError(http.StatusMethodNotAllowed, "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			writeJSONError(http.StatusBadRequest, "No se pudo leer el formulario.")
			return
		}
		productID := strings.TrimSpace(r.FormValue("producto_id"))
		qtyValue := strings.TrimSpace(r.FormValue("cantidad"))
		nota := strings.TrimSpace(r.FormValue("nota"))
		qty, err := strconv.Atoi(qtyValue)
		if productID == "" || err != nil || qty <= 0 {
			writeJSONError(http.StatusBadRequest, "Datos inválidos.")
			return
		}
		allowed, err := productAccessibleByID(db, userFromContext(r), productID)
		if err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo validar acceso al producto.")
			return
		}
		if !allowed {
			writeJSONError(http.StatusForbidden, "No tienes acceso a este producto.")
			return
		}

		tx, err := db.Begin()
		if err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo iniciar la transacción.")
			return
		}
		defer tx.Rollback()

		unitIDs, err := selectAndMarkUnitsByStatus(tx, tenantIDFromRequest(r), productID, qty, "Danada")
		if err != nil {
			if err == errInsufficientStock {
				writeJSONError(http.StatusBadRequest, "No hay stock disponible suficiente.")
				return
			}
			writeJSONError(http.StatusInternalServerError, "No se pudo registrar el daño.")
			return
		}

		now := time.Now().Format(time.RFC3339)
		if err := logMovimientos(tx, productID, unitIDs, "dano", nota, userFromContext(r), now); err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo registrar el movimiento.")
			return
		}

		if err := tx.Commit(); err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo confirmar la transacción.")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "producto_id": productID, "cantidad": qty})
	})

	mux.HandleFunc("/inventario/retoma", func(w http.ResponseWriter, r *http.Request) {
		writeJSONError := func(status int, message string) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
		}

		currentUser := userFromContext(r)
		if currentUser == nil || !isStaffRole(currentUser.Role) {
			writeJSONError(http.StatusForbidden, "Solo personal autorizado puede registrar retomas.")
			return
		}
		if r.Method != http.MethodPost {
			writeJSONError(http.StatusMethodNotAllowed, "Método no permitido.")
			return
		}
		_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantIDFromUser(currentUser))
		if err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo cargar la configuración de movimientos.")
			return
		}
		if !movementEnabled(movementEnabledMap, "retoma") {
			writeJSONError(http.StatusForbidden, "La retoma está deshabilitada en Configuración.")
			return
		}
		if err := r.ParseForm(); err != nil {
			writeJSONError(http.StatusBadRequest, "No se pudo leer el formulario.")
			return
		}

		qty, err := strconv.Atoi(strings.TrimSpace(r.FormValue("cantidad")))
		if err != nil {
			writeJSONError(http.StatusBadRequest, "Cantidad inválida.")
			return
		}
		valueParsed, err := parseCOPInteger(strings.TrimSpace(r.FormValue("valor_recibido")))
		if err != nil {
			writeJSONError(http.StatusBadRequest, "Valor recibido inválido.")
			return
		}
		var finalSalePrice *float64
		precioPublicadoValue := strings.TrimSpace(r.FormValue("precio_publicado"))
		if precioPublicadoValue != "" {
			precioPublicadoParsed, err := parseCOPInteger(precioPublicadoValue)
			if err != nil {
				writeJSONError(http.StatusBadRequest, "Precio final de venta inválido.")
				return
			}
			value := float64(precioPublicadoParsed)
			finalSalePrice = &value
		}

		result, err := registerRetoma(db, currentUser, retomaOperationInput{
			ProductID:      strings.TrimSpace(r.FormValue("producto_id")),
			Quantity:       qty,
			ValueReceived:  float64(valueParsed),
			ReceivedState:  strings.TrimSpace(r.FormValue("estado_recibido")),
			PublishToStock: r.FormValue("publicar_stock") != "",
			FinalSalePrice: finalSalePrice,
			Notes:          strings.TrimSpace(r.FormValue("nota")),
		}, "web", nil)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				writeJSONError(reqErr.Status, reqErr.Message)
				return
			}
			writeJSONError(http.StatusInternalServerError, "No se pudo registrar la retoma.")
			return
		}
		if result.FinalSalePrice != nil {
			productsMu.Lock()
			for idx := range products {
				if products[idx].ID == result.ProductID {
					products[idx].SalePrice = *result.FinalSalePrice
					break
				}
			}
			productsMu.Unlock()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":               true,
			"retoma_id":        result.RetomaID,
			"producto_id":      result.ProductID,
			"cantidad":         result.Quantity,
			"valor_recibido":   result.ValueReceived,
			"estado":           result.ReceivedState,
			"publicado_stock":  result.PublishedToStock,
			"unidades_creadas": result.UnitsCreated,
			"mensaje":          result.Message,
		})
	})

	mux.HandleFunc("/inventario/stock", func(w http.ResponseWriter, r *http.Request) {
		writeJSONError := func(status int, message string) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
		}

		currentUser := userFromContext(r)
		if currentUser == nil || !isStaffRole(currentUser.Role) {
			writeJSONError(http.StatusForbidden, "Solo personal autorizado puede ajustar stock y precio.")
			return
		}

		if r.Method != http.MethodPost {
			writeJSONError(http.StatusMethodNotAllowed, "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			writeJSONError(http.StatusBadRequest, "No se pudo leer el formulario.")
			return
		}
		productID := strings.TrimSpace(r.FormValue("producto_id"))
		target, err := strconv.Atoi(strings.TrimSpace(r.FormValue("cantidad")))
		if err != nil {
			writeJSONError(http.StatusBadRequest, "Cantidad objetivo inválida.")
			return
		}
		var salePrice *float64
		priceValue := strings.TrimSpace(r.FormValue("precio_venta"))
		if priceValue != "" {
			parsed, err := parseCOPInteger(priceValue)
			if err != nil {
				writeJSONError(http.StatusBadRequest, "Precio de venta inválido.")
				return
			}
			value := float64(parsed)
			salePrice = &value
		}
		var name *string
		nameValue := strings.TrimSpace(r.FormValue("nombre"))
		if nameValue != "" {
			name = &nameValue
		}
		var retomaEnabled *bool
		var retomaPrice *float64
		if r.FormValue("retoma_config_present") != "" {
			value := r.FormValue("retoma_enabled") != ""
			retomaEnabled = &value
			retomaPriceValue := strings.TrimSpace(r.FormValue("retoma_price"))
			if retomaPriceValue != "" {
				parsed, err := parseCOPInteger(retomaPriceValue)
				if err != nil {
					writeJSONError(http.StatusBadRequest, "Valor de retoma inválido.")
					return
				}
				price := float64(parsed)
				retomaPrice = &price
			}
		}

		result, err := adjustInventoryProduct(db, currentUser, inventoryAdjustInput{
			ProductID:      productID,
			TargetQuantity: &target,
			Notes:          strings.TrimSpace(r.FormValue("nota")),
			SalePrice:      salePrice,
			Name:           name,
			RetomaEnabled:  retomaEnabled,
			RetomaPrice:    retomaPrice,
		}, "manual", nil)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				writeJSONError(reqErr.Status, reqErr.Message)
				return
			}
			writeJSONError(http.StatusInternalServerError, "No se pudo actualizar el inventario.")
			return
		}
		if salePrice != nil || name != nil || retomaEnabled != nil {
			productsMu.Lock()
			for idx := range products {
				if products[idx].ID != productID {
					continue
				}
				if salePrice != nil {
					products[idx].SalePrice = *salePrice
				}
				if name != nil {
					products[idx].Name = *name
				}
				if retomaEnabled != nil {
					products[idx].RetomaEnabled = *retomaEnabled
					products[idx].HasRetomaPrice = retomaPrice != nil && *retomaEnabled
					if retomaPrice != nil && *retomaEnabled {
						products[idx].RetomaPrice = *retomaPrice
					} else {
						products[idx].RetomaPrice = 0
					}
				}
				break
			}
			productsMu.Unlock()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":          true,
			"producto_id": result.ProductID,
			"actual":      result.CurrentQuantity,
			"objetivo":    result.CurrentQuantity,
			"delta":       result.Delta,
			"mensaje":     result.Message,
		})
	})

	mux.HandleFunc("/inventario/producto/eliminar", func(w http.ResponseWriter, r *http.Request) {
		writeJSONError := func(status int, message string) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
		}

		currentUser := userFromContext(r)
		if currentUser == nil || !isAdminRole(currentUser.Role) {
			writeJSONError(http.StatusForbidden, "Solo administrador puede eliminar productos.")
			return
		}
		if r.Method != http.MethodPost {
			writeJSONError(http.StatusMethodNotAllowed, "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			writeJSONError(http.StatusBadRequest, "No se pudo leer el formulario.")
			return
		}

		productID := strings.TrimSpace(r.FormValue("producto_id"))
		if productID == "" {
			writeJSONError(http.StatusBadRequest, "Producto inválido.")
			return
		}

		tx, err := db.Begin()
		if err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo iniciar la transacción.")
			return
		}
		defer tx.Rollback()

		var exists int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM productos WHERE sku = ? OR id = ?`, productID, productID).Scan(&exists); err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo validar el producto.")
			return
		}
		if exists == 0 {
			writeJSONError(http.StatusBadRequest, "Producto inválido.")
			return
		}

		if _, err := tx.Exec(`DELETE FROM unidades WHERE producto_id = ?`, productID); err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudieron eliminar las unidades del producto.")
			return
		}

		// Legacy compatibility: SQLite installs may still have historical price tables with variant column names.
		if !isPostgresDB() {
			var hasPriceHistoryTable int
			if err := tx.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'precio_venta_historial'`).Scan(&hasPriceHistoryTable); err == nil && hasPriceHistoryTable > 0 {
				histCols := map[string]bool{}
				colRows, err := tx.Query(`PRAGMA table_info('precio_venta_historial')`)
				if err == nil {
					for colRows.Next() {
						var cid int
						var name, colType string
						var notNull, pk int
						var dflt sql.NullString
						if scanErr := colRows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk); scanErr == nil {
							histCols[strings.ToLower(strings.TrimSpace(name))] = true
						}
					}
					colRows.Close()
				}

				switch {
				case histCols["producto_id"]:
					if _, err := tx.Exec(`DELETE FROM precio_venta_historial WHERE producto_id = ?`, productID); err != nil {
						writeJSONError(http.StatusInternalServerError, "No se pudo limpiar el historial de precio del producto.")
						return
					}
				case histCols["product_id"]:
					if _, err := tx.Exec(`DELETE FROM precio_venta_historial WHERE product_id = ?`, productID); err != nil {
						writeJSONError(http.StatusInternalServerError, "No se pudo limpiar el historial de precio del producto.")
						return
					}
				case histCols["producto_sku"]:
					if _, err := tx.Exec(`DELETE FROM precio_venta_historial WHERE producto_sku = ?`, productID); err != nil {
						writeJSONError(http.StatusInternalServerError, "No se pudo limpiar el historial de precio del producto.")
						return
					}
				case histCols["sku"]:
					if _, err := tx.Exec(`DELETE FROM precio_venta_historial WHERE sku = ?`, productID); err != nil {
						writeJSONError(http.StatusInternalServerError, "No se pudo limpiar el historial de precio del producto.")
						return
					}
				}
			}
		}

		res, err := tx.Exec(`DELETE FROM productos WHERE sku = ? OR id = ?`, productID, productID)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "foreign key") {
				writeJSONError(http.StatusBadRequest, "No se pudo eliminar el producto porque tiene referencias activas.")
				return
			}
			writeJSONError(http.StatusInternalServerError, "No se pudo eliminar el producto.")
			return
		}
		affected, err := res.RowsAffected()
		if err != nil || affected == 0 {
			writeJSONError(http.StatusBadRequest, "No se pudo confirmar la eliminación del producto.")
			return
		}

		if err := tx.Commit(); err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo confirmar la transacción.")
			return
		}

		productsMu.Lock()
		filtered := make([]productOption, 0, len(products))
		for _, p := range products {
			if p.ID == productID {
				continue
			}
			filtered = append(filtered, p)
		}
		products = filtered
		productsMu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":          true,
			"producto_id": productID,
			"mensaje":     "Producto eliminado correctamente.",
		})
	})

	mux.HandleFunc("/productos/historial", func(w http.ResponseWriter, r *http.Request) {
		productID := strings.TrimSpace(r.URL.Query().Get("producto_id"))
		if productID == "" {
			http.Error(w, "Falta producto_id", http.StatusBadRequest)
			return
		}
		allowed, err := productAccessibleByID(db, userFromContext(r), productID)
		if err != nil {
			http.Error(w, "No se pudo validar acceso al producto", http.StatusInternalServerError)
			return
		}
		if !allowed {
			http.Error(w, "No tienes acceso a este producto", http.StatusForbidden)
			return
		}

		type movimientoRow struct {
			UnidadID string `json:"unidad_id"`
			Tipo     string `json:"tipo"`
			Nota     string `json:"nota"`
			Usuario  string `json:"usuario"`
			Fecha    string `json:"fecha"`
		}
		rows, err := db.Query(`
			SELECT unidad_id, tipo, nota, usuario, fecha
			FROM movimientos
			WHERE producto_id = ?
			ORDER BY fecha DESC
			LIMIT 60
		`, productID)
		if err != nil {
			http.Error(w, "Error al consultar historial", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		movs := []movimientoRow{}
		for rows.Next() {
			var m movimientoRow
			if err := rows.Scan(&m.UnidadID, &m.Tipo, &m.Nota, &m.Usuario, &m.Fecha); err != nil {
				http.Error(w, "Error al leer historial", http.StatusInternalServerError)
				return
			}
			m.Fecha = formatDateWithSettings(m.Fecha)
			movs = append(movs, m)
		}
		if err := rows.Err(); err != nil {
			http.Error(w, "Error al procesar historial", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "producto_id": productID, "movimientos": movs})
	})

	mux.HandleFunc("/api/productos/precio", func(w http.ResponseWriter, r *http.Request) {
		sku := strings.TrimSpace(r.URL.Query().Get("id"))
		if sku == "" {
			sku = strings.TrimSpace(r.URL.Query().Get("sku"))
		}
		if sku == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "Falta id."})
			return
		}
		allowed, err := productAccessibleByID(db, userFromContext(r), sku)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "No se pudo validar acceso al producto."})
			return
		}
		if !allowed {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "No tienes acceso a este producto."})
			return
		}

		var precioVenta float64
		err = db.QueryRow(`SELECT COALESCE(precio_venta, 0) FROM productos WHERE sku = ?`, sku).Scan(&precioVenta)
		if err != nil {
			if err == sql.ErrNoRows {
				precioVenta = 0
			} else {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "No se pudo consultar el precio."})
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "id": sku, "sku": sku, "precio_venta": precioVenta})
	})

	mux.HandleFunc("/api/settings/business", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		settings, err := apiBusinessSettingsForRequest(db, r)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo cargar la configuracion del negocio.", nil)
			return
		}
		tenant := tenantFromContext(r)
		tenantSlug := ""
		tenantName := ""
		if tenant != nil {
			tenantSlug = strings.TrimSpace(tenant.Slug)
			tenantName = strings.TrimSpace(tenant.Name)
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{
			"ok": true,
			"settings": map[string]any{
				"business_name": settings.BusinessName,
				"logo_path":     settings.LogoPath,
				"primary_color": settings.PrimaryColor,
				"currency":      settings.Currency,
				"date_format":   settings.DateFormat,
			},
			"tenant": map[string]any{
				"id":   normalizeTenantID(tenantIDFromRequest(r)),
				"slug": tenantSlug,
				"name": tenantName,
			},
		})
	})

	mux.HandleFunc("/api/settings/lines", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		activeOnly := true
		if user := userFromContext(r); user != nil && isAdminRole(user.Role) {
			includeInactive := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("include_inactive")))
			if includeInactive == "1" || includeInactive == "true" {
				activeOnly = false
			}
		}
		lines, err := loadBusinessLinesForTenant(db, tenantIDFromRequest(r), activeOnly)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar las líneas.", nil)
			return
		}
		items := make([]map[string]any, 0, len(lines))
		for _, line := range lines {
			items = append(items, map[string]any{
				"id":         line.ID,
				"name":       line.Name,
				"active":     line.Active,
				"created_at": line.CreatedAt,
				"updated_at": line.UpdatedAt,
			})
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "items": items, "count": len(items)})
	})

	mux.HandleFunc("/api/settings/owners", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		currentUser := userFromContext(r)
		if currentUser == nil || !isAdminRole(currentUser.Role) {
			writeAPIError(w, http.StatusForbidden, "Solo administrador puede consultar owners asignables.", nil)
			return
		}
		users, err := apiAssignableUsersForRequest(db, r)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar los usuarios asignables.", nil)
			return
		}
		items := make([]map[string]any, 0, len(users))
		for _, user := range users {
			items = append(items, map[string]any{
				"id":       user.ID,
				"username": user.Username,
			})
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "items": items, "count": len(items)})
	})

	mux.HandleFunc("/api/agent/business", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		settings, err := apiBusinessSettingsForRequest(db, r)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo cargar la configuracion del negocio.", nil)
			return
		}
		tenant := tenantFromContext(r)
		tenantSlug := ""
		tenantName := ""
		if tenant != nil {
			tenantSlug = strings.TrimSpace(tenant.Slug)
			tenantName = strings.TrimSpace(tenant.Name)
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{
			"ok": true,
			"item": map[string]any{
				"business_name": settings.BusinessName,
				"primary_color": settings.PrimaryColor,
				"currency":      settings.Currency,
				"date_format":   settings.DateFormat,
				"tenant": map[string]any{
					"id":   normalizeTenantID(tenantIDFromRequest(r)),
					"slug": tenantSlug,
					"name": tenantName,
				},
				"auth_mode":        apiAuthModeFromContext(r),
				"integration_name": apiIntegrationNameFromContext(r),
			},
		})
	})

	mux.HandleFunc("/api/agent/products/search", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		q := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("q")))
		currentUser := userFromContext(r)
		productsSnapshot, err := loadVisibleProductsForUser(db, userFromContext(r))
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar los productos.", nil)
			return
		}
		productsSnapshot = filterProductsForUser(productsSnapshot, currentUser)
		productIDs := make([]string, 0, len(productsSnapshot))
		filtered := make([]productOption, 0, len(productsSnapshot))
		for _, product := range productsSnapshot {
			if q != "" {
				haystack := strings.ToLower(product.ID + " " + product.Name + " " + product.Line + " " + product.Location + " " + product.DebtorName)
				if !strings.Contains(haystack, q) {
					continue
				}
			}
			filtered = append(filtered, product)
			productIDs = append(productIDs, product.ID)
		}
		countsByProduct, err := loadInventoryCountsForProducts(db, tenantIDFromRequest(r), productIDs)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo consultar el inventario.", nil)
			return
		}
		includeOwner := currentUser != nil && isAdminRole(currentUser.Role)
		items := make([]map[string]any, 0, len(filtered))
		for _, product := range filtered {
			items = append(items, agentProductItem(product, countsByProduct[product.ID], includeOwner))
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "items": items, "count": len(items)})
	})

	mux.HandleFunc("/api/agent/customers/search", handleAPIAgentCustomerSearch(db))
	mux.HandleFunc("/api/agent/credits", handleAPIAgentCredits(db))

	mux.HandleFunc("/api/agent/products/price", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		productID := strings.TrimSpace(r.URL.Query().Get("id"))
		if productID == "" {
			writeAPIError(w, http.StatusBadRequest, "Falta id.", nil)
			return
		}
		productsSnapshot, err := loadVisibleProductsForUser(db, userFromContext(r))
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar los productos.", nil)
			return
		}
		productsSnapshot = filterProductsForUser(productsSnapshot, userFromContext(r))
		product, ok := findVisibleProduct(productsSnapshot, productID)
		if !ok {
			writeAPIError(w, http.StatusNotFound, "Producto no encontrado.", nil)
			return
		}
		var retomaPrice any = nil
		if product.HasRetomaPrice {
			retomaPrice = product.RetomaPrice
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{
			"ok": true,
			"item": map[string]any{
				"id":             product.ID,
				"name":           product.Name,
				"location":       product.Location,
				"sale_price":     product.SalePrice,
				"retoma_enabled": product.RetomaEnabled,
				"retoma_price":   retomaPrice,
			},
		})
	})

	mux.HandleFunc("/api/agent/inventory", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		q := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("q")))
		currentUser := userFromContext(r)
		productsSnapshot, err := loadVisibleProductsForUser(db, userFromContext(r))
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo cargar el inventario.", nil)
			return
		}
		productsSnapshot = filterProductsForUser(productsSnapshot, currentUser)
		productIDs := make([]string, 0, len(productsSnapshot))
		filtered := make([]productOption, 0, len(productsSnapshot))
		for _, product := range productsSnapshot {
			if q != "" {
				haystack := strings.ToLower(product.ID + " " + product.Name + " " + product.Line + " " + product.Location + " " + product.DebtorName)
				if !strings.Contains(haystack, q) {
					continue
				}
			}
			filtered = append(filtered, product)
			productIDs = append(productIDs, product.ID)
		}
		countsByProduct, err := loadInventoryCountsForProducts(db, tenantIDFromRequest(r), productIDs)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo consultar el inventario.", nil)
			return
		}
		includeOwner := currentUser != nil && isAdminRole(currentUser.Role)
		items := make([]map[string]any, 0, len(filtered))
		for _, product := range filtered {
			items = append(items, agentProductItem(product, countsByProduct[product.ID], includeOwner))
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "items": items, "count": len(items)})
	})

	mux.HandleFunc("/api/products/search", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		q := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("q")))
		productsSnapshot, err := loadVisibleProductsForUser(db, userFromContext(r))
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar los productos.", nil)
			return
		}
		productsSnapshot = filterProductsForUser(productsSnapshot, userFromContext(r))
		items := make([]map[string]any, 0, len(productsSnapshot))
		for _, product := range productsSnapshot {
			if q != "" {
				haystack := strings.ToLower(product.ID + " " + product.Name + " " + product.Line + " " + product.Location + " " + product.DebtorName)
				if !strings.Contains(haystack, q) {
					continue
				}
			}
			var owner any = nil
			if product.HasOwner {
				owner = product.OwnerUserID
			}
			var retomaPrice any = nil
			if product.HasRetomaPrice {
				retomaPrice = product.RetomaPrice
			}
			items = append(items, map[string]any{
				"id":             product.ID,
				"name":           product.Name,
				"line":           product.Line,
				"location":       product.Location,
				"fecha_ingreso":  formatDateWithSettings(product.FechaIngreso),
				"sale_price":     product.SalePrice,
				"retoma_enabled": product.RetomaEnabled,
				"retoma_price":   retomaPrice,
				"owner_user_id":  owner,
			})
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "items": items, "count": len(items)})
	})

	mux.HandleFunc("/api/inventory", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		productsSnapshot, err := loadVisibleProductsForUser(db, userFromContext(r))
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo cargar el inventario.", nil)
			return
		}
		productsSnapshot = filterProductsForUser(productsSnapshot, userFromContext(r))
		items := make([]map[string]any, 0, len(productsSnapshot))
		for _, product := range productsSnapshot {
			var available, reserved, swapped, damaged int
			rows, err := db.Query(`SELECT estado, COUNT(*) FROM unidades WHERE tenant_id = ? AND producto_id = ? GROUP BY estado`, tenantIDFromRequest(r), product.ID)
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudo consultar el inventario.", nil)
				return
			}
			for rows.Next() {
				var estado string
				var count int
				if err := rows.Scan(&estado, &count); err != nil {
					rows.Close()
					writeAPIError(w, http.StatusInternalServerError, "No se pudo leer el inventario.", nil)
					return
				}
				switch estado {
				case "Disponible", "available":
					available = count
				case "Reservada", "reserved":
					reserved = count
				case "Cambio", "swapped":
					swapped = count
				case "Danada", "Dañada", "damaged":
					damaged = count
				}
			}
			rows.Close()
			var owner any = nil
			if product.HasOwner {
				owner = product.OwnerUserID
			}
			var retomaPrice any = nil
			if product.HasRetomaPrice {
				retomaPrice = product.RetomaPrice
			}
			items = append(items, map[string]any{
				"id":             product.ID,
				"name":           product.Name,
				"line":           product.Line,
				"location":       product.Location,
				"available":      available,
				"reserved":       reserved,
				"swapped":        swapped,
				"damaged":        damaged,
				"sale_price":     product.SalePrice,
				"retoma_enabled": product.RetomaEnabled,
				"retoma_price":   retomaPrice,
				"owner_user_id":  owner,
			})
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "items": items, "count": len(items)})
	})

	mux.HandleFunc("/api/inventory/adjust", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		currentUser := userFromContext(r)
		if currentUser == nil || !isStaffRole(currentUser.Role) {
			writeAPIError(w, http.StatusForbidden, "Solo personal autorizado puede ajustar stock y precio.", nil)
			return
		}
		var payload struct {
			ProductID      string   `json:"product_id"`
			TargetQuantity *int     `json:"target_quantity"`
			Notes          string   `json:"notes"`
			SalePrice      *float64 `json:"sale_price"`
			Name           *string  `json:"name"`
			RetomaEnabled  *bool    `json:"retoma_enabled"`
			RetomaPrice    *float64 `json:"retoma_price"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeAPIError(w, http.StatusBadRequest, "JSON inválido.", nil)
			return
		}
		result, err := adjustInventoryProduct(db, currentUser, inventoryAdjustInput{
			ProductID:      payload.ProductID,
			TargetQuantity: payload.TargetQuantity,
			Notes:          payload.Notes,
			SalePrice:      payload.SalePrice,
			Name:           payload.Name,
			RetomaEnabled:  payload.RetomaEnabled,
			RetomaPrice:    payload.RetomaPrice,
		}, "api", func(item map[string]any) map[string]any {
			return withAPIAuditMetadata(r, item)
		})
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				writeAPIError(w, reqErr.Status, reqErr.Message, reqErr.Fields)
				return
			}
			writeAPIError(w, http.StatusInternalServerError, "No se pudo actualizar el inventario.", nil)
			return
		}
		if payload.SalePrice != nil || payload.Name != nil || payload.RetomaEnabled != nil {
			productsMu.Lock()
			for idx := range products {
				if products[idx].ID != result.ProductID {
					continue
				}
				if payload.SalePrice != nil {
					products[idx].SalePrice = *payload.SalePrice
				}
				if payload.Name != nil {
					products[idx].Name = strings.TrimSpace(*payload.Name)
				}
				if payload.RetomaEnabled != nil {
					products[idx].RetomaEnabled = *payload.RetomaEnabled
					products[idx].HasRetomaPrice = payload.RetomaPrice != nil && *payload.RetomaEnabled
					if payload.RetomaPrice != nil && *payload.RetomaEnabled {
						products[idx].RetomaPrice = *payload.RetomaPrice
					} else {
						products[idx].RetomaPrice = 0
					}
				}
				break
			}
			productsMu.Unlock()
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{
			"ok":                true,
			"product_id":        result.ProductID,
			"previous_quantity": result.PreviousQuantity,
			"current_quantity":  result.CurrentQuantity,
			"delta":             result.Delta,
			"message":           result.Message,
		})
	})

	mux.HandleFunc("/api/sales/recent", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		items, err := listRecentSalesForUser(db, userFromContext(r), 50)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar las ventas.", nil)
			return
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "items": items, "count": len(items)})
	})

	mux.HandleFunc("/api/retomas", handleAPIRetomas(db, func(productID string, salePrice float64) {
		productsMu.Lock()
		defer productsMu.Unlock()
		for idx := range products {
			if products[idx].ID == productID {
				products[idx].SalePrice = salePrice
				break
			}
		}
	}))

	mux.HandleFunc("/api/customers", handleAPICustomers(db))

	mux.HandleFunc("/api/customers/", handleAPICustomerRoutes(db))

	mux.HandleFunc("/api/invoices", handleAPIInvoices(db))

	mux.HandleFunc("/api/invoices/", handleAPIInvoiceRoutes(db))

	mux.HandleFunc("/api/users", handleAPIUsers(db, usersCols))

	mux.HandleFunc("/api/users/", handleAPIUserRoutes(db, usersCols))

	mux.HandleFunc("/api/agent/invoices", handleAPIAgentInvoices(db))

	mux.HandleFunc("/api/credits", handleAPICredits(db))

	mux.HandleFunc("/api/credits/edited", handleAPICreditsEditedReport(db))

	mux.HandleFunc("/api/credits/", handleAPICreditRoutes(db))

	mux.HandleFunc("/api/credits/installments", handleAPICreditInstallments(db))

	mux.HandleFunc("/venta/new", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantIDFromUser(currentUser))
		if err != nil {
			http.Error(w, "Error al cargar tipos de movimiento", http.StatusInternalServerError)
			return
		}
		if !movementEnabled(movementEnabledMap, "venta") {
			redirectWithMessage(w, r, "/inventario", "", "La venta está deshabilitada en Configuración.")
			return
		}
		activePaymentMethods, err := loadPaymentMethodsForTenant(db, tenantIDFromUser(currentUser), true)
		if err != nil {
			http.Error(w, "Error al cargar métodos de pago", http.StatusInternalServerError)
			return
		}
		paymentMethodNamesActive := paymentMethodNames(activePaymentMethods)

		productsSnapshot, err := loadVisibleProductsForUser(db, currentUser)
		if err != nil {
			http.Error(w, "Error al cargar productos", http.StatusInternalServerError)
			return
		}

		productID := r.URL.Query().Get("producto_id")
		if productID == "" && len(productsSnapshot) > 0 {
			productID = productsSnapshot[0].ID
		}
		cantidad := 1
		if qty := r.URL.Query().Get("cantidad"); qty != "" {
			if parsed, err := strconv.Atoi(qty); err == nil && parsed > 0 {
				cantidad = parsed
			}
		}

		selectedProduct, ok := findProduct(productsSnapshot, productID)
		if !ok && len(productsSnapshot) > 0 {
			selectedProduct = productsSnapshot[0]
			productID = selectedProduct.ID
		}
		if len(productsSnapshot) == 0 {
			redirectWithMessage(w, r, "/inventario", "", "No tienes productos disponibles para vender.")
			return
		}

		stockByProd, err := availableCountsByProduct(db, tenantIDFromUser(currentUser))
		if err != nil {
			http.Error(w, "Error al consultar stock", http.StatusInternalServerError)
			return
		}
		if available := stockByProd[productID]; available > 0 && cantidad > available {
			cantidad = available
		}

		defaultMethod := ""
		if len(paymentMethodNamesActive) > 0 {
			defaultMethod = paymentMethodNamesActive[0]
		}

		data := ventaFormData{
			Title:       "Registrar venta",
			ProductoID:  productID,
			ProductoNom: selectedProduct.Name,
			Productos:   productsSnapshot,
			StockByProd: stockByProd,
			Cantidad:    cantidad,
			MetodoPago:  defaultMethod,
			MetodoPagos: paymentMethodNamesActive,
			CurrentUser: currentUser,
		}

		renderTemplate(w, "venta_new.html", data, "Error al renderizar el template")
	})

	mux.HandleFunc("/venta/comprobante", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		if r.Method != http.MethodGet {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}

		saleID, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("sale_id")))
		if err != nil || saleID <= 0 {
			http.Error(w, "Venta inválida", http.StatusBadRequest)
			return
		}

		data, err := loadSaleReceiptData(db, currentUser, saleID)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				http.Error(w, reqErr.Message, reqErr.Status)
				return
			}
			http.Error(w, "No se pudo generar el comprobante.", http.StatusInternalServerError)
			return
		}

		buyerName := strings.TrimSpace(r.URL.Query().Get("buyer_name"))
		buyerDocument := strings.TrimSpace(r.URL.Query().Get("buyer_document"))
		if buyerName == "" || buyerDocument == "" {
			data.NeedsBuyerData = true
			data.BuyerName = buyerName
			data.BuyerDocument = buyerDocument
			data.DownloadURL = saleReceiptDownloadURL(saleID)
			var buf bytes.Buffer
			if err := tmpl.ExecuteTemplate(&buf, "sale_receipt.html", data); err != nil {
				http.Error(w, "Error al renderizar el comprobante", http.StatusInternalServerError)
				return
			}
			_, _ = w.Write(buf.Bytes())
			return
		}
		data.BuyerName = buyerName
		data.BuyerDocument = buyerDocument
		data.DownloadURL = saleReceiptDownloadURLWithBuyer(saleID, buyerName, buyerDocument)

		download := r.URL.Query().Get("download") == "1"
		if download {
			filename := fmt.Sprintf("comprobante-venta-%d.html", saleID)
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
		}

		if auditErr := logAuditEvent(db, currentUser, "sale_receipt_generated", "sale", strconv.Itoa(saleID), "web", map[string]any{
			"sale_id":        saleID,
			"product_id":     data.ProductoID,
			"receipt_number": data.ReceiptNumber,
			"buyer_name":     buyerName,
			"buyer_document": buyerDocument,
			"download":       download,
		}); auditErr != nil {
			log.Printf("audit sale receipt generated: %v", auditErr)
		}

		var buf bytes.Buffer
		if err := tmpl.ExecuteTemplate(&buf, "sale_receipt.html", data); err != nil {
			http.Error(w, "Error al renderizar el comprobante", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(buf.Bytes())
	})

	mux.HandleFunc("/venta/ticket", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		if r.Method != http.MethodGet {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}

		saleID, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("sale_id")))
		if err != nil || saleID <= 0 {
			http.Error(w, "Venta inválida", http.StatusBadRequest)
			return
		}

		data, err := loadSaleReceiptData(db, currentUser, saleID)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				http.Error(w, reqErr.Message, reqErr.Status)
				return
			}
			http.Error(w, "No se pudo generar el ticket térmico.", http.StatusInternalServerError)
			return
		}
		paperValue := strings.TrimSpace(r.URL.Query().Get("paper"))
		if paperValue == "" {
			paperValue = settingsForUser(currentUser).TicketPaperWidth
		}
		paperKey, paperWidthMM, paperDPI, paperClass := thermalPaperDimensions(paperValue)
		data.PaperSize = paperKey
		data.PaperWidthMM = paperWidthMM
		data.PaperDPI = paperDPI
		data.PaperClass = paperClass

		buyerName := strings.TrimSpace(r.URL.Query().Get("buyer_name"))
		buyerDocument := strings.TrimSpace(r.URL.Query().Get("buyer_document"))
		if buyerName == "" || buyerDocument == "" {
			data.NeedsBuyerData = true
			data.BuyerName = buyerName
			data.BuyerDocument = buyerDocument
			data.DownloadURL = saleReceiptDownloadURL(saleID)
			data.ThermalURL = saleThermalTicketViewURL(saleID)
			var buf bytes.Buffer
			if err := tmpl.ExecuteTemplate(&buf, "sale_ticket_thermal.html", data); err != nil {
				http.Error(w, "Error al renderizar el ticket térmico", http.StatusInternalServerError)
				return
			}
			_, _ = w.Write(buf.Bytes())
			return
		}

		data.BuyerName = buyerName
		data.BuyerDocument = buyerDocument
		data.DownloadURL = saleReceiptDownloadURLWithBuyer(saleID, buyerName, buyerDocument)
		data.ThermalURL = saleThermalTicketViewURLWithBuyer(saleID, buyerName, buyerDocument)

		if auditErr := logAuditEvent(db, currentUser, "sale_receipt_generated", "sale", strconv.Itoa(saleID), "web", map[string]any{
			"sale_id":        saleID,
			"product_id":     data.ProductoID,
			"receipt_number": data.ReceiptNumber,
			"buyer_name":     buyerName,
			"buyer_document": buyerDocument,
			"format":         "thermal",
		}); auditErr != nil {
			log.Printf("audit thermal sale ticket generated: %v", auditErr)
		}

		var buf bytes.Buffer
		if err := tmpl.ExecuteTemplate(&buf, "sale_ticket_thermal.html", data); err != nil {
			http.Error(w, "Error al renderizar el ticket térmico", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(buf.Bytes())
	})

	mux.HandleFunc("/facturas/nueva", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		if currentUser == nil || !isStaffRole(currentUser.Role) {
			http.Error(w, "No autorizado", http.StatusForbidden)
			return
		}

		parseIDs := func(req *http.Request) (int, int) {
			return parseIntOrZero(req.FormValue("sale_id")), parseIntOrZero(req.FormValue("credit_sale_id"))
		}
		buildInput := func(req *http.Request) invoiceCreateInput {
			saleID, creditSaleID := parseIDs(req)
			return invoiceCreateInput{
				SaleID:       saleID,
				CreditSaleID: creditSaleID,
				Customer: customerInput{
					CustomerID:     parseIntOrZero(req.FormValue("customer_id")),
					Name:           strings.TrimSpace(req.FormValue("customer_name")),
					Phone:          strings.TrimSpace(req.FormValue("customer_phone")),
					DocumentType:   strings.TrimSpace(req.FormValue("customer_document_type")),
					DocumentNumber: strings.TrimSpace(req.FormValue("customer_document_number")),
					Address:        strings.TrimSpace(req.FormValue("customer_address")),
					City:           strings.TrimSpace(req.FormValue("customer_city")),
				},
				Notes: strings.TrimSpace(req.FormValue("notes")),
			}
		}
		renderForm := func(status int, input invoiceCreateInput, flash, errText string) {
			data, err := loadInvoiceFormData(db, currentUser, input, flash, errText)
			if err != nil {
				var reqErr requestError
				if errors.As(err, &reqErr) {
					http.Error(w, reqErr.Message, reqErr.Status)
					return
				}
				http.Error(w, "No se pudo cargar la factura.", http.StatusInternalServerError)
				return
			}
			var buf bytes.Buffer
			if err := tmpl.ExecuteTemplate(&buf, "invoice_new.html", data); err != nil {
				http.Error(w, "Error al renderizar la factura", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(status)
			_, _ = w.Write(buf.Bytes())
		}

		switch r.Method {
		case http.MethodGet:
			input := invoiceCreateInput{
				SaleID:       parseIntOrZero(r.URL.Query().Get("sale_id")),
				CreditSaleID: parseIntOrZero(r.URL.Query().Get("credit_sale_id")),
			}
			renderForm(http.StatusOK, input, strings.TrimSpace(r.URL.Query().Get("mensaje")), "")
		case http.MethodPost:
			if err := r.ParseForm(); err != nil {
				http.Error(w, "Formulario inválido", http.StatusBadRequest)
				return
			}
			input := buildInput(r)
			item, _, err := createInvoiceDocument(db, currentUser, input, "web", nil)
			if err != nil {
				var reqErr requestError
				if errors.As(err, &reqErr) {
					renderForm(reqErr.Status, input, "", reqErr.Message)
					return
				}
				http.Error(w, "No se pudo generar la factura.", http.StatusInternalServerError)
				return
			}
			invoiceID := parseIntOrZero(fmt.Sprint(item["id"]))
			http.Redirect(w, r, invoiceViewURL(invoiceID), http.StatusSeeOther)
		default:
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/facturas/", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		if r.Method != http.MethodGet {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}
		path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/facturas/"), "/")
		if path == "" || path == "nueva" {
			http.NotFound(w, r)
			return
		}
		invoiceID, err := strconv.Atoi(path)
		if err != nil || invoiceID <= 0 {
			http.Error(w, "Factura inválida", http.StatusBadRequest)
			return
		}
		data, err := loadInvoiceViewDataForUser(db, currentUser, invoiceID)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				http.Error(w, reqErr.Message, reqErr.Status)
				return
			}
			http.Error(w, "No se pudo cargar la factura.", http.StatusInternalServerError)
			return
		}
		paperValue := strings.TrimSpace(r.URL.Query().Get("paper"))
		if paperValue == "" {
			paperValue = settingsForUser(currentUser).InvoicePaperWidth
		}
		paperKey, paperWidthMM, paperDPI, paperClass := thermalPaperDimensions(paperValue)
		data.PaperSize = paperKey
		data.PaperWidthMM = paperWidthMM
		data.PaperDPI = paperDPI
		data.PaperClass = paperClass
		data.Title = "Factura operativa"
		data.Subtitle = "Documento simple para operación y soporte."
		renderTemplate(w, "invoice_document.html", data, "Error al renderizar la factura")
	})

	mux.HandleFunc("/cambio/new", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantIDFromUser(currentUser))
		if err != nil {
			http.Error(w, "Error al cargar tipos de movimiento", http.StatusInternalServerError)
			return
		}
		if !movementEnabled(movementEnabledMap, "cambio") {
			redirectWithMessage(w, r, "/inventario", "", "El cambio está deshabilitado en Configuración.")
			return
		}
		productsSnapshot, err := loadVisibleProductsForUser(db, currentUser)
		if err != nil {
			http.Error(w, "Error al cargar productos", http.StatusInternalServerError)
			return
		}
		if len(productsSnapshot) == 0 {
			redirectWithMessage(w, r, "/inventario", "", "No tienes productos disponibles para cambio.")
			return
		}

		productID := r.URL.Query().Get("producto_id")
		if productID == "" {
			productID = productsSnapshot[0].ID
		}
		cantidad := 1
		if qty := r.URL.Query().Get("cantidad"); qty != "" {
			if parsed, err := strconv.Atoi(qty); err == nil && parsed > 0 {
				cantidad = parsed
			}
		}

		selectedProduct, ok := findProduct(productsSnapshot, productID)
		if !ok {
			selectedProduct = productsSnapshot[0]
			productID = selectedProduct.ID
		}

		availableUnits, err := availableUnitsByProduct(db, tenantIDFromUser(currentUser), productID)
		if err != nil {
			http.Error(w, "Error al consultar unidades disponibles", http.StatusInternalServerError)
			return
		}

		salientes := make([]string, 0, cantidad)
		for i := 0; i < cantidad && i < len(availableUnits); i++ {
			salientes = append(salientes, availableUnits[i].ID)
		}

		data := cambioFormData{
			Title:               "Registrar cambio",
			ProductoID:          productID,
			Productos:           productsSnapshot,
			Unidades:            availableUnits,
			Salientes:           salientes,
			SalientesMap:        buildSalientesMap(salientes),
			IncomingMode:        "existing",
			IncomingExistingID:  productsSnapshot[0].ID,
			IncomingExistingQty: cantidad,
			CurrentUser:         currentUser,
		}

		renderTemplate(w, "cambio_new.html", data, "Error al renderizar el template")
	})

	mux.HandleFunc("/retoma/new", func(w http.ResponseWriter, r *http.Request) {
		_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantIDFromRequest(r))
		if err != nil {
			http.Error(w, "Error al cargar tipos de movimiento", http.StatusInternalServerError)
			return
		}
		if !movementEnabled(movementEnabledMap, "retoma") {
			redirectWithMessage(w, r, "/inventario", "", "La retoma está deshabilitada en Configuración.")
			return
		}
		productID := strings.TrimSpace(r.URL.Query().Get("producto_id"))
		if productID != "" {
			allowed, err := productAccessibleByID(db, userFromContext(r), productID)
			if err != nil {
				http.Error(w, "No se pudo validar acceso al producto", http.StatusInternalServerError)
				return
			}
			if !allowed {
				redirectWithMessage(w, r, "/inventario", "", "No tienes acceso a este producto.")
				return
			}
		}
		redirectWithMessage(w, r, "/inventario", "Retoma habilitada. El flujo detallado queda pendiente para una tarea posterior.", "")
	})

	mux.HandleFunc("/api/sales", handleAPISales(db))

	mux.HandleFunc("/api/swaps", handleAPISwaps(db))

	mux.HandleFunc("/venta", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		wantsJSON := strings.Contains(r.Header.Get("Accept"), "application/json") || r.Header.Get("X-Requested-With") == "XMLHttpRequest"
		_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantIDFromUser(currentUser))
		if err != nil {
			http.Error(w, "Error al cargar tipos de movimiento", http.StatusInternalServerError)
			return
		}
		if !movementEnabled(movementEnabledMap, "venta") {
			if wantsJSON {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "La venta está deshabilitada en Configuración."})
				return
			}
			redirectWithMessage(w, r, "/inventario", "", "La venta está deshabilitada en Configuración.")
			return
		}
		activePaymentMethods, err := loadPaymentMethodsForTenant(db, tenantIDFromUser(currentUser), true)
		if err != nil {
			if wantsJSON {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "No se pudieron cargar los métodos de pago."})
				return
			}
			http.Error(w, "No se pudieron cargar los métodos de pago", http.StatusInternalServerError)
			return
		}
		paymentMethodOptions := paymentMethodNames(activePaymentMethods)

		writeJSONError := func(status int, message string, fields map[string]string) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":     false,
				"error":  message,
				"fields": fields,
			})
		}

		if r.Method != http.MethodPost {
			http.Redirect(w, r, "/venta/new", http.StatusSeeOther)
			return
		}

		productsSnapshot, err := loadVisibleProductsForUser(db, currentUser)
		if err != nil {
			if wantsJSON {
				writeJSONError(http.StatusInternalServerError, "No se pudieron cargar los productos.", nil)
				return
			}
			http.Error(w, "No se pudieron cargar los productos", http.StatusInternalServerError)
			return
		}

		stockByProd, err := availableCountsByProduct(db, tenantIDFromUser(currentUser))
		if err != nil {
			if wantsJSON {
				writeJSONError(http.StatusInternalServerError, "Error al consultar stock.", nil)
				return
			}
			http.Error(w, "Error al consultar stock", http.StatusInternalServerError)
			return
		}

		if err := r.ParseForm(); err != nil {
			if wantsJSON {
				writeJSONError(http.StatusBadRequest, "No se pudo leer el formulario.", nil)
				return
			}
			http.Error(w, "No se pudo leer el formulario", http.StatusBadRequest)
			return
		}

		productID := r.FormValue("producto_id")
		saleMode := strings.TrimSpace(r.FormValue("sale_mode"))
		if saleMode == "" {
			saleMode = "normal"
		}
		qtyValue := r.FormValue("cantidad")
		precioValue := r.FormValue("precio_final_venta")
		valorVentaFinalValue := r.FormValue("valor_venta_final")
		metodoPago := r.FormValue("metodo_pago")
		notas := r.FormValue("notas")
		debtorName := strings.TrimSpace(r.FormValue("debtor_name"))
		debtorDocumentType := strings.TrimSpace(r.FormValue("debtor_document_type"))
		debtorDocumentNumber := strings.TrimSpace(r.FormValue("debtor_document_number"))
		debtorPhone := strings.TrimSpace(r.FormValue("debtor_phone"))
		customerAddress := strings.TrimSpace(r.FormValue("customer_address"))
		customerCity := strings.TrimSpace(r.FormValue("customer_city"))
		customerNotes := strings.TrimSpace(r.FormValue("customer_notes"))
		installmentsTotalValue := strings.TrimSpace(r.FormValue("installments_total"))
		totalValueRaw := strings.TrimSpace(r.FormValue("total_value"))
		interestPercentRaw := strings.TrimSpace(r.FormValue("interest_percent"))
		installmentValueRaw := strings.TrimSpace(r.FormValue("installment_value"))
		customerInput := customerInput{
			CustomerID:     parseIntOrZero(r.FormValue("customer_id")),
			Name:           debtorName,
			Phone:          debtorPhone,
			DocumentType:   debtorDocumentType,
			DocumentNumber: debtorDocumentNumber,
			Address:        customerAddress,
			City:           customerCity,
			Notes:          customerNotes,
		}
		if allowed, accessErr := productAccessibleByID(db, currentUser, productID); accessErr != nil {
			if wantsJSON {
				writeJSONError(http.StatusInternalServerError, "No se pudo validar acceso al producto.", nil)
				return
			}
			http.Error(w, "No se pudo validar acceso al producto", http.StatusInternalServerError)
			return
		} else if !allowed {
			if wantsJSON {
				writeJSONError(http.StatusForbidden, "No tienes acceso a este producto.", map[string]string{"producto_id": "No tienes acceso a este producto."})
				return
			}
			http.Error(w, "No tienes acceso a este producto", http.StatusForbidden)
			return
		}

		errors := make(map[string]string)
		selectedProduct, ok := findProduct(productsSnapshot, productID)
		if !ok && len(productsSnapshot) > 0 {
			selectedProduct = productsSnapshot[0]
		}
		cantidad, err := strconv.Atoi(qtyValue)
		if err != nil || cantidad <= 0 {
			errors["cantidad"] = "La cantidad debe ser un número positivo."
		}
		if productID == "" {
			errors["producto_id"] = "Selecciona un producto válido."
		}
		precioParsed := 0.0
		precioOk := false
		valorFinalParsed := 0.0
		valorFinalOk := false
		creditInstallmentsTotal := 0
		creditTotalValue := 0
		creditInterestPercent := 0.0
		creditInstallmentValue := 0.0
		if saleMode == "credit" {
			if !movementEnabled(movementEnabledMap, "credito") {
				errors["sale_mode"] = "La venta a crédito está deshabilitada en Configuración."
			}
			parsedInstallmentsTotal, parseErr := strconv.Atoi(installmentsTotalValue)
			if parseErr != nil || parsedInstallmentsTotal <= 0 {
				errors["installments_total"] = "La cantidad total de cuotas debe ser mayor a 0."
			} else {
				creditInstallmentsTotal = parsedInstallmentsTotal
			}
			parsedTotalValue, parseErr := parseCOPInteger(totalValueRaw)
			if parseErr != nil || parsedTotalValue <= 0 {
				errors["total_value"] = "El valor total debe ser mayor a 0."
			} else {
				creditTotalValue = parsedTotalValue
			}
			parsedInstallmentValue, parseErr := parseCOPInteger(installmentValueRaw)
			if parseErr != nil || parsedInstallmentValue <= 0 {
				errors["installment_value"] = "El valor por cuota debe ser mayor a 0."
			} else {
				creditInstallmentValue = float64(parsedInstallmentValue)
			}
			if debtorName == "" {
				errors["debtor_name"] = "El nombre del deudor es obligatorio."
			}
			switch debtorDocumentType {
			case "CC", "C Extranjeria", "Pasaporte":
			default:
				errors["debtor_document_type"] = "Selecciona un tipo de documento válido."
			}
			if debtorDocumentNumber == "" {
				errors["debtor_document_number"] = "El número de documento del deudor es obligatorio."
			}
			if debtorPhone == "" {
				errors["debtor_phone"] = "El teléfono del deudor es obligatorio."
			}
			if customerCity == "" {
				errors["customer_city"] = "La ciudad del cliente es obligatoria."
			}
			if interestPercentRaw != "" {
				parsedInterest, parseErr := strconv.ParseFloat(interestPercentRaw, 64)
				if parseErr != nil || parsedInterest < 0 {
					errors["interest_percent"] = "El porcentaje de interés debe ser un número mayor o igual a 0."
				} else {
					creditInterestPercent = parsedInterest
				}
			}
			if creditTotalValue > 0 && creditInstallmentsTotal > 0 {
				financedTotal := float64(creditTotalValue) + (float64(creditTotalValue) * creditInterestPercent / 100)
				creditInstallmentValue = math.Round((financedTotal/float64(creditInstallmentsTotal))*100) / 100
				if strings.TrimSpace(installmentValueRaw) != "" {
					if provided, parseErr := strconv.ParseFloat(strings.TrimSpace(installmentValueRaw), 64); parseErr != nil || provided < 0 {
						errors["installment_value"] = "El valor por cuota calculado no es válido."
					}
				}
			}
		} else {
			if strings.TrimSpace(precioValue) != "" {
				if parsed, err := strconv.ParseFloat(strings.TrimSpace(precioValue), 64); err == nil && parsed > 0 {
					precioParsed = parsed
					precioOk = true
				} else {
					errors["precio_final_venta"] = "El precio debe ser un número mayor a 0."
				}
			}

			if strings.TrimSpace(valorVentaFinalValue) != "" {
				if parsed, err := strconv.ParseFloat(strings.TrimSpace(valorVentaFinalValue), 64); err == nil && parsed > 0 {
					valorFinalParsed = parsed
					valorFinalOk = true
				} else {
					errors["valor_venta_final"] = "El valor final debe ser un número mayor a 0."
				}
			}

			if !valorFinalOk && !precioOk {
				if _, ok := errors["precio_final_venta"]; !ok {
					errors["precio_final_venta"] = "Ingresa el precio unitario o el valor final de la venta."
				}
			}

			validMethod := false
			for _, method := range paymentMethodOptions {
				if metodoPago == method {
					validMethod = true
					break
				}
			}
			if !validMethod {
				errors["metodo_pago"] = "Selecciona un método de pago válido."
			}
		}

		if productID != "" && cantidad > 0 {
			if available := stockByProd[productID]; available > 0 && cantidad > available {
				errors["cantidad"] = "No hay stock disponible suficiente para completar la venta."
			}
		}

		if len(errors) > 0 {
			if wantsJSON {
				message := "Datos inválidos."
				// Pick the first field error as a message for the modal.
				for _, key := range []string{"producto_id", "cantidad", "sale_mode", "debtor_name", "debtor_document_type", "debtor_document_number", "debtor_phone", "customer_city", "installments_total", "total_value", "interest_percent", "installment_value", "valor_venta_final", "precio_final_venta", "metodo_pago"} {
					if msg, ok := errors[key]; ok && msg != "" {
						message = msg
						break
					}
				}
				writeJSONError(http.StatusBadRequest, message, errors)
				return
			}
			data := ventaFormData{
				Title:                  "Registrar venta",
				ProductoID:             productID,
				ProductoNom:            selectedProduct.Name,
				Productos:              productsSnapshot,
				StockByProd:            stockByProd,
				Cantidad:               cantidad,
				PrecioFinal:            precioValue,
				ValorVentaFinal:        valorVentaFinalValue,
				CustomerName:           debtorName,
				CustomerPhone:          debtorPhone,
				CustomerDocumentType:   debtorDocumentType,
				CustomerDocumentNumber: debtorDocumentNumber,
				CustomerAddress:        customerAddress,
				CustomerCity:           customerCity,
				CustomerNotes:          customerNotes,
				MetodoPago:             metodoPago,
				Notas:                  notas,
				Errors:                 errors,
				MetodoPagos:            paymentMethodOptions,
				CurrentUser:            currentUser,
			}
			w.WriteHeader(http.StatusBadRequest)
			renderTemplate(w, "venta_new.html", data, "Error al renderizar el template")
			return
		}

		tx, err := db.Begin()
		if err != nil {
			if wantsJSON {
				writeJSONError(http.StatusInternalServerError, "Error al procesar la venta.", nil)
				return
			}
			http.Error(w, "Error al procesar la venta", http.StatusInternalServerError)
			return
		}

		soldUnitIDs, err := selectAndMarkUnitsSold(tx, tenantIDFromUser(currentUser), productID, cantidad)
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Printf("rollback venta: %v", rollbackErr)
			}
			if err == errInsufficientStock {
				if wantsJSON {
					writeJSONError(http.StatusBadRequest, "No hay stock disponible suficiente para completar la venta.", map[string]string{
						"cantidad": "No hay stock disponible suficiente para completar la venta.",
					})
					return
				}
				errors["cantidad"] = "No hay stock disponible suficiente para completar la venta."
				data := ventaFormData{
					Title:           "Registrar venta",
					ProductoID:      productID,
					Cantidad:        cantidad,
					PrecioFinal:     precioValue,
					ValorVentaFinal: valorVentaFinalValue,
					MetodoPago:      metodoPago,
					Notas:           notas,
					Errors:          errors,
					MetodoPagos:     paymentMethodOptions,
				}
				w.WriteHeader(http.StatusBadRequest)
				renderTemplate(w, "venta_new.html", data, "Error al renderizar el template")
				return
			}
			if wantsJSON {
				writeJSONError(http.StatusInternalServerError, "Error al actualizar inventario.", nil)
				return
			}
			http.Error(w, "Error al actualizar inventario", http.StatusInternalServerError)
			return
		}
		now := time.Now().Format(time.RFC3339)
		movementType := "venta"
		if saleMode == "credit" {
			movementType = "venta_credito"
		}
		notaMovimiento := notas
		if saleMode == "credit" {
			customer, resolveErr := resolveCustomerForCredit(tx, tenantIDFromUser(currentUser), customerInput)
			if resolveErr != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					log.Printf("rollback credit customer resolve: %v", rollbackErr)
				}
				if wantsJSON {
					writeJSONError(http.StatusInternalServerError, "Error al resolver el cliente del crédito.", nil)
					return
				}
				http.Error(w, "Error al resolver el cliente del crédito", http.StatusInternalServerError)
				return
			}
			debtorName = customer.Name
			debtorDocumentType = customer.DocumentType
			debtorDocumentNumber = customer.DocumentNumber
			debtorPhone = customer.Phone
			customerAddress = customer.Address
			customerCity = customer.City
			customerNotes = customer.Notes
			customerInput.CustomerID = customer.ID
			creditSummary := fmt.Sprintf("VENTA A CREDITO | Cliente: %s | Cuotas: %d | Interes: %.2f%% | Valor cuota: %.2f", customer.Name, creditInstallmentsTotal, creditInterestPercent, creditInstallmentValue)
			if strings.TrimSpace(notaMovimiento) != "" {
				notaMovimiento = creditSummary + " | " + strings.TrimSpace(notaMovimiento)
			} else {
				notaMovimiento = creditSummary
			}
		}
		if err := logMovimientos(tx, productID, soldUnitIDs, movementType, notaMovimiento, currentUser, now); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Printf("rollback venta log: %v", rollbackErr)
			}
			if wantsJSON {
				writeJSONError(http.StatusInternalServerError, "Error al registrar movimiento de venta.", nil)
				return
			}
			http.Error(w, "Error al registrar movimiento de venta", http.StatusInternalServerError)
			return
		}

		saleID := 0
		if saleMode == "credit" {
			creditSaleID, err := insertAndReturnID(tx,
				`INSERT INTO credit_sales (tenant_id, customer_id, kind, product_id, quantity, debtor_name, debtor_document_type, debtor_document_number, debtor_phone, installments_total, installments_paid, total_value, interest_percent, installment_value, notes, status, created_at, created_by)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)`,
				tenantIDFromUser(currentUser), customerInput.CustomerID, string(creditSaleKindProduct), productID, cantidad, debtorName, debtorDocumentType, debtorDocumentNumber, debtorPhone, creditInstallmentsTotal, float64(creditTotalValue), creditInterestPercent, creditInstallmentValue, notaMovimiento, string(creditStatusActive), now, nullableUserID(currentUser),
			)
			if err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					log.Printf("rollback credit sale insert: %v", rollbackErr)
				}
				if wantsJSON {
					writeJSONError(http.StatusInternalServerError, "Error al registrar la venta a crédito.", nil)
					return
				}
				http.Error(w, "Error al registrar la venta a crédito", http.StatusInternalServerError)
				return
			}
			if customerInput.CustomerID > 0 {
				if err := logCustomerEvent(tx, currentUser, customerInput.CustomerID, "credit_created", "credit_sale", strconv.FormatInt(creditSaleID, 10), creditDebtTotal(creditInstallmentsTotal, creditInstallmentValue), map[string]any{
					"kind":               string(creditSaleKindProduct),
					"kind_label":         creditKindLabel(creditSaleKindProduct),
					"product_id":         productID,
					"quantity":           cantidad,
					"installments_total": creditInstallmentsTotal,
					"installment_value":  creditInstallmentValue,
					"current_debt":       creditDebtTotal(creditInstallmentsTotal, creditInstallmentValue),
				}); err != nil {
					if rollbackErr := tx.Rollback(); rollbackErr != nil {
						log.Printf("rollback credit customer event: %v", rollbackErr)
					}
					if wantsJSON {
						writeJSONError(http.StatusInternalServerError, "Error al registrar la trazabilidad del cliente.", nil)
						return
					}
					http.Error(w, "Error al registrar la trazabilidad del cliente", http.StatusInternalServerError)
					return
				}
			}
			if err := logAuditEvent(tx, currentUser, "credit_sale_created", "credit_sale", strconv.FormatInt(creditSaleID, 10), "manual", map[string]any{
				"credit_sale_id":         creditSaleID,
				"customer_id":            customerInput.CustomerID,
				"customer_address":       customerAddress,
				"customer_city":          customerCity,
				"customer_notes":         customerNotes,
				"kind":                   string(creditSaleKindProduct),
				"kind_label":             creditKindLabel(creditSaleKindProduct),
				"product_id":             productID,
				"debtor_name":            debtorName,
				"debtor_document_type":   debtorDocumentType,
				"debtor_document_number": debtorDocumentNumber,
				"debtor_phone":           debtorPhone,
				"installments_total":     creditInstallmentsTotal,
				"installments_paid":      0,
				"total_value":            creditTotalValue,
				"debt_total":             creditDebtTotal(creditInstallmentsTotal, creditInstallmentValue),
				"total_paid":             0,
				"current_debt":           creditDebtTotal(creditInstallmentsTotal, creditInstallmentValue),
				"interest_percent":       creditInterestPercent,
				"installment_value":      creditInstallmentValue,
				"quantity":               cantidad,
			}); err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					log.Printf("rollback credit sale audit: %v", rollbackErr)
				}
				if wantsJSON {
					writeJSONError(http.StatusInternalServerError, "Error al registrar la auditoría del crédito.", nil)
					return
				}
				http.Error(w, "Error al registrar la auditoría del crédito", http.StatusInternalServerError)
				return
			}
		} else if insertedSaleID, err := insertAndReturnID(tx,
			`INSERT INTO ventas (tenant_id, producto_id, cantidad, precio_final, metodo_pago, notas, fecha)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			tenantIDFromUser(currentUser), productID, cantidad, func() float64 {
				precioFinal := precioParsed
				if valorFinalOk && cantidad > 0 {
					precioFinal = valorFinalParsed / float64(cantidad)
				}
				return precioFinal
			}(), metodoPago, notas, now,
		); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Printf("rollback venta insert: %v", rollbackErr)
			}
			if wantsJSON {
				writeJSONError(http.StatusInternalServerError, "Error al registrar la venta.", nil)
				return
			}
			http.Error(w, "Error al registrar la venta", http.StatusInternalServerError)
			return
		} else {
			saleID = int(insertedSaleID)
		}
		if saleMode != "credit" {
			precioFinal := precioParsed
			if valorFinalOk && cantidad > 0 {
				precioFinal = valorFinalParsed / float64(cantidad)
			}
			if err := logAuditEvent(tx, currentUser, "sale_registered", "sale", productID, "manual", map[string]any{
				"producto_id": productID,
				"producto":    selectedProduct.Name,
				"cantidad":    cantidad,
				"metodo_pago": metodoPago,
				"total":       precioFinal * float64(cantidad),
			}); err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					log.Printf("rollback sale audit: %v", rollbackErr)
				}
				if wantsJSON {
					writeJSONError(http.StatusInternalServerError, "Error al registrar la auditoría de la venta.", nil)
					return
				}
				http.Error(w, "Error al registrar la auditoría de la venta", http.StatusInternalServerError)
				return
			}
		}

		if err := tx.Commit(); err != nil {
			if wantsJSON {
				writeJSONError(http.StatusInternalServerError, "Error al confirmar la venta.", nil)
				return
			}
			http.Error(w, "Error al confirmar la venta", http.StatusInternalServerError)
			return
		}

		if wantsJSON {
			w.Header().Set("Content-Type", "application/json")
			message := "Venta registrada correctamente."
			if saleMode == "credit" {
				message = "Venta a crédito registrada correctamente."
			}
			resp := map[string]any{
				"ok":           true,
				"producto_id":  productID,
				"producto_nom": selectedProduct.Name,
				"cantidad":     cantidad,
				"mensaje":      message,
			}
			if saleMode != "credit" && saleID > 0 {
				resp["sale_id"] = saleID
				resp["receipt_url"] = saleReceiptViewURL(saleID)
				resp["receipt_download_url"] = saleReceiptDownloadURL(saleID)
				resp["thermal_ticket_url"] = saleThermalTicketViewURL(saleID)
				resp["invoice_create_url"] = invoiceNewFromSaleURL(saleID)
				resp["redirect_url"] = fmt.Sprintf("/inventario?mensaje=%s&receipt_sale_id=%d", url.QueryEscape(message), saleID)
			}
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		precioFinalText := precioValue
		if saleMode != "credit" && valorFinalOk && cantidad > 0 {
			precioFinalText = fmt.Sprintf("%.2f", valorFinalParsed/float64(cantidad))
		}

		confirmData := ventaConfirmData{
			Title:           "Venta registrada",
			SaleID:          saleID,
			ProductoID:      productID,
			ProductoNom:     selectedProduct.Name,
			Cantidad:        cantidad,
			PrecioFinal:     precioFinalText,
			ValorVentaFinal: valorVentaFinalValue,
			MetodoPago:      metodoPago,
			Notas:           notas,
			ReceiptViewURL: func() string {
				if saleID > 0 {
					return saleReceiptViewURL(saleID)
				}
				return ""
			}(),
			ReceiptDownloadURL: func() string {
				if saleID > 0 {
					return saleReceiptDownloadURL(saleID)
				}
				return ""
			}(),
			ThermalTicketURL: func() string {
				if saleID > 0 {
					return saleThermalTicketViewURL(saleID)
				}
				return ""
			}(),
			InvoiceCreateURL: func() string {
				if saleID > 0 {
					return invoiceNewFromSaleURL(saleID)
				}
				return ""
			}(),
			CurrentUser: currentUser,
		}
		renderTemplate(w, "venta_confirm.html", confirmData, "Error al renderizar el template")
	})

	mux.HandleFunc("/cambio", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantIDFromUser(currentUser))
		if err != nil {
			http.Error(w, "Error al cargar tipos de movimiento", http.StatusInternalServerError)
			return
		}
		if !movementEnabled(movementEnabledMap, "cambio") {
			redirectWithMessage(w, r, "/inventario", "", "El cambio está deshabilitado en Configuración.")
			return
		}
		productsMu.RLock()
		productsSnapshot := make([]productOption, len(products))
		copy(productsSnapshot, products)
		productsMu.RUnlock()
		productsSnapshot = filterProductsForUser(productsSnapshot, currentUser)

		if r.Method != http.MethodPost {
			http.Redirect(w, r, "/cambio/new", http.StatusSeeOther)
			return
		}

		if err := r.ParseForm(); err != nil {
			http.Error(w, "No se pudo leer el formulario", http.StatusBadRequest)
			return
		}

		productID := r.FormValue("producto_id")
		personaCambio := r.FormValue("persona_del_cambio")
		notas := r.FormValue("notas")
		salientes := r.Form["salientes"]
		incomingMode := r.FormValue("incoming_mode")
		incomingExistingID := r.FormValue("incoming_existing_id")
		incomingExistingQtyValue := r.FormValue("incoming_existing_qty")
		incomingNewSKU := r.FormValue("incoming_new_sku")
		incomingNewName := r.FormValue("incoming_new_name")
		incomingNewLine := r.FormValue("incoming_new_line")
		incomingNewQtyValue := r.FormValue("incoming_new_qty")

		errors := make(map[string]string)
		if allowed, accessErr := productAccessibleByID(db, currentUser, productID); accessErr != nil {
			http.Error(w, "No se pudo validar acceso al producto", http.StatusInternalServerError)
			return
		} else if !allowed {
			errors["producto_id"] = "No tienes acceso a este producto."
		}
		if len(productsSnapshot) == 0 {
			http.Error(w, "No tienes productos disponibles para cambio", http.StatusForbidden)
			return
		}

		selectedProduct, ok := findProduct(productsSnapshot, productID)
		if !ok {
			errors["producto_id"] = "Selecciona un producto válido."
			selectedProduct = productsSnapshot[0]
			productID = selectedProduct.ID
		}

		if personaCambio == "" {
			errors["persona_del_cambio"] = "Ingresa la persona responsable del cambio."
		}

		availableUnits, err := availableUnitsByProduct(db, tenantIDFromUser(currentUser), productID)
		if err != nil {
			http.Error(w, "Error al consultar unidades disponibles", http.StatusInternalServerError)
			return
		}

		unitLookup := make(map[string]struct{})
		for _, unit := range availableUnits {
			unitLookup[unit.ID] = struct{}{}
		}
		validSalientes := make([]string, 0, len(salientes))
		for _, unitID := range salientes {
			if _, ok := unitLookup[unitID]; ok {
				validSalientes = append(validSalientes, unitID)
			}
		}
		if len(availableUnits) == 0 {
			errors["salientes"] = "No hay unidades disponibles para el producto seleccionado."
		} else if len(validSalientes) == 0 {
			errors["salientes"] = "Selecciona al menos una unidad disponible como saliente."
		}
		salientes = validSalientes

		incomingExistingQty := 0
		if incomingExistingQtyValue != "" {
			if parsed, err := strconv.Atoi(incomingExistingQtyValue); err == nil {
				incomingExistingQty = parsed
			}
		}
		incomingNewQty := 0
		if incomingNewQtyValue != "" {
			if parsed, err := strconv.Atoi(incomingNewQtyValue); err == nil {
				incomingNewQty = parsed
			}
		}

		if incomingMode != "existing" && incomingMode != "new" {
			errors["incoming_mode"] = "Selecciona el tipo de entrada."
		}

		if incomingMode == "existing" {
			if incomingExistingID == "" {
				errors["incoming_existing_id"] = "Selecciona el producto entrante."
			} else if _, ok := findProduct(productsSnapshot, incomingExistingID); !ok {
				errors["incoming_existing_id"] = "Selecciona un producto entrante válido."
			}
			if incomingExistingQty <= 0 {
				errors["incoming_existing_qty"] = "Ingresa una cantidad válida para la entrada."
			}
		} else if incomingMode == "new" {
			if incomingNewSKU == "" {
				errors["incoming_new_sku"] = "Ingresa el SKU del producto nuevo."
			}
			if incomingNewName == "" {
				errors["incoming_new_name"] = "Ingresa el nombre del producto nuevo."
			}
			if incomingNewQty <= 0 {
				errors["incoming_new_qty"] = "Ingresa una cantidad válida para la entrada."
			}
		}

		if len(errors) > 0 {
			data := cambioFormData{
				Title:               "Registrar cambio",
				ProductoID:          productID,
				Productos:           productsSnapshot,
				Unidades:            availableUnits,
				PersonaCambio:       personaCambio,
				Notas:               notas,
				Salientes:           salientes,
				SalientesMap:        buildSalientesMap(salientes),
				IncomingMode:        incomingMode,
				IncomingExistingID:  incomingExistingID,
				IncomingExistingQty: incomingExistingQty,
				IncomingNewSKU:      incomingNewSKU,
				IncomingNewName:     incomingNewName,
				IncomingNewLine:     incomingNewLine,
				IncomingNewQty:      incomingNewQty,
				Errors:              errors,
				CurrentUser:         currentUser,
			}
			w.WriteHeader(http.StatusBadRequest)
			renderTemplate(w, "cambio_new.html", data, "Error al renderizar el template")
			return
		}

		tx, err := db.Begin()
		if err != nil {
			http.Error(w, "Error al iniciar el cambio", http.StatusInternalServerError)
			return
		}

		outgoingQty := len(salientes)
		salientesMarcadas, err := selectAndMarkUnitsByStatus(tx, tenantIDFromUser(currentUser), productID, outgoingQty, "Cambio")
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Printf("rollback cambio: %v", rollbackErr)
			}
			if err == errInsufficientStock {
				errors["salientes"] = "No hay stock disponible suficiente para completar el cambio."
				data := cambioFormData{
					Title:               "Registrar cambio",
					ProductoID:          productID,
					Productos:           productsSnapshot,
					Unidades:            availableUnits,
					PersonaCambio:       personaCambio,
					Notas:               notas,
					Salientes:           salientes,
					SalientesMap:        buildSalientesMap(salientes),
					IncomingMode:        incomingMode,
					IncomingExistingID:  incomingExistingID,
					IncomingExistingQty: incomingExistingQty,
					IncomingNewSKU:      incomingNewSKU,
					IncomingNewName:     incomingNewName,
					IncomingNewLine:     incomingNewLine,
					IncomingNewQty:      incomingNewQty,
					Errors:              errors,
				}
				w.WriteHeader(http.StatusBadRequest)
				renderTemplate(w, "cambio_new.html", data, "Error al renderizar el template")
				return
			}
			http.Error(w, "Error al actualizar unidades salientes", http.StatusInternalServerError)
			return
		}

		now := time.Now().Format(time.RFC3339)
		notaMovimiento := strings.TrimSpace(fmt.Sprintf("%s %s", personaCambio, notas))
		if err := logMovimientos(tx, productID, salientesMarcadas, "cambio_salida", notaMovimiento, currentUser, now); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Printf("rollback cambio log: %v", rollbackErr)
			}
			http.Error(w, "Error al registrar movimiento del cambio", http.StatusInternalServerError)
			return
		}

		entrantes := []string{}
		if incomingMode == "existing" {
			entrantes = buildEntranteIDs("ENT-"+incomingExistingID, incomingExistingQty)
		} else {
			entrantes = buildEntranteIDs("ENT-"+incomingNewSKU, incomingNewQty)
		}

		incomingProductID := incomingExistingID
		incomingQty := incomingExistingQty
		if incomingMode == "new" {
			incomingProductID = incomingNewSKU
			incomingQty = incomingNewQty
		}

		for i := 0; i < incomingQty; i++ {
			unitID := fmt.Sprintf("U-%d-%d", time.Now().UnixNano(), i+1)
			if _, err := tx.Exec(
				`INSERT INTO unidades (id, producto_id, estado, creado_en, caducidad)
				VALUES (?, ?, ?, ?, ?)`,
				unitID, incomingProductID, "Disponible", now, nil,
			); err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					log.Printf("rollback cambio insert: %v", rollbackErr)
				}
				http.Error(w, "Error al registrar unidades entrantes", http.StatusInternalServerError)
				return
			}
		}
		if err := logAuditEvent(tx, currentUser, "change_registered", "change", productID, "manual", map[string]any{
			"producto_saliente_id": productID,
			"producto_saliente":    selectedProduct.Name,
			"producto_entrante_id": incomingProductID,
			"cantidad_saliente":    outgoingQty,
			"cantidad_entrante":    incomingQty,
			"modo_entrada":         incomingMode,
		}); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Printf("rollback change audit: %v", rollbackErr)
			}
			http.Error(w, "Error al registrar la auditoría del cambio", http.StatusInternalServerError)
			return
		}

		if err := tx.Commit(); err != nil {
			http.Error(w, "Error al confirmar el cambio", http.StatusInternalServerError)
			return
		}

		confirmData := cambioConfirmData{
			Title:               "Cambio registrado",
			ProductoID:          productID,
			ProductoNombre:      selectedProduct.Name,
			PersonaCambio:       personaCambio,
			Notas:               notas,
			Salientes:           salientesMarcadas,
			Entrantes:           entrantes,
			IncomingMode:        incomingMode,
			IncomingExistingID:  incomingExistingID,
			IncomingExistingQty: incomingExistingQty,
			IncomingNewSKU:      incomingNewSKU,
			IncomingNewName:     incomingNewName,
			IncomingNewLine:     incomingNewLine,
			IncomingNewQty:      incomingNewQty,
			CurrentUser:         currentUser,
		}

		renderTemplate(w, "cambio_confirm.html", confirmData, "Error al renderizar el template")
	})

	mux.HandleFunc("/csv/template", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		renderTemplate(w, "csv_template.html", struct {
			Title       string
			Subtitle    string
			CurrentUser *User
		}{
			Title:       "Plantilla CSV - Carga masiva",
			Subtitle:    "",
			CurrentUser: userFromContext(r),
		}, "Error al renderizar plantilla CSV")
	}))

	mux.HandleFunc("/csv/export", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		renderTemplate(w, "csv_export.html", struct {
			Title       string
			Subtitle    string
			CurrentUser *User
		}{
			Title:       "Exportaciones CSV",
			Subtitle:    "",
			CurrentUser: userFromContext(r),
		}, "Error al renderizar exportaciones CSV")
	}))

	mux.HandleFunc("/productos/csv", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		writeJSONError := func(status int, message string) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
		}

		if r.Method != http.MethodPost {
			writeJSONError(http.StatusMethodNotAllowed, "Método no permitido.")
			return
		}

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			writeJSONError(http.StatusBadRequest, "No se pudo leer el archivo.")
			return
		}
		file, _, err := r.FormFile("file")
		if err != nil {
			writeJSONError(http.StatusBadRequest, "Archivo CSV no encontrado.")
			return
		}
		defer file.Close()

		reader := csv.NewReader(file)
		reader.FieldsPerRecord = -1
		records, err := reader.ReadAll()
		if err != nil {
			writeJSONError(http.StatusBadRequest, "No se pudo leer el CSV.")
			return
		}
		if len(records) < 2 {
			writeJSONError(http.StatusBadRequest, "El CSV no contiene filas para procesar.")
			return
		}

		header := make([]string, len(records[0]))
		for i, cell := range records[0] {
			header[i] = strings.ToLower(strings.TrimSpace(cell))
		}
		index := make(map[string]int, len(header))
		for i, name := range header {
			if name == "" {
				continue
			}
			index[name] = i
		}
		if _, ok := index["id"]; !ok {
			if _, legacyOK := index["sku"]; !legacyOK {
				writeJSONError(http.StatusBadRequest, "Falta la columna requerida id.")
				return
			}
		}
		for _, col := range []string{"linea", "nombre", "cantidad", "precio_venta"} {
			if _, ok := index[col]; !ok {
				writeJSONError(http.StatusBadRequest, "Faltan columnas requeridas en el CSV.")
				return
			}
		}

		get := func(row []string, col string) string {
			pos, ok := index[col]
			if !ok || pos < 0 || pos >= len(row) {
				return ""
			}
			return strings.TrimSpace(row[pos])
		}

		parseCSVFloat := func(value string) (float64, error) {
			value = strings.TrimSpace(value)
			if value == "" {
				return 0, fmt.Errorf("empty")
			}
			value = strings.ReplaceAll(value, ",", ".")
			return strconv.ParseFloat(value, 64)
		}

		parseCSVInt := func(value string) (int, error) {
			value = strings.TrimSpace(value)
			if value == "" {
				return 0, fmt.Errorf("empty")
			}
			return strconv.Atoi(value)
		}

		parseCSVBool := func(value string) (bool, error) {
			value = strings.TrimSpace(strings.ToLower(value))
			if value == "" {
				return false, fmt.Errorf("empty")
			}
			switch value {
			case "true", "1", "si", "sí", "yes":
				return true, nil
			case "false", "0", "no":
				return false, nil
			default:
				return false, fmt.Errorf("invalid")
			}
		}

		resp := csvUploadResponse{}
		tenantID := tenantIDFromRequest(r)
		assignableUsers, err := loadAssignableUsersForTenant(db, tenantID)
		if err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudieron cargar los usuarios asignables.")
			return
		}
		validOwners := make(map[string]struct{}, len(assignableUsers))
		for _, user := range assignableUsers {
			validOwners[strconv.Itoa(user.ID)] = struct{}{}
		}
		tx, err := db.Begin()
		if err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo iniciar la transacción.")
			return
		}

		now := time.Now().Format(time.RFC3339)
		for i, row := range records[1:] {
			rowIndex := i + 1 // matches the UI preview index (1-based excluding header)
			productID := get(row, "id")
			if productID == "" {
				productID = get(row, "sku")
			}
			linea := get(row, "linea")
			nombre := get(row, "nombre")
			anotaciones := get(row, "anotaciones")
			location := get(row, "location")
			if location == "" {
				location = get(row, "ubicacion")
			}
			ownerUserIDRaw := get(row, "owner_user_id")
			cantidadRaw := get(row, "cantidad")
			if cantidadRaw == "-" {
				cantidadRaw = "0"
			}

			if productID == "" || linea == "" || nombre == "" {
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: "ID, línea y nombre son obligatorios."})
				continue
			}

			cantidad, err := parseCSVInt(cantidadRaw)
			if err != nil || cantidad < 0 {
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: "Cantidad inválida (debe ser 0 o mayor)."})
				continue
			}

			precioVenta, err := parseCSVFloat(get(row, "precio_venta"))
			if err != nil {
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: "Precio venta inválido."})
				continue
			}

			var ownerUserID sql.NullInt64
			if ownerUserIDRaw != "" {
				if _, ok := validOwners[ownerUserIDRaw]; !ok {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: "owner_user_id no corresponde a un usuario activo del tenant."})
					continue
				}
				parsedOwnerID, err := parseCSVInt(ownerUserIDRaw)
				if err != nil || parsedOwnerID <= 0 {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: "owner_user_id inválido."})
					continue
				}
				ownerUserID = sql.NullInt64{Int64: int64(parsedOwnerID), Valid: true}
			}

			retomaEnabled := false
			retomaEnabledRaw := get(row, "retoma_enabled")
			if retomaEnabledRaw != "" {
				parsed, err := parseCSVBool(retomaEnabledRaw)
				if err != nil {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: "retoma_enabled debe ser true/false."})
					continue
				}
				retomaEnabled = parsed
			}
			var retomaPrice sql.NullFloat64
			retomaPriceRaw := get(row, "retoma_price")
			if retomaEnabled {
				if retomaPriceRaw == "" {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: "retoma_price es obligatorio si retoma_enabled es true."})
					continue
				}
				parsed, err := parseCSVFloat(retomaPriceRaw)
				if err != nil || parsed < 0 {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: "retoma_price inválido."})
					continue
				}
				if parsed > precioVenta {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: "retoma_price no debe superar precio_venta."})
					continue
				}
				retomaPrice = sql.NullFloat64{Float64: parsed, Valid: true}
			} else if retomaPriceRaw != "" {
				if _, err := parseCSVFloat(retomaPriceRaw); err != nil {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: "retoma_price inválido."})
					continue
				}
			}

			creditEnabled := false
			creditEnabledRaw := get(row, "credit_enabled")
			if creditEnabledRaw != "" {
				parsed, err := parseCSVBool(creditEnabledRaw)
				if err != nil {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: "credit_enabled debe ser true/false."})
					continue
				}
				creditEnabled = parsed
			}
			debtorName := get(row, "debtor_name")
			installmentsTotal := 0
			totalValue := 0.0
			installmentValue := 0.0
			if creditEnabled {
				if debtorName == "" {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: "debtor_name es obligatorio si credit_enabled es true."})
					continue
				}
				installmentsTotalRaw := get(row, "installments_total")
				parsedInstallments, err := parseCSVInt(installmentsTotalRaw)
				if err != nil || parsedInstallments <= 0 {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: "installments_total debe ser mayor a 0."})
					continue
				}
				installmentsTotal = parsedInstallments

				totalValueRaw := get(row, "total_value")
				parsedTotalValue, err := parseCSVFloat(totalValueRaw)
				if err != nil || parsedTotalValue <= 0 {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: "total_value debe ser mayor a 0."})
					continue
				}
				totalValue = parsedTotalValue

				installmentValueRaw := get(row, "installment_value")
				parsedInstallmentValue, err := parseCSVFloat(installmentValueRaw)
				if err != nil || parsedInstallmentValue <= 0 {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: "installment_value debe ser mayor a 0."})
					continue
				}
				installmentValue = parsedInstallmentValue
			} else {
				debtorName = ""
			}

			fechaCaducidad := get(row, "fecha_caducidad")
			aplicaCadRaw := get(row, "aplica_caducidad")
			aplicaCad := false
			if aplicaCadRaw != "" {
				parsed, err := parseCSVBool(aplicaCadRaw)
				if err != nil {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: "aplica_caducidad debe ser true/false."})
					continue
				}
				aplicaCad = parsed
			}
			if aplicaCad && fechaCaducidad == "" {
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: "fecha_caducidad requerida si aplica."})
				continue
			}
			if fechaCaducidad != "" {
				if _, err := time.Parse("2006-01-02", fechaCaducidad); err != nil {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: "fecha_caducidad debe ser YYYY-MM-DD."})
					continue
				}
			}

			if _, err := tx.Exec("SAVEPOINT csv_row"); err != nil {
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: "Error al preparar la fila."})
				continue
			}

			// Persist catalog.
			if err := upsertProducto(tx, tenantID, productID, nombre, linea, now); err != nil {
				_, _ = tx.Exec("ROLLBACK TO csv_row")
				_, _ = tx.Exec("RELEASE csv_row")
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: fmt.Sprintf("Error al guardar producto: %v", err)})
				continue
			}
			if _, err := tx.Exec(`
				UPDATE productos
				SET precio_venta = ?, anotaciones = ?, location = ?, owner_user_id = ?, retoma_enabled = ?, retoma_price = ?, credit_enabled = ?, debtor_name = ?, installments_total = ?, installments_paid = 0, total_value = ?, installment_value = ?
				WHERE tenant_id = ? AND sku = ?
			`, precioVenta, anotaciones, location, ownerUserID, boolToInt(retomaEnabled), retomaPrice, boolToInt(creditEnabled), debtorName, installmentsTotal, totalValue, installmentValue, tenantID, productID); err != nil {
				_, _ = tx.Exec("ROLLBACK TO csv_row")
				_, _ = tx.Exec("RELEASE csv_row")
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: fmt.Sprintf("Error al guardar los datos del producto: %v", err)})
				continue
			}

			// Update in-memory catalog (used by inventario/cambio screens).
			productsMu.Lock()
			found := false
			for idx := range products {
				if products[idx].ID == productID {
					products[idx].Name = nombre
					products[idx].Line = linea
					products[idx].Location = location
					products[idx].Notes = anotaciones
					products[idx].SalePrice = precioVenta
					products[idx].RetomaEnabled = retomaEnabled
					products[idx].HasRetomaPrice = retomaPrice.Valid
					if retomaPrice.Valid {
						products[idx].RetomaPrice = retomaPrice.Float64
					} else {
						products[idx].RetomaPrice = 0
					}
					products[idx].CreditEnabled = creditEnabled
					products[idx].DebtorName = debtorName
					products[idx].InstallmentsTotal = installmentsTotal
					products[idx].InstallmentsPaid = 0
					products[idx].TotalValue = totalValue
					products[idx].InstallmentValue = installmentValue
					products[idx].HasOwner = ownerUserID.Valid
					if ownerUserID.Valid {
						products[idx].OwnerUserID = int(ownerUserID.Int64)
					} else {
						products[idx].OwnerUserID = 0
					}
					found = true
					break
				}
			}
			if !found {
				products = append(products, productOption{
					ID:                productID,
					Name:              nombre,
					Line:              linea,
					Location:          location,
					Notes:             anotaciones,
					FechaIngreso:      time.Now().Format("2006-01-02"),
					SalePrice:         precioVenta,
					RetomaEnabled:     retomaEnabled,
					HasRetomaPrice:    retomaPrice.Valid,
					CreditEnabled:     creditEnabled,
					DebtorName:        debtorName,
					InstallmentsTotal: installmentsTotal,
					InstallmentsPaid:  0,
					TotalValue:        totalValue,
					InstallmentValue:  installmentValue,
				})
				if retomaPrice.Valid {
					products[len(products)-1].RetomaPrice = retomaPrice.Float64
				}
				if ownerUserID.Valid {
					products[len(products)-1].HasOwner = true
					products[len(products)-1].OwnerUserID = int(ownerUserID.Int64)
				}
				resp.CreatedProducts++
			} else {
				resp.UpdatedProducts++
			}
			resp.ProductIDs = append(resp.ProductIDs, productID)
			productsMu.Unlock()

			// Insert units into DB (inventory source of truth).
			baseID := time.Now().UnixNano()
			rowFailed := false
			for j := 0; j < cantidad; j++ {
				unitID := fmt.Sprintf("U-%s-%d", productID, baseID+int64(j))
				var caducidad any = nil
				if aplicaCad && fechaCaducidad != "" {
					caducidad = fechaCaducidad
				}
				if _, err := tx.Exec(
					`INSERT INTO unidades (id, tenant_id, producto_id, estado, creado_en, caducidad) VALUES (?, ?, ?, ?, ?, ?)`,
					unitID, tenantID, productID, "Disponible", now, caducidad,
				); err != nil {
					_, _ = tx.Exec("ROLLBACK TO csv_row")
					_, _ = tx.Exec("RELEASE csv_row")
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: productID, Error: fmt.Sprintf("Error al crear unidades: %v", err)})
					rowFailed = true
					break
				}
				resp.CreatedUnits++
			}

			if rowFailed {
				continue
			}
			_, _ = tx.Exec("RELEASE csv_row")
		}

		if err := tx.Commit(); err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo guardar el CSV.")
			return
		}
		if len(resp.ProductIDs) > 0 {
			resp.LabelPrintURL = productLabelPrintURL(resp.ProductIDs, "60x40")
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/inventario", http.StatusFound)
	})

	addr := ":" + port
	log.Printf("Servidor activo en http://localhost:%s/inventario", port)
	if err := http.ListenAndServe(addr, authMiddleware(db, mux)); err != nil {
		log.Fatal(err)
	}
}
