package main

import (
	"bytes"
	"compress/flate"
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
	"image"
	_ "image/jpeg"
	"image/png"
	"io"
	"log"
	"math"
	"mime/multipart"
	"net"
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
	"unicode/utf8"

	"github.com/boombuler/barcode"
	"github.com/boombuler/barcode/code128"
	pgxstdlib "github.com/jackc/pgx/v5/stdlib"

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
	Line           string
	Size           string
	Price          string
	BarcodeDataURI template.URL
}

type productLabelsPageData struct {
	Title              string
	Subtitle           string
	Size               string
	WidthMM            int
	HeightMM           int
	PaperWidthMM       int
	PaperHeightMM      int
	Columns            int
	GapMM              int
	RowGapMM           int
	PaperDPI           int
	PaperClass         string
	Items              []productLabelItem
	Rows               [][]productLabelItem
	AutoPrint          bool
	CanLoan            bool
	CanCredit          bool
	CurrentUser        *User
	Settings           BusinessSettings
	ContactLine        string
	CompactContactLine string
	Profile            LabelProfile
}

type productLabelBatchProduct struct {
	ID              string
	Name            string
	Line            string
	Size            string
	Price           string
	Available       int
	SuggestedCopies int
	CopiesKey       string
}

type productLabelsBatchPageData struct {
	Title            string
	Subtitle         string
	Flash            string
	Error            string
	Products         []productLabelBatchProduct
	DefaultSize      string // legacy template data retained for compatibility
	DefaultProfileID int
	Profiles         []LabelProfile
	CanManageLabels  bool
	CurrentUser      *User
	Settings         BusinessSettings
	MaxLabels        int
	MaxCopies        int
	SizeOptions      []labelPaperOption
	DefaultGapMM     int
}

type labelPaperOption struct {
	Value string
	Label string
}

type labelPrintProfile struct {
	Size          string
	LabelWidthMM  int
	LabelHeightMM int
	PaperWidthMM  int
	PaperHeightMM int
	Columns       int
	GapMM         int
	RowGapMM      int
	DPI           int
	PaperClass    string
}

// LabelProfile is a tenant-scoped, structured label layout. It intentionally
// avoids free positioning so HTML and PDF output stay physically predictable.
type LabelProfile struct {
	ID            int
	TenantID      int
	Name          string
	LabelWidthMM  int
	LabelHeightMM int
	Columns       int
	ColumnGapMM   int
	RowGapMM      int
	ShowBusiness  bool
	ShowContact   bool
	ShowLine      bool
	ShowSize      bool
	ShowPrice     bool
	ShowBarcode   bool
	ShowID        bool
	CreatedAt     string
	UpdatedAt     string
}

func (p LabelProfile) paperWidthMM() int {
	if p.Columns == 2 {
		return p.LabelWidthMM*2 + p.ColumnGapMM
	}
	return p.LabelWidthMM
}

func (p LabelProfile) paperHeightMM() int {
	return p.LabelHeightMM + p.RowGapMM
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
	SKU               string
	ID                string
	Name              string
	Line              string
	Location          string
	TallaRequerida    bool
	Talla             string
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
	ID    string `json:"id"`
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

func readUploadedCSVFile(file multipart.File, header *multipart.FileHeader, limit int64) ([]byte, error) {
	if header != nil {
		if ext := strings.ToLower(strings.TrimSpace(filepath.Ext(header.Filename))); ext != ".csv" {
			return nil, requestError{Status: http.StatusBadRequest, Message: "Solo se aceptan archivos CSV."}
		}
	}
	if limit <= 0 {
		limit = customerCSVUploadLimit
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, requestError{Status: http.StatusBadRequest, Message: "No se pudo leer el archivo CSV."}
	}
	if int64(len(data)) > limit {
		return nil, requestError{Status: http.StatusRequestEntityTooLarge, Message: "El archivo CSV excede el tamaño permitido."}
	}
	if len(data) == 0 {
		return nil, requestError{Status: http.StatusBadRequest, Message: "El archivo CSV está vacío."}
	}
	if !utf8.Valid(data) || bytes.IndexByte(data, 0) >= 0 {
		return nil, requestError{Status: http.StatusBadRequest, Message: "El archivo CSV debe ser texto UTF-8 válido."}
	}
	sniffLen := min(len(data), 512)
	contentType := strings.ToLower(strings.TrimSpace(http.DetectContentType(data[:sniffLen])))
	if !strings.HasPrefix(contentType, "text/plain") &&
		!strings.HasPrefix(contentType, "text/csv") &&
		!strings.HasPrefix(contentType, "application/csv") &&
		!strings.HasPrefix(contentType, "application/vnd.ms-excel") {
		return nil, requestError{Status: http.StatusBadRequest, Message: "El archivo no parece un CSV de texto válido."}
	}
	return data, nil
}

type customerCSVFailedRow struct {
	Row            int    `json:"row"`
	DocumentType   string `json:"document_type"`
	DocumentNumber string `json:"document_number"`
	Error          string `json:"error"`
}

type customerCSVImportResponse struct {
	ProcessedRows    int                    `json:"processed_rows"`
	CreatedCustomers int                    `json:"created_customers"`
	UpdatedCustomers int                    `json:"updated_customers"`
	RejectedRows     []customerCSVFailedRow `json:"failed_rows"`
	Delimiter        string                 `json:"delimiter"`
	AcceptedColumns  []string               `json:"accepted_columns"`
}

func productCSVColumnIndex(headerRow []string) (map[string]int, error) {
	header := make([]string, len(headerRow))
	for i, cell := range headerRow {
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
		return nil, requestError{Status: http.StatusBadRequest, Message: "Falta la columna requerida id."}
	}
	for _, col := range []string{"linea", "nombre", "cantidad", "precio_venta"} {
		if _, ok := index[col]; !ok {
			return nil, requestError{Status: http.StatusBadRequest, Message: "Faltan columnas requeridas en el CSV."}
		}
	}
	return index, nil
}

func normalizedProductSize(required bool, size string) (string, error) {
	size = strings.TrimSpace(size)
	if !required {
		return "", nil
	}
	if size == "" {
		return "", requestError{Status: http.StatusBadRequest, Message: "La talla es obligatoria cuando el producto la requiere."}
	}
	if len([]rune(size)) > 80 {
		return "", requestError{Status: http.StatusBadRequest, Message: "La talla no puede superar 80 caracteres."}
	}
	return size, nil
}

func normalizeCustomerCSVHeader(value string) string {
	value = strings.TrimSpace(strings.TrimPrefix(value, "\ufeff"))
	value = strings.ToLower(value)
	replacer := strings.NewReplacer("-", "_", " ", "_")
	return replacer.Replace(value)
}

func customerCSVColumnIndex(headerRow []string) (map[string]int, []string, rune, error) {
	if len(headerRow) == 0 {
		return nil, nil, ',', requestError{Status: http.StatusBadRequest, Message: "El CSV no tiene encabezado."}
	}

	aliases := map[string]string{
		"name":                     "name",
		"customer_name":            "name",
		"phone":                    "phone",
		"customer_phone":           "phone",
		"document_type":            "document_type",
		"customer_document_type":   "document_type",
		"document_number":          "document_number",
		"customer_document_number": "document_number",
		"address":                  "address",
		"customer_address":         "address",
		"city":                     "city",
		"customer_city":            "city",
		"notes":                    "notes",
		"customer_notes":           "notes",
	}
	forbidden := map[string]struct{}{
		"tenant_id":   {},
		"customer_id": {},
		"id":          {},
	}

	index := make(map[string]int, len(headerRow))
	accepted := make([]string, 0, len(headerRow))
	delimiter := ','

	for i, cell := range headerRow {
		if i == 0 && strings.Contains(cell, ";") && !strings.Contains(cell, ",") {
			delimiter = ';'
		}
		name := normalizeCustomerCSVHeader(cell)
		if name == "" {
			continue
		}
		if _, blocked := forbidden[name]; blocked {
			return nil, nil, delimiter, requestError{Status: http.StatusBadRequest, Message: "El CSV no permite columnas de tenant o IDs internos."}
		}
		canonical, ok := aliases[name]
		if !ok {
			return nil, nil, delimiter, requestError{Status: http.StatusBadRequest, Message: fmt.Sprintf("La columna %q no es soportada para importación de clientes.", strings.TrimSpace(cell))}
		}
		if _, exists := index[canonical]; exists {
			return nil, nil, delimiter, requestError{Status: http.StatusBadRequest, Message: fmt.Sprintf("La columna %q está duplicada en el CSV.", canonical)}
		}
		index[canonical] = i
		accepted = append(accepted, canonical)
	}

	for _, required := range []string{"name", "phone", "document_type", "document_number", "city"} {
		if _, ok := index[required]; !ok {
			return nil, nil, delimiter, requestError{Status: http.StatusBadRequest, Message: "Faltan columnas requeridas en el CSV de clientes."}
		}
	}

	sort.Strings(accepted)
	return index, accepted, delimiter, nil
}

func detectCSVDelimiter(content string) rune {
	firstLine := content
	if idx := strings.IndexAny(firstLine, "\r\n"); idx >= 0 {
		firstLine = firstLine[:idx]
	}
	if strings.Count(firstLine, ";") > strings.Count(firstLine, ",") {
		return ';'
	}
	return ','
}

func sanitizeCustomerCSVText(value string) string {
	value = strings.TrimSpace(strings.TrimPrefix(value, "\ufeff"))
	value = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == '\t' {
			return r
		}
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, value)
	return value
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
	TallaRequerida        bool
	Talla                 string
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

type productEditRecord struct {
	SKU               string
	ID                string
	Name              string
	Line              string
	Location          string
	TallaRequerida    int
	Talla             string
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

type productInventoryCounts struct {
	Available int
	Reserved  int
	Swapped   int
	Damaged   int
}

func requestedVisibleProductID(id, legacySKUAlias string) (string, error) {
	id = strings.TrimSpace(id)
	legacySKUAlias = strings.TrimSpace(legacySKUAlias)
	switch {
	case id == "" && legacySKUAlias == "":
		return "", nil
	case id == "":
		return legacySKUAlias, nil
	case legacySKUAlias == "":
		return id, nil
	case id == legacySKUAlias:
		return id, nil
	default:
		return "", requestError{
			Status:  http.StatusBadRequest,
			Message: "Datos inválidos.",
			Fields: map[string]string{
				"id":  "No envíes id y sku con valores distintos.",
				"sku": "No envíes sku como alias si no coincide con el id visible.",
			},
		}
	}
}

func (p productOption) refID() string {
	if strings.TrimSpace(p.SKU) != "" {
		return strings.TrimSpace(p.SKU)
	}
	return strings.TrimSpace(p.ID)
}

type productIdentityRecord struct {
	SKU         string
	VisibleID   string
	OwnerUserID sql.NullInt64
}

type BusinessSettings struct {
	ID                    int
	BusinessName          string
	LogoPath              string
	ContactPhone          string
	ContactEmail          string
	SocialMedia           string
	PrimaryColor          string
	Currency              string
	DateFormat            string
	LabelPaperWidth       string
	DefaultLabelProfileID int
	InvoicePaperWidth     string
	TicketPaperWidth      string
	UpdatedAt             string
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
	roleAPIKey        = "api_key"

	defaultTenantID   = 1
	defaultTenantSlug = "default"
	defaultTenantName = "Default tenant"
)

type dbEngine string

const (
	dbEnginePostgres dbEngine = "postgres"

	postgresDriverName = "stocki-postgres"
)

const (
	apiAuthModeAPIKey  = "api_key"
	apiAuthModeSession = "session"
)

type databaseConfig struct {
	Engine dbEngine
	DSN    string
	Label  string
}

var (
	errInsufficientStock    = fmt.Errorf("stock insuficiente")
	errMissingTenantContext = fmt.Errorf("tenant context required")
	errProductSKUConflict   = fmt.Errorf("product sku conflict")

	businessSettingsMu sync.RWMutex
	businessSettings   = defaultBusinessSettings()
)

func defaultBusinessSettings() BusinessSettings {
	return BusinessSettings{
		ID:                1,
		BusinessName:      "Stocki App",
		LogoPath:          "/static/img/logo1.svg",
		ContactPhone:      "",
		ContactEmail:      "",
		SocialMedia:       "",
		PrimaryColor:      "#0ea5c9",
		Currency:          "COP",
		DateFormat:        "2006-01-02",
		LabelPaperWidth:   "58mm",
		InvoicePaperWidth: "58mm",
		TicketPaperWidth:  "58mm",
	}
}

func defaultSeedProducts() []productOption {
	return []productOption{
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
}

type sqlExecer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

type sqlQueryExecer interface {
	sqlExecer
	QueryRow(query string, args ...any) *sql.Row
}

type sqlQueryRunner interface {
	sqlQueryExecer
	Query(query string, args ...any) (*sql.Rows, error)
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

func loadDatabaseConfig() (databaseConfig, error) {
	engineRaw := strings.TrimSpace(strings.ToLower(os.Getenv("DB_ENGINE")))
	if engineRaw != "" && engineRaw != string(dbEnginePostgres) {
		return databaseConfig{}, fmt.Errorf("DB_ENGINE=%s ya no es soportado; StockiAPP ahora requiere Postgres", engineRaw)
	}

	dsn := strings.TrimSpace(os.Getenv("DB_DSN"))
	if dsn == "" {
		dsn = strings.TrimSpace(os.Getenv("DATABASE_URL"))
	}
	if dsn == "" {
		return databaseConfig{}, fmt.Errorf("DB_DSN o DATABASE_URL es obligatorio; SQLite ya no es soportado")
	}

	return databaseConfig{
		Engine: dbEnginePostgres,
		DSN:    dsn,
		Label:  "Postgres",
	}, nil
}

const (
	loginFormBodyLimit      int64 = 64 << 10
	defaultFormBodyLimit    int64 = 256 << 10
	defaultJSONBodyLimit    int64 = 1 << 20
	brandingUploadBodyLimit int64 = 10 << 20
	customerCSVUploadLimit  int64 = 8 << 20
	csvUploadBodyLimit      int64 = 40 << 20

	loginRateWindow       = 10 * time.Minute
	loginRateLockDuration = 5 * time.Minute
	loginRateMaxFailures  = 5

	defaultResponseCSP   = "default-src 'self'; base-uri 'self'; connect-src 'self'; font-src 'self' data:; frame-ancestors 'self'; img-src 'self' data: blob:; object-src 'none'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'"
	staticSVGResponseCSP = "default-src 'none'; img-src 'self' data:; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'; sandbox"
)

type loginRateRecord struct {
	windowStart  time.Time
	failures     int
	blockedUntil time.Time
}

type loginRateLimiter struct {
	mu      sync.Mutex
	records map[string]loginRateRecord
}

var appLoginRateLimiter = newLoginRateLimiter()

func newLoginRateLimiter() *loginRateLimiter {
	return &loginRateLimiter{records: map[string]loginRateRecord{}}
}

func (l *loginRateLimiter) allow(key string, now time.Time) (time.Duration, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	record, ok := l.records[key]
	if !ok {
		return 0, true
	}
	if !record.blockedUntil.IsZero() && now.Before(record.blockedUntil) {
		return record.blockedUntil.Sub(now), false
	}
	if now.Sub(record.windowStart) > loginRateWindow {
		delete(l.records, key)
	}
	return 0, true
}

func (l *loginRateLimiter) recordFailure(key string, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()

	record := l.records[key]
	if record.windowStart.IsZero() || now.Sub(record.windowStart) > loginRateWindow {
		record = loginRateRecord{windowStart: now}
	}
	record.failures++
	if record.failures >= loginRateMaxFailures {
		record.blockedUntil = now.Add(loginRateLockDuration)
		record.failures = 0
		record.windowStart = now
	}
	l.records[key] = record
}

func (l *loginRateLimiter) reset(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.records, key)
}

func clientIPFromRequest(r *http.Request) string {
	if forwarded := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); forwarded != "" {
		parts := strings.Split(forwarded, ",")
		if len(parts) > 0 {
			candidate := strings.TrimSpace(parts[0])
			if candidate != "" {
				return candidate
			}
		}
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err == nil && host != "" {
		return host
	}
	return strings.TrimSpace(r.RemoteAddr)
}

func loginRateLimitKeys(r *http.Request, username string) []string {
	ip := strings.TrimSpace(clientIPFromRequest(r))
	username = strings.ToLower(strings.TrimSpace(username))
	keys := []string{}
	if ip != "" {
		keys = append(keys, "ip:"+ip)
	}
	if username != "" && ip != "" {
		keys = append(keys, "user:"+username+"|"+ip)
	} else if username != "" {
		keys = append(keys, "user:"+username)
	}
	return keys
}

func loginRequestAllowed(r *http.Request, username string, now time.Time) (time.Duration, bool) {
	keys := loginRateLimitKeys(r, username)
	if len(keys) == 0 {
		return 0, true
	}
	var retryAfter time.Duration
	for _, key := range keys {
		wait, allowed := appLoginRateLimiter.allow(key, now)
		if !allowed {
			if wait > retryAfter {
				retryAfter = wait
			}
		}
	}
	return retryAfter, retryAfter == 0
}

func registerLoginFailure(r *http.Request, username string, now time.Time) {
	for _, key := range loginRateLimitKeys(r, username) {
		appLoginRateLimiter.recordFailure(key, now)
	}
}

func resetLoginRateLimit(r *http.Request, username string) {
	for _, key := range loginRateLimitKeys(r, username) {
		appLoginRateLimiter.reset(key)
	}
}

func resetLoginRateLimitForUsername(username string) {
	username = strings.ToLower(strings.TrimSpace(username))
	if username == "" {
		return
	}
	appLoginRateLimiter.mu.Lock()
	defer appLoginRateLimiter.mu.Unlock()
	for key := range appLoginRateLimiter.records {
		if strings.HasPrefix(key, "user:"+username) {
			delete(appLoginRateLimiter.records, key)
		}
	}
}

func requestMutatesState(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func forwardedProto(r *http.Request) string {
	if raw := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")); raw != "" {
		return strings.ToLower(strings.TrimSpace(strings.Split(raw, ",")[0]))
	}
	for _, field := range strings.Split(r.Header.Get("Forwarded"), ";") {
		part := strings.TrimSpace(field)
		if !strings.HasPrefix(strings.ToLower(part), "proto=") {
			continue
		}
		value := strings.Trim(strings.TrimSpace(strings.TrimPrefix(part, "proto=")), `"`)
		if value != "" {
			return strings.ToLower(value)
		}
	}
	return ""
}

func requestScheme(r *http.Request) string {
	if forced := strings.TrimSpace(firstNonEmptyString(os.Getenv("SECURE_COOKIES"), os.Getenv("COOKIE_SECURE"))); forced != "" {
		if secure, err := strconv.ParseBool(forced); err == nil && secure {
			return "https"
		}
	}
	if proto := forwardedProto(r); proto == "https" {
		return "https"
	}
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

func requestUsesSecureCookies(r *http.Request) bool {
	if forced := strings.TrimSpace(firstNonEmptyString(os.Getenv("SECURE_COOKIES"), os.Getenv("COOKIE_SECURE"))); forced != "" {
		if secure, err := strconv.ParseBool(forced); err == nil {
			return secure
		}
	}
	return requestScheme(r) == "https"
}

func requestOriginHost(r *http.Request) string {
	if host := strings.TrimSpace(r.Host); host != "" {
		return host
	}
	return strings.TrimSpace(r.Header.Get("X-Forwarded-Host"))
}

func parseRequestOrigin(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("missing origin")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, errors.New("invalid origin")
	}
	return parsed, nil
}

func requestPassesCSRFSameOriginCheck(r *http.Request) bool {
	scheme := requestScheme(r)
	host := requestOriginHost(r)
	if host == "" {
		return false
	}

	check := func(raw string) bool {
		parsed, err := parseRequestOrigin(raw)
		if err != nil {
			return false
		}
		return strings.EqualFold(parsed.Scheme, scheme) && strings.EqualFold(parsed.Host, host)
	}

	if origin := strings.TrimSpace(r.Header.Get("Origin")); origin != "" {
		return check(origin)
	}
	if referer := strings.TrimSpace(r.Header.Get("Referer")); referer != "" {
		return check(referer)
	}
	return false
}

func apiKeyRequestAllowed(r *http.Request) bool {
	path := strings.TrimRight(strings.TrimSpace(r.URL.Path), "/")
	if path == "" {
		path = "/"
	}

	switch {
	case r.Method == http.MethodGet && path == "/api/health":
		return true
	case r.Method == http.MethodGet && path == "/api/settings/business":
		return true
	case r.Method == http.MethodGet && path == "/api/settings/lines":
		return true
	case r.Method == http.MethodGet && path == "/api/settings/owners":
		return true
	case r.Method == http.MethodGet && path == "/api/users":
		return true
	case r.Method == http.MethodGet && path == "/api/products":
		return true
	case r.Method == http.MethodGet && path == "/api/products/search":
		return true
	case (r.Method == http.MethodPatch || r.Method == http.MethodPut) && strings.HasPrefix(path, "/api/products/"):
		return true
	case r.Method == http.MethodGet && path == "/api/productos/precio":
		return true
	case r.Method == http.MethodGet && path == "/api/inventory":
		return true
	case r.Method == http.MethodPost && path == "/api/inventory/adjust":
		return true
	case r.Method == http.MethodGet && path == "/api/sales/recent":
		return true
	case (r.Method == http.MethodGet || r.Method == http.MethodPost) && path == "/api/sales":
		return true
	case r.Method == http.MethodPost && path == "/api/swaps":
		return true
	case (r.Method == http.MethodGet || r.Method == http.MethodPost) && path == "/api/retomas":
		return true
	case (r.Method == http.MethodGet || r.Method == http.MethodPost) && path == "/api/customers":
		return true
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/api/customers/"):
		return true
	case (r.Method == http.MethodGet || r.Method == http.MethodPost) && path == "/api/credits":
		return true
	case (r.Method == http.MethodGet || r.Method == http.MethodPost) && path == "/api/credits/installments":
		return true
	case r.Method == http.MethodGet && path == "/api/credits/edited":
		return false
	case (r.Method == http.MethodGet || r.Method == http.MethodPut || r.Method == http.MethodPatch) && strings.HasPrefix(path, "/api/credits/"):
		return true
	case (r.Method == http.MethodGet || r.Method == http.MethodPost) && path == "/api/invoices":
		return true
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/api/invoices/"):
		return true
	case r.Method == http.MethodGet && path == "/api/agent/business":
		return true
	case r.Method == http.MethodGet && path == "/api/agent/customers/search":
		return true
	case r.Method == http.MethodPost && path == "/api/agent/credits":
		return true
	case r.Method == http.MethodPost && path == "/api/agent/invoices":
		return true
	case r.Method == http.MethodGet && path == "/api/agent/products/search":
		return true
	case r.Method == http.MethodGet && path == "/api/agent/products/price":
		return true
	case r.Method == http.MethodGet && path == "/api/agent/inventory":
		return true
	case r.Method == http.MethodGet && path == "/api/agent/product-loans":
		return true
	default:
		return false
	}
}

func requestBodyLimit(r *http.Request) int64 {
	if !requestMutatesState(r.Method) {
		return 0
	}
	switch {
	case r.URL.Path == "/configuracion":
		return brandingUploadBodyLimit
	case r.URL.Path == "/clientes/csv":
		return customerCSVUploadLimit
	case r.URL.Path == "/productos/csv":
		return csvUploadBodyLimit
	case strings.HasPrefix(r.URL.Path, "/api/"):
		return defaultJSONBodyLimit
	default:
		return defaultFormBodyLimit
	}
}

func applyRequestBodyLimit(w http.ResponseWriter, r *http.Request) {
	if limit := requestBodyLimit(r); limit > 0 && r.Body != nil {
		r.Body = http.MaxBytesReader(w, r.Body, limit)
	}
}

func setSecurityHeaders(w http.ResponseWriter, r *http.Request) {
	csp := defaultResponseCSP
	if r != nil &&
		strings.HasPrefix(strings.TrimSpace(r.URL.Path), "/static/uploads/branding/") &&
		strings.HasSuffix(strings.ToLower(strings.TrimSpace(r.URL.Path)), ".svg") {
		csp = staticSVGResponseCSP
	}
	w.Header().Set("Content-Security-Policy", csp)
	w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "SAMEORIGIN")
}

func sanitizePostgresIdentifier(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return "stocki"
	}
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			if b.Len() == 0 {
				b.WriteString("s_")
			}
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	result := strings.Trim(b.String(), "_")
	if result == "" {
		return "stocki"
	}
	return result
}

func quotePostgresIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func postgresDSNWithSearchPath(dsn, schema string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func postgresDSNWithSessionTimeZone(dsn, timezone string) (string, error) {
	if strings.TrimSpace(dsn) == "" || strings.TrimSpace(timezone) == "" {
		return dsn, nil
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	if strings.TrimSpace(query.Get("timezone")) == "" && strings.TrimSpace(query.Get("TimeZone")) == "" {
		query.Set("timezone", timezone)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func syncPostgresIdentitySequence(db *sql.DB, table, column string) error {
	table = sanitizePostgresIdentifier(table)
	column = sanitizePostgresIdentifier(column)
	if table == "" || column == "" {
		return fmt.Errorf("tabla o columna inválida para sincronizar secuencia")
	}
	query := fmt.Sprintf(`
		SELECT setval(
			pg_get_serial_sequence('%s', '%s'),
			GREATEST(COALESCE((SELECT MAX(%s) FROM %s), 0), 1)
		)
	`, table, column, quotePostgresIdentifier(column), quotePostgresIdentifier(table))
	_, err := db.Exec(query)
	return err
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

func insertAndReturnID(exec sqlQueryExecer, query string, args ...any) (int64, error) {
	var id int64
	if err := exec.QueryRow(query+" RETURNING id", args...).Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func sqlDatePrefixExpr(column string) string {
	return fmt.Sprintf("substr(%s, 1, 10)", column)
}

func upsertProducto(exec sqlExecer, tenantID int, sku, productID, nombre, linea, now string) error {
	// productos table is part of the existing DB schema and uses sku as the primary key.
	// Other columns (prices, discount, notes) have defaults so manual creation can omit them.
	productID = strings.TrimSpace(productID)
	if productID == "" {
		return fmt.Errorf("id visible obligatorio para upsertProducto")
	}
	result, err := exec.Exec(`
		INSERT INTO productos (sku, tenant_id, id, linea, nombre, fecha_ingreso)
		VALUES (?, ?, ?, ?, ?, COALESCE((SELECT fecha_ingreso FROM productos WHERE sku = ? AND tenant_id = ?), ?))
		ON CONFLICT(sku) DO UPDATE SET
			tenant_id = excluded.tenant_id,
			id = excluded.id,
			linea = excluded.linea,
			nombre = excluded.nombre
		WHERE productos.tenant_id = excluded.tenant_id
		  AND productos.id = excluded.id
	`, sku, normalizeTenantID(tenantID), productID, linea, nombre, sku, normalizeTenantID(tenantID), strings.TrimSpace(now))
	if err != nil {
		return err
	}
	affected, affectedErr := result.RowsAffected()
	if affectedErr == nil && affected == 0 {
		return errProductSKUConflict
	}
	return nil
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

func repairMissingProductosFromUnits(db *sql.DB) error {
	if _, err := db.Exec(`
		INSERT INTO productos (sku, tenant_id, id, nombre, linea, fecha_ingreso)
		SELECT DISTINCT u.producto_id, COALESCE(NULLIF(u.tenant_id, 0), ?), NULL, u.producto_id, 'Sin línea', CURRENT_TIMESTAMP
		FROM unidades u
		WHERE NOT EXISTS (
			SELECT 1
			FROM productos p
			WHERE p.sku = u.producto_id AND p.tenant_id = COALESCE(NULLIF(u.tenant_id, 0), ?)
		)
		ON CONFLICT(sku) DO NOTHING
	`, defaultTenantID, defaultTenantID); err != nil {
		return err
	}
	return nil
}

func seedProductosIfMissing(db *sql.DB, defaults []productOption) error {
	for _, p := range defaults {
		var existingCount int
		if err := db.QueryRow(`
			SELECT COUNT(*)
			FROM productos
			WHERE tenant_id = ? AND (id = ? OR sku = ?)
		`, defaultTenantID, strings.TrimSpace(p.ID), strings.TrimSpace(p.ID)).Scan(&existingCount); err != nil {
			return err
		}
		if existingCount > 0 {
			continue
		}
		sku, err := generateNextProductSKU(db)
		if err != nil {
			return err
		}
		if err := upsertProducto(db, defaultTenantID, sku, p.ID, p.Name, p.Line, time.Now().Format(time.RFC3339)); err != nil {
			return err
		}
	}
	return nil
}

func loadProductosForTenant(db *sql.DB, tenantID int) ([]productOption, error) {
	rows, err := db.Query(`
		SELECT sku, COALESCE(NULLIF(id, ''), sku), nombre, linea, COALESCE(location, ''), COALESCE(talla_requerida, 0), COALESCE(talla, ''), COALESCE(credit_enabled, 0), COALESCE(debtor_name, ''), COALESCE(installments_total, 0), COALESCE(installments_paid, 0), COALESCE(total_value, 0), COALESCE(installment_value, 0), COALESCE(anotaciones, ''), COALESCE(fecha_ingreso, ''), COALESCE(precio_venta, 0), COALESCE(retoma_enabled, 0), retoma_price, owner_user_id
		FROM productos
		WHERE tenant_id = ?
		ORDER BY COALESCE(NULLIF(id, ''), sku), sku
	`, normalizeTenantID(tenantID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	products := []productOption{}
	for rows.Next() {
		var p productOption
		var tallaRequerida int
		var creditEnabled int
		var retomaEnabled int
		var retomaPrice sql.NullFloat64
		var ownerUserID sql.NullInt64
		if err := rows.Scan(&p.SKU, &p.ID, &p.Name, &p.Line, &p.Location, &tallaRequerida, &p.Talla, &creditEnabled, &p.DebtorName, &p.InstallmentsTotal, &p.InstallmentsPaid, &p.TotalValue, &p.InstallmentValue, &p.Notes, &p.FechaIngreso, &p.SalePrice, &retomaEnabled, &retomaPrice, &ownerUserID); err != nil {
			return nil, err
		}
		p.TallaRequerida = tallaRequerida == 1
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
	tenantID, err := tenantIDFromUserStrict(user)
	if err != nil {
		return nil, err
	}
	products, err := loadProductosForTenant(db, tenantID)
	if err != nil {
		return nil, err
	}
	return filterProductsForUser(products, user), nil
}

func loadProductIdentityByVisibleID(db *sql.DB, tenantID int, visibleID string) (productIdentityRecord, error) {
	visibleID = strings.TrimSpace(visibleID)
	if visibleID == "" {
		return productIdentityRecord{}, sql.ErrNoRows
	}
	var record productIdentityRecord
	err := db.QueryRow(`
		SELECT sku, id, owner_user_id
		FROM productos
		WHERE tenant_id = ? AND id = ?
		LIMIT 1
	`, normalizeTenantID(tenantID), visibleID).Scan(&record.SKU, &record.VisibleID, &record.OwnerUserID)
	if err != nil {
		return productIdentityRecord{}, err
	}
	if strings.TrimSpace(record.VisibleID) == "" {
		return productIdentityRecord{}, fmt.Errorf("producto %q sin id visible en tenant %d", record.SKU, normalizeTenantID(tenantID))
	}
	return record, nil
}

func loadProductIdentityBySKU(db *sql.DB, tenantID int, sku string) (productIdentityRecord, error) {
	sku = strings.TrimSpace(sku)
	if sku == "" {
		return productIdentityRecord{}, sql.ErrNoRows
	}
	var record productIdentityRecord
	err := db.QueryRow(`
		SELECT sku, id, owner_user_id
		FROM productos
		WHERE tenant_id = ? AND sku = ?
		LIMIT 1
	`, normalizeTenantID(tenantID), sku).Scan(&record.SKU, &record.VisibleID, &record.OwnerUserID)
	if err != nil {
		return productIdentityRecord{}, err
	}
	if strings.TrimSpace(record.VisibleID) == "" {
		return productIdentityRecord{}, fmt.Errorf("producto %q sin id visible en tenant %d", record.SKU, normalizeTenantID(tenantID))
	}
	return record, nil
}

func ensureVisibleProductIDAvailable(exec sqlQueryExecer, tenantID int, visibleID, excludeSKU string) error {
	visibleID = strings.TrimSpace(visibleID)
	excludeSKU = strings.TrimSpace(excludeSKU)
	if visibleID == "" {
		return requestError{
			Status:  http.StatusBadRequest,
			Message: "Datos inválidos.",
			Fields:  map[string]string{"id": "El ID visible es obligatorio."},
		}
	}

	args := []any{normalizeTenantID(tenantID), visibleID, visibleID}
	query := `
		SELECT COUNT(*)
		FROM productos
		WHERE tenant_id = ? AND (id = ? OR sku = ?)
	`
	if excludeSKU != "" {
		query += ` AND sku <> ?`
		args = append(args, excludeSKU)
	}

	var count int
	if err := exec.QueryRow(query, args...).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return requestError{
			Status:  http.StatusBadRequest,
			Message: "Ya existe otro producto con ese ID.",
			Fields:  map[string]string{"id": "Ya existe otro producto con ese ID."},
		}
	}
	return nil
}

func resolveProductRefForTenant(db *sql.DB, tenantID int, productID string) (string, string, error) {
	record, err := loadProductIdentityByVisibleID(db, tenantID, productID)
	if err != nil {
		return "", "", err
	}
	return record.SKU, record.VisibleID, nil
}

func resolveVisibleProductIDBySKUForTenant(db *sql.DB, tenantID int, sku string) (string, error) {
	record, err := loadProductIdentityBySKU(db, tenantID, sku)
	if err != nil {
		return "", err
	}
	return record.VisibleID, nil
}

func generateNextTenantProductID(exec sqlQueryRunner, tenantID int) (string, error) {
	rows, err := exec.Query(`
		SELECT id
		FROM productos
		WHERE tenant_id = ? AND id LIKE 'P-%'
	`, normalizeTenantID(tenantID))
	if err != nil {
		return "", err
	}
	defer rows.Close()

	maxNum := 0
	for rows.Next() {
		var productID string
		if err := rows.Scan(&productID); err != nil {
			return "", err
		}
		if !strings.HasPrefix(productID, "P-") {
			continue
		}
		n, err := strconv.Atoi(strings.TrimPrefix(productID, "P-"))
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
		if err := ensureVisibleProductIDAvailable(exec, tenantID, candidate, ""); err == nil {
			return candidate, nil
		} else {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				continue
			}
			return "", err
		}
	}
}

func isUniqueConstraintError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique constraint failed") || strings.Contains(msg, "duplicate key value violates unique constraint")
}

func isProductVisibleIDConflictError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "idx_productos_tenant_id_unique") || strings.Contains(msg, "productos.tenant_id, productos.id")
}

func isProductSKUConflictError(err error) bool {
	if errors.Is(err, errProductSKUConflict) {
		return true
	}
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "productos.sku") || strings.Contains(msg, "productos_pkey")
}

func insertProductWithGeneratedIdentity(tx sqlQueryRunner, tenantID int, requestedVisibleID, nombre, linea, now string) (string, string, error) {
	autoVisibleID := strings.TrimSpace(requestedVisibleID) == ""
	for attempt := 0; attempt < 16; attempt++ {
		visibleID := strings.TrimSpace(requestedVisibleID)
		if visibleID == "" {
			var err error
			visibleID, err = generateNextTenantProductID(tx, tenantID)
			if err != nil {
				return "", "", err
			}
		}
		internalSKU, err := generateNextProductSKU(tx)
		if err != nil {
			return "", "", err
		}
		err = upsertProducto(tx, tenantID, internalSKU, visibleID, nombre, linea, now)
		if err == nil {
			return internalSKU, visibleID, nil
		}
		if isProductSKUConflictError(err) {
			continue
		}
		if isProductVisibleIDConflictError(err) {
			if autoVisibleID {
				continue
			}
			return "", "", requestError{
				Status:  http.StatusBadRequest,
				Message: "Ya existe otro producto con ese ID.",
				Fields:  map[string]string{"id": "Ya existe otro producto con ese ID."},
			}
		}
		if isUniqueConstraintError(err) && autoVisibleID {
			continue
		}
		return "", "", err
	}
	return "", "", fmt.Errorf("no se pudo reservar una identidad única para el producto")
}

func backfillMissingProductVisibleIDs(db *sql.DB) error {
	rows, err := db.Query(`
		SELECT tenant_id, sku
		FROM productos
		WHERE id IS NULL OR TRIM(id) = ''
		ORDER BY tenant_id ASC, sku ASC
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	type missingProductVisibleID struct {
		TenantID int
		SKU      string
	}

	pending := make([]missingProductVisibleID, 0)
	for rows.Next() {
		var item missingProductVisibleID
		if err := rows.Scan(&item.TenantID, &item.SKU); err != nil {
			return err
		}
		pending = append(pending, item)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, item := range pending {
		visibleID, err := generateNextTenantProductID(db, item.TenantID)
		if err != nil {
			return err
		}
		if _, err := db.Exec(`
			UPDATE productos
			SET id = ?
			WHERE tenant_id = ? AND sku = ? AND (id IS NULL OR TRIM(id) = '')
		`, visibleID, normalizeTenantID(item.TenantID), item.SKU); err != nil {
			return err
		}
	}
	return nil
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

func canAccessProduct(user *User, product productOption) bool {
	if user != nil && hasTenantWideVisibility(user.Role) {
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
	tenantID, err := tenantIDFromUserStrict(user)
	if err != nil {
		return false, nil
	}
	record, err := loadProductIdentityByVisibleID(db, tenantID, productID)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if user != nil && hasTenantWideVisibility(user.Role) {
		return true, nil
	}
	if !record.OwnerUserID.Valid {
		return true, nil
	}
	if user == nil {
		return false, nil
	}
	return int(record.OwnerUserID.Int64) == user.ID, nil
}

func productAccessibleBySKU(db *sql.DB, user *User, sku string) (bool, error) {
	tenantID, err := tenantIDFromUserStrict(user)
	if err != nil {
		return false, nil
	}
	record, err := loadProductIdentityBySKU(db, tenantID, sku)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if user != nil && hasTenantWideVisibility(user.Role) {
		return true, nil
	}
	if !record.OwnerUserID.Valid {
		return true, nil
	}
	if user == nil {
		return false, nil
	}
	return int(record.OwnerUserID.Int64) == user.ID, nil
}

func loadProductEditRecord(db *sql.DB, tenantID int, productID string) (productEditRecord, error) {
	var record productEditRecord
	err := db.QueryRow(`
		SELECT sku, id, nombre, linea, COALESCE(location, ''), COALESCE(talla_requerida, 0), COALESCE(talla, ''), COALESCE(credit_enabled, 0), COALESCE(debtor_name, ''), COALESCE(installments_total, 0), COALESCE(installments_paid, 0), COALESCE(total_value, 0), COALESCE(installment_value, 0), COALESCE(precio_venta, 0), COALESCE(retoma_enabled, 0), retoma_price, COALESCE(anotaciones, ''), owner_user_id
		FROM productos
		WHERE tenant_id = ? AND id = ?
		LIMIT 1
	`, normalizeTenantID(tenantID), strings.TrimSpace(productID)).Scan(
		&record.SKU,
		&record.ID,
		&record.Name,
		&record.Line,
		&record.Location,
		&record.TallaRequerida,
		&record.Talla,
		&record.CreditEnabled,
		&record.DebtorName,
		&record.InstallmentsTotal,
		&record.InstallmentsPaid,
		&record.TotalValue,
		&record.InstallmentValue,
		&record.SalePrice,
		&record.RetomaEnabled,
		&record.RetomaPrice,
		&record.Notes,
		&record.OwnerUserID,
	)
	return record, err
}

func renameProductIdentifier(tx *sql.Tx, tenantID int, previousSKU, newSKU string) error {
	previousSKU = strings.TrimSpace(previousSKU)
	newSKU = strings.TrimSpace(newSKU)
	if previousSKU == "" || newSKU == "" {
		return nil
	}

	var currentID string
	if err := tx.QueryRow(`
		SELECT COALESCE(NULLIF(id, ''), sku)
		FROM productos
		WHERE tenant_id = ? AND sku = ?
		LIMIT 1
	`, normalizeTenantID(tenantID), previousSKU).Scan(&currentID); err != nil {
		return err
	}
	if currentID == newSKU {
		return nil
	}

	if err := ensureVisibleProductIDAvailable(tx, tenantID, newSKU, previousSKU); err != nil {
		return err
	}

	if _, err := tx.Exec(`
		UPDATE productos
		SET id = ?
		WHERE tenant_id = ? AND sku = ?
	`, newSKU, normalizeTenantID(tenantID), previousSKU); err != nil {
		return err
	}
	return nil
}

func productVisibilityPredicate(alias string, user *User) (string, []any) {
	if alias == "" {
		alias = "p"
	}
	tenantID, err := tenantIDFromUserStrict(user)
	if err != nil {
		return "1 = 0", nil
	}
	if user != nil && hasTenantWideVisibility(user.Role) {
		return fmt.Sprintf("%s.tenant_id = ?", alias), []any{tenantID}
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
	tenantID, err := tenantIDFromUserStrict(user)
	if err != nil {
		return "1 = 0", nil
	}
	if user != nil && hasTenantWideVisibility(user.Role) {
		return fmt.Sprintf("%s.tenant_id = ?", entityAlias), []any{tenantID}
	}
	return fmt.Sprintf("(%s.tenant_id = ? AND (%s.sku IS NULL OR %s.owner_user_id IS NULL OR %s.owner_user_id = ?))", entityAlias, productAlias, productAlias, productAlias), []any{tenantID, user.ID}
}

func creditVisibilityPredicate(creditAlias string, user *User) (string, []any) {
	if creditAlias == "" {
		creditAlias = "cs"
	}
	tenantID, err := tenantIDFromUserStrict(user)
	if err != nil {
		return "1 = 0", nil
	}
	if user != nil && hasTenantWideVisibility(user.Role) {
		return fmt.Sprintf("%s.tenant_id = ?", creditAlias), []any{tenantID}
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
		SELECT v.id, v.fecha, COALESCE(NULLIF(p.id, ''), v.producto_id), COALESCE(p.nombre, v.producto_id), v.cantidad, v.precio_final, COALESCE(v.metodo_pago, '')
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
			COALESCE(NULLIF(p.id, ''), v.producto_id),
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
		query += ` AND (LOWER(COALESCE(NULLIF(p.id, ''), v.producto_id)) LIKE ? OR LOWER(COALESCE(p.nombre, '')) LIKE ? OR LOWER(COALESCE(v.channel, '')) LIKE ? OR LOWER(COALESCE(v.sold_by, '')) LIKE ? OR LOWER(COALESCE(v.notas, '')) LIKE ? OR LOWER(COALESCE(v.metodo_pago, '')) LIKE ?)`
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
			COALESCE(NULLIF(p.id, ''), r.producto_id),
			COALESCE(p.nombre, r.producto_id),
			COALESCE(r.customer_id, 0),
			COALESCE(c.name, ''),
			r.cantidad,
			r.valor_recibido,
			r.estado_recibido,
			r.publicado_stock,
			r.precio_publicado,
			COALESCE(r.notas, '')
		FROM retomas r
		LEFT JOIN productos p ON p.sku = r.producto_id AND p.tenant_id = r.tenant_id
		LEFT JOIN customers c ON c.id = r.customer_id AND c.tenant_id = r.tenant_id
		WHERE ` + accessSQL
	if q != "" {
		query += ` AND (LOWER(COALESCE(NULLIF(p.id, ''), r.producto_id)) LIKE ? OR LOWER(COALESCE(p.nombre, '')) LIKE ? OR LOWER(COALESCE(c.name, '')) LIKE ? OR LOWER(COALESCE(r.estado_recibido, '')) LIKE ? OR LOWER(COALESCE(r.notas, '')) LIKE ?)`
		qLike := "%" + q + "%"
		args = append(args, qLike, qLike, qLike, qLike, qLike)
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
			customerID       int
			customerName     string
			quantity         int
			valueReceived    float64
			receivedState    string
			publishedToStock int
			finalSalePrice   sql.NullFloat64
			notes            string
		)
		if err := rows.Scan(&id, &fecha, &productID, &productName, &customerID, &customerName, &quantity, &valueReceived, &receivedState, &publishedToStock, &finalSalePrice, &notes); err != nil {
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
			"customer_id":        customerID,
			"customer_name":      customerName,
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
			COALESCE(NULLIF(p.id, ''), cs.product_id, ''),
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
			COALESCE(pay.total_paid, 0),
			COALESCE(pay.paid_installments_count, COALESCE(cs.installments_paid, 0)),
			COALESCE(cs.notes, ''),
			COALESCE(last_payment.amount_paid, 0),
			COALESCE(last_payment.created_at, ''),
			COALESCE(last_payment.payment_type, '')
			,
			COALESCE(cs.status, '')
		FROM credit_sales cs
		LEFT JOIN productos p ON p.sku = cs.product_id AND p.tenant_id = cs.tenant_id
		LEFT JOIN customers c ON c.id = cs.customer_id AND c.tenant_id = cs.tenant_id
		LEFT JOIN (
			SELECT
				tenant_id,
				credit_sale_id,
				SUM(amount_paid) AS total_paid,
				SUM(CASE WHEN COALESCE(payment_type, 'cuota') = 'cuota' THEN 1 ELSE 0 END) AS paid_installments_count
			FROM credit_installments
			GROUP BY tenant_id, credit_sale_id
		) pay ON pay.tenant_id = cs.tenant_id AND pay.credit_sale_id = cs.id
		LEFT JOIN (
			SELECT tenant_id, credit_sale_id, amount_paid, created_at, payment_type
			FROM (
				SELECT
					tenant_id,
					credit_sale_id,
					amount_paid,
					created_at,
					COALESCE(payment_type, 'cuota') AS payment_type,
					ROW_NUMBER() OVER (
						PARTITION BY tenant_id, credit_sale_id
						ORDER BY created_at DESC, id DESC
					) AS row_num
				FROM credit_installments
			) ranked_credit_installments
			WHERE row_num = 1
		) last_payment ON last_payment.tenant_id = cs.tenant_id AND last_payment.credit_sale_id = cs.id
		WHERE ` + accessSQL
	args = append([]any{string(creditSaleKindProduct), string(creditSaleKindProduct), string(creditSaleKindCash)}, args...)
	if q != "" {
		query += ` AND (
			LOWER(COALESCE(NULLIF(p.id, ''), cs.product_id, '')) LIKE ?
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

func listCreditInstallmentsForUser(db *sql.DB, user *User, q, fromStr, toStr string, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 200
	}
	if limit > 500 {
		limit = 500
	}
	q = strings.TrimSpace(strings.ToLower(q))
	fromStr = strings.TrimSpace(fromStr)
	toStr = strings.TrimSpace(toStr)

	accessSQL, accessArgs := creditVisibilityPredicate("cs", user)
	query := `
		SELECT
			ci.id,
			cs.id,
			COALESCE(c.name, cs.debtor_name, ''),
			CASE
				WHEN COALESCE(cs.kind, ?) = ? THEN 'Préstamo de dinero'
				ELSE COALESCE(NULLIF(p.nombre, ''), cs.product_id, '')
			END,
			COALESCE(ci.amount_paid, 0),
			COALESCE(ci.payment_type, 'cuota'),
			COALESCE(ci.created_at, '')
		FROM credit_installments ci
		JOIN credit_sales cs ON cs.id = ci.credit_sale_id AND cs.tenant_id = ci.tenant_id
		LEFT JOIN productos p ON p.sku = cs.product_id AND p.tenant_id = cs.tenant_id
		LEFT JOIN customers c ON c.id = cs.customer_id AND c.tenant_id = cs.tenant_id
		WHERE ` + accessSQL
	args := append([]any{string(creditSaleKindProduct), string(creditSaleKindCash)}, accessArgs...)
	if fromStr != "" {
		query += ` AND ` + sqlDatePrefixExpr("ci.created_at") + ` >= ?`
		args = append(args, fromStr)
	}
	if toStr != "" {
		query += ` AND ` + sqlDatePrefixExpr("ci.created_at") + ` <= ?`
		args = append(args, toStr)
	}
	if q != "" {
		query += ` AND (
			LOWER(COALESCE(c.name, cs.debtor_name, '')) LIKE ?
			OR LOWER(CASE
				WHEN COALESCE(cs.kind, ?) = ? THEN 'Préstamo de dinero'
				ELSE COALESCE(NULLIF(p.nombre, ''), cs.product_id, '')
			END) LIKE ?
		)`
		qLike := "%" + q + "%"
		args = append(args, qLike, string(creditSaleKindProduct), string(creditSaleKindCash), qLike)
	}
	query += ` ORDER BY ci.created_at DESC, ci.id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0, limit)
	for rows.Next() {
		var (
			id           int
			creditSaleID int
			customerName string
			productName  string
			amountPaid   float64
			paymentType  string
			createdAt    string
		)
		if err := rows.Scan(&id, &creditSaleID, &customerName, &productName, &amountPaid, &paymentType, &createdAt); err != nil {
			return nil, err
		}
		items = append(items, map[string]any{
			"id":             id,
			"credit_sale_id": creditSaleID,
			"customer_name":  customerName,
			"product_name":   productName,
			"amount_paid":    amountPaid,
			"payment_type":   string(normalizeCreditPaymentType(paymentType)),
			"created_at":     formatDateWithSettings(createdAt),
		})
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
			COALESCE(NULLIF(p.id, ''), cs.product_id, ''),
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
			COALESCE(NULLIF(p.id, ''), pl.product_id, ''),
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
			LOWER(COALESCE(NULLIF(p.id, ''), pl.product_id, '')) LIKE ?
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

type inventoryProductUnitStats struct {
	AvailableCount int
	LoanedCount    int
	ChangeCount    int
	ReservedCount  int
	DamagedCount   int
	FirstCreatedAt string
}

func loadInventoryUnitsByProductIDs(db *sql.DB, tenantID int, productIDs []string) (map[string][]inventoryUnit, map[string]inventoryProductUnitStats, error) {
	unitsByProduct := make(map[string][]inventoryUnit, len(productIDs))
	statsByProduct := make(map[string]inventoryProductUnitStats, len(productIDs))
	if len(productIDs) == 0 {
		return unitsByProduct, statsByProduct, nil
	}

	seen := make(map[string]struct{}, len(productIDs))
	args := make([]any, 0, len(productIDs)+1)
	placeholders := make([]string, 0, len(productIDs))
	args = append(args, normalizeTenantID(tenantID))
	for _, productID := range productIDs {
		productID = strings.TrimSpace(productID)
		if productID == "" {
			continue
		}
		if _, ok := seen[productID]; ok {
			continue
		}
		seen[productID] = struct{}{}
		placeholders = append(placeholders, "?")
		args = append(args, productID)
	}
	if len(placeholders) == 0 {
		return unitsByProduct, statsByProduct, nil
	}

	rows, err := db.Query(`
		SELECT producto_id, id, estado, creado_en, caducidad
		FROM unidades
		WHERE tenant_id = ? AND producto_id IN (`+strings.Join(placeholders, ",")+`)
		ORDER BY producto_id ASC, creado_en ASC, id ASC
	`, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			productID string
			id        string
			estado    string
			creadoEn  string
			caducidad sql.NullString
		)
		if err := rows.Scan(&productID, &id, &estado, &creadoEn, &caducidad); err != nil {
			return nil, nil, err
		}
		stats := statsByProduct[productID]
		if stats.FirstCreatedAt == "" {
			stats.FirstCreatedAt = strings.TrimSpace(creadoEn)
		}
		fifo := "-"
		switch estado {
		case "Disponible", "available":
			stats.AvailableCount++
			fifo = strconv.Itoa(stats.AvailableCount)
		case "Prestada", "Prestado", "loaned":
			stats.LoanedCount++
		case "Reservada", "reserved":
			stats.ReservedCount++
		case "Cambio", "swapped":
			stats.ChangeCount++
		case "Danada", "Dañada", "damaged":
			stats.DamagedCount++
		}
		statsByProduct[productID] = stats
		unitsByProduct[productID] = append(unitsByProduct[productID], inventoryUnit{
			ID:          id,
			Estado:      estado,
			EstadoClass: estadoClass(estado),
			CreadoEn:    formatDateWithSettings(creadoEn),
			Caducidad:   formatDateWithSettings(caducidad.String),
			FIFO:        fifo,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	return unitsByProduct, statsByProduct, nil
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

func cashLoansInventoryURL(source *url.URL) string {
	params := url.Values{}
	params.Set("lineFilter", "Préstamo de dinero")

	switch normalizeCreditStatusFilter(strings.TrimSpace(source.Query().Get("status"))) {
	case string(creditStatusCompleted):
		params.Set("statusFilter", "credit_completed")
	case string(creditStatusSuspended):
		params.Set("statusFilter", "credit_suspended")
	case string(creditStatusCancelled):
		params.Set("statusFilter", "credit_cancelled")
	default:
		params.Set("statusFilter", "credit_active")
	}

	searchValue := strings.TrimSpace(source.Query().Get("credit_sale_id"))
	if searchValue == "" {
		searchValue = strings.TrimSpace(source.Query().Get("customer"))
	}
	if searchValue == "" {
		searchValue = strings.TrimSpace(source.Query().Get("username"))
	}
	if searchValue != "" {
		params.Set("searchFilter", searchValue)
	}

	return "/inventario?" + params.Encode()
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
			COALESCE(NULLIF(p.id, ''), pl.product_id, ''),
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
			COALESCE(NULLIF(p.id, ''), cs.product_id, ''),
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

func generateNextProductSKU(exec sqlQueryRunner) (string, error) {
	rows, err := exec.Query(`SELECT sku FROM productos WHERE sku LIKE 'SKU-%'`)
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
		if !strings.HasPrefix(sku, "SKU-") {
			continue
		}
		n, err := strconv.Atoi(strings.TrimPrefix(sku, "SKU-"))
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
		candidate := fmt.Sprintf("SKU-%06d", next)
		var count int
		if err := exec.QueryRow(`SELECT COUNT(*) FROM productos WHERE sku = ?`, candidate).Scan(&count); err != nil {
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

	upsertUserTimelineBucket := func(buckets map[string]*userTimelineBucket, label, fecha string, total float64) {
		label = strings.TrimSpace(label)
		if label == "" {
			label = "Sin usuario"
		}
		bucket, ok := buckets[label]
		if !ok {
			bucket = &userTimelineBucket{
				label:       label,
				valueByDate: map[string]float64{},
			}
			buckets[label] = bucket
		}
		bucket.valueByDate[fecha] += total
		bucket.total += total
	}
	parseDashboardAuditTotal := func(payloadRaw string) float64 {
		if strings.TrimSpace(payloadRaw) == "" {
			return 0
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(payloadRaw), &payload); err != nil {
			return 0
		}
		switch v := payload["total"].(type) {
		case float64:
			return v
		case string:
			if parsed, parseErr := strconv.ParseFloat(strings.TrimSpace(v), 64); parseErr == nil {
				return parsed
			}
		}
		return 0
	}
	parseDashboardAuditSoldBy := func(payloadRaw string) string {
		if strings.TrimSpace(payloadRaw) == "" {
			return ""
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(payloadRaw), &payload); err != nil {
			return ""
		}
		soldBy, _ := payload["sold_by"].(string)
		return strings.TrimSpace(soldBy)
	}

	userBuckets := map[string]*userTimelineBucket{}

	userTimelineSalesArgs := append([]any{startStr, endStr}, visibilityArgs...)
	userTimelineSalesRows, err := db.Query(`
		SELECT
			`+salesDateExpr+` as fecha,
			TRIM(COALESCE(v.sold_by, '')) as sold_by,
			COALESCE(SUM(v.precio_final * v.cantidad), 0) as total
		FROM ventas v
		LEFT JOIN productos p ON p.sku = v.producto_id
		WHERE `+salesDateExpr+` BETWEEN ? AND ?
			AND `+visibilitySQL+`
			AND NULLIF(TRIM(COALESCE(v.sold_by, '')), '') IS NOT NULL
		GROUP BY `+salesDateExpr+`, TRIM(COALESCE(v.sold_by, ''))
		ORDER BY sold_by ASC, fecha ASC
	`, userTimelineSalesArgs...)
	if err != nil {
		return dashboardDataResponse{}, err
	}
	defer userTimelineSalesRows.Close()

	for userTimelineSalesRows.Next() {
		var (
			fecha  string
			soldBy string
			total  float64
		)
		if err := userTimelineSalesRows.Scan(&fecha, &soldBy, &total); err != nil {
			return dashboardDataResponse{}, err
		}
		if total <= 0 {
			continue
		}
		upsertUserTimelineBucket(userBuckets, soldBy, fecha, total)
	}
	if err := userTimelineSalesRows.Err(); err != nil {
		return dashboardDataResponse{}, err
	}

	userTimelineAuditArgs := append([]any{startStr, endStr}, visibilityArgs...)
	userTimelineAuditRows, err := db.Query(`
		SELECT
			`+sqlDatePrefixExpr("a.created_at")+` as fecha,
			COALESCE(NULLIF(TRIM(u.username), ''), '') as user_label,
			COALESCE(a.payload_json, '{}') as payload_json
		FROM audit_events a
		LEFT JOIN users u ON u.id = a.user_id
		LEFT JOIN productos p ON p.sku = a.entity_id OR p.id = a.entity_id
		WHERE a.event_type = 'sale_registered'
			AND `+sqlDatePrefixExpr("a.created_at")+` BETWEEN ? AND ?
			AND `+visibilitySQL+`
		ORDER BY user_label ASC, fecha ASC
	`, userTimelineAuditArgs...)
	if err != nil {
		return dashboardDataResponse{}, err
	}
	defer userTimelineAuditRows.Close()

	for userTimelineAuditRows.Next() {
		var (
			fecha      string
			userLabel  string
			payloadRaw string
		)
		if err := userTimelineAuditRows.Scan(&fecha, &userLabel, &payloadRaw); err != nil {
			return dashboardDataResponse{}, err
		}
		if parseDashboardAuditSoldBy(payloadRaw) != "" {
			continue
		}
		total := parseDashboardAuditTotal(payloadRaw)
		if total <= 0 {
			continue
		}
		upsertUserTimelineBucket(userBuckets, userLabel, fecha, total)
	}
	if err := userTimelineAuditRows.Err(); err != nil {
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

	tenantID, err := tenantIDFromUserStrict(user)
	if err != nil {
		return dashboardDataResponse{}, err
	}
	_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantID)
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
	id = strings.TrimSpace(id)
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
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS movimientos (
			id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
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
	`)
	return err
}

func logMovimientos(tx *sql.Tx, productoID string, unidadIDs []string, tipo, nota string, user *User, now string) error {
	username := ""
	tenantID, err := tenantIDFromUserStrict(user)
	if err != nil {
		return err
	}
	username = user.Username
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

func hasCustomerInput(input customerInput) bool {
	return input.CustomerID > 0 ||
		strings.TrimSpace(input.Name) != "" ||
		strings.TrimSpace(input.Phone) != "" ||
		strings.TrimSpace(input.DocumentType) != "" ||
		strings.TrimSpace(input.DocumentNumber) != "" ||
		strings.TrimSpace(input.Address) != "" ||
		strings.TrimSpace(input.City) != "" ||
		strings.TrimSpace(input.Notes) != ""
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

func formatDateTimeForAPI(raw string) string {
	if raw == "" {
		return ""
	}
	if parsed, ok := parseFlexibleTime(raw); ok {
		return parsed.In(appTimeLocation).Format(time.RFC3339)
	}
	return raw
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

func normalizeCustomerDocumentType(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "_", " ")
	normalized = strings.ReplaceAll(normalized, "-", " ")
	normalized = strings.Join(strings.Fields(normalized), " ")
	switch normalized {
	case "cc":
		return "CC"
	case "ce", "c extranjeria", "c extranjería":
		return "C Extranjeria"
	case "pasaporte":
		return "Pasaporte"
	default:
		return strings.TrimSpace(value)
	}
}

func customerDocumentKey(documentType, documentNumber string) string {
	documentType = normalizeCustomerDocumentType(documentType)
	documentNumber = strings.TrimSpace(documentNumber)
	if documentType == "" || documentNumber == "" {
		return ""
	}
	return strings.ToLower(documentType) + "::" + strings.ToLower(documentNumber)
}

func validateCustomerInput(input customerInput) map[string]string {
	fields := map[string]string{}
	input.Name = strings.TrimSpace(input.Name)
	input.Phone = strings.TrimSpace(input.Phone)
	input.DocumentType = normalizeCustomerDocumentType(input.DocumentType)
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
	input.DocumentType = normalizeCustomerDocumentType(input.DocumentType)
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

func validateCustomerCSVRow(input customerInput) map[string]string {
	fields := validateCustomerInput(input)
	if strings.TrimSpace(input.City) == "" {
		fields["customer_city"] = "La ciudad del cliente es obligatoria."
	}
	if len(input.Name) > 160 {
		fields["customer_name"] = "El nombre del cliente supera el máximo permitido."
	}
	if len(input.Phone) > 40 {
		fields["customer_phone"] = "El teléfono del cliente supera el máximo permitido."
	}
	if len(input.DocumentNumber) > 60 {
		fields["customer_document_number"] = "El documento del cliente supera el máximo permitido."
	}
	if len(input.Address) > 180 {
		fields["customer_address"] = "La dirección supera el máximo permitido."
	}
	if len(input.City) > 80 {
		fields["customer_city"] = "La ciudad supera el máximo permitido."
	}
	if len(input.Notes) > 500 {
		fields["customer_notes"] = "Las notas superan el máximo permitido."
	}
	return fields
}

func importCustomersFromCSV(db *sql.DB, currentUser *User, raw []byte, source string, decorateAudit func(map[string]any) map[string]any) (customerCSVImportResponse, error) {
	if currentUser == nil || !isStaffRole(currentUser.Role) {
		return customerCSVImportResponse{}, requestError{Status: http.StatusForbidden, Message: "Solo personal autorizado puede importar clientes."}
	}
	tenantID := normalizeTenantID(tenantIDFromUser(currentUser))
	content := strings.TrimPrefix(string(raw), "\ufeff")
	if strings.TrimSpace(content) == "" {
		return customerCSVImportResponse{}, requestError{Status: http.StatusBadRequest, Message: "El CSV no contiene datos para procesar."}
	}

	reader := csv.NewReader(strings.NewReader(content))
	reader.Comma = detectCSVDelimiter(content)
	reader.FieldsPerRecord = -1
	reader.TrimLeadingSpace = true
	records, err := reader.ReadAll()
	if err != nil {
		return customerCSVImportResponse{}, requestError{Status: http.StatusBadRequest, Message: "No se pudo leer el CSV de clientes."}
	}
	if len(records) < 2 {
		return customerCSVImportResponse{}, requestError{Status: http.StatusBadRequest, Message: "El CSV no contiene filas para procesar."}
	}

	index, acceptedColumns, delimiter, err := customerCSVColumnIndex(records[0])
	if err != nil {
		var reqErr requestError
		if errors.As(err, &reqErr) {
			return customerCSVImportResponse{}, reqErr
		}
		return customerCSVImportResponse{}, requestError{Status: http.StatusBadRequest, Message: "El encabezado del CSV es inválido."}
	}

	get := func(row []string, col string) string {
		pos, ok := index[col]
		if !ok || pos < 0 || pos >= len(row) {
			return ""
		}
		return sanitizeCustomerCSVText(row[pos])
	}

	resp := customerCSVImportResponse{
		Delimiter:       string(delimiter),
		AcceptedColumns: acceptedColumns,
		RejectedRows:    []customerCSVFailedRow{},
	}
	seenDocuments := map[string]int{}

	tx, err := db.Begin()
	if err != nil {
		return customerCSVImportResponse{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo iniciar la importación de clientes."}
	}
	defer tx.Rollback()

	for i, row := range records[1:] {
		if len(row) == 0 {
			continue
		}
		rowIndex := i + 2
		if strings.TrimSpace(strings.Join(row, "")) == "" {
			continue
		}
		resp.ProcessedRows++

		input := customerInput{
			Name:           get(row, "name"),
			Phone:          get(row, "phone"),
			DocumentType:   normalizeCustomerDocumentType(get(row, "document_type")),
			DocumentNumber: get(row, "document_number"),
			Address:        get(row, "address"),
			City:           get(row, "city"),
			Notes:          get(row, "notes"),
		}
		documentKey := input.DocumentType + "|" + input.DocumentNumber

		if previousRow, duplicated := seenDocuments[documentKey]; duplicated {
			resp.RejectedRows = append(resp.RejectedRows, customerCSVFailedRow{
				Row:            rowIndex,
				DocumentType:   input.DocumentType,
				DocumentNumber: input.DocumentNumber,
				Error:          fmt.Sprintf("Documento duplicado dentro del archivo; ya apareció en la fila %d.", previousRow),
			})
			continue
		}

		if fields := validateCustomerCSVRow(input); len(fields) > 0 {
			reasons := make([]string, 0, len(fields))
			for _, value := range fields {
				reasons = append(reasons, value)
			}
			sort.Strings(reasons)
			resp.RejectedRows = append(resp.RejectedRows, customerCSVFailedRow{
				Row:            rowIndex,
				DocumentType:   input.DocumentType,
				DocumentNumber: input.DocumentNumber,
				Error:          strings.Join(reasons, " "),
			})
			continue
		}
		seenDocuments[documentKey] = rowIndex

		if _, err := tx.Exec("SAVEPOINT customer_csv_row"); err != nil {
			resp.RejectedRows = append(resp.RejectedRows, customerCSVFailedRow{
				Row:            rowIndex,
				DocumentType:   input.DocumentType,
				DocumentNumber: input.DocumentNumber,
				Error:          "No se pudo preparar la fila para importación.",
			})
			continue
		}

		existingCustomerID := 0
		if err := tx.QueryRow(`
			SELECT id
			FROM customers
			WHERE tenant_id = ? AND document_type = ? AND document_number = ?
		`, tenantID, input.DocumentType, input.DocumentNumber).Scan(&existingCustomerID); err != nil && err != sql.ErrNoRows {
			_, _ = tx.Exec("ROLLBACK TO customer_csv_row")
			_, _ = tx.Exec("RELEASE customer_csv_row")
			resp.RejectedRows = append(resp.RejectedRows, customerCSVFailedRow{
				Row:            rowIndex,
				DocumentType:   input.DocumentType,
				DocumentNumber: input.DocumentNumber,
				Error:          "No se pudo validar si el cliente ya existe en el tenant.",
			})
			continue
		}

		customer, err := resolveCustomerForCredit(tx, tenantID, input)
		if err != nil {
			_, _ = tx.Exec("ROLLBACK TO customer_csv_row")
			_, _ = tx.Exec("RELEASE customer_csv_row")
			resp.RejectedRows = append(resp.RejectedRows, customerCSVFailedRow{
				Row:            rowIndex,
				DocumentType:   input.DocumentType,
				DocumentNumber: input.DocumentNumber,
				Error:          "No se pudo guardar el cliente.",
			})
			continue
		}

		created := existingCustomerID == 0
		eventType := "customer_updated"
		customerEventType := "profile_updated"
		if created {
			resp.CreatedCustomers++
			eventType = "customer_created"
			customerEventType = "profile_created"
		} else {
			resp.UpdatedCustomers++
		}

		customerPayload := map[string]any{
			"name":            customer.Name,
			"phone":           customer.Phone,
			"document_type":   customer.DocumentType,
			"document_number": customer.DocumentNumber,
			"address":         customer.Address,
			"city":            customer.City,
			"notes":           customer.Notes,
			"row":             rowIndex,
			"imported_via":    "csv",
		}
		if err := logCustomerEvent(tx, currentUser, customer.ID, customerEventType, "customer", strconv.Itoa(customer.ID), 0, customerPayload); err != nil {
			_, _ = tx.Exec("ROLLBACK TO customer_csv_row")
			_, _ = tx.Exec("RELEASE customer_csv_row")
			resp.RejectedRows = append(resp.RejectedRows, customerCSVFailedRow{
				Row:            rowIndex,
				DocumentType:   input.DocumentType,
				DocumentNumber: input.DocumentNumber,
				Error:          "No se pudo registrar la trazabilidad del cliente.",
			})
			continue
		}

		auditPayload := map[string]any{
			"customer_id":      customer.ID,
			"customer_name":    customer.Name,
			"customer_phone":   customer.Phone,
			"document_type":    customer.DocumentType,
			"document_number":  customer.DocumentNumber,
			"customer_address": customer.Address,
			"customer_city":    customer.City,
			"row":              rowIndex,
			"imported_via":     "csv",
			"created":          created,
		}
		if decorateAudit != nil {
			auditPayload = decorateAudit(auditPayload)
		}
		if err := logAuditEvent(tx, currentUser, eventType, "customer", strconv.Itoa(customer.ID), source, auditPayload); err != nil {
			_, _ = tx.Exec("ROLLBACK TO customer_csv_row")
			_, _ = tx.Exec("RELEASE customer_csv_row")
			resp.RejectedRows = append(resp.RejectedRows, customerCSVFailedRow{
				Row:            rowIndex,
				DocumentType:   input.DocumentType,
				DocumentNumber: input.DocumentNumber,
				Error:          "No se pudo registrar la auditoría del cliente importado.",
			})
			continue
		}

		_, _ = tx.Exec("RELEASE customer_csv_row")
	}

	importAuditPayload := map[string]any{
		"processed_rows":    resp.ProcessedRows,
		"created_customers": resp.CreatedCustomers,
		"updated_customers": resp.UpdatedCustomers,
		"rejected_rows":     len(resp.RejectedRows),
		"accepted_columns":  resp.AcceptedColumns,
		"delimiter":         resp.Delimiter,
	}
	if decorateAudit != nil {
		importAuditPayload = decorateAudit(importAuditPayload)
	}
	if err := logAuditEvent(tx, currentUser, "customers_csv_imported", "customer_import", strconv.Itoa(tenantID), source, importAuditPayload); err != nil {
		return customerCSVImportResponse{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la auditoría del import."}
	}

	if err := tx.Commit(); err != nil {
		return customerCSVImportResponse{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo confirmar la importación de clientes."}
	}

	return resp, nil
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
	productSKU, visibleID, err := resolveProductRefForTenant(db, tenantIDFromUser(currentUser), input.ProductID)
	if err != nil {
		if err == sql.ErrNoRows {
			return productLoanOperationResult{}, requestError{Status: http.StatusNotFound, Message: "Producto no encontrado."}
		}
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo cargar el producto."}
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
	unitIDs, err := selectAndMarkUnitsByStatus(tx, tenantIDFromUser(currentUser), visibleID, input.Quantity, "Prestada")
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
	`, tenantIDFromUser(currentUser), productSKU, nullableIntValue(customer.ID), input.Quantity, customer.Name, customer.Phone, customer.DocumentType, customer.DocumentNumber, customer.Address, customer.City, input.Notes, string(productLoanStatusActive), now, input.DueAt, nullableUserID(currentUser))
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
	if err := logMovimientos(tx, productSKU, unitIDs, "prestamo", movementNote, currentUser, now); err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar el movimiento del préstamo."}
	}
	auditPayload := map[string]any{
		"product_loan_id":          productLoanID,
		"product_id":               visibleID,
		"product_sku":              productSKU,
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
		"product_id":  visibleID,
		"product_sku": productSKU,
		"quantity":    input.Quantity,
		"due_at":      input.DueAt,
		"unit_ids":    unitIDs,
	}); err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la trazabilidad del cliente."}
	}
	if err := tx.Commit(); err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo confirmar el préstamo."}
	}
	return productLoanOperationResult{
		ProductLoanID: int(productLoanID),
		ProductID:     visibleID,
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
	allowed, err := productAccessibleBySKU(db, currentUser, productID)
	if err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo validar acceso al producto."}
	}
	if !allowed {
		return productLoanOperationResult{}, requestError{Status: http.StatusForbidden, Message: "No tienes acceso a este préstamo."}
	}
	visibleProductID, err := resolveVisibleProductIDBySKUForTenant(db, tenantIDFromUser(currentUser), productID)
	if err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo resolver el ID visible del producto."}
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
		"product_id":      visibleProductID,
		"product_sku":     productID,
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
			"product_id":  visibleProductID,
			"product_sku": productID,
			"status":      string(status),
			"unit_ids":    unitIDs,
		}); err != nil {
			return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la trazabilidad del cliente."}
		}
	}
	if err := tx.Commit(); err != nil {
		return productLoanOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo confirmar el cierre del préstamo."}
	}
	return productLoanOperationResult{
		ProductLoanID: input.ProductLoanID,
		ProductID:     visibleProductID,
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

func customerSummaryForTenant(db *sql.DB, tenantID, customerID int) (map[string]any, error) {
	customer, err := findCustomerByID(db, tenantID, customerID)
	if err != nil {
		return nil, err
	}

	row := db.QueryRow(`
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
		WHERE c.tenant_id = ? AND c.id = ?
		GROUP BY c.id, c.name, c.phone, c.document_type, c.document_number, c.address, c.city, c.notes, c.created_at, c.updated_at
		LIMIT 1
	`, normalizeTenantID(tenantID), customerID)

	var (
		item          Customer
		creditsCount  int
		unitsOnCredit int
		debtTotal     float64
		totalPaid     float64
		activeCredits int
		lastCreditAt  string
	)
	if err := row.Scan(&item.ID, &item.Name, &item.Phone, &item.DocumentType, &item.DocumentNumber, &item.Address, &item.City, &item.Notes, &item.CreatedAt, &item.UpdatedAt, &creditsCount, &unitsOnCredit, &debtTotal, &totalPaid, &activeCredits, &lastCreditAt); err != nil {
		if err == sql.ErrNoRows {
			item = *customer
		} else {
			return nil, err
		}
	}

	currentDebt := creditCurrentDebt(debtTotal, totalPaid)
	return map[string]any{
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
	}, nil
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
	selected, err := customerSummaryForTenant(db, tenantID, customerID)
	if err != nil {
		return nil, err
	}

	recentCredits := make([]map[string]any, 0, 10)
	rows, err := db.Query(`
		SELECT
			cs.id,
			cs.created_at,
			COALESCE(cs.kind, ?),
			COALESCE(NULLIF(p.id, ''), cs.product_id, ''),
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
			COALESCE(NULLIF(p.id, ''), pl.product_id, ''),
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
				COALESCE(NULLIF(p.id, ''), cs.product_id, '') AS product_id,
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

func requestErrorDetails(err error) (requestError, bool) {
	var reqErr requestError
	if errors.As(err, &reqErr) {
		return reqErr, true
	}
	return requestError{}, false
}

type retomaOperationInput struct {
	ProductID      string
	Quantity       int
	ValueReceived  float64
	ReceivedState  string
	PublishToStock bool
	FinalSalePrice *float64
	Notes          string
	Customer       customerInput
}

type retomaOperationResult struct {
	RetomaID         int64
	ProductID        string
	ProductName      string
	CustomerID       int
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
	input.Customer.Name = strings.TrimSpace(input.Customer.Name)
	input.Customer.Phone = strings.TrimSpace(input.Customer.Phone)
	input.Customer.DocumentType = strings.TrimSpace(input.Customer.DocumentType)
	input.Customer.DocumentNumber = strings.TrimSpace(input.Customer.DocumentNumber)
	input.Customer.Address = strings.TrimSpace(input.Customer.Address)
	input.Customer.City = strings.TrimSpace(input.Customer.City)
	input.Customer.Notes = strings.TrimSpace(input.Customer.Notes)
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
	if hasCustomerInput(input.Customer) {
		for key, value := range validateCustomerInput(input.Customer) {
			fields[key] = value
		}
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
	productSKU, visibleID, err := resolveProductRefForTenant(db, tenantID, input.ProductID)
	if err != nil {
		if err == sql.ErrNoRows {
			return retomaOperationResult{}, requestError{Status: http.StatusNotFound, Message: "Producto no encontrado."}
		}
		return retomaOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo cargar el producto."}
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
	`, tenantID, productSKU).Scan(&productName, &retomaEnabled, &defaultRetomaRaw); err != nil {
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

	var customer *Customer
	if hasCustomerInput(input.Customer) {
		customer, err = resolveCustomerForCredit(tx, tenantID, input.Customer)
		if err != nil {
			if reqErr, ok := requestErrorDetails(err); ok {
				return retomaOperationResult{}, reqErr
			}
			if err == sql.ErrNoRows {
				return retomaOperationResult{}, requestError{Status: http.StatusBadRequest, Message: "Cliente inválido.", Fields: map[string]string{
					"customer_id": "Selecciona un cliente válido.",
				}}
			}
			return retomaOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo resolver el cliente de la retoma."}
		}
	}

	now := time.Now().Format(time.RFC3339)
	precioPublicado := sql.NullFloat64{}
	if input.PublishToStock && input.FinalSalePrice != nil {
		precioPublicado = sql.NullFloat64{Float64: *input.FinalSalePrice, Valid: true}
	}
	retomaID, err := insertAndReturnID(tx, `
		INSERT INTO retomas (tenant_id, producto_id, customer_id, cantidad, valor_recibido, estado_recibido, publicado_stock, precio_publicado, notas, fecha)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, tenantID, productSKU, nullableIntValue(func() int {
		if customer == nil {
			return 0
		}
		return customer.ID
	}()), input.Quantity, input.ValueReceived, input.ReceivedState, boolToInt(input.PublishToStock), precioPublicado, input.Notes, now)
	if err != nil {
		return retomaOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la retoma."}
	}

	unitIDs := make([]string, 0, input.Quantity)
	baseID := time.Now().UnixNano()
	for i := 0; i < input.Quantity; i++ {
		unitIDs = append(unitIDs, fmt.Sprintf("RETOMA-%s-%d-%d", productSKU, baseID, i+1))
	}
	movementNote := fmt.Sprintf("Estado recibido: %s | Valor recibido: %s", input.ReceivedState, formatCurrency(input.ValueReceived))
	if customer != nil {
		movementNote += " | Cliente: " + customer.Name
	}
	if input.Notes != "" {
		movementNote += " | " + input.Notes
	}
	if err := logMovimientos(tx, productSKU, unitIDs, "retoma", movementNote, currentUser, now); err != nil {
		return retomaOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar el movimiento de retoma."}
	}

	stockCreatedIDs := make([]string, 0, input.Quantity)
	if input.PublishToStock {
		if precioPublicado.Valid {
			if _, err := tx.Exec(`UPDATE productos SET precio_venta = ? WHERE tenant_id = ? AND sku = ?`, precioPublicado.Float64, tenantID, productSKU); err != nil {
				return retomaOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo actualizar el precio final del producto."}
			}
		}
		baseID = time.Now().UnixNano()
		for i := 0; i < input.Quantity; i++ {
			unitID := fmt.Sprintf("U-%s-RET-%d-%d", productSKU, baseID, i+1)
			if _, err := tx.Exec(
				`INSERT INTO unidades (id, tenant_id, producto_id, estado, creado_en, caducidad) VALUES (?, ?, ?, ?, ?, ?)`,
				unitID, tenantID, productSKU, "Disponible", now, nil,
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
		if err := logMovimientos(tx, productSKU, stockCreatedIDs, "retoma_stock", stockNote, currentUser, now); err != nil {
			return retomaOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar el ingreso a stock de la retoma."}
		}
	}

	auditPayload := map[string]any{
		"retoma_id":            retomaID,
		"product_id":           visibleID,
		"product_sku":          productSKU,
		"product_name":         productName,
		"quantity":             input.Quantity,
		"value_received":       input.ValueReceived,
		"estado_recibido":      input.ReceivedState,
		"published_to_stock":   input.PublishToStock,
		"units_created":        len(stockCreatedIDs),
		"notas":                input.Notes,
		"default_retoma_price": defaultRetomaRaw,
	}
	if customer != nil {
		auditPayload["customer_id"] = customer.ID
		auditPayload["customer_name"] = customer.Name
		auditPayload["customer_phone"] = customer.Phone
		auditPayload["customer_document_type"] = customer.DocumentType
		auditPayload["customer_document_number"] = customer.DocumentNumber
		auditPayload["customer_address"] = customer.Address
		auditPayload["customer_city"] = customer.City
		auditPayload["customer_notes"] = customer.Notes
	}
	if precioPublicado.Valid {
		auditPayload["final_sale_price"] = precioPublicado.Float64
	} else {
		auditPayload["final_sale_price"] = nil
	}
	if decoratePayload != nil {
		auditPayload = decoratePayload(auditPayload)
	}
	if err := logAuditEvent(tx, currentUser, "retoma_registered", "retoma", strconv.FormatInt(retomaID, 10), source, auditPayload); err != nil {
		return retomaOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la auditoría de la retoma."}
	}
	if customer != nil {
		var customerFinalSalePrice any
		if precioPublicado.Valid {
			customerFinalSalePrice = precioPublicado.Float64
		}
		if err := logCustomerEvent(tx, currentUser, customer.ID, "retoma_registered", "retoma", strconv.FormatInt(retomaID, 10), input.ValueReceived, map[string]any{
			"product_id":             visibleID,
			"product_sku":            productSKU,
			"product_name":           productName,
			"quantity":               input.Quantity,
			"value_received":         input.ValueReceived,
			"received_state":         input.ReceivedState,
			"published_to_stock":     input.PublishToStock,
			"final_sale_price":       customerFinalSalePrice,
			"customer_document_type": customer.DocumentType,
			"customer_document":      customer.DocumentNumber,
		}); err != nil {
			return retomaOperationResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la trazabilidad del cliente."}
		}
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
	customerID := 0
	if customer != nil {
		customerID = customer.ID
	}
	return retomaOperationResult{
		RetomaID:         retomaID,
		ProductID:        visibleID,
		ProductName:      productName,
		CustomerID:       customerID,
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
	productSKU, visibleID, err := resolveProductRefForTenant(db, tenantID, input.ProductID)
	if err != nil {
		if err == sql.ErrNoRows {
			return inventoryAdjustResult{}, requestError{Status: http.StatusNotFound, Message: "Producto no encontrado."}
		}
		return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo cargar el producto."}
	}

	var currentSalePrice float64
	if err := db.QueryRow(`SELECT COALESCE(precio_venta, 0) FROM productos WHERE tenant_id = ? AND sku = ?`, tenantID, productSKU).Scan(&currentSalePrice); err != nil {
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
	`, tenantID, productSKU)
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
			unitID := fmt.Sprintf("U-%s-AJ-%d-%d", productSKU, baseID, i)
			if _, err := tx.Exec(
				`INSERT INTO unidades (id, tenant_id, producto_id, estado, creado_en, caducidad) VALUES (?, ?, ?, ?, ?, ?)`,
				unitID, tenantID, productSKU, "Disponible", now, nil,
			); err != nil {
				return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo incrementar el stock."}
			}
			createdIDs = append(createdIDs, unitID)
		}
		logNote := input.Notes
		if logNote == "" {
			logNote = fmt.Sprintf("Ajuste manual de stock: %d -> %d", current, target)
		}
		if err := logMovimientos(tx, productSKU, createdIDs, "ajuste_stock_entrada", logNote, currentUser, now); err != nil {
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
		args = append(args, productSKU)
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
		if err := logMovimientos(tx, productSKU, removeIDs, "ajuste_stock_salida", logNote, currentUser, now); err != nil {
			return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar el ajuste."}
		}
	}

	updatedFields := map[string]any{}
	if input.SalePrice != nil {
		res, err := tx.Exec(`UPDATE productos SET precio_venta = ? WHERE tenant_id = ? AND sku = ?`, *input.SalePrice, tenantID, productSKU)
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
		res, err := tx.Exec(`UPDATE productos SET nombre = ? WHERE tenant_id = ? AND sku = ?`, *input.Name, tenantID, productSKU)
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
		res, err := tx.Exec(`UPDATE productos SET retoma_enabled = ?, retoma_price = ? WHERE tenant_id = ? AND sku = ?`, boolToInt(*input.RetomaEnabled), newRetomaPrice, tenantID, productSKU)
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
			"product_id":        visibleID,
			"product_sku":       productSKU,
			"previous_quantity": current,
			"target_quantity":   target,
			"current_quantity":  target,
			"delta":             delta,
			"notes":             input.Notes,
		}
		if decoratePayload != nil {
			auditPayload = decoratePayload(auditPayload)
		}
		if err := logAuditEvent(tx, currentUser, "inventory_adjusted", "product", productSKU, source, auditPayload); err != nil {
			return inventoryAdjustResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo registrar la auditoría del ajuste de inventario."}
		}
	}
	if len(updatedFields) > 0 {
		updatedFields["product_id"] = visibleID
		updatedFields["product_sku"] = productSKU
		if decoratePayload != nil {
			updatedFields = decoratePayload(updatedFields)
		}
		if err := logAuditEvent(tx, currentUser, "product_updated", "product", productSKU, source, updatedFields); err != nil {
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
		ProductID:        visibleID,
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
	productSKU := ""
	if input.Kind == creditSaleKindCash {
		input.ProductID = ""
		if input.ProductName == "" {
			input.ProductName = "Préstamo de dinero"
		}
	} else {
		if input.ProductID == "" {
			return creditSaleCreateResult{}, requestError{Status: http.StatusBadRequest, Message: "Producto inválido para el crédito."}
		}
		var resolvedName string
		if err := tx.QueryRow(`
			SELECT sku, id, COALESCE(nombre, '')
			FROM productos
			WHERE tenant_id = ? AND id = ?
			LIMIT 1
		`, tenantID, input.ProductID).Scan(&productSKU, &input.ProductID, &resolvedName); err != nil {
			if err == sql.ErrNoRows {
				return creditSaleCreateResult{}, requestError{Status: http.StatusBadRequest, Message: "Producto inválido para el crédito."}
			}
			return creditSaleCreateResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo cargar el producto del crédito."}
		}
		if input.ProductName == "" {
			input.ProductName = resolvedName
		}
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
	if input.Kind == creditSaleKindCash || productSKU == "" {
		productIDValue = nil
	} else {
		productIDValue = productSKU
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
		"product_sku":        productSKU,
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
		"product_sku":              productSKU,
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
	if currentUser == nil || (!isAdminRole(currentUser.Role) && !isAPIKeyRole(currentUser.Role)) {
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
		internalProductSKU := strings.TrimSpace(result.ProductID)
		allowed, err := productAccessibleBySKU(db, currentUser, internalProductSKU)
		if err != nil {
			return creditSaleUpdateResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo validar acceso al producto."}
		}
		if !allowed {
			return creditSaleUpdateResult{}, requestError{Status: http.StatusForbidden, Message: "No tienes acceso a este crédito."}
		}
		visibleID, err := resolveVisibleProductIDBySKUForTenant(db, tenantID, internalProductSKU)
		if err != nil {
			return creditSaleUpdateResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo resolver el ID visible del producto."}
		}
		result.ProductID = visibleID
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
		allowed, err := productAccessibleBySKU(db, currentUser, accessProductID)
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
	internalProductSKU := strings.TrimSpace(result.ProductID)
	if result.Kind == creditSaleKindProduct && internalProductSKU != "" {
		visibleID, err := resolveVisibleProductIDBySKUForTenant(db, tenantID, internalProductSKU)
		if err != nil {
			return creditInstallmentResult{}, requestError{Status: http.StatusInternalServerError, Message: "No se pudo resolver el ID visible del producto."}
		}
		result.ProductID = visibleID
	}
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
	var installmentProductID any = internalProductSKU
	if internalProductSKU == "" {
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
		if err := logMovimientos(tx, selectedProduct.refID(), soldUnitIDs, "venta_credito", creditSummary, currentUser, now); err != nil {
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
		SELECT COALESCE(NULLIF(p.id, ''), p.sku), u.estado, COUNT(*)
		FROM unidades u
		JOIN productos p ON p.sku = u.producto_id AND p.tenant_id = u.tenant_id
		WHERE u.tenant_id = ? AND COALESCE(NULLIF(p.id, ''), p.sku) IN (`+strings.Join(placeholders, ",")+`)
		GROUP BY COALESCE(NULLIF(p.id, ''), p.sku), u.estado
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
		"id":              product.ID,
		"name":            product.Name,
		"line":            product.Line,
		"location":        product.Location,
		"talla_requerida": product.TallaRequerida,
		"talla":           product.Talla,
		"sale_price":      product.SalePrice,
		"retoma_enabled":  product.RetomaEnabled,
		"retoma_price":    retomaPrice,
		"available":       counts.Available,
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
	productSKU := ""
	err := tx.QueryRow(`
		SELECT sku
		FROM productos
		WHERE tenant_id = ? AND id = ?
		LIMIT 1
	`, normalizeTenantID(tenantID), strings.TrimSpace(productID)).Scan(&productSKU)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("producto no encontrado")
		}
		return nil, fmt.Errorf("resolver producto: %w", err)
	}

	rows, err := tx.Query(`
		SELECT id
		FROM unidades
		WHERE tenant_id = ? AND producto_id = ? AND estado IN ('Disponible', 'available')
		ORDER BY creado_en, id
		LIMIT ?`, normalizeTenantID(tenantID), productSKU, qty)
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
	productSKU, _, err := resolveProductRefForTenant(db, tenantID, productID)
	if err != nil {
		if err == sql.ErrNoRows {
			return []unitOption{}, nil
		}
		return nil, err
	}
	rows, err := db.Query(`
		SELECT id
		FROM unidades
		WHERE tenant_id = ? AND producto_id = ? AND estado IN ('Disponible', 'available')
		ORDER BY creado_en, id`, normalizeTenantID(tenantID), productSKU)
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
		SELECT COALESCE(NULLIF(p.id, ''), p.sku), COUNT(*)
		FROM unidades u
		JOIN productos p ON p.sku = u.producto_id AND p.tenant_id = u.tenant_id
		WHERE u.tenant_id = ? AND u.estado IN ('Disponible', 'available')
		GROUP BY COALESCE(NULLIF(p.id, ''), p.sku)`, normalizeTenantID(tenantID))
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
	settings.ContactPhone = strings.TrimSpace(settings.ContactPhone)
	settings.ContactEmail = strings.TrimSpace(settings.ContactEmail)
	settings.SocialMedia = strings.TrimSpace(settings.SocialMedia)
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
	isUploadedSVG := func(value string) bool {
		value = strings.TrimSpace(strings.ToLower(value))
		return strings.HasPrefix(value, "/static/uploads/branding/") && strings.HasSuffix(value, ".svg")
	}

	if logoPath == "" {
		if globalLogoPath != "" && !isUploadedSVG(globalLogoPath) {
			return globalLogoPath
		}
		return defaultLogoPath
	}

	// For non-default tenants without custom branding (legacy/default logo),
	// prefer current global branding configured by platform admin.
	if logoPath == defaultLogoPath {
		if user, ok := currentUserFromTemplateData(data); ok && tenantIDFromUser(user) != defaultTenantID {
			if globalLogoPath != "" && !isUploadedSVG(globalLogoPath) {
				return globalLogoPath
			}
		}
	}

	if isUploadedSVG(logoPath) {
		if globalLogoPath != "" && !isUploadedSVG(globalLogoPath) {
			return globalLogoPath
		}
		return defaultLogoPath
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
	defaultLabelProfileExpr := "0"
	invoiceExpr := "'58mm'"
	ticketExpr := "'58mm'"
	contactPhoneExpr := "''"
	contactEmailExpr := "''"
	socialMediaExpr := "''"
	if cols["label_paper_width"] {
		labelExpr = "label_paper_width"
	}
	if cols["default_label_profile_id"] {
		defaultLabelProfileExpr = "default_label_profile_id"
	}
	if cols["invoice_paper_width"] {
		invoiceExpr = "invoice_paper_width"
	}
	if cols["ticket_paper_width"] {
		ticketExpr = "ticket_paper_width"
	}
	if cols["contact_phone"] {
		contactPhoneExpr = "contact_phone"
	}
	if cols["contact_email"] {
		contactEmailExpr = "contact_email"
	}
	if cols["social_media"] {
		socialMediaExpr = "social_media"
	}
	query := fmt.Sprintf(`
		SELECT id, business_name, logo_path, %s AS contact_phone, %s AS contact_email, %s AS social_media, primary_color, currency, date_format, %s AS label_paper_width, %s AS default_label_profile_id, %s AS invoice_paper_width, %s AS ticket_paper_width, updated_at
		FROM business_settings
		WHERE tenant_id = ?
		ORDER BY id ASC
		LIMIT 1
	`, contactPhoneExpr, contactEmailExpr, socialMediaExpr, labelExpr, defaultLabelProfileExpr, invoiceExpr, ticketExpr)
	row := db.QueryRow(query, normalizeTenantID(tenantID))
	var updatedAt sql.NullString
	err = row.Scan(&settings.ID, &settings.BusinessName, &settings.LogoPath, &settings.ContactPhone, &settings.ContactEmail, &settings.SocialMedia, &settings.PrimaryColor, &settings.Currency, &settings.DateFormat, &settings.LabelPaperWidth, &settings.DefaultLabelProfileID, &settings.InvoicePaperWidth, &settings.TicketPaperWidth, &updatedAt)
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
	if cols["default_label_profile_id"] {
		insertCols = append(insertCols, "default_label_profile_id")
		args = append(args, settings.DefaultLabelProfileID)
		updateCols = append(updateCols, "default_label_profile_id = excluded.default_label_profile_id")
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
	if cols["contact_phone"] {
		insertCols = append(insertCols, "contact_phone")
		args = append(args, settings.ContactPhone)
		updateCols = append(updateCols, "contact_phone = excluded.contact_phone")
	}
	if cols["contact_email"] {
		insertCols = append(insertCols, "contact_email")
		args = append(args, settings.ContactEmail)
		updateCols = append(updateCols, "contact_email = excluded.contact_email")
	}
	if cols["social_media"] {
		insertCols = append(insertCols, "social_media")
		args = append(args, settings.SocialMedia)
		updateCols = append(updateCols, "social_media = excluded.social_media")
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

func ensureBusinessLineExists(exec sqlQueryExecer, tenantID int, currentUser *User, name, now, source string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	var existingID int
	err := exec.QueryRow(`
		SELECT id
		FROM business_lines
		WHERE tenant_id = ? AND name = ?
		LIMIT 1
	`, normalizeTenantID(tenantID), name).Scan(&existingID)
	if err == nil {
		return nil
	}
	if err != sql.ErrNoRows {
		return err
	}
	if _, err := exec.Exec(`
		INSERT INTO business_lines (tenant_id, name, active, created_at, updated_at)
		VALUES (?, ?, 1, ?, ?)
	`, normalizeTenantID(tenantID), name, now, now); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			return nil
		}
		return err
	}
	if currentUser != nil {
		if err := logAuditEvent(exec, currentUser, "business_line_created", "business_line", name, "manual", map[string]any{
			"name":        name,
			"active":      true,
			"created_via": strings.TrimSpace(source),
		}); err != nil {
			log.Printf("audit business line create (%s): %v", source, err)
		}
	}
	return nil
}

func ensureBusinessLinesForTenant(exec sqlQueryExecer, tenantID int, currentUser *User, names []string, now, source string) error {
	seen := make(map[string]struct{}, len(names))
	for _, rawName := range names {
		name := strings.TrimSpace(rawName)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		if err := ensureBusinessLineExists(exec, tenantID, currentUser, name, now, source); err != nil {
			return err
		}
	}
	return nil
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
		SELECT id, name, tenant_id, active, created_at, updated_at
		FROM api_keys
		WHERE tenant_id = ?
		ORDER BY active DESC, updated_at DESC, id DESC
	`, normalizeTenantID(tenantID))
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
	header := make([]byte, 512)
	readBytes, err := io.ReadFull(file, header)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return "", err
	}
	header = header[:readBytes]
	contentType := strings.ToLower(http.DetectContentType(header))
	allowedByType := map[string]string{
		"image/png":  ".png",
		"image/jpeg": ".jpg",
		"image/webp": ".webp",
	}
	ext, ok := allowedByType[contentType]
	if !ok {
		return "", fmt.Errorf("formato de logo no soportado")
	}
	originalExt := strings.ToLower(filepath.Ext(originalName))
	if contentType == "image/jpeg" && originalExt == ".jpeg" {
		originalExt = ".jpg"
	}
	if originalExt != "" && originalExt != ext {
		return "", fmt.Errorf("el archivo no coincide con un formato de imagen soportado")
	}

	fileName := fmt.Sprintf("logo-%d%s", time.Now().UnixNano(), ext)
	relPath := filepath.Join("uploads", "branding", fileName)
	fullPath := filepath.Join("static", relPath)
	dst, err := os.Create(fullPath)
	if err != nil {
		return "", err
	}
	defer dst.Close()
	if len(header) > 0 {
		if _, err := dst.Write(header); err != nil {
			return "", err
		}
	}
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

func saleThermalTicketViewURLWithPaper(saleID int, paper string) string {
	values := url.Values{}
	values.Set("sale_id", strconv.Itoa(saleID))
	if normalized := strings.TrimSpace(paper); normalized != "" {
		values.Set("paper", normalized)
	}
	return "/venta/ticket?" + values.Encode()
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

func invoiceViewURLWithPaper(invoiceID int, paper string) string {
	paper = strings.TrimSpace(paper)
	if paper == "" {
		return invoiceViewURL(invoiceID)
	}
	values := url.Values{}
	values.Set("paper", paper)
	return invoiceViewURL(invoiceID) + "?" + values.Encode()
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

const (
	maxLabelBatchLabels = 500
	maxLabelBatchCopies = maxLabelBatchLabels
	defaultLabelGapMM   = 2
)

func labelPaperOptions() []labelPaperOption {
	return []labelPaperOption{
		{Value: "57mm", Label: "57 × 30 mm"},
		{Value: "58mm", Label: "58 × 40 mm"},
		{Value: "80mm", Label: "80 × 50 mm"},
	}
}

func labelPrintProfileFor(size string, columns, gapMM int) labelPrintProfile {
	normalized, labelWidthMM, labelHeightMM := labelSizeDimensions(size)
	_, _, dpi, paperClass := thermalPaperDimensions(normalized)
	if columns != 2 {
		columns = 1
	}
	if gapMM < 0 || gapMM > 10 {
		gapMM = defaultLabelGapMM
	}
	paperWidthMM := labelWidthMM
	if columns == 2 {
		paperWidthMM = labelWidthMM*2 + gapMM
	}
	return labelPrintProfile{
		Size:          normalized,
		LabelWidthMM:  labelWidthMM,
		LabelHeightMM: labelHeightMM,
		PaperWidthMM:  paperWidthMM,
		PaperHeightMM: labelHeightMM,
		Columns:       columns,
		GapMM:         gapMM,
		RowGapMM:      0,
		DPI:           dpi,
		PaperClass:    paperClass,
	}
}

func labelPrintProfileFromProfile(profile LabelProfile) labelPrintProfile {
	profile = normalizeLabelProfile(profile)
	paperClass := "standard"
	if profile.LabelHeightMM <= 30 {
		paperClass = "compact"
	} else if profile.LabelWidthMM >= 80 {
		paperClass = "wide"
	}
	return labelPrintProfile{
		Size:          fmt.Sprintf("%dx%d", profile.LabelWidthMM, profile.LabelHeightMM),
		LabelWidthMM:  profile.LabelWidthMM,
		LabelHeightMM: profile.LabelHeightMM,
		PaperWidthMM:  profile.paperWidthMM(),
		PaperHeightMM: profile.paperHeightMM(),
		Columns:       profile.Columns,
		GapMM:         profile.ColumnGapMM,
		RowGapMM:      profile.RowGapMM,
		DPI:           203,
		PaperClass:    paperClass,
	}
}

func defaultLabelProfileSpecs() []LabelProfile {
	return []LabelProfile{
		{Name: "Compacta 57 × 30", LabelWidthMM: 57, LabelHeightMM: 30, Columns: 1, ShowBusiness: true, ShowPrice: true, ShowBarcode: true, ShowID: true},
		{Name: "Estándar 58 × 40", LabelWidthMM: 58, LabelHeightMM: 40, Columns: 1, ShowBusiness: true, ShowContact: true, ShowPrice: true, ShowBarcode: true, ShowID: true},
		{Name: "Amplia 80 × 50", LabelWidthMM: 80, LabelHeightMM: 50, Columns: 1, ShowBusiness: true, ShowContact: true, ShowSize: true, ShowPrice: true, ShowBarcode: true, ShowID: true},
	}
}

func normalizeLabelProfile(profile LabelProfile) LabelProfile {
	profile.Name = strings.TrimSpace(profile.Name)
	if profile.Name == "" {
		profile.Name = "Etiqueta"
	}
	if profile.LabelWidthMM < 20 || profile.LabelWidthMM > 120 {
		profile.LabelWidthMM = 58
	}
	if profile.LabelHeightMM < 15 || profile.LabelHeightMM > 120 {
		profile.LabelHeightMM = 40
	}
	if profile.Columns != 2 {
		profile.Columns = 1
	}
	if profile.ColumnGapMM < 0 || profile.ColumnGapMM > 10 {
		profile.ColumnGapMM = 0
	}
	if profile.RowGapMM < 0 || profile.RowGapMM > 10 {
		profile.RowGapMM = 0
	}
	// "Línea" remains inventory metadata, but it is no longer part of the
	// label editor or printable layout. Keep the persisted column untouched so
	// existing profiles and API contracts remain compatible.
	profile.ShowLine = false
	// The product name is deliberately fixed: a label without it is not useful
	// in day-to-day inventory operation.
	return profile
}

func loadLabelProfilesForTenant(db *sql.DB, tenantID int) ([]LabelProfile, error) {
	rows, err := db.Query(`
		SELECT id, tenant_id, name, label_width_mm, label_height_mm, columns, column_gap_mm, row_gap_mm,
			show_business, show_contact, show_line, show_size, show_price, show_barcode, show_id,
			created_at, updated_at
		FROM label_profiles
		WHERE tenant_id = ?
		ORDER BY id ASC
	`, normalizeTenantID(tenantID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	profiles := make([]LabelProfile, 0)
	for rows.Next() {
		var (
			profile                                                                       LabelProfile
			showBusiness, showContact, showLine, showSize, showPrice, showBarcode, showID int
		)
		if err := rows.Scan(&profile.ID, &profile.TenantID, &profile.Name, &profile.LabelWidthMM, &profile.LabelHeightMM, &profile.Columns, &profile.ColumnGapMM, &profile.RowGapMM,
			&showBusiness, &showContact, &showLine, &showSize, &showPrice, &showBarcode, &showID,
			&profile.CreatedAt, &profile.UpdatedAt); err != nil {
			return nil, err
		}
		profile.ShowBusiness = showBusiness != 0
		profile.ShowContact = showContact != 0
		profile.ShowLine = showLine != 0
		profile.ShowSize = showSize != 0
		profile.ShowPrice = showPrice != 0
		profile.ShowBarcode = showBarcode != 0
		profile.ShowID = showID != 0
		profiles = append(profiles, normalizeLabelProfile(profile))
	}
	return profiles, rows.Err()
}

func ensureLabelProfilesForTenant(db *sql.DB, tenantID int, legacySize string) ([]LabelProfile, int, error) {
	tenantID = normalizeTenantID(tenantID)
	profiles, err := loadLabelProfilesForTenant(db, tenantID)
	if err != nil {
		return nil, 0, err
	}
	if len(profiles) == 0 {
		now := time.Now().Format(time.RFC3339)
		for _, spec := range defaultLabelProfileSpecs() {
			spec = normalizeLabelProfile(spec)
			if _, err := db.Exec(`
				INSERT INTO label_profiles (
					tenant_id, name, label_width_mm, label_height_mm, columns, column_gap_mm, row_gap_mm,
					show_business, show_contact, show_line, show_size, show_price, show_barcode, show_id,
					created_at, updated_at
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				ON CONFLICT (tenant_id, name) DO NOTHING
			`, tenantID, spec.Name, spec.LabelWidthMM, spec.LabelHeightMM, spec.Columns, spec.ColumnGapMM, spec.RowGapMM,
				boolToInt(spec.ShowBusiness), boolToInt(spec.ShowContact), boolToInt(spec.ShowLine), boolToInt(spec.ShowSize), boolToInt(spec.ShowPrice), boolToInt(spec.ShowBarcode), boolToInt(spec.ShowID), now, now); err != nil {
				return nil, 0, err
			}
		}
		profiles, err = loadLabelProfilesForTenant(db, tenantID)
		if err != nil {
			return nil, 0, err
		}
	}
	if len(profiles) == 0 {
		return nil, 0, fmt.Errorf("no se pudieron preparar perfiles de etiqueta")
	}
	defaultID := 0
	if err := db.QueryRow(`SELECT COALESCE(default_label_profile_id, 0) FROM business_settings WHERE tenant_id = ?`, tenantID).Scan(&defaultID); err != nil && err != sql.ErrNoRows {
		return nil, 0, err
	}
	validDefault := false
	for _, profile := range profiles {
		if profile.ID == defaultID {
			validDefault = true
			break
		}
	}
	if !validDefault {
		_, widthMM, heightMM := labelSizeDimensions(legacySize)
		defaultID = profiles[0].ID
		for _, profile := range profiles {
			if profile.Columns == 1 && profile.LabelWidthMM == widthMM && profile.LabelHeightMM == heightMM {
				defaultID = profile.ID
				break
			}
		}
		if _, err := db.Exec(`UPDATE business_settings SET default_label_profile_id = ?, updated_at = ? WHERE tenant_id = ?`, defaultID, time.Now().Format(time.RFC3339), tenantID); err != nil {
			return nil, 0, err
		}
	}
	return profiles, defaultID, nil
}

func labelProfileForTenant(db *sql.DB, tenantID int, defaultLegacySize, requestedLegacySize string, requestedID int) (LabelProfile, error) {
	profiles, defaultID, err := ensureLabelProfilesForTenant(db, tenantID, defaultLegacySize)
	if err != nil {
		return LabelProfile{}, err
	}
	if requestedID > 0 {
		for _, profile := range profiles {
			if profile.ID == requestedID {
				return profile, nil
			}
		}
		return LabelProfile{}, requestError{Status: http.StatusNotFound, Message: "El perfil de etiqueta no existe."}
	}
	if strings.TrimSpace(requestedLegacySize) != "" {
		_, widthMM, heightMM := labelSizeDimensions(requestedLegacySize)
		for _, profile := range profiles {
			if profile.Columns == 1 && profile.LabelWidthMM == widthMM && profile.LabelHeightMM == heightMM {
				return profile, nil
			}
		}
	}
	for _, profile := range profiles {
		if profile.ID == defaultID {
			return profile, nil
		}
	}
	return profiles[0], nil
}

func labelProfileFromForm(r *http.Request) (LabelProfile, error) {
	parseBounded := func(field string, minValue, maxValue int) (int, error) {
		value, err := strconv.Atoi(strings.TrimSpace(r.FormValue(field)))
		if err != nil || value < minValue || value > maxValue {
			return 0, requestError{Status: http.StatusBadRequest, Message: fmt.Sprintf("%s debe estar entre %d y %d.", field, minValue, maxValue)}
		}
		return value, nil
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" || len([]rune(name)) > 80 {
		return LabelProfile{}, requestError{Status: http.StatusBadRequest, Message: "El nombre del perfil debe tener entre 1 y 80 caracteres."}
	}
	widthMM, err := parseBounded("label_width_mm", 20, 120)
	if err != nil {
		return LabelProfile{}, err
	}
	heightMM, err := parseBounded("label_height_mm", 15, 120)
	if err != nil {
		return LabelProfile{}, err
	}
	columns, err := parseBounded("columns", 1, 2)
	if err != nil {
		return LabelProfile{}, err
	}
	gapMM, err := parseBounded("column_gap_mm", 0, 10)
	if err != nil {
		return LabelProfile{}, err
	}
	rowGapMM := 0
	if strings.TrimSpace(r.FormValue("row_gap_mm")) != "" {
		rowGapMM, err = parseBounded("row_gap_mm", 0, 10)
		if err != nil {
			return LabelProfile{}, err
		}
	}
	return LabelProfile{
		Name:          name,
		LabelWidthMM:  widthMM,
		LabelHeightMM: heightMM,
		Columns:       columns,
		ColumnGapMM:   gapMM,
		RowGapMM:      rowGapMM,
		ShowBusiness:  r.FormValue("show_business") == "on",
		ShowContact:   r.FormValue("show_contact") == "on",
		ShowLine:      false,
		ShowSize:      r.FormValue("show_size") == "on",
		ShowPrice:     r.FormValue("show_price") == "on",
		ShowBarcode:   r.FormValue("show_barcode") == "on",
		ShowID:        r.FormValue("show_id") == "on",
	}, nil
}

func labelProfileByIDForTenant(db *sql.DB, tenantID, profileID int) (LabelProfile, error) {
	profiles, err := loadLabelProfilesForTenant(db, tenantID)
	if err != nil {
		return LabelProfile{}, err
	}
	for _, profile := range profiles {
		if profile.ID == profileID {
			return profile, nil
		}
	}
	return LabelProfile{}, requestError{Status: http.StatusNotFound, Message: "El perfil de etiqueta no existe."}
}

func createLabelProfileForTenant(db *sql.DB, tenantID int, profile LabelProfile) (LabelProfile, error) {
	profile = normalizeLabelProfile(profile)
	tenantID = normalizeTenantID(tenantID)
	now := time.Now().Format(time.RFC3339)
	err := db.QueryRow(`
		INSERT INTO label_profiles (
			tenant_id, name, label_width_mm, label_height_mm, columns, column_gap_mm, row_gap_mm,
			show_business, show_contact, show_line, show_size, show_price, show_barcode, show_id,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id
	`, tenantID, profile.Name, profile.LabelWidthMM, profile.LabelHeightMM, profile.Columns, profile.ColumnGapMM, profile.RowGapMM,
		boolToInt(profile.ShowBusiness), boolToInt(profile.ShowContact), boolToInt(profile.ShowLine), boolToInt(profile.ShowSize), boolToInt(profile.ShowPrice), boolToInt(profile.ShowBarcode), boolToInt(profile.ShowID), now, now).Scan(&profile.ID)
	if err != nil {
		return LabelProfile{}, err
	}
	profile.TenantID = tenantID
	profile.CreatedAt = now
	profile.UpdatedAt = now
	return profile, nil
}

func updateLabelProfileForTenant(db *sql.DB, tenantID, profileID int, profile LabelProfile) error {
	profile = normalizeLabelProfile(profile)
	result, err := db.Exec(`
		UPDATE label_profiles
		SET name = ?, label_width_mm = ?, label_height_mm = ?, columns = ?, column_gap_mm = ?, row_gap_mm = ?,
			show_business = ?, show_contact = ?, show_line = ?, show_size = ?, show_price = ?, show_barcode = ?, show_id = ?, updated_at = ?
		WHERE tenant_id = ? AND id = ?
	`, profile.Name, profile.LabelWidthMM, profile.LabelHeightMM, profile.Columns, profile.ColumnGapMM, profile.RowGapMM,
		boolToInt(profile.ShowBusiness), boolToInt(profile.ShowContact), boolToInt(profile.ShowLine), boolToInt(profile.ShowSize), boolToInt(profile.ShowPrice), boolToInt(profile.ShowBarcode), boolToInt(profile.ShowID), time.Now().Format(time.RFC3339), normalizeTenantID(tenantID), profileID)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return requestError{Status: http.StatusNotFound, Message: "El perfil de etiqueta no existe."}
	}
	return nil
}

func labelRows(items []productLabelItem, columns int) [][]productLabelItem {
	if columns != 2 {
		columns = 1
	}
	rows := make([][]productLabelItem, 0, (len(items)+columns-1)/columns)
	for start := 0; start < len(items); start += columns {
		end := start + columns
		if end > len(items) {
			end = len(items)
		}
		rows = append(rows, items[start:end])
	}
	return rows
}

func businessContactLine(settings BusinessSettings) string {
	parts := make([]string, 0, 3)
	if phone := strings.TrimSpace(settings.ContactPhone); phone != "" {
		parts = append(parts, "Tel. "+phone)
	}
	if email := strings.TrimSpace(settings.ContactEmail); email != "" {
		parts = append(parts, email)
	}
	if social := strings.TrimSpace(settings.SocialMedia); social != "" {
		parts = append(parts, social)
	}
	return strings.Join(parts, " · ")
}

// compactLabelContactLine reserves the narrow left column of a small thermal
// label for the most actionable contact method. The full contact line remains
// available on larger profiles.
func compactLabelContactLine(settings BusinessSettings) string {
	if phone := strings.TrimSpace(settings.ContactPhone); phone != "" {
		return "Tel. " + phone
	}
	return businessContactLine(settings)
}

func legacyLabelProfile(profile labelPrintProfile) LabelProfile {
	return normalizeLabelProfile(LabelProfile{
		Name:          "Etiqueta",
		LabelWidthMM:  profile.LabelWidthMM,
		LabelHeightMM: profile.LabelHeightMM,
		Columns:       profile.Columns,
		ColumnGapMM:   profile.GapMM,
		RowGapMM:      profile.RowGapMM,
		ShowBusiness:  true,
		ShowContact:   true,
		ShowPrice:     true,
		ShowBarcode:   true,
		ShowID:        true,
	})
}

func productLabelsPageDataFor(items []productLabelItem, profile labelPrintProfile, currentUser *User, settings BusinessSettings) productLabelsPageData {
	return productLabelsPageDataForProfile(items, profile, legacyLabelProfile(profile), currentUser, settings)
}

func productLabelsPageDataForProfile(items []productLabelItem, profile labelPrintProfile, labelProfile LabelProfile, currentUser *User, settings BusinessSettings) productLabelsPageData {
	return productLabelsPageData{
		Title:              "Etiquetas de producto",
		Subtitle:           "Documento preparado para impresión térmica y PDF.",
		Size:               profile.Size,
		WidthMM:            profile.LabelWidthMM,
		HeightMM:           profile.LabelHeightMM,
		PaperWidthMM:       profile.PaperWidthMM,
		PaperHeightMM:      profile.PaperHeightMM,
		Columns:            profile.Columns,
		GapMM:              profile.GapMM,
		RowGapMM:           profile.RowGapMM,
		PaperDPI:           profile.DPI,
		PaperClass:         profile.PaperClass,
		Items:              items,
		Rows:               labelRows(items, profile.Columns),
		CurrentUser:        currentUser,
		Settings:           settings,
		ContactLine:        businessContactLine(settings),
		CompactContactLine: compactLabelContactLine(settings),
		Profile:            normalizeLabelProfile(labelProfile),
	}
}

func parseLabelBatchCopies(r *http.Request, productIDs []string) (map[string]int, int, error) {
	copiesByID := make(map[string]int, len(productIDs))
	total := 0
	seen := make(map[string]struct{}, len(productIDs))
	for _, rawID := range productIDs {
		productID := strings.TrimSpace(rawID)
		if productID == "" {
			continue
		}
		if _, exists := seen[productID]; exists {
			continue
		}
		seen[productID] = struct{}{}
		copies := 1
		if raw := strings.TrimSpace(r.FormValue("copies_" + productID)); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed < 1 || parsed > maxLabelBatchCopies {
				return nil, 0, requestError{Status: http.StatusBadRequest, Message: fmt.Sprintf("Las copias para %s deben estar entre 1 y %d.", productID, maxLabelBatchCopies)}
			}
			copies = parsed
		}
		if total+copies > maxLabelBatchLabels {
			return nil, 0, requestError{Status: http.StatusBadRequest, Message: fmt.Sprintf("El lote supera el máximo de %d etiquetas.", maxLabelBatchLabels)}
		}
		copiesByID[productID] = copies
		total += copies
	}
	if len(copiesByID) == 0 {
		return nil, 0, requestError{Status: http.StatusBadRequest, Message: "Selecciona al menos un producto."}
	}
	return copiesByID, total, nil
}

func expandProductLabelItems(items []productLabelItem, productIDs []string, copiesByID map[string]int) []productLabelItem {
	byID := make(map[string]productLabelItem, len(items))
	for _, item := range items {
		byID[item.ID] = item
	}
	expanded := make([]productLabelItem, 0)
	seen := make(map[string]struct{}, len(productIDs))
	for _, rawID := range productIDs {
		productID := strings.TrimSpace(rawID)
		if _, exists := seen[productID]; exists {
			continue
		}
		seen[productID] = struct{}{}
		item, allowed := byID[productID]
		if !allowed {
			continue
		}
		for copyIndex := 0; copyIndex < copiesByID[productID]; copyIndex++ {
			expanded = append(expanded, item)
		}
	}
	return expanded
}

type labelPDFImage struct {
	Width  int
	Height int
	Data   []byte
}

type labelPDFWriter struct {
	objects [][]byte
}

func (w *labelPDFWriter) addObject(body []byte) int {
	w.objects = append(w.objects, body)
	return len(w.objects)
}

func (w *labelPDFWriter) setObject(id int, body []byte) {
	if id <= 0 || id > len(w.objects) {
		return
	}
	w.objects[id-1] = body
}

func millimetersToPoints(mm int) float64 {
	return float64(mm) * 72 / 25.4
}

func pdfEscapeText(raw string) string {
	var escaped strings.Builder
	for _, r := range strings.TrimSpace(raw) {
		if r == '(' || r == ')' || r == '\\' {
			escaped.WriteByte('\\')
		}
		if r >= 32 && r <= 255 {
			escaped.WriteByte(byte(r))
		} else if r == '\n' || r == '\r' || r == '\t' {
			escaped.WriteByte(' ')
		} else {
			escaped.WriteByte('?')
		}
	}
	return escaped.String()
}

func pdfTextLine(buffer *bytes.Buffer, x, y, size float64, value string) {
	pdfTextLineWithFont(buffer, "F1", x, y, size, value)
}

func pdfTextLineWithFont(buffer *bytes.Buffer, fontName string, x, y, size float64, value string) {
	value = pdfEscapeText(value)
	if value == "" {
		return
	}
	fmt.Fprintf(buffer, "BT /%s %.2f Tf 0 g 1 0 0 1 %.2f %.2f Tm (%s) Tj ET\n", fontName, size, x, y, value)
}

func pdfTruncateLabelText(value string, maxRunes int) string {
	value = strings.TrimSpace(value)
	if maxRunes < 1 || utf8.RuneCountInString(value) <= maxRunes {
		return value
	}
	runes := []rune(value)
	if maxRunes <= 3 {
		return string(runes[:maxRunes])
	}
	return string(runes[:maxRunes-3]) + "..."
}

func pdfApproxTextWidth(value string, size float64) float64 {
	return float64(utf8.RuneCountInString(pdfEscapeText(value))) * size * 0.56
}

func pdfLabelNameLines(name string, maxRunes int) []string {
	name = strings.TrimSpace(name)
	if name == "" {
		return []string{"Producto"}
	}
	words := strings.Fields(name)
	lines := make([]string, 0, 2)
	current := ""
	for _, word := range words {
		candidate := strings.TrimSpace(current + " " + word)
		if utf8.RuneCountInString(candidate) > maxRunes && current != "" {
			lines = append(lines, current)
			current = word
			if len(lines) == 2 {
				break
			}
			continue
		}
		current = candidate
	}
	if current != "" && len(lines) < 2 {
		lines = append(lines, current)
	}
	if len(lines) == 0 {
		return []string{"Producto"}
	}
	return lines
}

func decodeLabelPDFImage(uri template.URL) (labelPDFImage, error) {
	raw := string(uri)
	comma := strings.IndexByte(raw, ',')
	if comma < 0 {
		return labelPDFImage{}, fmt.Errorf("código de barras inválido")
	}
	pngBytes, err := base64.StdEncoding.DecodeString(raw[comma+1:])
	if err != nil {
		return labelPDFImage{}, err
	}
	decoded, err := png.Decode(bytes.NewReader(pngBytes))
	if err != nil {
		return labelPDFImage{}, err
	}
	return labelPDFImageFromDecoded(decoded)
}

func labelPDFImageFromStaticPath(rawPath string) (labelPDFImage, error) {
	rawPath = strings.TrimSpace(rawPath)
	if !strings.HasPrefix(rawPath, "/static/") {
		return labelPDFImage{}, fmt.Errorf("logo de etiqueta fuera de archivos estáticos")
	}
	relativePath := filepath.Clean(strings.TrimPrefix(rawPath, "/static/"))
	if relativePath == "." || relativePath == ".." || strings.HasPrefix(relativePath, ".."+string(filepath.Separator)) {
		return labelPDFImage{}, fmt.Errorf("ruta de logo de etiqueta inválida")
	}
	file, err := os.Open(filepath.Join("static", relativePath))
	if err != nil {
		return labelPDFImage{}, err
	}
	defer file.Close()
	decoded, _, err := image.Decode(file)
	if err != nil {
		return labelPDFImage{}, err
	}
	return labelPDFImageFromDecoded(decoded)
}

func labelPDFImageFromDecoded(decoded image.Image) (labelPDFImage, error) {
	bounds := decoded.Bounds()
	width, height := bounds.Dx(), bounds.Dy()
	if width <= 0 || height <= 0 {
		return labelPDFImage{}, fmt.Errorf("código de barras sin dimensiones")
	}
	rgb := make([]byte, 0, width*height*3)
	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			r, g, b, _ := decoded.At(x, y).RGBA()
			rgb = append(rgb, byte(r>>8), byte(g>>8), byte(b>>8))
		}
	}
	var compressed bytes.Buffer
	writer, err := flate.NewWriter(&compressed, flate.BestSpeed)
	if err != nil {
		return labelPDFImage{}, err
	}
	if _, err := writer.Write(rgb); err != nil {
		return labelPDFImage{}, err
	}
	if err := writer.Close(); err != nil {
		return labelPDFImage{}, err
	}
	return labelPDFImage{Width: width, Height: height, Data: compressed.Bytes()}, nil
}

func labelPDFImageObject(image labelPDFImage) []byte {
	var body bytes.Buffer
	fmt.Fprintf(&body, "<< /Type /XObject /Subtype /Image /Width %d /Height %d /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /FlateDecode /Length %d >>\nstream\n", image.Width, image.Height, len(image.Data))
	body.Write(image.Data)
	body.WriteString("\nendstream")
	return body.Bytes()
}

func labelPDFStreamObject(content []byte) []byte {
	return []byte(fmt.Sprintf("<< /Length %d >>\nstream\n%s\nendstream", len(content), content))
}

func productLabelsPDF(items []productLabelItem, profile labelPrintProfile, businessName string) ([]byte, error) {
	return productLabelsPDFWithSettings(items, profile, BusinessSettings{BusinessName: businessName})
}

func productLabelsPDFWithSettings(items []productLabelItem, profile labelPrintProfile, settings BusinessSettings) ([]byte, error) {
	return productLabelsPDFWithSettingsAndProfile(items, profile, legacyLabelProfile(profile), settings)
}

func productLabelsPDFWithSettingsAndProfile(items []productLabelItem, profile labelPrintProfile, labelProfile LabelProfile, settings BusinessSettings) ([]byte, error) {
	if len(items) == 0 {
		return nil, fmt.Errorf("no hay etiquetas para exportar")
	}
	settings = normalizeBusinessSettings(settings)
	labelProfile = normalizeLabelProfile(labelProfile)
	businessName := settings.BusinessName
	contactLine := businessContactLine(settings)
	compactContactLine := compactLabelContactLine(settings)
	writer := &labelPDFWriter{}
	catalogID := writer.addObject(nil)
	pagesID := writer.addObject(nil)
	fontID := writer.addObject([]byte("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>"))
	fontBoldID := writer.addObject([]byte("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold /Encoding /WinAnsiEncoding >>"))
	imageIDs := make(map[string]int, len(items))
	logoImageID := 0
	var logoImage labelPDFImage
	if labelProfile.ShowBusiness && profile.LabelHeightMM <= 30 {
		if image, err := labelPDFImageFromStaticPath(settings.LogoPath); err == nil {
			logoImage = image
			logoImageID = writer.addObject(labelPDFImageObject(image))
		}
	}
	if labelProfile.ShowBarcode {
		for _, item := range items {
			if _, exists := imageIDs[item.ID]; exists {
				continue
			}
			image, err := decodeLabelPDFImage(item.BarcodeDataURI)
			if err != nil {
				return nil, err
			}
			imageIDs[item.ID] = writer.addObject(labelPDFImageObject(image))
		}
	}

	paperWidth := millimetersToPoints(profile.PaperWidthMM)
	paperHeight := millimetersToPoints(profile.PaperHeightMM)
	labelWidth := millimetersToPoints(profile.LabelWidthMM)
	labelHeight := millimetersToPoints(profile.LabelHeightMM)
	contentOffsetY := millimetersToPoints(profile.RowGapMM)
	contentTopY := contentOffsetY + labelHeight
	pad := millimetersToPoints(3)
	gap := millimetersToPoints(profile.GapMM)
	compact := profile.LabelHeightMM <= 30
	if compact {
		pad = 1.5 * 72 / 25.4
	}
	pageIDs := make([]int, 0, len(labelRows(items, profile.Columns)))
	for _, row := range labelRows(items, profile.Columns) {
		var content bytes.Buffer
		for column, item := range row {
			x := float64(column) * (labelWidth + gap)
			businessSize, nameSize, priceLabelSize, priceValueSize, priceLabelY, barcodeHeight, barcodeY, codeY := 6.5, 8.3, 6.2, 11.5, contentTopY-pad-36, millimetersToPoints(12), contentOffsetY+millimetersToPoints(7), contentOffsetY+millimetersToPoints(3)
			nameLines := pdfLabelNameLines(item.Name, 26)
			printContact := labelProfile.ShowContact && contactLine != ""
			sizeLabel := ""
			if labelProfile.ShowSize && strings.TrimSpace(item.Size) != "" {
				sizeLabel = "Talla " + item.Size
			}
			if compact {
				// The 50 x 25 mm format follows a classic retail-label hierarchy:
				// identity and phone on the left; product, reference, barcode and
				// price on the right. This reserves dots for the details that must
				// be legible at 203 DPI.
				leftColumnWidth := 15.5 * 72 / 25.4
				columnGap := .8 * 72 / 25.4
				rightX := x + pad + leftColumnWidth + columnGap
				brandHeight := 8.5 * 72 / 25.4
				if logoImageID > 0 {
					logoWidth := brandHeight * float64(logoImage.Width) / float64(logoImage.Height)
					maxLogoWidth := brandHeight
					if logoWidth > maxLogoWidth {
						logoWidth = maxLogoWidth
					}
					logoX := x + pad + (leftColumnWidth-logoWidth)/2
					logoY := contentTopY - pad - brandHeight
					fmt.Fprintf(&content, "q %.2f 0 0 %.2f %.2f %.2f cm /L%d Do Q\n", logoWidth, brandHeight, logoX, logoY, logoImageID)
				} else if labelProfile.ShowBusiness {
					business := pdfTruncateLabelText(strings.ToUpper(strings.TrimSpace(businessName)), 10)
					businessX := x + pad + (leftColumnWidth-pdfApproxTextWidth(business, 5.6))/2
					pdfTextLineWithFont(&content, "F2", businessX, contentTopY-pad-7, 5.6, business)
				}
				compactNameLines := pdfLabelNameLines(item.Name, 15)
				for lineIndex, line := range compactNameLines {
					pdfTextLineWithFont(&content, "F2", rightX, contentTopY-pad-9-float64(lineIndex)*11, 10.4, pdfTruncateLabelText(line, 15))
				}
				referenceY := contentTopY - pad - 30.5
				referenceX := rightX
				if labelProfile.ShowID {
					referenceText := item.ID
					if sizeLabel != "" {
						referenceText += " · "
					}
					pdfTextLineWithFont(&content, "F2", referenceX, referenceY, 8.4, referenceText)
					referenceX += pdfApproxTextWidth(referenceText, 8.4)
				}
				if sizeLabel != "" {
					pdfTextLineWithFont(&content, "F2", referenceX, referenceY, 9.4, pdfTruncateLabelText(sizeLabel, 10))
				}
				compactPriceX := x + labelWidth - pad
				if labelProfile.ShowPrice {
					// Thermal heads and drivers often lose the last dot columns near
					// the right edge. Keep the entire amount 2.5 mm inside the label.
					priceSafeInset := pad + 2.5*72/25.4
					compactPriceX = x + labelWidth - priceSafeInset - pdfApproxTextWidth(item.Price, 12.2)
					pdfTextLineWithFont(&content, "F2", compactPriceX, contentOffsetY+2*72/25.4, 12.2, item.Price)
				}
				if printContact {
					contactX := x + pad
					contactWidth := compactPriceX - contactX - 2
					if contactWidth > 0 {
						contactY := contentOffsetY + 2*72/25.4
						fmt.Fprintf(&content, "q %.2f %.2f %.2f %.2f re W n\n", contactX, contactY-3, contactWidth, 16.0)
						pdfTextLineWithFont(&content, "F2", contactX, contactY, 9.4, compactContactLine)
						content.WriteString("Q\n")
					}
				}
				if labelProfile.ShowBarcode {
					barcodeX := x + pad
					barcodeWidth := x + labelWidth - pad - barcodeX
					barcodeHeight := 5.2 * 72 / 25.4
					barcodeY := contentOffsetY + 6.8*72/25.4
					fmt.Fprintf(&content, "q %.2f 0 0 %.2f %.2f %.2f cm /I%d Do Q\n", barcodeWidth, barcodeHeight, barcodeX, barcodeY, imageIDs[item.ID])
				}
				continue
			}
			if printContact && len(nameLines) > 1 {
				nameLines = nameLines[:1]
			}
			businessY := contentTopY - pad - 6
			nameStartY := contentTopY - pad - 17
			contactY := contentTopY - pad - 13
			if labelProfile.ShowBusiness {
				pdfTextLineWithFont(&content, "F2", x+pad, businessY, businessSize, pdfTruncateLabelText(strings.ToUpper(strings.TrimSpace(businessName)), 32))
			}
			if !labelProfile.ShowBusiness {
				nameStartY = contentTopY - pad - 8
			}
			if printContact {
				printableContact := contactLine
				contactRunes := []rune(printableContact)
				contactLimit := 54
				if len(contactRunes) > contactLimit {
					printableContact = string(contactRunes[:contactLimit-3]) + "..."
				}
				pdfTextLine(&content, x+pad, contactY, 4.8, printableContact)
				nameStartY -= 7
			}
			for lineIndex, line := range nameLines {
				pdfTextLine(&content, x+pad, nameStartY-float64(lineIndex)*8, nameSize, line)
			}
			if sizeLabel != "" {
				pdfTextLineWithFont(&content, "F2", x+pad, nameStartY-float64(len(nameLines))*8-1, 6.2, sizeLabel)
			}
			if labelProfile.ShowPrice {
				pdfTextLineWithFont(&content, "F2", x+pad, priceLabelY, priceLabelSize, "PRECIO")
				pdfTextLineWithFont(&content, "F2", x+pad+27, priceLabelY, priceValueSize, item.Price)
			}
			if labelProfile.ShowBarcode {
				barcodeWidth := labelWidth - pad*2
				fmt.Fprintf(&content, "q %.2f 0 0 %.2f %.2f %.2f cm /I%d Do Q\n", barcodeWidth, barcodeHeight, x+pad, barcodeY, imageIDs[item.ID])
			}
			if labelProfile.ShowID {
				pdfTextLineWithFont(&content, "F2", x+pad, codeY, 7.2, item.ID)
			}
		}
		contentID := writer.addObject(labelPDFStreamObject(content.Bytes()))
		var xObjects strings.Builder
		seenImages := map[string]struct{}{}
		for _, item := range row {
			if !labelProfile.ShowBarcode {
				continue
			}
			if _, seen := seenImages[item.ID]; seen {
				continue
			}
			seenImages[item.ID] = struct{}{}
			fmt.Fprintf(&xObjects, "/I%d %d 0 R ", imageIDs[item.ID], imageIDs[item.ID])
		}
		if logoImageID > 0 {
			fmt.Fprintf(&xObjects, "/L%d %d 0 R ", logoImageID, logoImageID)
		}
		pageBody := fmt.Sprintf("<< /Type /Page /Parent %d 0 R /MediaBox [0 0 %.2f %.2f] /Resources << /Font << /F1 %d 0 R /F2 %d 0 R >> /XObject << %s>> >> /Contents %d 0 R >>", pagesID, paperWidth, paperHeight, fontID, fontBoldID, xObjects.String(), contentID)
		pageIDs = append(pageIDs, writer.addObject([]byte(pageBody)))
	}
	var kids strings.Builder
	for _, pageID := range pageIDs {
		fmt.Fprintf(&kids, "%d 0 R ", pageID)
	}
	writer.setObject(pagesID, []byte(fmt.Sprintf("<< /Type /Pages /Kids [ %s] /Count %d >>", kids.String(), len(pageIDs))))
	writer.setObject(catalogID, []byte(fmt.Sprintf("<< /Type /Catalog /Pages %d 0 R >>", pagesID)))

	var document bytes.Buffer
	document.WriteString("%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
	offsets := make([]int, len(writer.objects)+1)
	for index, object := range writer.objects {
		offsets[index+1] = document.Len()
		fmt.Fprintf(&document, "%d 0 obj\n", index+1)
		document.Write(object)
		document.WriteString("\nendobj\n")
	}
	xrefOffset := document.Len()
	fmt.Fprintf(&document, "xref\n0 %d\n0000000000 65535 f \n", len(writer.objects)+1)
	for index := 1; index < len(offsets); index++ {
		fmt.Fprintf(&document, "%010d 00000 n \n", offsets[index])
	}
	fmt.Fprintf(&document, "trailer\n<< /Size %d /Root %d 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(writer.objects)+1, catalogID, xrefOffset)
	return document.Bytes(), nil
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
	return productLabelItemsForUserWithDimensions(db, currentUser, productIDs, widthMM, heightMM)
}

// productLabelItemsForUserWithDimensions keeps barcode pixels aligned with the
// actual profile. Custom profiles such as 50 x 25 mm must not inherit the
// legacy 58 x 40 mm bitmap size and then be resampled by the browser or PDF.
func productLabelItemsForUserWithDimensions(db *sql.DB, currentUser *User, productIDs []string, widthMM, heightMM int) ([]productLabelItem, int, int, error) {
	if currentUser == nil || !isStaffRole(currentUser.Role) {
		return nil, 0, 0, requestError{Status: http.StatusForbidden, Message: "Solo personal autorizado puede imprimir etiquetas."}
	}
	if widthMM < 20 || widthMM > 120 {
		widthMM = 58
	}
	if heightMM < 15 || heightMM > 120 {
		heightMM = 40
	}
	seen := map[string]struct{}{}
	items := make([]productLabelItem, 0, len(productIDs))
	const labelDPI = 203
	// The compact layout uses 1.6 mm left/right padding, leaving about 47 mm
	// for a 50 mm label. Generate at that physical width to avoid resampling.
	printableWidthMM := max(20, widthMM-3)
	barcodeWidth := int(math.Ceil(float64(printableWidthMM*labelDPI) / 25.4))
	barcodeHeightMM := 12
	if heightMM <= 30 {
		barcodeHeightMM = 7
	}
	barcodeHeight := int(math.Ceil(float64(barcodeHeightMM*labelDPI) / 25.4))

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
			visibleID    string
			name         string
			line         string
			requiresSize int
			size         string
			salePrice    float64
		)
		err = db.QueryRow(`
			SELECT id, COALESCE(nombre, sku), COALESCE(linea, ''), COALESCE(talla_requerida, 0), COALESCE(talla, ''), COALESCE(precio_venta, 0)
			FROM productos
			WHERE tenant_id = ? AND id = ?
			LIMIT 1
		`, tenantIDFromUser(currentUser), productID).Scan(&visibleID, &name, &line, &requiresSize, &size, &salePrice)
		if err != nil {
			if err == sql.ErrNoRows {
				continue
			}
			return nil, 0, 0, err
		}

		barcodeURI, err := barcodeDataURI(visibleID, barcodeWidth, barcodeHeight)
		if err != nil {
			return nil, 0, 0, err
		}
		productSize := ""
		if requiresSize != 0 {
			productSize = strings.TrimSpace(size)
		}
		items = append(items, productLabelItem{
			ID:             visibleID,
			Name:           name,
			Line:           line,
			Size:           productSize,
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
		createdAtRaw         string
		productID            string
		productName          string
		quantity             int
		unitPrice            float64
		paymentMethod        string
		channel              string
		soldBy               string
		notes                string
		receiptBuyerName     string
		receiptBuyerDocument string
	)

	err := db.QueryRow(`
		SELECT
			v.fecha,
			COALESCE(NULLIF(p.id, ''), p.sku, v.producto_id),
			COALESCE(p.nombre, COALESCE(NULLIF(p.id, ''), p.sku, v.producto_id)),
			v.cantidad,
			v.precio_final,
			COALESCE(v.metodo_pago, ''),
			COALESCE(v.channel, ''),
			COALESCE(v.sold_by, ''),
			COALESCE(v.notas, ''),
			COALESCE(v.receipt_buyer_name, ''),
			COALESCE(v.receipt_buyer_document, '')
		FROM ventas v
		LEFT JOIN productos p ON p.sku = v.producto_id AND p.tenant_id = v.tenant_id
		WHERE v.tenant_id = ? AND v.id = ?
		LIMIT 1
	`, tenantID, saleID).Scan(&createdAtRaw, &productID, &productName, &quantity, &unitPrice, &paymentMethod, &channel, &soldBy, &notes, &receiptBuyerName, &receiptBuyerDocument)
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
		BuyerName:        strings.TrimSpace(receiptBuyerName),
		BuyerDocument:    strings.TrimSpace(receiptBuyerDocument),
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
		allowed, err := productAccessibleBySKU(db, currentUser, productID)
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
		"thermal_ticket_url":       invoiceViewURLWithPaper(data.InvoiceID, data.Settings.TicketPaperWidth),
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
	settings, err := loadBusinessSettingsForTenant(db, tenantID)
	if err != nil {
		settings = currentBusinessSettings()
	}
	creditAccessSQL, creditAccessArgs := creditVisibilityPredicate("cs", currentUser)
	saleAccessSQL, saleAccessArgs := tenantScopedProductAccessPredicate("v", "p", currentUser)
	q = strings.TrimSpace(strings.ToLower(q))
	args := []any{tenantID}
	query := `
		SELECT
			i.id,
			i.invoice_number,
			i.source_type,
			COALESCE(i.sale_id, 0),
			COALESCE(i.credit_sale_id, 0),
			COALESCE(i.customer_name, ''),
			COALESCE(i.customer_document_number, ''),
			COALESCE(i.total, 0),
			COALESCE(i.status, 'issued'),
			COALESCE(i.created_at, '')
		FROM invoices i
		LEFT JOIN ventas v ON v.tenant_id = i.tenant_id AND v.id = i.sale_id
		LEFT JOIN productos p ON p.sku = v.producto_id AND p.tenant_id = v.tenant_id
		LEFT JOIN credit_sales cs ON cs.tenant_id = i.tenant_id AND cs.id = i.credit_sale_id
		WHERE i.tenant_id = ? AND (
			(COALESCE(i.source_type, 'sale') = 'credit' AND ` + creditAccessSQL + `)
			OR
			(COALESCE(i.source_type, 'sale') <> 'credit' AND ` + saleAccessSQL + `)
		)
	`
	args = append(args, creditAccessArgs...)
	args = append(args, saleAccessArgs...)
	if q != "" {
		query += ` AND (LOWER(i.invoice_number) LIKE ? OR LOWER(i.customer_name) LIKE ? OR LOWER(i.customer_document_number) LIKE ?)`
		like := "%" + q + "%"
		args = append(args, like, like, like)
	}
	if fromStr != "" {
		query += ` AND ` + sqlDatePrefixExpr("i.created_at") + ` >= ?`
		args = append(args, fromStr)
	}
	if toStr != "" {
		query += ` AND ` + sqlDatePrefixExpr("i.created_at") + ` <= ?`
		args = append(args, toStr)
	}
	query += ` ORDER BY i.created_at DESC, i.id DESC LIMIT ?`
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
		if parsed, ok := parseFlexibleTime(createdAt); ok {
			createdAt = parsed.In(appTimeLocation).Format("2006-01-02 15:04")
		}
		items = append(items, map[string]any{
			"id":                 invoiceID,
			"invoice_number":     invoiceNumber,
			"source_type":        sourceType,
			"source_label":       invoiceSourceLabel(sourceType),
			"sale_id":            saleID,
			"credit_sale_id":     creditSaleID,
			"customer_name":      customerName,
			"customer_document":  customerDocument,
			"total":              total,
			"status":             status,
			"status_label":       invoiceStatusLabel(status),
			"created_at":         createdAt,
			"view_url":           invoiceViewURL(invoiceID),
			"thermal_ticket_url": invoiceViewURLWithPaper(invoiceID, settings.TicketPaperWidth),
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

func saveSaleReceiptSnapshot(exec sqlExecer, currentUser *User, saleID int, buyerName, buyerDocument, format string) error {
	if saleID <= 0 {
		return requestError{Status: http.StatusBadRequest, Message: "Venta inválida."}
	}
	if currentUser == nil {
		return requestError{Status: http.StatusForbidden, Message: "No autorizado."}
	}
	buyerName = strings.TrimSpace(buyerName)
	buyerDocument = strings.TrimSpace(buyerDocument)
	format = strings.TrimSpace(format)
	if buyerName == "" || buyerDocument == "" {
		return requestError{Status: http.StatusBadRequest, Message: "Nombre y documento son obligatorios para guardar el comprobante."}
	}
	result, err := exec.Exec(`
		UPDATE ventas
		SET receipt_buyer_name = ?, receipt_buyer_document = ?, receipt_generated_at = ?, receipt_generated_by = ?, receipt_last_format = ?
		WHERE tenant_id = ? AND id = ?
	`, buyerName, buyerDocument, time.Now().Format(time.RFC3339), nullableUserID(currentUser), format, tenantIDFromUser(currentUser), saleID)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 0 {
		return requestError{Status: http.StatusNotFound, Message: "Venta no encontrada."}
	}
	return nil
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
	// Common timestamp formats persisted by StockiAPP:
	// - RFC3339 for movimiento/unidad timestamps
	// - "YYYY-MM-DD HH:MM:SS" from legacy timestamps
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

func setSessionCookie(w http.ResponseWriter, r *http.Request, token string, expiresAt time.Time) {
	http.SetCookie(w, &http.Cookie{
		Name:     "session_token",
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   requestUsesSecureCookies(r),
		Expires:  expiresAt,
	})
}

func clearSessionCookie(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     "session_token",
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   requestUsesSecureCookies(r),
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

func tenantIDFromUserStrict(user *User) (int, error) {
	if user == nil || user.TenantID <= 0 {
		return 0, errMissingTenantContext
	}
	return normalizeTenantID(user.TenantID), nil
}

func tenantIDFromRequestStrict(r *http.Request) (int, error) {
	if tenant := tenantFromContext(r); tenant != nil && tenant.ID > 0 {
		return normalizeTenantID(tenant.ID), nil
	}
	return tenantIDFromUserStrict(userFromContext(r))
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

func isAPIKeyRole(role string) bool {
	return role == roleAPIKey
}

func isAdminRole(role string) bool {
	return role == roleAdmin || role == rolePlatformAdmin
}

func hasTenantWideVisibility(role string) bool {
	return isAdminRole(role) || isAPIKeyRole(role)
}

func isStaffRole(role string) bool {
	return isAdminRole(role) || role == roleEmployee || isAPIKeyRole(role)
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
					WHERE u.tenant_id = t.id
						AND u.role = 'admin'
					ORDER BY u.id ASC
					LIMIT 1
				), ''),
				COALESCE((
					SELECT k.name
					FROM api_keys k
					WHERE k.tenant_id = t.id
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
		`)
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

type tenantResetScope string

const (
	tenantResetScopeInventory tenantResetScope = "inventory"
	tenantResetScopeCredits   tenantResetScope = "credits"
	tenantResetScopeUsers     tenantResetScope = "users"
)

func parseTenantResetScope(raw string) (tenantResetScope, error) {
	switch tenantResetScope(strings.ToLower(strings.TrimSpace(raw))) {
	case tenantResetScopeInventory:
		return tenantResetScopeInventory, nil
	case tenantResetScopeCredits:
		return tenantResetScopeCredits, nil
	case tenantResetScopeUsers:
		return tenantResetScopeUsers, nil
	default:
		return "", requestError{Status: http.StatusBadRequest, Message: "Scope de hard reset inválido."}
	}
}

func tenantResetScopeLabel(scope tenantResetScope) string {
	switch scope {
	case tenantResetScopeInventory:
		return "inventario"
	case tenantResetScopeCredits:
		return "créditos"
	case tenantResetScopeUsers:
		return "usuarios"
	default:
		return string(scope)
	}
}

type tenantResetSummary struct {
	Scope      tenantResetScope
	TenantID   int
	TenantName string
	TenantSlug string
	Counts     map[string]int64
}

func (s tenantResetSummary) TotalDeleted() int64 {
	var total int64
	for _, count := range s.Counts {
		total += count
	}
	return total
}

func execDeleteCount(exec sqlExecer, query string, args ...any) (int64, error) {
	result, err := exec.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rowsAffected, nil
}

func hardResetTenantScope(db *sql.DB, currentUser *User, tenantID int, scope tenantResetScope) (tenantResetSummary, error) {
	if !canManageTenants(currentUser) {
		return tenantResetSummary{}, requestError{Status: http.StatusForbidden, Message: "No tienes permisos para hacer hard reset de empresas."}
	}

	tenant, err := loadTenantForManagement(db, tenantID)
	if err != nil {
		return tenantResetSummary{}, err
	}
	if tenant.IsDefault {
		return tenantResetSummary{}, requestError{Status: http.StatusBadRequest, Message: "El tenant base no se puede resetear."}
	}

	var normalizedScope tenantResetScope
	switch scope {
	case tenantResetScopeInventory, tenantResetScopeCredits, tenantResetScopeUsers:
		normalizedScope = scope
	default:
		return tenantResetSummary{}, requestError{Status: http.StatusBadRequest, Message: "Scope de hard reset inválido."}
	}

	tx, err := db.Begin()
	if err != nil {
		return tenantResetSummary{}, err
	}
	defer tx.Rollback()

	summary := tenantResetSummary{
		Scope:      normalizedScope,
		TenantID:   tenant.ID,
		TenantName: tenant.Name,
		TenantSlug: tenant.Slug,
		Counts:     map[string]int64{},
	}

	now := time.Now().Format(time.RFC3339)
	switch normalizedScope {
	case tenantResetScopeInventory:
		if rows, err := execDeleteCount(tx, `
			DELETE FROM customer_events
			WHERE tenant_id = ?
			  AND (
				ref_type = 'retoma'
				OR ref_type = 'product_loan'
				OR (
					ref_type = 'invoice'
					AND ref_id IN (
						SELECT CAST(id AS TEXT)
						FROM invoices
						WHERE tenant_id = ?
						  AND source_type = 'sale'
					)
				)
			  )
		`, tenant.ID, tenant.ID); err != nil {
			return tenantResetSummary{}, err
		} else {
			summary.Counts["customer_events"] = rows
		}
		if rows, err := execDeleteCount(tx, `
			DELETE FROM invoice_items
			WHERE tenant_id = ?
			  AND invoice_id IN (
				SELECT id
				FROM invoices
				WHERE tenant_id = ?
				  AND source_type = 'sale'
			  )
		`, tenant.ID, tenant.ID); err != nil {
			return tenantResetSummary{}, err
		} else {
			summary.Counts["invoice_items"] = rows
		}
		if rows, err := execDeleteCount(tx, `
			DELETE FROM invoices
			WHERE tenant_id = ?
			  AND source_type = 'sale'
		`, tenant.ID); err != nil {
			return tenantResetSummary{}, err
		} else {
			summary.Counts["invoices"] = rows
		}
		if rows, err := execDeleteCount(tx, `
			DELETE FROM product_loan_units
			WHERE tenant_id = ?
		`, tenant.ID); err != nil {
			return tenantResetSummary{}, err
		} else {
			summary.Counts["product_loan_units"] = rows
		}
		if rows, err := execDeleteCount(tx, `
			DELETE FROM product_loans
			WHERE tenant_id = ?
		`, tenant.ID); err != nil {
			return tenantResetSummary{}, err
		} else {
			summary.Counts["product_loans"] = rows
		}
		if rows, err := execDeleteCount(tx, `DELETE FROM movimientos WHERE tenant_id = ?`, tenant.ID); err != nil {
			return tenantResetSummary{}, err
		} else {
			summary.Counts["movimientos"] = rows
		}
		if rows, err := execDeleteCount(tx, `DELETE FROM ventas WHERE tenant_id = ?`, tenant.ID); err != nil {
			return tenantResetSummary{}, err
		} else {
			summary.Counts["ventas"] = rows
		}
		if rows, err := execDeleteCount(tx, `DELETE FROM retomas WHERE tenant_id = ?`, tenant.ID); err != nil {
			return tenantResetSummary{}, err
		} else {
			summary.Counts["retomas"] = rows
		}
		if rows, err := execDeleteCount(tx, `DELETE FROM unidades WHERE tenant_id = ?`, tenant.ID); err != nil {
			return tenantResetSummary{}, err
		} else {
			summary.Counts["unidades"] = rows
		}
	case tenantResetScopeCredits:
		if rows, err := execDeleteCount(tx, `
			DELETE FROM customer_events
			WHERE tenant_id = ?
			  AND (
				ref_type = 'credit_sale'
				OR (
					ref_type = 'invoice'
					AND ref_id IN (
						SELECT CAST(id AS TEXT)
						FROM invoices
						WHERE tenant_id = ?
						  AND source_type = 'credit'
					)
				)
				OR event_type IN ('credit_created', 'credit_updated', 'credit_payment_recorded')
			  )
		`, tenant.ID, tenant.ID); err != nil {
			return tenantResetSummary{}, err
		} else {
			summary.Counts["customer_events"] = rows
		}
		if rows, err := execDeleteCount(tx, `
			DELETE FROM invoice_items
			WHERE tenant_id = ?
			  AND invoice_id IN (
				SELECT id
				FROM invoices
				WHERE tenant_id = ?
				  AND source_type = 'credit'
			  )
		`, tenant.ID, tenant.ID); err != nil {
			return tenantResetSummary{}, err
		} else {
			summary.Counts["invoice_items"] = rows
		}
		if rows, err := execDeleteCount(tx, `
			DELETE FROM invoices
			WHERE tenant_id = ?
			  AND source_type = 'credit'
		`, tenant.ID); err != nil {
			return tenantResetSummary{}, err
		} else {
			summary.Counts["invoices"] = rows
		}
		if rows, err := execDeleteCount(tx, `DELETE FROM credit_installments WHERE tenant_id = ?`, tenant.ID); err != nil {
			return tenantResetSummary{}, err
		} else {
			summary.Counts["credit_installments"] = rows
		}
		if rows, err := execDeleteCount(tx, `DELETE FROM credit_sales WHERE tenant_id = ?`, tenant.ID); err != nil {
			return tenantResetSummary{}, err
		} else {
			summary.Counts["credit_sales"] = rows
		}
	case tenantResetScopeUsers:
		if rows, err := execDeleteCount(tx, `UPDATE productos SET owner_user_id = NULL WHERE tenant_id = ?`, tenant.ID); err != nil {
			return tenantResetSummary{}, err
		} else {
			summary.Counts["products_owner_cleared"] = rows
		}
		if rows, err := execDeleteCount(tx, `DELETE FROM sessions WHERE tenant_id = ?`, tenant.ID); err != nil {
			return tenantResetSummary{}, err
		} else {
			summary.Counts["sessions"] = rows
		}
		if rows, err := execDeleteCount(tx, `DELETE FROM api_keys WHERE tenant_id = ?`, tenant.ID); err != nil {
			return tenantResetSummary{}, err
		} else {
			summary.Counts["api_keys"] = rows
		}
		if rows, err := execDeleteCount(tx, `DELETE FROM users WHERE tenant_id = ? AND role <> 'platform_admin'`, tenant.ID); err != nil {
			return tenantResetSummary{}, err
		} else {
			summary.Counts["users"] = rows
		}
	}

	if err := logAuditEvent(tx, currentUser, "tenant_hard_reset", "tenant", strconv.Itoa(tenant.ID), "manual", map[string]any{
		"tenant_name": tenant.Name,
		"tenant_slug": tenant.Slug,
		"scope":       string(normalizedScope),
		"scope_label": tenantResetScopeLabel(normalizedScope),
		"counts":      summary.Counts,
		"total_rows":  summary.TotalDeleted(),
		"executed_at": now,
	}); err != nil {
		return tenantResetSummary{}, err
	}

	if err := tx.Commit(); err != nil {
		return tenantResetSummary{}, err
	}
	return summary, nil
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
		SELECT id, name, tenant_id, active, created_at, updated_at
		FROM api_keys
		WHERE id = ? AND tenant_id = ?
		LIMIT 1
	`, keyID, tenantID).Scan(&item.ID, &item.Name, &item.TenantID, &active, &item.CreatedAt, &item.UpdatedAt)
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
		WHERE id = ? AND tenant_id = ?
	`, strings.TrimSpace(name), boolToInt(active), time.Now().Format(time.RFC3339), keyID, tenantID)
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
		WHERE tenant_id = ? AND name = ?
		ORDER BY id ASC
		LIMIT 1
	`, tenant.ID, initialName).Scan(&existingID, &existingName)
	if err == sql.ErrNoRows {
		err = tx.QueryRow(`
			SELECT id, name
			FROM api_keys
			WHERE tenant_id = ? AND name LIKE ?
			ORDER BY id ASC
			LIMIT 1
		`, tenant.ID, "%-inicial").Scan(&existingID, &existingName)
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
	businessSettingsCols, err := tableColumns(db, "business_settings")
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

	settingsInsertCols := []string{"tenant_id", "business_name", "logo_path", "primary_color", "currency", "date_format", "updated_at"}
	settingsInsertArgs := []any{tenantID, name, sourceSettings.LogoPath, sourceSettings.PrimaryColor, sourceSettings.Currency, sourceSettings.DateFormat, now}
	if businessSettingsCols["label_paper_width"] {
		settingsInsertCols = append(settingsInsertCols, "label_paper_width")
		settingsInsertArgs = append(settingsInsertArgs, sourceSettings.LabelPaperWidth)
	}
	if businessSettingsCols["invoice_paper_width"] {
		settingsInsertCols = append(settingsInsertCols, "invoice_paper_width")
		settingsInsertArgs = append(settingsInsertArgs, sourceSettings.InvoicePaperWidth)
	}
	if businessSettingsCols["ticket_paper_width"] {
		settingsInsertCols = append(settingsInsertCols, "ticket_paper_width")
		settingsInsertArgs = append(settingsInsertArgs, sourceSettings.TicketPaperWidth)
	}
	if businessSettingsCols["contact_phone"] {
		settingsInsertCols = append(settingsInsertCols, "contact_phone")
		settingsInsertArgs = append(settingsInsertArgs, sourceSettings.ContactPhone)
	}
	if businessSettingsCols["contact_email"] {
		settingsInsertCols = append(settingsInsertCols, "contact_email")
		settingsInsertArgs = append(settingsInsertArgs, sourceSettings.ContactEmail)
	}
	if businessSettingsCols["social_media"] {
		settingsInsertCols = append(settingsInsertCols, "social_media")
		settingsInsertArgs = append(settingsInsertArgs, sourceSettings.SocialMedia)
	}
	settingsPlaceholders := make([]string, len(settingsInsertCols))
	for i := range settingsPlaceholders {
		settingsPlaceholders[i] = "?"
	}
	if _, err := tx.Exec(fmt.Sprintf(`
		INSERT INTO business_settings (%s)
		VALUES (%s)
	`, strings.Join(settingsInsertCols, ", "), strings.Join(settingsPlaceholders, ", ")), settingsInsertArgs...); err != nil {
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
		return nil, err
	}
	tenant.Active = active == 1
	return &tenant, nil
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
		       s.tenant_id,
		       u.tenant_id,
		       s.expires_at
		FROM sessions s
		JOIN users u ON u.id = s.user_id
		WHERE s.token = ?`
	if err := db.QueryRow(query, cookie.Value).Scan(&user.ID, &user.Username, &user.Role, &isActive, &sessionTenantID, &userTenantID, &expiresRaw); err != nil {
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
	if sessionTenantID <= 0 || userTenantID <= 0 {
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
	// API keys stay tenant-scoped, but no longer impersonate a tenant admin.
	return &User{
		Username: "api:" + integrationName,
		Role:     roleAPIKey,
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
			SELECT name, active, tenant_id
			FROM api_keys
			WHERE token_hash = ?
		`, hashAPIToken(token)).Scan(&integrationName, &active, &tenantID)
		if err != nil {
			return nil, "", "", err
		}
		if active != 1 {
			return nil, "", "", sql.ErrNoRows
		}
		if tenantID <= 0 {
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
				productSKU       string
			)
			if payload.ProductID != "" {
				if resolvedSKU, _, resolveErr := resolveProductRefForTenant(db, tenantIDFromUser(currentUser), payload.ProductID); resolveErr != nil {
					if resolveErr == sql.ErrNoRows {
						fields["product_id"] = "Selecciona un producto válido."
					} else {
						writeAPIError(w, http.StatusInternalServerError, "No se pudo cargar el producto.", nil)
						return
					}
				} else if err := db.QueryRow(`SELECT nombre, COALESCE(precio_venta, 0), sku FROM productos WHERE tenant_id = ? AND sku = ? LIMIT 1`, tenantIDFromUser(currentUser), resolvedSKU).Scan(&productName, &productSalePrice, &productSKU); err != nil {
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
			if err := logMovimientos(tx, productSKU, soldUnitIDs, "venta", payload.Notes, currentUser, now); err != nil {
				writeAPIError(w, http.StatusInternalServerError, "Error al registrar movimiento de venta.", nil)
				return
			}
			saleID, err := insertAndReturnID(tx, `INSERT INTO ventas (tenant_id, producto_id, cantidad, precio_final, metodo_pago, channel, sold_by, notas, fecha) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, tenantIDFromUser(currentUser), productSKU, quantity, salePrice, paymentMethod, payload.Channel, payload.SoldBy, payload.Notes, now)
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "Error al registrar la venta.", nil)
				return
			}
			if err := logAuditEvent(tx, currentUser, "sale_registered", "sale", payload.ProductID, "api", withAPIAuditMetadata(r, map[string]any{
				"sale_id":     saleID,
				"producto_id": payload.ProductID,
				"product_sku": productSKU,
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

func handleAPIInventoryAdjust(db *sql.DB, syncProduct func(productID string, salePrice *float64, name *string, retomaEnabled *bool, retomaPrice *float64)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		if syncProduct != nil && (payload.SalePrice != nil || payload.Name != nil || payload.RetomaEnabled != nil) {
			syncProduct(result.ProductID, payload.SalePrice, payload.Name, payload.RetomaEnabled, payload.RetomaPrice)
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{
			"ok":                true,
			"product_id":        result.ProductID,
			"previous_quantity": result.PreviousQuantity,
			"current_quantity":  result.CurrentQuantity,
			"delta":             result.Delta,
			"message":           result.Message,
		})
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
		productSKU := ""
		if ok {
			productSKU = selectedProduct.refID()
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
				fields["incoming_new_sku"] = "Ingresa el ID visible del producto nuevo."
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
		if err := logMovimientos(tx, productSKU, salientesMarcadas, "cambio_salida", notaMovimiento, currentUser, now); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "Error al registrar movimiento del cambio.", nil)
			return
		}
		incomingProductID := payload.IncomingExistingID
		incomingProductSKU := ""
		incomingQty := payload.IncomingExistingQty
		if payload.IncomingMode == "new" {
			incomingProductID = payload.IncomingNewSKU
			incomingQty = payload.IncomingNewQty
			incomingProductSKU, incomingProductID, err = insertProductWithGeneratedIdentity(tx, tenantIDFromUser(currentUser), incomingProductID, payload.IncomingNewName, payload.IncomingNewLine, now)
			if err != nil {
				var reqErr requestError
				if errors.As(err, &reqErr) {
					writeAPIError(w, reqErr.Status, reqErr.Message, reqErr.Fields)
				} else {
					writeAPIError(w, http.StatusInternalServerError, "No se pudo crear el producto entrante.", nil)
				}
				return
			}
		} else {
			incomingProduct, found := findProduct(productsSnapshot, payload.IncomingExistingID)
			if !found {
				writeAPIError(w, http.StatusBadRequest, "Producto entrante inválido.", map[string]string{"incoming_existing_id": "Selecciona un producto entrante válido."})
				return
			}
			incomingProductID = incomingProduct.ID
			incomingProductSKU = incomingProduct.refID()
		}
		for i := 0; i < incomingQty; i++ {
			unitID := fmt.Sprintf("U-%d-%d", time.Now().UnixNano(), i+1)
			if _, err := tx.Exec(`INSERT INTO unidades (id, tenant_id, producto_id, estado, creado_en, caducidad) VALUES (?, ?, ?, ?, ?, ?)`, unitID, normalizeTenantID(tenantIDFromUser(currentUser)), incomingProductSKU, "Disponible", now, nil); err != nil {
				writeAPIError(w, http.StatusInternalServerError, "Error al registrar unidades entrantes.", nil)
				return
			}
		}
		if err := logAuditEvent(tx, currentUser, "change_registered", "change", payload.ProductID, "api", withAPIAuditMetadata(r, map[string]any{
			"producto_saliente_id":  payload.ProductID,
			"producto_saliente_sku": productSKU,
			"producto_saliente":     selectedProduct.Name,
			"producto_entrante_id":  incomingProductID,
			"producto_entrante_sku": incomingProductSKU,
			"cantidad_saliente":     payload.Quantity,
			"cantidad_entrante":     incomingQty,
			"modo_entrada":          payload.IncomingMode,
		})); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "Error al registrar la auditoría del cambio.", nil)
			return
		}
		if err := tx.Commit(); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "Error al confirmar el cambio.", nil)
			return
		}
		writeAPIJSON(w, http.StatusCreated, map[string]any{"ok": true, "product_id": payload.ProductID, "product_sku": productSKU, "incoming_product_id": incomingProductID, "incoming_product_sku": incomingProductSKU, "quantity": payload.Quantity, "incoming_quantity": incomingQty, "message": "Cambio registrado correctamente."})
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
				ProductID              string   `json:"product_id"`
				Quantity               int      `json:"quantity"`
				ValueReceived          float64  `json:"value_received"`
				ReceivedState          string   `json:"received_state"`
				PublishToStock         bool     `json:"publish_to_stock"`
				FinalSalePrice         *float64 `json:"final_sale_price"`
				Notes                  string   `json:"notes"`
				CustomerID             int      `json:"customer_id"`
				CustomerName           string   `json:"customer_name"`
				CustomerPhone          string   `json:"customer_phone"`
				CustomerDocumentType   string   `json:"customer_document_type"`
				CustomerDocumentNumber string   `json:"customer_document_number"`
				CustomerAddress        string   `json:"customer_address"`
				CustomerCity           string   `json:"customer_city"`
				CustomerNotes          string   `json:"customer_notes"`
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
				Customer: customerInput{
					CustomerID:     payload.CustomerID,
					Name:           payload.CustomerName,
					Phone:          payload.CustomerPhone,
					DocumentType:   payload.CustomerDocumentType,
					DocumentNumber: payload.CustomerDocumentNumber,
					Address:        payload.CustomerAddress,
					City:           payload.CustomerCity,
					Notes:          payload.CustomerNotes,
				},
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
				"customer_id":        result.CustomerID,
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
		currentUser := userFromContext(r)
		switch r.Method {
		case http.MethodGet:
			q := strings.TrimSpace(r.URL.Query().Get("q"))
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
			items, err := listCreditInstallmentsForUser(db, currentUser, q, fromStr, toStr, 500)
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar los pagos del crédito.", nil)
				return
			}
			writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "items": items, "count": len(items)})
		case http.MethodPost:
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
		default:
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
		}
	}
}

func handleAPIProductRoutes(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		if currentUser == nil || !(isAdminRole(currentUser.Role) || isAPIKeyRole(currentUser.Role)) {
			writeAPIError(w, http.StatusForbidden, "Solo administrador puede editar productos vía API.", nil)
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/api/products/")
		path = strings.Trim(path, "/")
		if path == "" {
			writeAPIError(w, http.StatusNotFound, "Producto no encontrado.", nil)
			return
		}
		if strings.Contains(path, "/") {
			writeAPIError(w, http.StatusNotFound, "Ruta de producto no encontrada.", nil)
			return
		}
		if r.Method != http.MethodPatch && r.Method != http.MethodPut {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}

		var payload struct {
			ID    string `json:"id"`
			SKU   string `json:"sku"`
			NewID string `json:"new_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeAPIError(w, http.StatusBadRequest, "JSON inválido.", nil)
			return
		}
		newID, err := requestedVisibleProductID(firstNonEmptyString(payload.NewID, payload.ID), payload.SKU)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				writeAPIError(w, reqErr.Status, reqErr.Message, reqErr.Fields)
				return
			}
			writeAPIError(w, http.StatusBadRequest, "Datos inválidos.", map[string]string{"id": "El nuevo ID es inválido."})
			return
		}
		if newID == "" {
			writeAPIError(w, http.StatusBadRequest, "Datos inválidos.", map[string]string{"id": "El nuevo ID es obligatorio."})
			return
		}

		previous, err := loadProductEditRecord(db, tenantIDFromUser(currentUser), path)
		if err != nil {
			if err == sql.ErrNoRows {
				writeAPIError(w, http.StatusNotFound, "Producto no encontrado.", nil)
				return
			}
			writeAPIError(w, http.StatusInternalServerError, "No se pudo cargar el producto.", nil)
			return
		}

		tx, err := db.Begin()
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo iniciar la transacción.", nil)
			return
		}
		defer tx.Rollback()

		if err := renameProductIdentifier(tx, tenantIDFromUser(currentUser), previous.SKU, newID); err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				writeAPIError(w, reqErr.Status, reqErr.Message, reqErr.Fields)
				return
			}
			writeAPIError(w, http.StatusInternalServerError, "No se pudo actualizar el ID del producto.", nil)
			return
		}

		if previous.ID != newID {
			if err := logAuditEvent(tx, currentUser, "product_updated", "product", newID, "api", withAPIAuditMetadata(r, map[string]any{
				"product_sku": previous.SKU,
				"previous_id": previous.ID,
				"new_id":      newID,
				"name":        previous.Name,
			})); err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudo registrar la auditoría del producto.", nil)
				return
			}
		}

		if err := tx.Commit(); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo confirmar la edición del producto.", nil)
			return
		}

		writeAPIJSON(w, http.StatusOK, map[string]any{
			"ok":          true,
			"previous_id": previous.ID,
			"sku":         previous.SKU,
			"id":          newID,
			"message":     "ID de producto actualizado correctamente.",
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

func handleAPIAgentProductLoans(db *sql.DB) http.HandlerFunc {
	type apiAgentProductLoanItem struct {
		LoanID        int    `json:"loan_id"`
		CustomerName  string `json:"customer_name"`
		CustomerPhone string `json:"customer_phone"`
		ProductSKU    string `json:"product_sku"`
		ProductName   string `json:"product_name"`
		UnitSerial    string `json:"unit_serial"`
		FechaInicio   string `json:"fecha_inicio"`
		Estado        string `json:"estado"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}

		tenantID := tenantIDFromRequest(r)
		customerIDRaw := strings.TrimSpace(r.URL.Query().Get("customer_id"))
		customerID := 0
		if customerIDRaw != "" {
			parsed, err := strconv.Atoi(customerIDRaw)
			if err != nil || parsed <= 0 {
				writeAPIError(w, http.StatusBadRequest, "customer_id inválido.", nil)
				return
			}
			customerID = parsed
		}

		query := `
			SELECT
				pl.id,
				COALESCE(c.name, pl.borrower_name, ''),
				COALESCE(c.phone, pl.borrower_phone, ''),
				COALESCE(pl.product_id, ''),
				COALESCE(NULLIF(p.nombre, ''), pl.product_id, ''),
				COALESCE(plu.unit_id, ''),
				COALESCE(pl.loaned_at, ''),
				COALESCE(pl.status, 'active')
			FROM product_loans pl
			INNER JOIN product_loan_units plu
				ON plu.product_loan_id = pl.id AND plu.tenant_id = pl.tenant_id
			LEFT JOIN customers c
				ON c.id = pl.customer_id AND c.tenant_id = pl.tenant_id
			LEFT JOIN productos p
				ON p.sku = pl.product_id AND p.tenant_id = pl.tenant_id
			WHERE pl.tenant_id = ? AND COALESCE(pl.status, 'active') = 'active'
		`
		args := []any{tenantID}
		if customerID > 0 {
			query += ` AND pl.customer_id = ?`
			args = append(args, customerID)
		}
		query += ` ORDER BY pl.loaned_at DESC, pl.id DESC, plu.id ASC`

		rows, err := db.Query(query, args...)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudieron consultar los préstamos de producto.", nil)
			return
		}
		defer rows.Close()

		items := make([]apiAgentProductLoanItem, 0, 32)
		for rows.Next() {
			var (
				item      apiAgentProductLoanItem
				loanedAt  string
				statusRaw string
			)
			if err := rows.Scan(&item.LoanID, &item.CustomerName, &item.CustomerPhone, &item.ProductSKU, &item.ProductName, &item.UnitSerial, &loanedAt, &statusRaw); err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudieron leer los préstamos de producto.", nil)
				return
			}
			item.FechaInicio = formatDateTimeForAPI(loanedAt)
			item.Estado = string(normalizeProductLoanStatus(statusRaw))
			items = append(items, item)
		}
		if err := rows.Err(); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudieron procesar los préstamos de producto.", nil)
			return
		}

		if err := logAuditEvent(db, userFromContext(r), "product_loans_listed", "product_loan", strconv.Itoa(tenantID), "api", withAPIAuditMetadata(r, map[string]any{
			"customer_id": customerID,
			"count":       len(items),
		})); err != nil {
			log.Printf("audit product_loans_listed: %v", err)
		}

		writeAPIJSON(w, http.StatusOK, map[string]any{
			"ok":    true,
			"items": items,
			"count": len(items),
		})
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
		switch r.Method {
		case http.MethodGet:
			if currentUser == nil || !(isAdminRole(currentUser.Role) || isAPIKeyRole(currentUser.Role)) {
				writeAPIError(w, http.StatusForbidden, "Solo administrador puede consultar usuarios.", nil)
				return
			}
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
			if currentUser == nil || !isAdminRole(currentUser.Role) {
				writeAPIError(w, http.StatusForbidden, "Solo administrador puede operar usuarios.", nil)
				return
			}
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
			if _, err := db.Exec(fmt.Sprintf("UPDATE users SET %s WHERE id = ? AND tenant_id = ?", strings.Join(setCols, ", ")), args...); err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudo actualizar la contraseña.", nil)
				return
			}
			_, _ = db.Exec(`DELETE FROM sessions WHERE user_id = ?`, record.ID)
			resetLoginRateLimit(r, record.Username)
			resetLoginRateLimitForUsername(record.Username)
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

func handleCustomerCSVImport(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSONError := func(status int, message string) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":    false,
				"error": message,
			})
		}

		currentUser := userFromContext(r)
		if currentUser == nil || !isStaffRole(currentUser.Role) {
			writeJSONError(http.StatusForbidden, "Solo personal autorizado puede importar clientes.")
			return
		}
		if r.Method != http.MethodPost {
			writeJSONError(http.StatusMethodNotAllowed, "Método no permitido.")
			return
		}

		if err := r.ParseMultipartForm(8 << 20); err != nil {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) || strings.Contains(strings.ToLower(err.Error()), "request body too large") {
				writeJSONError(http.StatusRequestEntityTooLarge, "El archivo CSV excede el tamaño permitido.")
				return
			}
			writeJSONError(http.StatusBadRequest, "No se pudo leer el archivo CSV.")
			return
		}

		file, header, err := r.FormFile("file")
		if err != nil {
			writeJSONError(http.StatusBadRequest, "Archivo CSV no encontrado.")
			return
		}
		defer file.Close()

		data, err := readUploadedCSVFile(file, header, customerCSVUploadLimit)
		if err != nil {
			if reqErr, ok := requestErrorDetails(err); ok {
				writeJSONError(reqErr.Status, reqErr.Message)
				return
			}
			writeJSONError(http.StatusBadRequest, "No se pudo validar el archivo CSV.")
			return
		}

		resp, err := importCustomersFromCSV(db, currentUser, data, "web", nil)
		if err != nil {
			if reqErr, ok := requestErrorDetails(err); ok {
				writeJSONError(reqErr.Status, reqErr.Message)
				return
			}
			writeJSONError(http.StatusInternalServerError, "No se pudo importar el CSV de clientes.")
			return
		}

		writeAPIJSON(w, http.StatusOK, map[string]any{
			"ok":                true,
			"processed_rows":    resp.ProcessedRows,
			"created_customers": resp.CreatedCustomers,
			"updated_customers": resp.UpdatedCustomers,
			"rejected_rows":     len(resp.RejectedRows),
			"failed_rows":       resp.RejectedRows,
		})
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
		setSecurityHeaders(w, r)

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
			applyRequestBodyLimit(w, reqWithCtx)
			if authMode == "api_key" && !apiKeyRequestAllowed(reqWithCtx) {
				writeAPIError(w, http.StatusForbidden, "La API key no tiene permiso para operar esta ruta.", nil)
				return
			}
			if authMode == "session" && requestMutatesState(reqWithCtx.Method) && !requestPassesCSRFSameOriginCheck(reqWithCtx) {
				writeAPIError(w, http.StatusForbidden, "La validación CSRF falló para esta operación.", nil)
				return
			}
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
			clearSessionCookie(w, r)
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		ctx = context.WithValue(ctx, tenantContextKey, tenant)
		reqWithCtx := r.WithContext(ctx)
		applyRequestBodyLimit(w, reqWithCtx)
		if requestMutatesState(reqWithCtx.Method) && !requestPassesCSRFSameOriginCheck(reqWithCtx) {
			http.Error(w, "La validación CSRF falló para esta operación.", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, reqWithCtx)
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
	cols = append(cols, "role", "tenant_id")
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
	query := fmt.Sprintf("SELECT %s FROM users WHERE tenant_id = ?", strings.Join(managedUserSelectColumns(usersCols), ", "))
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
	query := fmt.Sprintf("SELECT %s FROM users WHERE id = ? AND tenant_id = ?", strings.Join(managedUserSelectColumns(usersCols), ", "))
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
		WHERE tenant_id = ?
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
		fmt.Sprintf("UPDATE users SET %s WHERE id = ? AND tenant_id = ?", strings.Join(setCols, ", ")),
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
	rows, err = db.Query(`
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = ?
	`, table)
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
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = current_schema() AND table_name = ?
	`, table).Scan(&exists); err != nil {
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

	businessSettingsExists, err := tableExists(db, "business_settings")
	if err != nil {
		return err
	}
	if businessSettingsExists {
		businessSettingsCols, err := tableColumns(db, "business_settings")
		if err != nil {
			return err
		}
		for _, column := range []struct {
			name       string
			definition string
		}{
			{name: "label_paper_width", definition: "TEXT NOT NULL DEFAULT '58mm'"},
			{name: "default_label_profile_id", definition: "INTEGER NOT NULL DEFAULT 0"},
			{name: "invoice_paper_width", definition: "TEXT NOT NULL DEFAULT '58mm'"},
			{name: "ticket_paper_width", definition: "TEXT NOT NULL DEFAULT '58mm'"},
			{name: "contact_phone", definition: "TEXT NOT NULL DEFAULT ''"},
			{name: "contact_email", definition: "TEXT NOT NULL DEFAULT ''"},
			{name: "social_media", definition: "TEXT NOT NULL DEFAULT ''"},
		} {
			if !businessSettingsCols[column.name] {
				if _, err := db.Exec("ALTER TABLE business_settings ADD COLUMN " + column.name + " " + column.definition); err != nil {
					return err
				}
			}
		}
	}

	labelProfilesExists, err := tableExists(db, "label_profiles")
	if err != nil {
		return err
	}
	if labelProfilesExists {
		labelProfileCols, err := tableColumns(db, "label_profiles")
		if err != nil {
			return err
		}
		if !labelProfileCols["row_gap_mm"] {
			if _, err := db.Exec("ALTER TABLE label_profiles ADD COLUMN row_gap_mm INTEGER NOT NULL DEFAULT 0"); err != nil {
				return err
			}
		}
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
	if !productosCols["fecha_ingreso"] {
		if _, err := db.Exec("ALTER TABLE productos ADD COLUMN fecha_ingreso TEXT"); err != nil {
			return err
		}
		productosCols["fecha_ingreso"] = true
	}
	// Legacy repair only: infer missing catalog rows from persisted units before visible-id backfill runs.
	if err := repairMissingProductosFromUnits(db); err != nil {
		return err
	}
	if err := backfillMissingProductVisibleIDs(db); err != nil {
		return err
	}
	if _, err := db.Exec("DROP INDEX IF EXISTS idx_productos_id_unique"); err != nil {
		return err
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_productos_tenant_id_unique ON productos(tenant_id, id)"); err != nil {
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
		{name: "retoma_enabled", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "retoma_price", definition: "REAL"},
		{name: "location", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "talla_requerida", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "talla", definition: "TEXT NOT NULL DEFAULT ''"},
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
	if !retomasCols["customer_id"] {
		if _, err := db.Exec("ALTER TABLE retomas ADD COLUMN customer_id INTEGER"); err != nil {
			return err
		}
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_retomas_tenant_customer ON retomas(tenant_id, customer_id, fecha)"); err != nil {
		return err
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
		{name: "receipt_buyer_name", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "receipt_buyer_document", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "receipt_generated_at", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "receipt_generated_by", definition: "INTEGER"},
		{name: "receipt_last_format", definition: "TEXT NOT NULL DEFAULT ''"},
	} {
		if !ventasCols[column.name] {
			if _, err := db.Exec("ALTER TABLE ventas ADD COLUMN " + column.name + " " + column.definition); err != nil {
				return err
			}
		}
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_ventas_tenant_receipt_generated ON ventas(tenant_id, receipt_generated_at)"); err != nil {
		return err
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
	schema := `
	CREATE TABLE IF NOT EXISTS customers (
		id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		name TEXT NOT NULL,
		phone TEXT NOT NULL DEFAULT '',
		document_type TEXT NOT NULL DEFAULT '',
		document_number TEXT NOT NULL DEFAULT '',
		address TEXT NOT NULL DEFAULT '',
		city TEXT NOT NULL DEFAULT '',
		notes TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text),
		updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text)
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_customers_tenant_document ON customers (tenant_id, document_type, document_number);
	CREATE INDEX IF NOT EXISTS idx_customers_tenant_name ON customers (tenant_id, name);
	CREATE INDEX IF NOT EXISTS idx_customers_tenant_city ON customers (tenant_id, city);

	CREATE TABLE IF NOT EXISTS customer_events (
		id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		customer_id INTEGER NOT NULL,
		event_type TEXT NOT NULL,
		ref_type TEXT NOT NULL DEFAULT '',
		ref_id TEXT NOT NULL DEFAULT '',
		amount REAL NOT NULL DEFAULT 0,
		payload_json TEXT NOT NULL DEFAULT '{}',
		created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text),
		created_by INTEGER
	);
	CREATE INDEX IF NOT EXISTS idx_customer_events_tenant_customer_created ON customer_events (tenant_id, customer_id, created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_customer_events_tenant_event_type ON customer_events (tenant_id, event_type);

	CREATE TABLE IF NOT EXISTS invoices (
		id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
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
		created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text)
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_invoices_tenant_number ON invoices (tenant_id, invoice_number);
	CREATE INDEX IF NOT EXISTS idx_invoices_tenant_created ON invoices (tenant_id, created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_invoices_tenant_sale ON invoices (tenant_id, sale_id);
	CREATE INDEX IF NOT EXISTS idx_invoices_tenant_credit ON invoices (tenant_id, credit_sale_id);
	CREATE INDEX IF NOT EXISTS idx_invoices_tenant_customer ON invoices (tenant_id, customer_id);
	CREATE UNIQUE INDEX IF NOT EXISTS uq_invoices_tenant_sale ON invoices (tenant_id, sale_id);
	CREATE UNIQUE INDEX IF NOT EXISTS uq_invoices_tenant_credit ON invoices (tenant_id, credit_sale_id);

	CREATE TABLE IF NOT EXISTS invoice_items (
		id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		invoice_id INTEGER NOT NULL,
		product_id TEXT NOT NULL DEFAULT '',
		description TEXT NOT NULL,
		quantity INTEGER NOT NULL DEFAULT 1,
		unit_price REAL NOT NULL DEFAULT 0,
		total REAL NOT NULL DEFAULT 0
	);
	CREATE INDEX IF NOT EXISTS idx_invoice_items_tenant_invoice ON invoice_items (tenant_id, invoice_id);
	`
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
	schema := `
	CREATE TABLE IF NOT EXISTS product_loans (
		id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
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
		loaned_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text),
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
		id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		product_loan_id INTEGER NOT NULL,
		unit_id TEXT NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_product_loan_units_tenant_loan ON product_loan_units (tenant_id, product_loan_id);
	CREATE INDEX IF NOT EXISTS idx_product_loan_units_tenant_unit ON product_loan_units (tenant_id, unit_id);
	`
	if _, err := db.Exec(schema); err != nil {
		return err
	}
	return nil
}

func initDB(schemaLabel string, paymentMethods []string) (*sql.DB, error) {
	cfg, err := loadDatabaseConfig()
	if err != nil {
		return nil, err
	}
	schemaName := "test_" + sanitizePostgresIdentifier(filepath.Base(schemaLabel)) + "_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	adminDB, err := sql.Open(postgresDriverName, cfg.DSN)
	if err != nil {
		return nil, err
	}
	if _, err := adminDB.Exec(`CREATE SCHEMA IF NOT EXISTS ` + quotePostgresIdentifier(schemaName)); err != nil {
		_ = adminDB.Close()
		return nil, err
	}
	_ = adminDB.Close()
	cfg.DSN, err = postgresDSNWithSearchPath(cfg.DSN, schemaName)
	if err != nil {
		return nil, err
	}
	return initDBWithConfig(cfg, paymentMethods)
}

func initDBWithConfig(cfg databaseConfig, paymentMethods []string) (*sql.DB, error) {
	cfg.Engine = dbEngine(strings.TrimSpace(strings.ToLower(string(cfg.Engine))))
	if cfg.Engine != dbEnginePostgres {
		return nil, fmt.Errorf("StockiAPP requiere Postgres; configura DB_DSN o DATABASE_URL")
	}
	return initPostgresDB(cfg.DSN, paymentMethods)
}

func initPostgresDB(dsn string, paymentMethods []string) (*sql.DB, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("DB_DSN o DATABASE_URL es obligatorio")
	}

	dsn, err := postgresDSNWithSessionTimeZone(dsn, appTimeLocation.String())
	if err != nil {
		return nil, err
	}

	db, err := sql.Open(postgresDriverName, dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}

	schema := `
	CREATE TABLE IF NOT EXISTS tenants (
		id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		slug TEXT NOT NULL UNIQUE,
		name TEXT NOT NULL,
		active INTEGER NOT NULL DEFAULT 1,
		created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text),
		updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text)
	);
	CREATE INDEX IF NOT EXISTS idx_tenants_slug ON tenants (slug);

	CREATE TABLE IF NOT EXISTS productos (
		sku TEXT PRIMARY KEY,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		id TEXT,
		linea TEXT NOT NULL,
		nombre TEXT NOT NULL,
		location TEXT NOT NULL DEFAULT '',
		talla_requerida INTEGER NOT NULL DEFAULT 0,
		talla TEXT NOT NULL DEFAULT '',
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
		fecha_ingreso TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text)
	);
	CREATE INDEX IF NOT EXISTS idx_productos_linea ON productos (linea);
	CREATE INDEX IF NOT EXISTS idx_productos_tenant_id ON productos (tenant_id);

	CREATE TABLE IF NOT EXISTS ventas (
		id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		producto_id TEXT NOT NULL,
		cantidad INTEGER NOT NULL,
		precio_final REAL NOT NULL,
		metodo_pago TEXT NOT NULL,
		channel TEXT NOT NULL DEFAULT '',
		sold_by TEXT NOT NULL DEFAULT '',
		notas TEXT NOT NULL DEFAULT '',
		receipt_buyer_name TEXT NOT NULL DEFAULT '',
		receipt_buyer_document TEXT NOT NULL DEFAULT '',
		receipt_generated_at TEXT NOT NULL DEFAULT '',
		receipt_generated_by INTEGER,
		receipt_last_format TEXT NOT NULL DEFAULT '',
		fecha TEXT NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_ventas_tenant_fecha ON ventas (tenant_id, fecha);
	CREATE INDEX IF NOT EXISTS idx_ventas_tenant_metodo ON ventas (tenant_id, metodo_pago);

	CREATE TABLE IF NOT EXISTS retomas (
		id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		producto_id TEXT NOT NULL,
		customer_id INTEGER,
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
		id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
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
		id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		name TEXT NOT NULL UNIQUE,
		token_hash TEXT NOT NULL UNIQUE,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		active INTEGER NOT NULL DEFAULT 1,
		created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text),
		updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text)
	);
	CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys (active);
	CREATE INDEX IF NOT EXISTS idx_api_keys_tenant_id ON api_keys (tenant_id);

	CREATE TABLE IF NOT EXISTS business_settings (
		id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		tenant_id INTEGER NOT NULL UNIQUE,
		business_name TEXT NOT NULL,
		logo_path TEXT NOT NULL DEFAULT '',
		contact_phone TEXT NOT NULL DEFAULT '',
		contact_email TEXT NOT NULL DEFAULT '',
		social_media TEXT NOT NULL DEFAULT '',
		primary_color TEXT NOT NULL DEFAULT '#0ea5c9',
		currency TEXT NOT NULL DEFAULT 'COP',
		date_format TEXT NOT NULL DEFAULT '2006-01-02',
		label_paper_width TEXT NOT NULL DEFAULT '58mm',
		default_label_profile_id INTEGER NOT NULL DEFAULT 0,
		invoice_paper_width TEXT NOT NULL DEFAULT '58mm',
		ticket_paper_width TEXT NOT NULL DEFAULT '58mm',
		updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text)
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_business_settings_tenant_id ON business_settings (tenant_id);

	CREATE TABLE IF NOT EXISTS label_profiles (
		id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		name TEXT NOT NULL,
		label_width_mm INTEGER NOT NULL,
		label_height_mm INTEGER NOT NULL,
		columns INTEGER NOT NULL DEFAULT 1,
		column_gap_mm INTEGER NOT NULL DEFAULT 0,
		row_gap_mm INTEGER NOT NULL DEFAULT 0,
		show_business INTEGER NOT NULL DEFAULT 1,
		show_contact INTEGER NOT NULL DEFAULT 0,
		show_line INTEGER NOT NULL DEFAULT 0,
		show_size INTEGER NOT NULL DEFAULT 0,
		show_price INTEGER NOT NULL DEFAULT 1,
		show_barcode INTEGER NOT NULL DEFAULT 1,
		show_id INTEGER NOT NULL DEFAULT 1,
		created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text),
		updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text),
		CHECK (label_width_mm BETWEEN 20 AND 120),
		CHECK (label_height_mm BETWEEN 15 AND 120),
		CHECK (columns IN (1, 2)),
		CHECK (column_gap_mm BETWEEN 0 AND 10),
		CHECK (row_gap_mm BETWEEN 0 AND 10)
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_label_profiles_tenant_name ON label_profiles (tenant_id, name);
	CREATE INDEX IF NOT EXISTS idx_label_profiles_tenant_id ON label_profiles (tenant_id, id);

	CREATE TABLE IF NOT EXISTS business_lines (
		id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		name TEXT NOT NULL,
		active INTEGER NOT NULL DEFAULT 1,
		created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text),
		updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text)
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_business_lines_tenant_name ON business_lines (tenant_id, name);
	CREATE INDEX IF NOT EXISTS idx_business_lines_tenant_active_name ON business_lines (tenant_id, active, name);

	CREATE TABLE IF NOT EXISTS payment_methods (
		id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		name TEXT NOT NULL,
		active INTEGER NOT NULL DEFAULT 1,
		sort_order INTEGER NOT NULL DEFAULT 0,
		created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text),
		updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text)
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_payment_methods_tenant_name ON payment_methods (tenant_id, name);
	CREATE INDEX IF NOT EXISTS idx_payment_methods_tenant_sort ON payment_methods (tenant_id, sort_order, id);

	CREATE TABLE IF NOT EXISTS movement_settings (
		id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		movement_type TEXT NOT NULL,
		enabled INTEGER NOT NULL DEFAULT 1,
		updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text)
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_movement_settings_tenant_type ON movement_settings (tenant_id, movement_type);
	CREATE INDEX IF NOT EXISTS idx_movement_settings_tenant_enabled ON movement_settings (tenant_id, enabled);

	CREATE TABLE IF NOT EXISTS audit_events (
		id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		event_type TEXT NOT NULL,
		entity_type TEXT NOT NULL,
		entity_id TEXT NOT NULL DEFAULT '',
		user_id INTEGER,
		source TEXT NOT NULL DEFAULT 'manual',
		payload_json TEXT NOT NULL DEFAULT '{}',
		created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text)
	);
	CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_created_at ON audit_events (tenant_id, created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_event_type ON audit_events (tenant_id, event_type);
	CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_entity_type ON audit_events (tenant_id, entity_type);

	CREATE TABLE IF NOT EXISTS credit_installments (
		id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		credit_sale_id INTEGER,
		product_id TEXT,
		installment_number INTEGER NOT NULL,
		amount_paid REAL NOT NULL DEFAULT 0,
		created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text),
		created_by INTEGER
	);
	CREATE INDEX IF NOT EXISTS idx_credit_installments_tenant_product_id ON credit_installments (tenant_id, product_id, installment_number);

	CREATE TABLE IF NOT EXISTS credit_sales (
		id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
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
		created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text),
		created_by INTEGER
	);
	CREATE INDEX IF NOT EXISTS idx_credit_sales_tenant_product_id ON credit_sales (tenant_id, product_id, created_at);
	CREATE INDEX IF NOT EXISTS idx_credit_sales_tenant_debtor_name ON credit_sales (tenant_id, debtor_name);
	`

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
	if err := syncPostgresIdentitySequence(db, "tenants", "id"); err != nil {
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
	if err := seedAdminUser(db); err != nil {
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
	stmt, err := tx.Prepare(`INSERT INTO ventas (tenant_id, producto_id, cantidad, precio_final, metodo_pago, notas, fecha)
		VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("prepare ventas: %w (rollback: %v)", err, rollbackErr)
		}
		return err
	}
	defer stmt.Close()

	baseDate := time.Now()
	visibleProductIDs := []string{"P-001", "P-002", "P-003"}
	productSKUs := make(map[string]string, len(visibleProductIDs))
	for _, visibleID := range visibleProductIDs {
		sku, _, err := resolveProductRefForTenant(db, defaultTenantID, visibleID)
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return fmt.Errorf("resolve ventas product %s: %w (rollback: %v)", visibleID, err, rollbackErr)
			}
			return err
		}
		productSKUs[visibleID] = sku
	}
	for i := 0; i < 14; i++ {
		date := baseDate.AddDate(0, 0, -i).Format("2006-01-02")
		entries := (i % 3) + 2
		for j := 0; j < entries; j++ {
			visibleID := visibleProductIDs[(i+j)%len(visibleProductIDs)]
			productoID := productSKUs[visibleID]
			cantidad := (j % 3) + 1
			precio := float64(18000 + (i * 1200) + (j * 800))
			metodo := paymentMethods[(i+j)%len(paymentMethods)]
			if _, err := stmt.Exec(defaultTenantID, productoID, cantidad, precio, metodo, "Venta seed", date); err != nil {
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
	stmt, err := tx.Prepare(`INSERT INTO unidades (id, tenant_id, producto_id, estado, creado_en, caducidad)
		VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("prepare unidades: %w (rollback: %v)", err, rollbackErr)
		}
		return err
	}
	defer stmt.Close()

	statuses := []string{"Disponible", "Vendida", "Cambio"}
	visibleProductIDs := []string{"P-001", "P-002", "P-003"}
	productSKUs := make(map[string]string, len(visibleProductIDs))
	for _, visibleID := range visibleProductIDs {
		sku, _, err := resolveProductRefForTenant(db, defaultTenantID, visibleID)
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return fmt.Errorf("resolve unidades product %s: %w (rollback: %v)", visibleID, err, rollbackErr)
			}
			return err
		}
		productSKUs[visibleID] = sku
	}
	now := time.Now()
	for i := 1; i <= 36; i++ {
		id := fmt.Sprintf("U-%03d", i)
		visibleID := visibleProductIDs[i%len(visibleProductIDs)]
		productoID := productSKUs[visibleID]
		estado := statuses[i%len(statuses)]
		createdAt := now.AddDate(0, 0, -i).Format(time.RFC3339)
		expiryAt := now.AddDate(0, 0, 20+i).Format("2006-01-02")
		if _, err := stmt.Exec(id, defaultTenantID, productoID, estado, createdAt, expiryAt); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return fmt.Errorf("insert unidades: %w (rollback: %v)", err, rollbackErr)
			}
			return err
		}
	}

	return tx.Commit()
}

func adminUserNameForBootstrap() string {
	adminUser := strings.TrimSpace(os.Getenv("ADMIN_USER"))
	if adminUser != "" {
		return adminUser
	}
	return "admin"
}

func ensureUsersRoleSupport(db *sql.DB) error {
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

func seedAdminUser(db *sql.DB) error {
	adminUser := os.Getenv("ADMIN_USER")
	adminPass := os.Getenv("ADMIN_PASS")
	if adminUser == "" || adminPass == "" {
		log.Print("ADMIN_USER/ADMIN_PASS no configurados, omitiendo creación automática de admin.")
		return nil
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
	dbConfig, err := loadDatabaseConfig()
	if err != nil {
		log.Fatalf("Configuración de base de datos inválida: %v", err)
	}

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
		"templates/manual_page.html",
		"templates/customers.html",
		"templates/customer_detail.html",
		"templates/credit_edits_report.html",
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
		"templates/product_labels_batch.html",
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
		log.Printf("DB_ENGINE=%s DB_DSN=%s cwd=%s", dbConfig.Engine, dbConfig.DSN, wd)
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
	defaultProducts := defaultSeedProducts()
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
		TallaRequerida    bool
		Talla             string
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

	type manualPageData struct {
		Title       string
		FrameURL    string
		CurrentUser *User
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

		r.Body = http.MaxBytesReader(w, r.Body, loginFormBodyLimit)
		if err := r.ParseForm(); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "request body too large") {
				http.Error(w, "El formulario de login excede el tamaño permitido.", http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, "No se pudo leer el formulario", http.StatusBadRequest)
			return
		}

		username := strings.TrimSpace(r.FormValue("username"))
		password := r.FormValue("password")
		now := time.Now()
		if retryAfter, allowed := loginRequestAllowed(r, username, now); !allowed {
			retrySeconds := int(math.Ceil(retryAfter.Seconds()))
			if retrySeconds <= 0 {
				retrySeconds = int(loginRateLockDuration.Seconds())
			}
			w.Header().Set("Retry-After", strconv.Itoa(retrySeconds))
			data := loginPageData{
				Title:    "Iniciar sesión",
				Error:    "Demasiados intentos fallidos. Espera unos minutos antes de intentar de nuevo.",
				Username: username,
			}
			w.WriteHeader(http.StatusTooManyRequests)
			renderTemplate(w, "login.html", data, "Error al renderizar login")
			return
		}

		var (
			user     User
			hash     string
			isActive int
		)
		err := db.QueryRow(`
						SELECT id, username, password_hash, role, is_active, tenant_id
						FROM users
						WHERE username = ?
					`, username).Scan(&user.ID, &user.Username, &hash, &user.Role, &isActive, &user.TenantID)
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
			registerLoginFailure(r, username, now)
			w.WriteHeader(http.StatusUnauthorized)
			renderTemplate(w, "login.html", data, "Error al renderizar login")
			return
		}
		if user.TenantID <= 0 {
			log.Printf("login: user without tenant username=%q", username)
			data := loginPageData{
				Title:    "Iniciar sesión",
				Error:    "La empresa asociada a este usuario es inválida.",
				Username: username,
			}
			registerLoginFailure(r, username, now)
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
			registerLoginFailure(r, username, now)
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
			registerLoginFailure(r, username, now)
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

		resetLoginRateLimit(r, username)
		setSessionCookie(w, r, token, expiresAt)
		http.Redirect(w, r, "/inventario", http.StatusSeeOther)
	})

	mux.HandleFunc("/logout", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			http.Redirect(w, r, "/inventario", http.StatusSeeOther)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}
		if cookie, err := r.Cookie("session_token"); err == nil {
			_, _ = db.Exec("DELETE FROM sessions WHERE token = ?", cookie.Value)
		}
		clearSessionCookie(w, r)
		http.Redirect(w, r, "/login", http.StatusSeeOther)
	})

	mux.HandleFunc("/manual", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}
		currentUser := userFromContext(r)
		if currentUser == nil {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		data := manualPageData{
			Title:       "Manual de usuario",
			FrameURL:    "/manual/contenido",
			CurrentUser: currentUser,
		}
		renderTemplate(w, "manual_page.html", data, "Error al renderizar manual")
	})

	mux.HandleFunc("/manual/contenido", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}
		if userFromContext(r) == nil {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		w.Header().Set("X-Robots-Tag", "noindex, nofollow")
		http.ServeFile(w, r, filepath.Join("docs", "manual-stockiapp.html"))
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

	mux.HandleFunc("/clientes/csv", handleCustomerCSVImport(db))

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
		http.Redirect(w, r, cashLoansInventoryURL(r.URL), http.StatusFound)
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
			VersionLabel:         "Versión 1.0.2",
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
		settings.ContactPhone = strings.TrimSpace(r.FormValue("contact_phone"))
		settings.ContactEmail = strings.TrimSpace(r.FormValue("contact_email"))
		settings.SocialMedia = strings.TrimSpace(r.FormValue("social_media"))
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
				redirectWithMessage(w, r, "/configuracion", "", "No se pudo guardar el logo. Usa PNG, JPG o WEBP.")
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
			"contact_phone":       savedSettings.ContactPhone,
			"contact_email":       savedSettings.ContactEmail,
			"social_media":        savedSettings.SocialMedia,
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

	mux.HandleFunc("/configuracion/etiquetas/create", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "No se pudo leer el perfil de etiqueta.")
			return
		}
		profile, err := labelProfileFromForm(r)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", reqErr.Message)
				return
			}
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "El perfil de etiqueta es inválido.")
			return
		}
		created, err := createLabelProfileForTenant(db, tenantIDFromRequest(r), profile)
		if err != nil {
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "No se pudo crear el perfil. Verifica que el nombre no esté repetido.")
			return
		}
		if err := logAuditEvent(db, userFromContext(r), "label_profile_created", "label_profile", strconv.Itoa(created.ID), "manual", map[string]any{"name": created.Name, "width_mm": created.LabelWidthMM, "height_mm": created.LabelHeightMM, "columns": created.Columns}); err != nil {
			log.Printf("audit label profile created: %v", err)
		}
		redirectWithMessage(w, r, "/productos/etiquetas/masivas", "Perfil de etiqueta creado.", "")
	}))

	mux.HandleFunc("/configuracion/etiquetas/update", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "No se pudo leer el perfil de etiqueta.")
			return
		}
		profileID, err := strconv.Atoi(strings.TrimSpace(r.FormValue("profile_id")))
		if err != nil || profileID <= 0 {
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "Perfil de etiqueta inválido.")
			return
		}
		profile, err := labelProfileFromForm(r)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", reqErr.Message)
				return
			}
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "El perfil de etiqueta es inválido.")
			return
		}
		if err := updateLabelProfileForTenant(db, tenantIDFromRequest(r), profileID, profile); err != nil {
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "No se pudo actualizar el perfil. Verifica que el nombre no esté repetido.")
			return
		}
		if err := logAuditEvent(db, userFromContext(r), "label_profile_updated", "label_profile", strconv.Itoa(profileID), "manual", map[string]any{"name": profile.Name, "width_mm": profile.LabelWidthMM, "height_mm": profile.LabelHeightMM, "columns": profile.Columns}); err != nil {
			log.Printf("audit label profile updated: %v", err)
		}
		redirectWithMessage(w, r, "/productos/etiquetas/masivas", "Perfil de etiqueta actualizado.", "")
	}))

	mux.HandleFunc("/configuracion/etiquetas/default", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "No se pudo leer el perfil de etiqueta.")
			return
		}
		profileID, err := strconv.Atoi(strings.TrimSpace(r.FormValue("profile_id")))
		if err != nil || profileID <= 0 {
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "Perfil de etiqueta inválido.")
			return
		}
		tenantID := tenantIDFromRequest(r)
		if _, err := labelProfileByIDForTenant(db, tenantID, profileID); err != nil {
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "El perfil de etiqueta no existe.")
			return
		}
		if _, err := db.Exec(`UPDATE business_settings SET default_label_profile_id = ?, updated_at = ? WHERE tenant_id = ?`, profileID, time.Now().Format(time.RFC3339), tenantID); err != nil {
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "No se pudo seleccionar el perfil predeterminado.")
			return
		}
		if err := logAuditEvent(db, userFromContext(r), "label_profile_default_changed", "label_profile", strconv.Itoa(profileID), "manual", nil); err != nil {
			log.Printf("audit label profile default: %v", err)
		}
		redirectWithMessage(w, r, "/productos/etiquetas/masivas", "Perfil predeterminado actualizado.", "")
	}))

	mux.HandleFunc("/configuracion/etiquetas/delete", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "Método no permitido.")
			return
		}
		if err := r.ParseForm(); err != nil {
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "No se pudo leer el perfil de etiqueta.")
			return
		}
		profileID, err := strconv.Atoi(strings.TrimSpace(r.FormValue("profile_id")))
		if err != nil || profileID <= 0 {
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "Perfil de etiqueta inválido.")
			return
		}
		tenantID := tenantIDFromRequest(r)
		settings := settingsForUser(userFromContext(r))
		profiles, defaultProfileID, err := ensureLabelProfilesForTenant(db, tenantID, settings.LabelPaperWidth)
		if err != nil || len(profiles) <= 1 {
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "Debe conservar al menos un perfil de etiqueta.")
			return
		}
		if profileID == defaultProfileID {
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "Elige otro perfil predeterminado antes de eliminar este.")
			return
		}
		result, err := db.Exec(`DELETE FROM label_profiles WHERE tenant_id = ? AND id = ?`, tenantID, profileID)
		if err != nil {
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "No se pudo eliminar el perfil de etiqueta.")
			return
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			redirectWithMessage(w, r, "/productos/etiquetas/masivas", "", "El perfil de etiqueta no existe.")
			return
		}
		if err := logAuditEvent(db, userFromContext(r), "label_profile_deleted", "label_profile", strconv.Itoa(profileID), "manual", nil); err != nil {
			log.Printf("audit label profile deleted: %v", err)
		}
		redirectWithMessage(w, r, "/productos/etiquetas/masivas", "Perfil de etiqueta eliminado.", "")
	}))

	mux.HandleFunc("/configuracion/tenants/reset", platformAdminOnly(func(w http.ResponseWriter, r *http.Request) {
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
			redirectWithMessage(w, r, "/configuracion", "", "Solo un platform admin puede hacer hard reset de empresas.")
			return
		}

		tenantID, err := strconv.Atoi(strings.TrimSpace(r.FormValue("tenant_id")))
		if err != nil || tenantID <= 0 {
			redirectWithMessage(w, r, "/configuracion", "", "Empresa inválida.")
			return
		}

		scope, err := parseTenantResetScope(r.FormValue("scope"))
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				redirectWithMessage(w, r, "/configuracion", "", reqErr.Message)
				return
			}
			redirectWithMessage(w, r, "/configuracion", "", "Scope inválido.")
			return
		}

		tenant, err := loadTenantForManagement(db, tenantID)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				redirectWithMessage(w, r, "/configuracion", "", reqErr.Message)
				return
			}
			log.Printf("load tenant for hard reset: %v", err)
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo cargar la empresa.")
			return
		}
		if strings.TrimSpace(r.FormValue("confirm_slug")) != tenant.Slug {
			redirectWithMessage(w, r, "/configuracion", "", "Escribe exactamente el slug de la empresa para confirmar.")
			return
		}

		summary, err := hardResetTenantScope(db, currentUser, tenantID, scope)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				redirectWithMessage(w, r, "/configuracion", "", reqErr.Message)
				return
			}
			log.Printf("tenant hard reset failed tenant=%d scope=%s err=%v", tenantID, scope, err)
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo completar el hard reset.")
			return
		}

		redirectWithMessage(
			w,
			r,
			"/configuracion",
			fmt.Sprintf("Hard reset de %s completado para %s (%d filas).", tenantResetScopeLabel(summary.Scope), summary.TenantName, summary.TotalDeleted()),
			"",
		)
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
		var existingID int
		err := db.QueryRow(`
			SELECT id
			FROM business_lines
			WHERE tenant_id = ? AND name = ?
			LIMIT 1
		`, tenantID, name).Scan(&existingID)
		if err == nil {
			redirectWithMessage(w, r, "/configuracion", "", "Ya existe una línea con ese nombre.")
			return
		}
		if err != nil && err != sql.ErrNoRows {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo validar la línea.")
			return
		}
		if err := ensureBusinessLineExists(db, tenantID, userFromContext(r), name, now, "settings"); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "unique") {
				redirectWithMessage(w, r, "/configuracion", "", "Ya existe una línea con ese nombre.")
				return
			}
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo crear la línea.")
			return
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
		if _, err := db.Exec(fmt.Sprintf("UPDATE users SET %s WHERE id = ? AND tenant_id = ?", strings.Join(setCols, ", ")), args...); err != nil {
			redirectWithMessage(w, r, "/admin/users", "", "No se pudo actualizar la contraseña.")
			return
		}
		_, _ = db.Exec(`DELETE FROM sessions WHERE user_id = ?`, userID)
		resetLoginRateLimit(r, targetRecord.Username)
		resetLoginRateLimitForUsername(targetRecord.Username)
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
		if _, err := db.Exec(`DELETE FROM users WHERE id = ? AND tenant_id = ?`, userID, targetRecord.TenantID); err != nil {
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
		nextSKU, err := generateNextTenantProductID(db, tenantIDFromRequest(r))
		if err != nil {
			http.Error(w, "No se pudo generar el ID", http.StatusInternalServerError)
			return
		}
		assignableUsers, err := loadAssignableUsersForTenant(db, tenantIDFromRequest(r))
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
		requestedProfileID := 0
		if rawProfileID := strings.TrimSpace(r.URL.Query().Get("profile_id")); rawProfileID != "" {
			var err error
			requestedProfileID, err = strconv.Atoi(rawProfileID)
			if err != nil || requestedProfileID <= 0 {
				http.Error(w, "Perfil de etiqueta inválido.", http.StatusBadRequest)
				return
			}
		}
		requestedSize := r.URL.Query().Get("size")
		if strings.TrimSpace(requestedSize) == "" {
			requestedSize = r.URL.Query().Get("paper")
		}
		settings := settingsForUser(currentUser)
		labelProfile, err := labelProfileForTenant(db, tenantIDFromUser(currentUser), settings.LabelPaperWidth, requestedSize, requestedProfileID)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				http.Error(w, reqErr.Message, reqErr.Status)
				return
			}
			http.Error(w, "No se pudo cargar el perfil de etiqueta.", http.StatusInternalServerError)
			return
		}
		printProfile := labelPrintProfileFromProfile(labelProfile)
		items, _, _, err := productLabelItemsForUserWithDimensions(db, currentUser, productIDs, printProfile.LabelWidthMM, printProfile.LabelHeightMM)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				http.Error(w, reqErr.Message, reqErr.Status)
				return
			}
			http.Error(w, "No se pudieron generar las etiquetas.", http.StatusInternalServerError)
			return
		}
		data := productLabelsPageDataForProfile(items, printProfile, labelProfile, currentUser, settings)
		var rendered bytes.Buffer
		if err := tmpl.ExecuteTemplate(&rendered, "product_labels.html", data); err != nil {
			http.Error(w, "Error al renderizar etiquetas", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(rendered.Bytes())
	})

	mux.HandleFunc("/productos/etiquetas/masivas", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		if currentUser == nil || !isStaffRole(currentUser.Role) {
			http.Error(w, "No autorizado", http.StatusForbidden)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}
		products, err := loadVisibleProductsForUser(db, currentUser)
		if err != nil {
			http.Error(w, "No se pudieron cargar los productos.", http.StatusInternalServerError)
			return
		}
		availableByProduct, err := availableCountsByProduct(db, tenantIDFromUser(currentUser))
		if err != nil {
			http.Error(w, "No se pudo consultar la existencia disponible.", http.StatusInternalServerError)
			return
		}
		batchProducts := make([]productLabelBatchProduct, 0, len(products))
		for _, product := range products {
			visibleID := strings.TrimSpace(product.ID)
			if visibleID == "" {
				continue
			}
			available := max(0, availableByProduct[visibleID])
			suggestedCopies := max(1, available)
			if suggestedCopies > maxLabelBatchCopies {
				suggestedCopies = maxLabelBatchCopies
			}
			batchProducts = append(batchProducts, productLabelBatchProduct{
				ID:   visibleID,
				Name: product.Name,
				Line: product.Line,
				Size: func() string {
					if !product.TallaRequerida {
						return ""
					}
					return strings.TrimSpace(product.Talla)
				}(),
				Price:           formatCurrency(product.SalePrice),
				Available:       available,
				SuggestedCopies: suggestedCopies,
				CopiesKey:       "copies_" + visibleID,
			})
		}
		settings := settingsForUser(currentUser)
		profiles, defaultProfileID, err := ensureLabelProfilesForTenant(db, tenantIDFromUser(currentUser), settings.LabelPaperWidth)
		if err != nil {
			http.Error(w, "No se pudieron cargar los perfiles de etiqueta.", http.StatusInternalServerError)
			return
		}
		renderTemplate(w, "product_labels_batch.html", productLabelsBatchPageData{
			Title:            "Etiquetas masivas",
			Subtitle:         "Selecciona productos y prepara un lote para impresora térmica o PDF.",
			Flash:            strings.TrimSpace(r.URL.Query().Get("mensaje")),
			Error:            strings.TrimSpace(r.URL.Query().Get("error")),
			Products:         batchProducts,
			DefaultProfileID: defaultProfileID,
			Profiles:         profiles,
			CanManageLabels:  isAdminRole(currentUser.Role),
			CurrentUser:      currentUser,
			Settings:         settings,
			MaxLabels:        maxLabelBatchLabels,
			MaxCopies:        maxLabelBatchCopies,
			SizeOptions:      labelPaperOptions(),
			DefaultGapMM:     defaultLabelGapMM,
		}, "Error al renderizar etiquetas masivas")
	})

	mux.HandleFunc("/productos/etiquetas/lote", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		if currentUser == nil || !isStaffRole(currentUser.Role) {
			http.Error(w, "No autorizado", http.StatusForbidden)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if err := r.ParseForm(); err != nil {
			http.Error(w, "No se pudo leer el lote de etiquetas.", http.StatusBadRequest)
			return
		}
		productIDs := r.Form["id"]
		copiesByID, _, err := parseLabelBatchCopies(r, productIDs)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				http.Error(w, reqErr.Message, reqErr.Status)
				return
			}
			http.Error(w, "No se pudo preparar el lote.", http.StatusBadRequest)
			return
		}
		requestedProfileID := 0
		if rawProfileID := strings.TrimSpace(r.FormValue("profile_id")); rawProfileID != "" {
			requestedProfileID, err = strconv.Atoi(rawProfileID)
			if err != nil || requestedProfileID <= 0 {
				http.Error(w, "Perfil de etiqueta inválido.", http.StatusBadRequest)
				return
			}
		}
		settings := settingsForUser(currentUser)
		labelProfile, err := labelProfileForTenant(db, tenantIDFromUser(currentUser), settings.LabelPaperWidth, r.FormValue("size"), requestedProfileID)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				http.Error(w, reqErr.Message, reqErr.Status)
				return
			}
			http.Error(w, "No se pudo cargar el perfil de etiqueta.", http.StatusInternalServerError)
			return
		}
		profile := labelPrintProfileFromProfile(labelProfile)
		baseItems, _, _, err := productLabelItemsForUserWithDimensions(db, currentUser, productIDs, profile.LabelWidthMM, profile.LabelHeightMM)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				http.Error(w, reqErr.Message, reqErr.Status)
				return
			}
			http.Error(w, "No se pudieron generar las etiquetas.", http.StatusInternalServerError)
			return
		}
		items := expandProductLabelItems(baseItems, productIDs, copiesByID)
		if len(items) == 0 {
			http.Error(w, "No hay productos accesibles para imprimir.", http.StatusNotFound)
			return
		}
		if strings.EqualFold(strings.TrimSpace(r.FormValue("output")), "pdf") {
			pdf, err := productLabelsPDFWithSettingsAndProfile(items, profile, labelProfile, settings)
			if err != nil {
				http.Error(w, "No se pudo generar el PDF de etiquetas.", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/pdf")
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", "etiquetas-"+time.Now().Format("20060102-150405")+".pdf"))
			w.Header().Set("Content-Length", strconv.Itoa(len(pdf)))
			_, _ = w.Write(pdf)
			return
		}
		data := productLabelsPageDataForProfile(items, profile, labelProfile, currentUser, settings)
		data.AutoPrint = true
		var rendered bytes.Buffer
		if err := tmpl.ExecuteTemplate(&rendered, "product_labels.html", data); err != nil {
			http.Error(w, "Error al renderizar etiquetas", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
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
		customSKU, err := requestedVisibleProductID(strings.TrimSpace(r.FormValue("id")), strings.TrimSpace(r.FormValue("sku")))
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				http.Error(w, reqErr.Message, reqErr.Status)
				return
			}
			http.Error(w, "No se pudo validar el ID visible", http.StatusBadRequest)
			return
		}
		linea := strings.TrimSpace(r.FormValue("linea"))
		location := strings.TrimSpace(r.FormValue("location"))
		tallaRequerida := r.FormValue("talla_requerida") != ""
		tallaRaw := r.FormValue("talla")
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
		assignableUsers, err := loadAssignableUsersForTenant(db, tenantIDFromRequest(r))
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
			if err := ensureVisibleProductIDAvailable(db, tenantIDFromRequest(r), customSKU, ""); err != nil {
				if reqErr, ok := err.(requestError); ok {
					errors["sku"] = reqErr.Fields["id"]
				} else {
					http.Error(w, "No se pudo validar el ID", http.StatusInternalServerError)
					return
				}
			}
		}
		if linea == "" {
			if len(activeLines) == 0 {
				errors["linea"] = "Primero crea una línea de negocio en Configuración."
			} else {
				errors["linea"] = "Línea obligatoria."
			}
		}
		talla, tallaErr := normalizedProductSize(tallaRequerida, tallaRaw)
		if tallaErr != nil {
			errors["talla"] = tallaErr.(requestError).Message
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
				nextSKU, skuErr = generateNextTenantProductID(db, tenantIDFromRequest(r))
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
				TallaRequerida:    tallaRequerida,
				Talla:             strings.TrimSpace(tallaRaw),
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

		now := time.Now().Format(time.RFC3339)
		internalSKU, sku, err := insertProductWithGeneratedIdentity(tx, tenantIDFromRequest(r), customSKU, nombre, linea, now)
		if err != nil {
			if reqErr, ok := requestErrorDetails(err); ok {
				http.Error(w, reqErr.Message, reqErr.Status)
			} else {
				http.Error(w, "No se pudo guardar el producto", http.StatusInternalServerError)
			}
			return
		}
		if _, err := tx.Exec(`
				UPDATE productos
			SET precio_venta = ?, retoma_enabled = ?, retoma_price = ?, credit_enabled = ?, debtor_name = ?, installments_total = ?, installments_paid = ?, total_value = ?, installment_value = ?, location = ?, talla_requerida = ?, talla = ?
			WHERE tenant_id = ? AND sku = ?
		`, float64(precioVenta), boolToInt(retomaEnabled), retomaPrice, boolToInt(isCreditProduct), debtorName, installmentsTotal, 0, float64(totalValue), float64(installmentValue), location, boolToInt(tallaRequerida), talla, tenantIDFromRequest(r), internalSKU); err != nil {
			http.Error(w, "No se pudo guardar el precio del producto", http.StatusInternalServerError)
			return
		}
		if _, err := tx.Exec(`UPDATE productos SET owner_user_id = ? WHERE tenant_id = ? AND sku = ?`, ownerUserID, tenantIDFromRequest(r), internalSKU); err != nil {
			http.Error(w, "No se pudo guardar la asignación del producto", http.StatusInternalServerError)
			return
		}
		if err := logAuditEvent(tx, userFromContext(r), "product_created", "product", internalSKU, "manual", map[string]any{
			"sku":             internalSKU,
			"id":              sku,
			"name":            nombre,
			"line":            linea,
			"retoma_enabled":  retomaEnabled,
			"retoma_price":    retomaPrice,
			"owner_user_id":   ownerUserID,
			"location":        location,
			"talla_requerida": tallaRequerida,
			"talla":           talla,
			"cantidad":        cantidad,
		}); err != nil {
			http.Error(w, "No se pudo registrar la auditoría del producto", http.StatusInternalServerError)
			return
		}
		if isCreditProduct {
			if err := logAuditEvent(tx, userFromContext(r), "credit_created", "product", internalSKU, "manual", map[string]any{
				"product_id":         sku,
				"product_sku":        internalSKU,
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
			if err := logAuditEvent(tx, userFromContext(r), "product_assigned", "product", internalSKU, "manual", map[string]any{
				"sku":           internalSKU,
				"id":            sku,
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
			unitID := fmt.Sprintf("U-%s-%d", internalSKU, baseID+int64(j))
			var cad any = nil
			if aplicaCad && caducidad != "" {
				cad = caducidad
			}
			if _, err := tx.Exec(
				`INSERT INTO unidades (id, tenant_id, producto_id, estado, creado_en, caducidad) VALUES (?, ?, ?, ?, ?, ?)`,
				unitID, tenantID, internalSKU, "Disponible", now, cad,
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
			if products[idx].SKU == internalSKU {
				products[idx].Name = nombre
				products[idx].Line = linea
				products[idx].Location = location
				products[idx].TallaRequerida = tallaRequerida
				products[idx].Talla = talla
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
				SKU:               internalSKU,
				ID:                sku,
				Name:              nombre,
				Line:              linea,
				Location:          location,
				TallaRequerida:    tallaRequerida,
				Talla:             talla,
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

		target := "/productos/new?mensaje=" + url.QueryEscape("Producto agregado correctamente.") + "&label_url=" + url.QueryEscape(productLabelPrintURL([]string{sku}, ""))
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
					"talla_requerida":    product.TallaRequerida,
					"talla":              product.Talla,
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
			ID             string `json:"id"`
			SKU            string `json:"sku"`
			Name           string `json:"name"`
			Line           string `json:"line"`
			Location       string `json:"location"`
			TallaRequerida bool   `json:"talla_requerida"`
			Talla          string `json:"talla"`
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
		payload.Talla = strings.TrimSpace(payload.Talla)
		payload.FechaCaducidad = strings.TrimSpace(payload.FechaCaducidad)
		activeLines, err := loadBusinessLinesForTenant(db, tenantIDFromRequest(r), true)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar las líneas de negocio.", nil)
			return
		}
		assignableUsers, err := loadAssignableUsersForTenant(db, tenantIDFromUser(currentUser))
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
		talla, tallaErr := normalizedProductSize(payload.TallaRequerida, payload.Talla)
		if tallaErr != nil {
			fields["talla"] = tallaErr.(requestError).Message
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
		productID, err := requestedVisibleProductID(payload.ID, payload.SKU)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				writeAPIError(w, reqErr.Status, reqErr.Message, reqErr.Fields)
				return
			}
			writeAPIError(w, http.StatusBadRequest, "Datos inválidos.", map[string]string{"id": "No se pudo validar el ID visible."})
			return
		}
		if productID != "" {
			if err := ensureVisibleProductIDAvailable(db, tenantIDFromUser(currentUser), productID, ""); err != nil {
				var reqErr requestError
				if errors.As(err, &reqErr) {
					writeAPIError(w, reqErr.Status, reqErr.Message, reqErr.Fields)
				} else {
					writeAPIError(w, http.StatusInternalServerError, "No se pudo validar el ID.", nil)
				}
				return
			}
		}
		tx, err := db.Begin()
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo iniciar la transacción.", nil)
			return
		}
		defer tx.Rollback()
		now := time.Now().Format(time.RFC3339)
		sku, productID, err := insertProductWithGeneratedIdentity(tx, tenantIDFromUser(currentUser), productID, payload.Name, payload.Line, now)
		if err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				writeAPIError(w, reqErr.Status, reqErr.Message, reqErr.Fields)
			} else {
				writeAPIError(w, http.StatusInternalServerError, "No se pudo guardar el producto.", nil)
			}
			return
		}
		if _, err := tx.Exec(`UPDATE productos SET precio_venta = ?, retoma_enabled = ?, retoma_price = ?, owner_user_id = ?, location = ?, talla_requerida = ?, talla = ? WHERE tenant_id = ? AND sku = ?`, float64(payload.SalePrice), boolToInt(payload.RetomaEnabled), retomaPrice, ownerUserID, payload.Location, boolToInt(payload.TallaRequerida), talla, tenantIDFromUser(currentUser), sku); err != nil {
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
			"sku":             sku,
			"id":              productID,
			"name":            payload.Name,
			"line":            payload.Line,
			"sale_price":      payload.SalePrice,
			"retoma_enabled":  payload.RetomaEnabled,
			"retoma_price":    retomaPrice,
			"owner_user_id":   ownerUserID,
			"location":        payload.Location,
			"talla_requerida": payload.TallaRequerida,
			"talla":           talla,
			"cantidad":        payload.Quantity,
		})); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo registrar la auditoría.", nil)
			return
		}
		if ownerUserID.Valid {
			if err := logAuditEvent(tx, currentUser, "product_assigned", "product", sku, "api", withAPIAuditMetadata(r, map[string]any{
				"sku":           sku,
				"id":            productID,
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
		writeAPIJSON(w, http.StatusCreated, map[string]any{"ok": true, "id": productID, "sku": sku, "location": payload.Location, "talla_requerida": payload.TallaRequerida, "talla": talla, "message": "Producto creado correctamente."})
	})
	mux.HandleFunc("/api/products/", handleAPIProductRoutes(db))

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
				COALESCE(NULLIF(p.id, ''), v.producto_id),
				COALESCE(p.nombre, ''),
				v.cantidad,
				v.precio_final,
				v.metodo_pago,
				v.notas
			FROM ventas v
			LEFT JOIN productos p ON p.sku = v.producto_id AND p.tenant_id = v.tenant_id
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

		_ = cw.Write([]string{"venta_id", "fecha", "product_id", "producto", "cantidad", "precio_unitario", "total", "metodo_pago", "notas"})

		for rows.Next() {
			var (
				id         int
				fechaRaw   string
				productID  string
				nombre     string
				cantidad   int
				precioUnit float64
				metodo     string
				notas      string
			)
			if err := rows.Scan(&id, &fechaRaw, &productID, &nombre, &cantidad, &precioUnit, &metodo, &notas); err != nil {
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
				productID,
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
		tenantID := tenantIDFromUser(currentUser)
		flash := r.URL.Query().Get("mensaje")
		receiptSaleID, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("receipt_sale_id")))
		activePaymentMethods, err := loadPaymentMethodsForTenant(db, tenantID, true)
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
			assignableUsers, err = loadAssignableUsersForTenant(db, tenantID)
			if err != nil {
				http.Error(w, "Error al cargar usuarios asignables", http.StatusInternalServerError)
				return
			}
		}

		inventoryProducts := make([]inventoryProduct, 0, len(productsSnapshot))
		allowedProducts := make(map[string]productOption, len(productsSnapshot))
		productRefs := make([]string, 0, len(productsSnapshot))
		for _, product := range productsSnapshot {
			allowedProducts[product.refID()] = product
			allowedProducts[product.ID] = product
			productRefs = append(productRefs, product.refID())
		}
		unitsByProduct, unitStatsByProduct, err := loadInventoryUnitsByProductIDs(db, tenantID, productRefs)
		if err != nil {
			http.Error(w, "Error al consultar unidades", http.StatusInternalServerError)
			return
		}

		for _, product := range productsSnapshot {
			units := unitsByProduct[product.refID()]
			stats := unitStatsByProduct[product.refID()]
			availableCount := stats.AvailableCount
			loanedCount := stats.LoanedCount
			changeCount := stats.ChangeCount
			reservedCount := stats.ReservedCount
			damagedCount := stats.DamagedCount
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
			if fechaIngresoRaw == "" && stats.FirstCreatedAt != "" {
				fechaIngresoRaw = stats.FirstCreatedAt
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
				TallaRequerida:    product.TallaRequerida,
				Talla:             product.Talla,
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
				CreditKind:            string(creditSaleKindProduct),
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
		cashLoanRows, err := db.Query(`
			SELECT
				cs.id,
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
				COALESCE(u.username, ''),
				cs.created_at
			FROM credit_sales cs
			LEFT JOIN customers c ON c.id = cs.customer_id AND c.tenant_id = cs.tenant_id
			LEFT JOIN users u ON u.id = cs.created_by AND u.tenant_id = cs.tenant_id
			WHERE cs.tenant_id = ? AND COALESCE(cs.kind, ?) = ?
			ORDER BY created_at DESC, id DESC
		`, tenantIDFromUser(currentUser), string(creditSaleKindCash), string(creditSaleKindCash))
		if err != nil {
			http.Error(w, "Error al consultar préstamos de dinero", http.StatusInternalServerError)
			return
		}
		defer cashLoanRows.Close()
		for cashLoanRows.Next() {
			var (
				creditID              int
				customerID            int
				debtorName            string
				debtorDocumentType    string
				debtorDocumentNumber  string
				debtorPhone           string
				customerAddress       string
				customerCity          string
				customerNotes         string
				installmentsTotal     int
				installmentsPaid      int
				paidInstallmentsCount int
				totalValue            float64
				interestPercent       float64
				installmentValue      float64
				totalPaid             float64
				lastPaymentAmount     float64
				lastPaymentAt         string
				lastPaymentType       string
				notes                 string
				statusRaw             string
				managedByName         string
				createdAt             string
			)
			if err := cashLoanRows.Scan(&creditID, &customerID, &debtorName, &debtorDocumentType, &debtorDocumentNumber, &debtorPhone, &customerAddress, &customerCity, &customerNotes, &installmentsTotal, &installmentsPaid, &paidInstallmentsCount, &totalValue, &interestPercent, &installmentValue, &totalPaid, &lastPaymentAmount, &lastPaymentAt, &lastPaymentType, &notes, &statusRaw, &managedByName, &createdAt); err != nil {
				http.Error(w, "Error al leer préstamos de dinero", http.StatusInternalServerError)
				return
			}
			if paidInstallmentsCount < installmentsPaid {
				paidInstallmentsCount = installmentsPaid
			}
			legacyTotalPaid := math.Round((float64(installmentsPaid)*installmentValue)*100) / 100
			if totalPaid < legacyTotalPaid {
				totalPaid = legacyTotalPaid
			}
			debtTotal := creditDebtTotal(installmentsTotal, installmentValue)
			currentDebt := creditCurrentDebt(debtTotal, totalPaid)
			creditStatusValue := effectiveCreditStatus(statusRaw, currentDebt, debtTotal)
			inventoryProducts = append(inventoryProducts, inventoryProduct{
				EntryType:             "credit",
				CreditSaleID:          creditID,
				CustomerID:            customerID,
				CreditKind:            string(creditSaleKindCash),
				BaseProductID:         "",
				ID:                    fmt.Sprintf("PD-%d", creditID),
				Name:                  "Préstamo de dinero",
				Line:                  "Préstamo de dinero",
				CreditEnabled:         true,
				InterestPercent:       interestPercent,
				DebtorName:            debtorName,
				DebtorDocumentType:    debtorDocumentType,
				DebtorDocumentNumber:  debtorDocumentNumber,
				DebtorPhone:           debtorPhone,
				CustomerAddress:       customerAddress,
				CustomerCity:          customerCity,
				CustomerNotes:         customerNotes,
				ManagedByName:         managedByName,
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
				EstadoLabel:           cashLoanStatusLabel(creditStatusValue),
				EstadoClass:           creditStatusClass(creditStatusValue),
				Disponible:            0,
				Unidades:              []inventoryUnit{},
				DisabledSale:          true,
				FechaIngreso:          formatDateWithSettings(createdAt),
				SalePrice:             0,
				RetomaEnabled:         false,
			})
		}
		if err := cashLoanRows.Err(); err != nil {
			http.Error(w, "Error al procesar préstamos de dinero", http.StatusInternalServerError)
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
		loanIndexByID := make(map[int]int)
		loanIDs := make([]int, 0, 32)
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
			loanName := product.Name
			if quantity > 1 {
				loanName = fmt.Sprintf("%s x%d", product.Name, quantity)
			}
			loanStatus := normalizeProductLoanStatus(statusRaw)
			loanIndexByID[productLoanID] = len(inventoryProducts)
			loanIDs = append(loanIDs, productLoanID)
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
				Unidades:             []inventoryUnit{},
				DisabledSale:         true,
				FechaIngreso:         formatDateWithSettings(loanedAt),
				SalePrice:            product.SalePrice,
			})
		}
		if err := loanRows.Err(); err != nil {
			http.Error(w, "Error al procesar préstamos de producto", http.StatusInternalServerError)
			return
		}
		loanUnitMap, err := loadProductLoanUnitIDs(db, tenantID, loanIDs)
		if err != nil {
			http.Error(w, "Error al consultar unidades del préstamo", http.StatusInternalServerError)
			return
		}
		for productLoanID, index := range loanIndexByID {
			item := &inventoryProducts[index]
			unitIDs := loanUnitMap[productLoanID]
			loanUnits := make([]inventoryUnit, 0, len(unitIDs))
			for _, unitID := range unitIDs {
				loanUnits = append(loanUnits, inventoryUnit{
					ID:          unitID,
					Estado:      "Prestada",
					EstadoClass: "loaned",
					CreadoEn:    item.FechaIngreso,
					Caducidad:   "",
					FIFO:        "-",
				})
			}
			item.Unidades = loanUnits
		}
		_, movementEnabledMap, err := loadMovementSettingsForTenant(db, tenantID)
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
		newSKU, err := requestedVisibleProductID(strings.TrimSpace(r.FormValue("id")), strings.TrimSpace(r.FormValue("sku")))
		if err != nil {
			writeJSONError(http.StatusBadRequest, "No envíes id y sku con valores distintos.")
			return
		}
		newName := strings.TrimSpace(r.FormValue("nombre"))
		newLine := strings.TrimSpace(r.FormValue("linea"))
		locationValue := strings.TrimSpace(r.FormValue("location"))
		tallaRequeridaValue := r.FormValue("talla_requerida") != ""
		tallaValue := r.FormValue("talla")
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

		previous, err := loadProductEditRecord(db, tenantIDFromUser(currentUser), productID)
		if err != nil {
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
		finalTallaRequerida := previous.TallaRequerida == 1
		finalTalla := previous.Talla
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
			parsedTalla, tallaErr := normalizedProductSize(tallaRequeridaValue, tallaValue)
			if tallaErr != nil {
				writeJSONError(http.StatusBadRequest, tallaErr.(requestError).Message)
				return
			}
			finalTallaRequerida = tallaRequeridaValue
			finalTalla = parsedTalla
			finalOwner = sql.NullInt64{}
			if ownerUserIDRaw != "" {
				assignableUsers, err := loadAssignableUsersForTenant(db, tenantIDFromUser(currentUser))
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

		if err := renameProductIdentifier(tx, tenantIDFromUser(currentUser), previous.SKU, newSKU); err != nil {
			var reqErr requestError
			if errors.As(err, &reqErr) {
				writeJSONError(reqErr.Status, reqErr.Message)
				return
			}
			writeJSONError(http.StatusInternalServerError, "No se pudo actualizar el ID del producto.")
			return
		}

		if isAdminRole(currentUser.Role) {
			if _, err := tx.Exec(`
				UPDATE productos
				SET nombre = ?, linea = ?, location = ?, talla_requerida = ?, talla = ?, owner_user_id = ?, precio_venta = ?, retoma_enabled = ?, retoma_price = ?, anotaciones = ?, credit_enabled = ?, debtor_name = ?, installments_total = ?, installments_paid = ?, total_value = ?, installment_value = ?
				WHERE tenant_id = ? AND sku = ?
			`, finalName, finalLine, finalLocation, boolToInt(finalTallaRequerida), finalTalla, finalOwner, float64(parsedPrice), boolToInt(retomaEnabled), newRetomaPrice, notesValue, boolToInt(finalCreditEnabled), finalDebtorName, finalInstallmentsTotal, finalInstallmentsPaid, finalTotalValue, finalInstallmentValue, tenantIDFromUser(currentUser), previous.SKU); err != nil {
				writeJSONError(http.StatusInternalServerError, "No se pudo actualizar el producto.")
				return
			}
		} else {
			if _, err := tx.Exec(`
				UPDATE productos
				SET precio_venta = ?, retoma_enabled = ?, retoma_price = ?, anotaciones = ?, location = ?
				WHERE tenant_id = ? AND sku = ?
			`, float64(parsedPrice), boolToInt(retomaEnabled), newRetomaPrice, notesValue, finalLocation, tenantIDFromUser(currentUser), previous.SKU); err != nil {
				writeJSONError(http.StatusInternalServerError, "No se pudo actualizar el producto.")
				return
			}
		}

		payload := map[string]any{}
		if previous.ID != newSKU {
			payload["product_sku"] = previous.SKU
			payload["previous_id"] = previous.ID
			payload["new_id"] = newSKU
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
			if (previous.TallaRequerida == 1) != finalTallaRequerida {
				payload["previous_talla_requerida"] = previous.TallaRequerida == 1
				payload["new_talla_requerida"] = finalTallaRequerida
			}
			if previous.Talla != finalTalla {
				payload["previous_talla"] = previous.Talla
				payload["new_talla"] = finalTalla
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
			if err := logAuditEvent(tx, currentUser, "product_updated", "product", previous.SKU, "web", payload); err != nil {
				writeJSONError(http.StatusInternalServerError, "No se pudo registrar la auditoría del producto.")
				return
			}
		}
		if isAdminRole(currentUser.Role) && ((previous.CreditEnabled == 1) || finalCreditEnabled) {
			creditPayload := map[string]any{
				"product_id":         newSKU,
				"product_sku":        previous.SKU,
				"debtor_name":        finalDebtorName,
				"installments_total": finalInstallmentsTotal,
				"installments_paid":  finalInstallmentsPaid,
				"total_value":        finalTotalValue,
				"installment_value":  finalInstallmentValue,
			}
			if err := logAuditEvent(tx, currentUser, "credit_updated", "product", previous.SKU, "web", creditPayload); err != nil {
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
			if products[idx].SKU == previous.SKU {
				products[idx].ID = newSKU
				if isAdminRole(currentUser.Role) {
					products[idx].Name = finalName
					products[idx].Line = finalLine
					products[idx].Location = finalLocation
					products[idx].TallaRequerida = finalTallaRequerida
					products[idx].Talla = finalTalla
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
			"sku":      previous.SKU,
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
		productSKU, visibleID, err := resolveProductRefForTenant(db, tenantIDFromRequest(r), productID)
		if err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo resolver el producto.")
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
		if err := logMovimientos(tx, productSKU, unitIDs, "reservar", nota, userFromContext(r), now); err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo registrar el movimiento.")
			return
		}

		if err := tx.Commit(); err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo confirmar la transacción.")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "producto_id": visibleID, "sku": productSKU, "cantidad": qty})
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
		productSKU, visibleID, err := resolveProductRefForTenant(db, tenantIDFromRequest(r), productID)
		if err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo resolver el producto.")
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
		if err := logMovimientos(tx, productSKU, unitIDs, "dano", nota, userFromContext(r), now); err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo registrar el movimiento.")
			return
		}

		if err := tx.Commit(); err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo confirmar la transacción.")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "producto_id": visibleID, "sku": productSKU, "cantidad": qty})
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
			Customer: customerInput{
				CustomerID:     parseIntOrZero(r.FormValue("customer_id")),
				Name:           strings.TrimSpace(r.FormValue("customer_name")),
				Phone:          strings.TrimSpace(r.FormValue("customer_phone")),
				DocumentType:   strings.TrimSpace(r.FormValue("customer_document_type")),
				DocumentNumber: strings.TrimSpace(r.FormValue("customer_document_number")),
				Address:        strings.TrimSpace(r.FormValue("customer_address")),
				City:           strings.TrimSpace(r.FormValue("customer_city")),
				Notes:          strings.TrimSpace(r.FormValue("customer_notes")),
			},
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
			"customer_id":      result.CustomerID,
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

		productSKU, visibleID, err := resolveProductRefForTenant(db, tenantIDFromUser(currentUser), productID)
		if err != nil {
			if err != sql.ErrNoRows {
				writeJSONError(http.StatusInternalServerError, "No se pudo validar el producto.")
				return
			}
			writeJSONError(http.StatusBadRequest, "Producto inválido.")
			return
		}

		if _, err := tx.Exec(`DELETE FROM unidades WHERE tenant_id = ? AND producto_id = ?`, tenantIDFromUser(currentUser), productSKU); err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudieron eliminar las unidades del producto.")
			return
		}
		res, err := tx.Exec(`DELETE FROM productos WHERE tenant_id = ? AND sku = ?`, tenantIDFromUser(currentUser), productSKU)
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
			"producto_id": visibleID,
			"sku":         productSKU,
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
		productSKU, visibleID, err := resolveProductRefForTenant(db, tenantIDFromRequest(r), productID)
		if err != nil {
			if err == sql.ErrNoRows {
				http.Error(w, "Producto no encontrado", http.StatusNotFound)
				return
			}
			http.Error(w, "No se pudo resolver el producto", http.StatusInternalServerError)
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
			WHERE tenant_id = ? AND producto_id = ?
			ORDER BY fecha DESC
			LIMIT 60
		`, tenantIDFromRequest(r), productSKU)
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
			movs = append(movs, m)
		}
		if err := rows.Err(); err != nil {
			http.Error(w, "Error al procesar historial", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "producto_id": visibleID, "sku": productSKU, "movimientos": movs})
	})

	mux.HandleFunc("/api/productos/precio", func(w http.ResponseWriter, r *http.Request) {
		productID := strings.TrimSpace(r.URL.Query().Get("id"))
		productSKU := strings.TrimSpace(r.URL.Query().Get("sku"))
		if productID == "" && productSKU == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "Falta id."})
			return
		}
		currentUser := userFromContext(r)
		tenantID := tenantIDFromRequest(r)

		resolvedID := ""
		resolvedSKU := ""
		switch {
		case productID != "":
			var err error
			resolvedSKU, resolvedID, err = resolveProductRefForTenant(db, tenantID, productID)
			if err != nil {
				if err == sql.ErrNoRows {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusNotFound)
					_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "Producto no encontrado."})
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "No se pudo resolver el producto."})
				return
			}
			allowed, accessErr := productAccessibleByID(db, currentUser, resolvedID)
			if accessErr != nil {
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
		default:
			var err error
			resolvedSKU = productSKU
			resolvedID, err = resolveVisibleProductIDBySKUForTenant(db, tenantID, resolvedSKU)
			if err != nil {
				if err == sql.ErrNoRows {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusNotFound)
					_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "Producto no encontrado."})
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "No se pudo resolver el producto."})
				return
			}
			allowed, accessErr := productAccessibleBySKU(db, currentUser, resolvedSKU)
			if accessErr != nil {
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
		}

		var precioVenta float64
		err := db.QueryRow(`SELECT COALESCE(precio_venta, 0) FROM productos WHERE tenant_id = ? AND sku = ?`, tenantID, resolvedSKU).Scan(&precioVenta)
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
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "id": resolvedID, "sku": resolvedSKU, "precio_venta": precioVenta})
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
				"contact_phone": settings.ContactPhone,
				"contact_email": settings.ContactEmail,
				"social_media":  settings.SocialMedia,
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
		if currentUser == nil || !(isAdminRole(currentUser.Role) || isAPIKeyRole(currentUser.Role)) {
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
				"contact_phone": settings.ContactPhone,
				"contact_email": settings.ContactEmail,
				"social_media":  settings.SocialMedia,
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
	mux.HandleFunc("/api/agent/product-loans", handleAPIAgentProductLoans(db))
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
			rows, err := db.Query(`SELECT estado, COUNT(*) FROM unidades WHERE tenant_id = ? AND producto_id = ? GROUP BY estado`, tenantIDFromRequest(r), product.refID())
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

	mux.HandleFunc("/api/inventory/adjust", handleAPIInventoryAdjust(db, func(productID string, salePrice *float64, name *string, retomaEnabled *bool, retomaPrice *float64) {
		productsMu.Lock()
		defer productsMu.Unlock()
		for idx := range products {
			if products[idx].ID != productID {
				continue
			}
			if salePrice != nil {
				products[idx].SalePrice = *salePrice
			}
			if name != nil {
				products[idx].Name = strings.TrimSpace(*name)
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
	}))

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
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}
		if r.Method == http.MethodPost {
			if err := r.ParseForm(); err != nil {
				http.Error(w, "No se pudo leer el formulario del comprobante.", http.StatusBadRequest)
				return
			}
		}

		saleIDRaw := strings.TrimSpace(r.URL.Query().Get("sale_id"))
		if r.Method == http.MethodPost {
			saleIDRaw = strings.TrimSpace(r.FormValue("sale_id"))
		}
		saleID, err := strconv.Atoi(saleIDRaw)
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
		hasStoredBuyerSnapshot := strings.TrimSpace(data.BuyerName) != "" && strings.TrimSpace(data.BuyerDocument) != ""

		buyerName := strings.TrimSpace(r.URL.Query().Get("buyer_name"))
		buyerDocument := strings.TrimSpace(r.URL.Query().Get("buyer_document"))
		if r.Method == http.MethodPost {
			buyerName = strings.TrimSpace(r.FormValue("buyer_name"))
			buyerDocument = strings.TrimSpace(r.FormValue("buyer_document"))
		}
		if buyerName == "" {
			buyerName = strings.TrimSpace(data.BuyerName)
		}
		if buyerDocument == "" {
			buyerDocument = strings.TrimSpace(data.BuyerDocument)
		}
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
		if hasStoredBuyerSnapshot || r.Method == http.MethodPost {
			data.DownloadURL = saleReceiptDownloadURL(saleID)
			data.ThermalURL = saleThermalTicketViewURL(saleID)
		} else {
			data.DownloadURL = saleReceiptDownloadURLWithBuyer(saleID, buyerName, buyerDocument)
			data.ThermalURL = saleThermalTicketViewURLWithBuyer(saleID, buyerName, buyerDocument)
		}
		download := r.URL.Query().Get("download") == "1"
		if r.Method == http.MethodPost {
			download = strings.TrimSpace(r.FormValue("download")) == "1"
			if err := saveSaleReceiptSnapshot(db, currentUser, saleID, buyerName, buyerDocument, "standard"); err != nil {
				var reqErr requestError
				if errors.As(err, &reqErr) {
					http.Error(w, reqErr.Message, reqErr.Status)
					return
				}
				http.Error(w, "No se pudo guardar el comprobante.", http.StatusInternalServerError)
				return
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
		}
		if download {
			filename := fmt.Sprintf("comprobante-venta-%d.html", saleID)
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
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
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}
		if r.Method == http.MethodPost {
			if err := r.ParseForm(); err != nil {
				http.Error(w, "No se pudo leer el formulario del ticket térmico.", http.StatusBadRequest)
				return
			}
		}

		saleIDRaw := strings.TrimSpace(r.URL.Query().Get("sale_id"))
		if r.Method == http.MethodPost {
			saleIDRaw = strings.TrimSpace(r.FormValue("sale_id"))
		}
		saleID, err := strconv.Atoi(saleIDRaw)
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
		hasStoredBuyerSnapshot := strings.TrimSpace(data.BuyerName) != "" && strings.TrimSpace(data.BuyerDocument) != ""
		paperValue := strings.TrimSpace(r.URL.Query().Get("paper"))
		if r.Method == http.MethodPost {
			paperValue = strings.TrimSpace(r.FormValue("paper"))
		}
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
		if r.Method == http.MethodPost {
			buyerName = strings.TrimSpace(r.FormValue("buyer_name"))
			buyerDocument = strings.TrimSpace(r.FormValue("buyer_document"))
		}
		if buyerName == "" {
			buyerName = strings.TrimSpace(data.BuyerName)
		}
		if buyerDocument == "" {
			buyerDocument = strings.TrimSpace(data.BuyerDocument)
		}
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
		if hasStoredBuyerSnapshot || r.Method == http.MethodPost {
			data.DownloadURL = saleReceiptViewURL(saleID)
			data.ThermalURL = saleThermalTicketViewURLWithPaper(saleID, paperKey)
		} else {
			data.DownloadURL = saleReceiptViewURLWithBuyer(saleID, buyerName, buyerDocument)
			data.ThermalURL = saleThermalTicketViewURLWithBuyer(saleID, buyerName, buyerDocument)
		}
		if r.Method == http.MethodPost {
			if err := saveSaleReceiptSnapshot(db, currentUser, saleID, buyerName, buyerDocument, "thermal"); err != nil {
				var reqErr requestError
				if errors.As(err, &reqErr) {
					http.Error(w, reqErr.Message, reqErr.Status)
					return
				}
				http.Error(w, "No se pudo guardar el ticket térmico.", http.StatusInternalServerError)
				return
			}
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
		productSKU := selectedProduct.refID()
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
		if err := logMovimientos(tx, productSKU, soldUnitIDs, movementType, notaMovimiento, currentUser, now); err != nil {
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
				tenantIDFromUser(currentUser), customerInput.CustomerID, string(creditSaleKindProduct), productSKU, cantidad, debtorName, debtorDocumentType, debtorDocumentNumber, debtorPhone, creditInstallmentsTotal, float64(creditTotalValue), creditInterestPercent, creditInstallmentValue, notaMovimiento, string(creditStatusActive), now, nullableUserID(currentUser),
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
					"product_sku":        productSKU,
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
				"product_sku":            productSKU,
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
			`INSERT INTO ventas (tenant_id, producto_id, cantidad, precio_final, metodo_pago, sold_by, notas, fecha)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			tenantIDFromUser(currentUser), productSKU, cantidad, func() float64 {
				precioFinal := precioParsed
				if valorFinalOk && cantidad > 0 {
					precioFinal = valorFinalParsed / float64(cantidad)
				}
				return precioFinal
			}(), metodoPago, strings.TrimSpace(currentUser.Username), notas, now,
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
			soldBy := strings.TrimSpace(currentUser.Username)
			precioFinal := precioParsed
			if valorFinalOk && cantidad > 0 {
				precioFinal = valorFinalParsed / float64(cantidad)
			}
			if err := logAuditEvent(tx, currentUser, "sale_registered", "sale", productID, "manual", map[string]any{
				"producto_id": productID,
				"product_sku": productSKU,
				"producto":    selectedProduct.Name,
				"cantidad":    cantidad,
				"metodo_pago": metodoPago,
				"total":       precioFinal * float64(cantidad),
				"sold_by":     soldBy,
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
		productSKU := selectedProduct.refID()

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
				errors["incoming_new_sku"] = "Ingresa el ID visible del producto nuevo."
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
		if err := logMovimientos(tx, productSKU, salientesMarcadas, "cambio_salida", notaMovimiento, currentUser, now); err != nil {
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
		incomingProductSKU := ""
		incomingQty := incomingExistingQty
		if incomingMode == "new" {
			incomingProductID = incomingNewSKU
			incomingQty = incomingNewQty
			incomingProductSKU, incomingProductID, err = insertProductWithGeneratedIdentity(tx, tenantIDFromUser(currentUser), incomingProductID, incomingNewName, incomingNewLine, now)
			if err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					log.Printf("rollback cambio crear producto entrante: %v", rollbackErr)
				}
				if reqErr, ok := requestErrorDetails(err); ok {
					http.Error(w, reqErr.Message, reqErr.Status)
				} else {
					http.Error(w, "No se pudo crear el producto entrante", http.StatusInternalServerError)
				}
				return
			}
		} else {
			incomingProduct, ok := findProduct(productsSnapshot, incomingExistingID)
			if !ok {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					log.Printf("rollback cambio producto entrante: %v", rollbackErr)
				}
				http.Error(w, "No se pudo resolver el producto entrante", http.StatusBadRequest)
				return
			}
			incomingProductID = incomingProduct.ID
			incomingProductSKU = incomingProduct.refID()
		}

		for i := 0; i < incomingQty; i++ {
			unitID := fmt.Sprintf("U-%d-%d", time.Now().UnixNano(), i+1)
			if _, err := tx.Exec(
				`INSERT INTO unidades (id, tenant_id, producto_id, estado, creado_en, caducidad)
				VALUES (?, ?, ?, ?, ?, ?)`,
				unitID, normalizeTenantID(tenantIDFromUser(currentUser)), incomingProductSKU, "Disponible", now, nil,
			); err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					log.Printf("rollback cambio insert: %v", rollbackErr)
				}
				http.Error(w, "Error al registrar unidades entrantes", http.StatusInternalServerError)
				return
			}
		}
		if err := logAuditEvent(tx, currentUser, "change_registered", "change", productID, "manual", map[string]any{
			"producto_saliente_id":  productID,
			"producto_saliente_sku": productSKU,
			"producto_saliente":     selectedProduct.Name,
			"producto_entrante_id":  incomingProductID,
			"producto_entrante_sku": incomingProductSKU,
			"cantidad_saliente":     outgoingQty,
			"cantidad_entrante":     incomingQty,
			"modo_entrada":          incomingMode,
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

		index, err := productCSVColumnIndex(records[0])
		if err != nil {
			if reqErr, ok := requestErrorDetails(err); ok {
				writeJSONError(reqErr.Status, reqErr.Message)
			} else {
				writeJSONError(http.StatusBadRequest, "CSV inválido.")
			}
			return
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
		lineNames := make([]string, 0, len(records)-1)
		for _, row := range records[1:] {
			linea := get(row, "linea")
			if strings.TrimSpace(linea) == "" {
				continue
			}
			lineNames = append(lineNames, linea)
		}
		if err := ensureBusinessLinesForTenant(tx, tenantID, userFromContext(r), lineNames, now, "csv_import"); err != nil {
			_ = tx.Rollback()
			writeJSONError(http.StatusInternalServerError, "No se pudieron registrar las líneas del CSV.")
			return
		}
		for i, row := range records[1:] {
			rowIndex := i + 1 // matches the UI preview index (1-based excluding header)
			productID := get(row, "id")
			linea := get(row, "linea")
			nombre := get(row, "nombre")
			anotaciones := get(row, "anotaciones")
			tallaRequeridaRaw := get(row, "talla_requerida")
			tallaRaw := get(row, "talla")
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
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: "ID, línea y nombre son obligatorios."})
				continue
			}

			cantidad, err := parseCSVInt(cantidadRaw)
			if err != nil || cantidad < 0 {
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: "Cantidad inválida (debe ser 0 o mayor)."})
				continue
			}

			precioVenta, err := parseCSVFloat(get(row, "precio_venta"))
			if err != nil {
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: "Precio venta inválido."})
				continue
			}

			tallaRequerida := false
			if tallaRequeridaRaw != "" {
				parsed, err := parseCSVBool(tallaRequeridaRaw)
				if err != nil {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: "talla_requerida debe ser true/false."})
					continue
				}
				tallaRequerida = parsed
			}
			talla, tallaErr := normalizedProductSize(tallaRequerida, tallaRaw)
			if tallaErr != nil {
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: tallaErr.(requestError).Message})
				continue
			}

			var ownerUserID sql.NullInt64
			if ownerUserIDRaw != "" {
				if _, ok := validOwners[ownerUserIDRaw]; !ok {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: "owner_user_id no corresponde a un usuario activo del tenant."})
					continue
				}
				parsedOwnerID, err := parseCSVInt(ownerUserIDRaw)
				if err != nil || parsedOwnerID <= 0 {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: "owner_user_id inválido."})
					continue
				}
				ownerUserID = sql.NullInt64{Int64: int64(parsedOwnerID), Valid: true}
			}

			retomaEnabled := false
			retomaEnabledRaw := get(row, "retoma_enabled")
			if retomaEnabledRaw != "" {
				parsed, err := parseCSVBool(retomaEnabledRaw)
				if err != nil {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: "retoma_enabled debe ser true/false."})
					continue
				}
				retomaEnabled = parsed
			}
			var retomaPrice sql.NullFloat64
			retomaPriceRaw := get(row, "retoma_price")
			if retomaEnabled {
				if retomaPriceRaw == "" {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: "retoma_price es obligatorio si retoma_enabled es true."})
					continue
				}
				parsed, err := parseCSVFloat(retomaPriceRaw)
				if err != nil || parsed < 0 {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: "retoma_price inválido."})
					continue
				}
				if parsed > precioVenta {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: "retoma_price no debe superar precio_venta."})
					continue
				}
				retomaPrice = sql.NullFloat64{Float64: parsed, Valid: true}
			} else if retomaPriceRaw != "" {
				if _, err := parseCSVFloat(retomaPriceRaw); err != nil {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: "retoma_price inválido."})
					continue
				}
			}

			creditEnabled := false
			creditEnabledRaw := get(row, "credit_enabled")
			if creditEnabledRaw != "" {
				parsed, err := parseCSVBool(creditEnabledRaw)
				if err != nil {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: "credit_enabled debe ser true/false."})
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
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: "debtor_name es obligatorio si credit_enabled es true."})
					continue
				}
				installmentsTotalRaw := get(row, "installments_total")
				parsedInstallments, err := parseCSVInt(installmentsTotalRaw)
				if err != nil || parsedInstallments <= 0 {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: "installments_total debe ser mayor a 0."})
					continue
				}
				installmentsTotal = parsedInstallments

				totalValueRaw := get(row, "total_value")
				parsedTotalValue, err := parseCSVFloat(totalValueRaw)
				if err != nil || parsedTotalValue <= 0 {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: "total_value debe ser mayor a 0."})
					continue
				}
				totalValue = parsedTotalValue

				installmentValueRaw := get(row, "installment_value")
				parsedInstallmentValue, err := parseCSVFloat(installmentValueRaw)
				if err != nil || parsedInstallmentValue <= 0 {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: "installment_value debe ser mayor a 0."})
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
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: "aplica_caducidad debe ser true/false."})
					continue
				}
				aplicaCad = parsed
			}
			if aplicaCad && fechaCaducidad == "" {
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: "fecha_caducidad requerida si aplica."})
				continue
			}
			if fechaCaducidad != "" {
				if _, err := time.Parse("2006-01-02", fechaCaducidad); err != nil {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: "fecha_caducidad debe ser YYYY-MM-DD."})
					continue
				}
			}

			if _, err := tx.Exec("SAVEPOINT csv_row"); err != nil {
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: "Error al preparar la fila."})
				continue
			}

			internalSKU, _, err := resolveProductRefForTenant(db, tenantID, productID)
			if err != nil {
				if err != sql.ErrNoRows {
					_, _ = tx.Exec("ROLLBACK TO csv_row")
					_, _ = tx.Exec("RELEASE csv_row")
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: fmt.Sprintf("Error al resolver el producto: %v", err)})
					continue
				}
				internalSKU, productID, err = insertProductWithGeneratedIdentity(tx, tenantID, productID, nombre, linea, now)
				if err != nil {
					_, _ = tx.Exec("ROLLBACK TO csv_row")
					_, _ = tx.Exec("RELEASE csv_row")
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: fmt.Sprintf("Error al crear el producto: %v", err)})
					continue
				}
			}

			// Persist catalog.
			if err := upsertProducto(tx, tenantID, internalSKU, productID, nombre, linea, now); err != nil {
				_, _ = tx.Exec("ROLLBACK TO csv_row")
				_, _ = tx.Exec("RELEASE csv_row")
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: fmt.Sprintf("Error al guardar producto: %v", err)})
				continue
			}
			if _, err := tx.Exec(`
				UPDATE productos
				SET precio_venta = ?, anotaciones = ?, location = ?, talla_requerida = ?, talla = ?, owner_user_id = ?, retoma_enabled = ?, retoma_price = ?, credit_enabled = ?, debtor_name = ?, installments_total = ?, installments_paid = 0, total_value = ?, installment_value = ?
				WHERE tenant_id = ? AND sku = ?
			`, precioVenta, anotaciones, location, boolToInt(tallaRequerida), talla, ownerUserID, boolToInt(retomaEnabled), retomaPrice, boolToInt(creditEnabled), debtorName, installmentsTotal, totalValue, installmentValue, tenantID, internalSKU); err != nil {
				_, _ = tx.Exec("ROLLBACK TO csv_row")
				_, _ = tx.Exec("RELEASE csv_row")
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: fmt.Sprintf("Error al guardar los datos del producto: %v", err)})
				continue
			}

			// Update in-memory catalog (used by inventario/cambio screens).
			productsMu.Lock()
			found := false
			for idx := range products {
				if products[idx].SKU == internalSKU {
					products[idx].Name = nombre
					products[idx].Line = linea
					products[idx].Location = location
					products[idx].TallaRequerida = tallaRequerida
					products[idx].Talla = talla
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
					SKU:               internalSKU,
					ID:                productID,
					Name:              nombre,
					Line:              linea,
					Location:          location,
					TallaRequerida:    tallaRequerida,
					Talla:             talla,
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
				unitID := fmt.Sprintf("U-%s-%d", internalSKU, baseID+int64(j))
				var caducidad any = nil
				if aplicaCad && fechaCaducidad != "" {
					caducidad = fechaCaducidad
				}
				if _, err := tx.Exec(
					`INSERT INTO unidades (id, tenant_id, producto_id, estado, creado_en, caducidad) VALUES (?, ?, ?, ?, ?, ?)`,
					unitID, tenantID, internalSKU, "Disponible", now, caducidad,
				); err != nil {
					_, _ = tx.Exec("ROLLBACK TO csv_row")
					_, _ = tx.Exec("RELEASE csv_row")
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, ID: productID, Error: fmt.Sprintf("Error al crear unidades: %v", err)})
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
			resp.LabelPrintURL = productLabelPrintURL(resp.ProductIDs, "")
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
