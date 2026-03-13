package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"golang.org/x/crypto/bcrypt"
)

type inventoryPageData struct {
	Title       string
	Subtitle    string
	RoutePrefix string
	Flash       string
	MetodoPagos []string
	Products    []inventoryProduct
	CanSell     bool
	CanSwap     bool
	CanRetoma   bool
	CurrentUser *User
}

type unitOption struct {
	ID string
}

type productOption struct {
	ID           string
	Name         string
	Line         string
	FechaIngreso string
	SalePrice    float64
	OwnerUserID  int
	HasOwner     bool
	Units        []unitOption
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
	ID                string
	Name              string
	Line              string
	EstadoLabel       string
	EstadoClass       string
	Disponible        int
	Unidades          []inventoryUnit
	DisabledSale      bool
	FechaIngreso      string
	MesesEnStock      int
	AlertaPermanencia bool
	SalePrice         float64
}

type BusinessSettings struct {
	ID           int
	BusinessName string
	LogoPath     string
	PrimaryColor string
	Currency     string
	DateFormat   string
	UpdatedAt    string
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

type APIKey struct {
	ID        int
	Name      string
	Active    bool
	CreatedAt string
	UpdatedAt string
}

var (
	errInsufficientStock = fmt.Errorf("stock insuficiente")

	businessSettingsMu sync.RWMutex
	businessSettings   = defaultBusinessSettings()
)

func defaultBusinessSettings() BusinessSettings {
	return BusinessSettings{
		ID:           1,
		BusinessName: "Stocki App",
		LogoPath:     "/static/img/logo1.svg",
		PrimaryColor: "#0ea5c9",
		Currency:     "COP",
		DateFormat:   "2006-01-02",
	}
}

type sqlExecer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

func upsertProducto(exec sqlExecer, sku, nombre, linea, now string) error {
	// productos table is part of the existing DB schema and uses sku as the primary key.
	// Other columns (prices, discount, notes) have defaults so manual creation can omit them.
	_ = now // kept for backwards-compat in case we later add created_at.
	_, err := exec.Exec(`
		INSERT INTO productos (sku, id, linea, nombre, fecha_ingreso)
		VALUES (?, ?, ?, ?, COALESCE((SELECT fecha_ingreso FROM productos WHERE sku = ?), CURRENT_TIMESTAMP))
		ON CONFLICT(sku) DO UPDATE SET
			id = excluded.id,
			linea = excluded.linea,
			nombre = excluded.nombre
	`, sku, sku, linea, nombre, sku)
	return err
}

func seedProductosIfMissing(db *sql.DB, defaults []productOption) error {
	// Backfill unknown products that already exist in inventory units.
	if _, err := db.Exec(`
		INSERT OR IGNORE INTO productos (sku, id, nombre, linea, fecha_ingreso)
		SELECT DISTINCT producto_id, producto_id, producto_id, 'Sin línea', CURRENT_TIMESTAMP
		FROM unidades
	`); err != nil {
		return err
	}

	for _, p := range defaults {
		if _, err := db.Exec(`
			INSERT OR IGNORE INTO productos (sku, id, nombre, linea, fecha_ingreso)
			VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
		`, p.ID, p.ID, p.Name, p.Line); err != nil {
			return err
		}
	}
	return nil
}

func loadProductos(db *sql.DB) ([]productOption, error) {
	rows, err := db.Query(`
		SELECT sku, nombre, linea, COALESCE(fecha_ingreso, ''), COALESCE(precio_venta, 0), owner_user_id
		FROM productos
		ORDER BY sku
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	products := []productOption{}
	for rows.Next() {
		var p productOption
		var ownerUserID sql.NullInt64
		if err := rows.Scan(&p.ID, &p.Name, &p.Line, &p.FechaIngreso, &p.SalePrice, &ownerUserID); err != nil {
			return nil, err
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

func loadAssignableUsers(db *sql.DB) ([]assignableUser, error) {
	rows, err := db.Query(`
		SELECT id, username
		FROM users
		WHERE is_active = 1
		ORDER BY LOWER(username), id
	`)
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
	if user != nil && user.Role == "admin" {
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
	if user != nil && user.Role == "admin" {
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
		WHERE sku = ? OR id = ?
		LIMIT 1
	`, productID, productID).Scan(&ownerUserID)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if user != nil && user.Role == "admin" {
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
	if user != nil && user.Role == "admin" {
		return "1=1", nil
	}
	if alias == "" {
		alias = "p"
	}
	if user == nil {
		return fmt.Sprintf("(%s.sku IS NULL OR %s.owner_user_id IS NULL)", alias, alias), nil
	}
	return fmt.Sprintf("(%s.sku IS NULL OR %s.owner_user_id IS NULL OR %s.owner_user_id = ?)", alias, alias, alias), []any{user.ID}
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
	Sales           []dashboardSaleDetail
	CurrentUser     *User
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

	MetodosPago     []metodoPagoTotal     `json:"metodos_pago"`
	PieSlices       []pieSlice            `json:"pie_slices"`
	PieTotal        string                `json:"pie_total"`
	MaxTimeline     float64               `json:"max_timeline"`
	MaxTimelineText string                `json:"max_timeline_text"`
	Timeline        []timelinePoint       `json:"timeline"`
	Sales           []dashboardSaleDetail `json:"sales"`
}

func buildDashboardSalesData(db *sql.DB, user *User, startStr, endStr string, startDate, endDate time.Time) (dashboardDataResponse, error) {
	resp := dashboardDataResponse{
		Ok:         true,
		RangeStart: startStr,
		RangeEnd:   endStr,
	}
	visibilitySQL, visibilityArgs := productVisibilityPredicate("p", user)
	salesDateExpr := "substr(v.fecha, 1, 10)"

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
}

type contextKey string

const (
	userContextKey               contextKey = "user"
	apiIntegrationNameContextKey contextKey = "api_integration_name"
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
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			producto_id TEXT NOT NULL,
			unidad_id TEXT NOT NULL,
			tipo TEXT NOT NULL,
			nota TEXT NOT NULL DEFAULT '',
			usuario TEXT NOT NULL DEFAULT '',
			fecha TEXT NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_movimientos_producto_fecha ON movimientos (producto_id, fecha);
		CREATE INDEX IF NOT EXISTS idx_movimientos_unidad_fecha ON movimientos (unidad_id, fecha);
	`)
	return err
}

func logMovimientos(tx *sql.Tx, productoID string, unidadIDs []string, tipo, nota string, user *User, now string) error {
	username := ""
	if user != nil {
		username = user.Username
	}
	stmt, err := tx.Prepare(`INSERT INTO movimientos (producto_id, unidad_id, tipo, nota, usuario, fecha) VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, unidadID := range unidadIDs {
		if _, err := stmt.Exec(productoID, unidadID, tipo, nota, username, now); err != nil {
			return err
		}
	}
	return nil
}

func normalizeAuditSource(source string) string {
	switch strings.TrimSpace(strings.ToLower(source)) {
	case "api", "n8n", "agent":
		return strings.TrimSpace(strings.ToLower(source))
	default:
		return "manual"
	}
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
		INSERT INTO audit_events (event_type, entity_type, entity_id, user_id, source, payload_json, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, strings.TrimSpace(eventType), strings.TrimSpace(entityType), strings.TrimSpace(entityID), userID, normalizeAuditSource(source), payloadJSON, time.Now().Format(time.RFC3339))
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

func selectAndMarkUnitsSold(tx *sql.Tx, productID string, qty int) ([]string, error) {
	return selectAndMarkUnitsByStatus(tx, productID, qty, "Vendida")
}

func selectAndMarkUnitsByStatus(tx *sql.Tx, productID string, qty int, nextStatus string) ([]string, error) {
	if qty <= 0 {
		return nil, fmt.Errorf("cantidad inválida")
	}

	rows, err := tx.Query(`
		SELECT id
		FROM unidades
		WHERE producto_id = ? AND estado IN ('Disponible', 'available')
		ORDER BY creado_en, id
		LIMIT ?`, productID, qty)
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

	query := fmt.Sprintf("UPDATE unidades SET estado = ? WHERE id IN (%s) AND estado IN ('Disponible', 'available')", strings.Join(placeholders, ","))
	updateArgs := make([]interface{}, 0, len(args)+1)
	updateArgs = append(updateArgs, nextStatus)
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

func availableUnitsByProduct(db *sql.DB, productID string) ([]unitOption, error) {
	rows, err := db.Query(`
		SELECT id
		FROM unidades
		WHERE producto_id = ? AND estado IN ('Disponible', 'available')
		ORDER BY creado_en, id`, productID)
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

func availableCountsByProduct(db *sql.DB) (map[string]int, error) {
	rows, err := db.Query(`
		SELECT producto_id, COUNT(*)
		FROM unidades
		WHERE estado IN ('Disponible', 'available')
		GROUP BY producto_id`)
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
	return settings
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
		if t, err := time.Parse(layout, value); err == nil {
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
		return t.Format(settings.DateFormat)
	}
	return raw
}

func loadBusinessSettings(db *sql.DB) (BusinessSettings, error) {
	settings := defaultBusinessSettings()
	row := db.QueryRow(`
		SELECT id, business_name, logo_path, primary_color, currency, date_format, updated_at
		FROM business_settings
		ORDER BY id ASC
		LIMIT 1
	`)
	var updatedAt sql.NullString
	err := row.Scan(&settings.ID, &settings.BusinessName, &settings.LogoPath, &settings.PrimaryColor, &settings.Currency, &settings.DateFormat, &updatedAt)
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
	settings = normalizeBusinessSettings(settings)
	settings.ID = 1
	settings.UpdatedAt = time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`
		INSERT INTO business_settings (id, business_name, logo_path, primary_color, currency, date_format, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			business_name = excluded.business_name,
			logo_path = excluded.logo_path,
			primary_color = excluded.primary_color,
			currency = excluded.currency,
			date_format = excluded.date_format,
			updated_at = excluded.updated_at
	`, settings.ID, settings.BusinessName, settings.LogoPath, settings.PrimaryColor, settings.Currency, settings.DateFormat, settings.UpdatedAt); err != nil {
		return BusinessSettings{}, err
	}
	setCurrentBusinessSettings(settings)
	return settings, nil
}

func loadBusinessLines(db *sql.DB, activeOnly bool) ([]BusinessLine, error) {
	query := `
		SELECT id, name, active, created_at, updated_at
		FROM business_lines
	`
	args := []any{}
	if activeOnly {
		query += ` WHERE active = 1`
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
	return []string{"venta", "cambio", "retoma"}
}

func loadPaymentMethods(db *sql.DB, activeOnly bool) ([]PaymentMethod, error) {
	query := `
		SELECT id, name, active, sort_order, created_at, updated_at
		FROM payment_methods
	`
	if activeOnly {
		query += ` WHERE active = 1`
	}
	query += ` ORDER BY sort_order ASC, LOWER(name) ASC, id ASC`
	rows, err := db.Query(query)
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
	rows, err := db.Query(`
		SELECT id, name, active, created_at, updated_at
		FROM api_keys
		ORDER BY active DESC, updated_at DESC, id DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]APIKey, 0, 16)
	for rows.Next() {
		var item APIKey
		var active int
		if err := rows.Scan(&item.ID, &item.Name, &active, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		item.Active = active == 1
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
	if err := db.QueryRow(`SELECT COUNT(*) FROM payment_methods`).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	now := time.Now().Format(time.RFC3339)
	for idx, name := range defaultPaymentMethodNames() {
		if _, err := db.Exec(`
			INSERT INTO payment_methods (name, active, sort_order, created_at, updated_at)
			VALUES (?, 1, ?, ?, ?)
		`, name, idx+1, now, now); err != nil {
			return err
		}
	}
	return nil
}

func seedMovementSettingsIfMissing(db *sql.DB) error {
	now := time.Now().Format(time.RFC3339)
	for _, movementType := range defaultMovementTypes() {
		if _, err := db.Exec(`
			INSERT OR IGNORE INTO movement_settings (movement_type, enabled, updated_at)
			VALUES (?, 1, ?)
		`, movementType, now); err != nil {
			return err
		}
	}
	return nil
}

func loadMovementSettings(db *sql.DB) ([]MovementSetting, map[string]bool, error) {
	rows, err := db.Query(`
		SELECT id, movement_type, enabled, updated_at
		FROM movement_settings
		ORDER BY CASE movement_type
			WHEN 'venta' THEN 1
			WHEN 'cambio' THEN 2
			WHEN 'retoma' THEN 3
			ELSE 99
		END, id
	`)
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
		if t, err := time.Parse(layout, value); err == nil {
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

func userFromContext(r *http.Request) *User {
	if user, ok := r.Context().Value(userContextKey).(*User); ok {
		return user
	}
	return nil
}

func apiIntegrationNameFromContext(r *http.Request) string {
	if name, ok := r.Context().Value(apiIntegrationNameContextKey).(string); ok {
		return strings.TrimSpace(name)
	}
	return ""
}

func userFromRequest(db *sql.DB, r *http.Request) (*User, error) {
	cookie, err := r.Cookie("session_token")
	if err != nil {
		return nil, err
	}

	var (
		user       User
		isActive   int
		expiresRaw string
	)
	query := `
		SELECT u.id, u.username, u.role, u.is_active, s.expires_at
		FROM sessions s
		JOIN users u ON u.id = s.user_id
		WHERE s.token = ?`
	if err := db.QueryRow(query, cookie.Value).Scan(&user.ID, &user.Username, &user.Role, &isActive, &expiresRaw); err != nil {
		return nil, err
	}
	expiresAt, err := time.Parse(time.RFC3339, expiresRaw)
	if err != nil {
		return nil, err
	}
	if time.Now().After(expiresAt) {
		_, _ = db.Exec("DELETE FROM sessions WHERE token = ?", cookie.Value)
		return nil, sql.ErrNoRows
	}
	user.IsActive = isActive == 1
	if !user.IsActive {
		_, _ = db.Exec("DELETE FROM sessions WHERE token = ?", cookie.Value)
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

func integrationUserFromDB(db *sql.DB) (*User, error) {
	var user User
	var isActive int
	err := db.QueryRow(`
		SELECT id, username, role, is_active
		FROM users
		WHERE role = 'admin' AND is_active = 1
		ORDER BY id
		LIMIT 1
	`).Scan(&user.ID, &user.Username, &user.Role, &isActive)
	if err != nil {
		return nil, err
	}
	user.IsActive = isActive == 1
	return &user, nil
}

func apiAuthFromRequest(db *sql.DB, r *http.Request) (*User, string, error) {
	if user, err := userFromRequest(db, r); err == nil && user != nil {
		return user, "", nil
	}

	token := bearerTokenFromRequest(r)
	if token == "" {
		return nil, "", sql.ErrNoRows
	}

	var integrationName string
	var active int
	err := db.QueryRow(`
		SELECT name, active
		FROM api_keys
		WHERE token_hash = ?
	`, hashAPIToken(token)).Scan(&integrationName, &active)
	if err != nil {
		return nil, "", err
	}
	if active != 1 {
		return nil, "", sql.ErrNoRows
	}

	user, err := integrationUserFromDB(db)
	if err != nil {
		return nil, "", err
	}
	return user, strings.TrimSpace(integrationName), nil
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
			user, integrationName, err := apiAuthFromRequest(db, r)
			if err != nil {
				writeAPIError(w, http.StatusUnauthorized, "Autenticación requerida para la API.", nil)
				return
			}
			ctx := context.WithValue(r.Context(), userContextKey, user)
			if integrationName != "" {
				ctx = context.WithValue(ctx, apiIntegrationNameContextKey, integrationName)
			}
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		user, err := userFromRequest(db, r)
		if err != nil {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		ctx := context.WithValue(r.Context(), userContextKey, user)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func adminOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := userFromContext(r)
		if user == nil || user.Role != "admin" {
			http.Error(w, "Acceso restringido a administradores.", http.StatusForbidden)
			return
		}
		next(w, r)
	}
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
	case strings.Contains(msg, "CHECK constraint failed"):
		return "Datos inválidos (revisa el rol)."
	case strings.Contains(msg, "database is locked"):
		return "La base de datos está ocupada. Intenta de nuevo."
	default:
		return "No se pudo crear el usuario."
	}
}

func tableColumns(db *sql.DB, table string) (map[string]bool, error) {
	// SQLite PRAGMA table_info does not reliably accept a bound parameter for table name,
	// so we build the statement after validating the identifier.
	for i, r := range table {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' || (i > 0 && r >= '0' && r <= '9') {
			continue
		}
		return nil, fmt.Errorf("invalid table name: %q", table)
	}
	rows, err := db.Query(fmt.Sprintf("SELECT name FROM pragma_table_info('%s')", table))
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

func initDB(path string, paymentMethods []string) (*sql.DB, error) {
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
	CREATE TABLE IF NOT EXISTS productos (
		sku TEXT PRIMARY KEY,
		id TEXT,
		linea TEXT NOT NULL,
		nombre TEXT NOT NULL,
		owner_user_id INTEGER,
		precio_base REAL NOT NULL DEFAULT 0,
		precio_venta REAL NOT NULL DEFAULT 0,
		precio_consultora REAL NOT NULL DEFAULT 0,
		descuento REAL NOT NULL DEFAULT 0,
		anotaciones TEXT NOT NULL DEFAULT '',
		aplica_caducidad INTEGER NOT NULL DEFAULT 0,
		fecha_ingreso TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
	);
	CREATE INDEX IF NOT EXISTS idx_productos_linea ON productos (linea);

	CREATE TABLE IF NOT EXISTS ventas (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		producto_id TEXT NOT NULL,
		cantidad INTEGER NOT NULL,
		precio_final REAL NOT NULL,
		metodo_pago TEXT NOT NULL,
		notas TEXT NOT NULL DEFAULT '',
		fecha TEXT NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_ventas_fecha ON ventas (fecha);
	CREATE INDEX IF NOT EXISTS idx_ventas_metodo ON ventas (metodo_pago);

	CREATE TABLE IF NOT EXISTS unidades (
		id TEXT PRIMARY KEY,
		producto_id TEXT NOT NULL,
		estado TEXT NOT NULL,
		creado_en TEXT NOT NULL,
		caducidad TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_unidades_estado ON unidades (estado);

	CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		username TEXT NOT NULL UNIQUE,
		password_hash TEXT NOT NULL,
		role TEXT NOT NULL CHECK (role IN ('admin', 'empleado')),
		created_at TEXT NOT NULL,
		is_active INTEGER NOT NULL DEFAULT 1
	);
	CREATE INDEX IF NOT EXISTS idx_users_role ON users (role);

	CREATE TABLE IF NOT EXISTS sessions (
		token TEXT PRIMARY KEY,
		user_id INTEGER NOT NULL,
		created_at TEXT NOT NULL,
		expires_at TEXT NOT NULL,
		FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS api_keys (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		token_hash TEXT NOT NULL UNIQUE,
		active INTEGER NOT NULL DEFAULT 1,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys (active);

	CREATE TABLE IF NOT EXISTS business_settings (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		business_name TEXT NOT NULL,
		logo_path TEXT NOT NULL DEFAULT '',
		primary_color TEXT NOT NULL DEFAULT '#0ea5c9',
		currency TEXT NOT NULL DEFAULT 'COP',
		date_format TEXT NOT NULL DEFAULT '2006-01-02',
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS business_lines (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		active INTEGER NOT NULL DEFAULT 1,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS payment_methods (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		active INTEGER NOT NULL DEFAULT 1,
		sort_order INTEGER NOT NULL DEFAULT 0,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS movement_settings (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		movement_type TEXT NOT NULL UNIQUE,
		enabled INTEGER NOT NULL DEFAULT 1,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS audit_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		event_type TEXT NOT NULL,
		entity_type TEXT NOT NULL,
		entity_id TEXT NOT NULL DEFAULT '',
		user_id INTEGER,
		source TEXT NOT NULL DEFAULT 'manual',
		payload_json TEXT NOT NULL DEFAULT '{}',
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_audit_events_created_at ON audit_events (created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_audit_events_event_type ON audit_events (event_type);
	CREATE INDEX IF NOT EXISTS idx_audit_events_entity_type ON audit_events (entity_type);
	`

	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}

	if err := ensureMovimientosTable(db); err != nil {
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

	// Ensure fecha_ingreso exists for permanence-based alerts.
	var productosHasFecha int
	if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('productos') WHERE name = 'fecha_ingreso'").Scan(&productosHasFecha); err != nil {
		return nil, err
	}
	if productosHasFecha == 0 {
		if _, err := db.Exec("ALTER TABLE productos ADD COLUMN fecha_ingreso TEXT"); err != nil {
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

	if err := seedAdminUser(db, path); err != nil {
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
	_, err = db.Exec(`
		INSERT INTO users (username, password_hash, role, created_at, is_active)
		VALUES (?, ?, 'admin', ?, 1)
	`, adminUser, string(hashed), time.Now().Format(time.RFC3339))
	return err
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "data.db"
	}

	tmpl := template.Must(template.New("").Funcs(template.FuncMap{
		"businessName": func() string {
			return currentBusinessSettings().BusinessName
		},
		"businessLogoPath": func() string {
			return currentBusinessSettings().LogoPath
		},
		"businessPrimaryColor": func() string {
			return currentBusinessSettings().PrimaryColor
		},
		"businessPrimaryStrong": func() string {
			return shadeHexColor(currentBusinessSettings().PrimaryColor, -24)
		},
		"businessPrimarySoft": func() string {
			return shadeHexColor(currentBusinessSettings().PrimaryColor, 208)
		},
	}).ParseFiles(
		"templates/partials/app_styles.html",
		"templates/admin_users.html",
		"templates/business_settings.html",
		"templates/audit_events.html",
		"templates/dashboard.html",
		"templates/inventario.html",
		"templates/login.html",
		"templates/product_new.html",
		"templates/venta_new.html",
		"templates/venta_confirm.html",
		"templates/cambio_new.html",
		"templates/cambio_confirm.html",
		"templates/csv_template.html",
		"templates/csv_export.html",
		"templates/partials/header.html",
	))

	paymentMethods := defaultPaymentMethodNames()

	db, err := initDB(dbPath, paymentMethods)
	if err != nil {
		log.Fatalf("Error al abrir SQLite: %v", err)
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
	activePaymentMethods, err := loadPaymentMethods(db, true)
	if err != nil {
		log.Fatalf("Error al cargar métodos de pago: %v", err)
	}
	paymentMethods = paymentMethodNames(activePaymentMethods)

	// Diagnostics to confirm which DB is being used at runtime (helps debug login issues).
	if wd, err := os.Getwd(); err == nil {
		if abs, err := filepath.Abs(dbPath); err == nil {
			log.Printf("DB_PATH=%s (abs=%s) cwd=%s", dbPath, abs, wd)
		} else {
			log.Printf("DB_PATH=%s cwd=%s", dbPath, wd)
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

	type ventaFormData struct {
		Title           string
		Subtitle        string
		ProductoID      string
		ProductoNom     string
		Productos       []productOption
		StockByProd     map[string]int
		Cantidad        int
		PrecioFinal     string
		ValorVentaFinal string
		MetodoPago      string
		Notas           string
		Errors          map[string]string
		MetodoPagos     []string
		RoutePrefix     string
		CurrentUser     *User
	}

	type ventaConfirmData struct {
		Title           string
		Subtitle        string
		ProductoID      string
		ProductoNom     string
		Cantidad        int
		PrecioFinal     string
		ValorVentaFinal string
		MetodoPago      string
		Notas           string
		CurrentUser     *User
	}

	type loginPageData struct {
		Title    string
		Error    string
		Username string
	}

	type adminUserRow struct {
		ID        int
		Username  string
		Name      string
		Email     string
		Role      string
		IsActive  bool
		CreatedAt string
	}

	type adminUsersData struct {
		Title       string
		Subtitle    string
		Flash       string
		Error       string
		Users       []adminUserRow
		CurrentUser *User
	}

	type businessSettingsPageData struct {
		Title             string
		Subtitle          string
		Flash             string
		Error             string
		Settings          BusinessSettings
		Lines             []BusinessLine
		PaymentMethods    []PaymentMethod
		APIKeys           []APIKey
		NewAPIKeyName     string
		CreatedAPIToken   string
		MovementSettings  []MovementSetting
		NewPaymentMethod  string
		NewLineName       string
		EditingLineID     int
		EditingLineName   string
		CurrencyOptions   []string
		DateFormatOptions []string
		CurrentUser       *User
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

	type productNewData struct {
		Title           string
		Subtitle        string
		SKU             string
		Nombre          string
		Linea           string
		OwnerUserID     string
		PrecioVenta     string
		Lineas          []string
		HasLineas       bool
		AssignableUsers []assignableUser
		Cantidad        int
		AplicaCad       bool
		Caducidad       string
		Errors          map[string]string
		CurrentUser     *User
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
		writeAPIJSON(w, http.StatusOK, map[string]any{
			"ok":      true,
			"service": "stocki-app",
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
			if err := tmpl.ExecuteTemplate(w, "login.html", data); err != nil {
				http.Error(w, "Error al renderizar login", http.StatusInternalServerError)
			}
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
					SELECT id, username, password_hash, role, is_active
					FROM users
					WHERE username = ?
				`, username).Scan(&user.ID, &user.Username, &hash, &user.Role, &isActive)
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
			if err := tmpl.ExecuteTemplate(w, "login.html", data); err != nil {
				http.Error(w, "Error al renderizar login", http.StatusInternalServerError)
			}
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
			if err := tmpl.ExecuteTemplate(w, "login.html", data); err != nil {
				http.Error(w, "Error al renderizar login", http.StatusInternalServerError)
			}
			return
		}

		token, err := generateToken()
		if err != nil {
			http.Error(w, "No se pudo generar sesión", http.StatusInternalServerError)
			return
		}
		expiresAt := time.Now().Add(24 * time.Hour)
		_, err = db.Exec(`
			INSERT INTO sessions (token, user_id, created_at, expires_at)
			VALUES (?, ?, ?, ?)
		`, token, user.ID, time.Now().Format(time.RFC3339), expiresAt.Format(time.RFC3339))
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

	mux.HandleFunc("/admin/users", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		flash := r.URL.Query().Get("mensaje")
		errText := r.URL.Query().Get("error")

		// Support legacy schemas by selecting optional columns if they exist.
		selectCols := []string{"id", "username"}
		if usersCols["name"] {
			selectCols = append(selectCols, "name")
		} else {
			selectCols = append(selectCols, "'' as name")
		}
		if usersCols["email"] {
			selectCols = append(selectCols, "email")
		} else {
			selectCols = append(selectCols, "'' as email")
		}
		selectCols = append(selectCols, "role")
		if usersCols["is_active"] {
			selectCols = append(selectCols, "is_active")
		} else if usersCols["active"] {
			selectCols = append(selectCols, "active as is_active")
		} else {
			selectCols = append(selectCols, "1 as is_active")
		}
		if usersCols["created_at"] {
			selectCols = append(selectCols, "created_at")
		} else {
			selectCols = append(selectCols, "'' as created_at")
		}

		rows, err := db.Query(fmt.Sprintf("SELECT %s FROM users ORDER BY id", strings.Join(selectCols, ", ")))
		if err != nil {
			http.Error(w, "Error al consultar usuarios", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		users := []adminUserRow{}
		for rows.Next() {
			var user adminUserRow
			var isActive int
			var username sql.NullString
			var name sql.NullString
			var email sql.NullString
			if err := rows.Scan(&user.ID, &username, &name, &email, &user.Role, &isActive, &user.CreatedAt); err != nil {
				http.Error(w, "Error al leer usuarios", http.StatusInternalServerError)
				return
			}
			user.Username = username.String
			user.Name = name.String
			user.Email = email.String
			user.CreatedAt = formatDateWithSettings(user.CreatedAt)
			user.IsActive = isActive == 1
			users = append(users, user)
		}
		if err := rows.Err(); err != nil {
			http.Error(w, "Error al procesar usuarios", http.StatusInternalServerError)
			return
		}

		data := adminUsersData{
			Title:       "Roles de usuario",
			Subtitle:    "Control de accesos y roles del inventario.",
			Flash:       flash,
			Error:       errText,
			Users:       users,
			CurrentUser: userFromContext(r),
		}
		if err := tmpl.ExecuteTemplate(w, "admin_users.html", data); err != nil {
			http.Error(w, "Error al renderizar usuarios", http.StatusInternalServerError)
		}
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
			WHERE 1=1
		`
		args := make([]any, 0, 3)
		if eventType != "" {
			query += ` AND a.event_type = ?`
			args = append(args, eventType)
		}
		if dateFrom != "" {
			query += ` AND date(a.created_at) >= ?`
			args = append(args, dateFrom)
		}
		if dateTo != "" {
			query += ` AND date(a.created_at) <= ?`
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
		if err := tmpl.ExecuteTemplate(w, "audit_events.html", data); err != nil {
			http.Error(w, "Error al renderizar auditoría", http.StatusInternalServerError)
		}
	}))

	renderBusinessSettingsPage := func(w http.ResponseWriter, r *http.Request, flash, errText, createdToken, newAPIKeyName string) {
		lines, err := loadBusinessLines(db, false)
		if err != nil {
			http.Error(w, "Error al cargar líneas de negocio", http.StatusInternalServerError)
			return
		}
		paymentMethodsCfg, err := loadPaymentMethods(db, false)
		if err != nil {
			http.Error(w, "Error al cargar canales de pago", http.StatusInternalServerError)
			return
		}
		apiKeys, err := loadAPIKeys(db)
		if err != nil {
			http.Error(w, "Error al cargar API keys", http.StatusInternalServerError)
			return
		}
		movementSettings, _, err := loadMovementSettings(db)
		if err != nil {
			http.Error(w, "Error al cargar tipos de movimiento", http.StatusInternalServerError)
			return
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
			Title:             "Configuración",
			Subtitle:          "Separa branding general del negocio y catálogos operativos desde un único panel.",
			Flash:             flash,
			Error:             errText,
			Settings:          currentBusinessSettings(),
			Lines:             lines,
			PaymentMethods:    paymentMethodsCfg,
			APIKeys:           apiKeys,
			NewAPIKeyName:     strings.TrimSpace(newAPIKeyName),
			CreatedAPIToken:   strings.TrimSpace(createdToken),
			MovementSettings:  movementSettings,
			NewPaymentMethod:  strings.TrimSpace(r.URL.Query().Get("new_payment_method")),
			NewLineName:       strings.TrimSpace(r.URL.Query().Get("new_line")),
			EditingLineID:     editingID,
			EditingLineName:   editingName,
			CurrencyOptions:   currencyOptions,
			DateFormatOptions: dateFormatOptions,
			CurrentUser:       userFromContext(r),
		}
		if err := tmpl.ExecuteTemplate(w, "business_settings.html", data); err != nil {
			http.Error(w, "Error al renderizar configuración", http.StatusInternalServerError)
		}
	}

	mux.HandleFunc("/configuracion", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			renderBusinessSettingsPage(w, r, r.URL.Query().Get("mensaje"), r.URL.Query().Get("error"), "", strings.TrimSpace(r.URL.Query().Get("new_api_key_name")))
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

		settings := currentBusinessSettings()
		settings.BusinessName = strings.TrimSpace(r.FormValue("business_name"))
		settings.PrimaryColor = normalizeHexColor(r.FormValue("primary_color"), settings.PrimaryColor)
		settings.Currency = normalizeCurrency(r.FormValue("currency"))
		settings.DateFormat = normalizeDateFormat(r.FormValue("date_format"))

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

		if _, err := saveBusinessSettings(db, settings); err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo guardar la configuración.")
			return
		}
		if err := logAuditEvent(db, userFromContext(r), "business_settings_updated", "business_settings", "1", "manual", map[string]any{
			"business_name": settings.BusinessName,
			"logo_path":     settings.LogoPath,
			"primary_color": settings.PrimaryColor,
			"currency":      settings.Currency,
			"date_format":   settings.DateFormat,
		}); err != nil {
			log.Printf("audit business settings: %v", err)
		}

		redirectWithMessage(w, r, "/configuracion", "Configuración actualizada.", "")
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
		now := time.Now().Format(time.RFC3339)
		if _, err := db.Exec(`
			INSERT INTO business_lines (name, active, created_at, updated_at)
			VALUES (?, 1, ?, ?)
		`, name, now, now); err != nil {
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
		if _, err := db.Exec(`
			UPDATE business_lines
			SET name = ?, active = ?, updated_at = ?
			WHERE id = ?
		`, name, active, time.Now().Format(time.RFC3339), lineID); err != nil {
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
		if err := db.QueryRow(`SELECT COALESCE(MAX(sort_order), 0) + 1 FROM payment_methods`).Scan(&nextOrder); err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo calcular el orden del canal.")
			return
		}
		now := time.Now().Format(time.RFC3339)
		if _, err := db.Exec(`
			INSERT INTO payment_methods (name, active, sort_order, created_at, updated_at)
			VALUES (?, 1, ?, ?, ?)
		`, name, nextOrder, now, now); err != nil {
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
		if _, err := db.Exec(`
			UPDATE payment_methods
			SET name = ?, active = ?, sort_order = ?, updated_at = ?
			WHERE id = ?
		`, name, active, sortOrder, time.Now().Format(time.RFC3339), methodID); err != nil {
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
		token, err := generateToken()
		if err != nil {
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo generar el token.")
			return
		}
		now := time.Now().Format(time.RFC3339)
		if _, err := db.Exec(`
			INSERT INTO api_keys (name, token_hash, active, created_at, updated_at)
			VALUES (?, ?, 1, ?, ?)
		`, name, hashAPIToken(token), now, now); err != nil {
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
		renderBusinessSettingsPage(w, r, "API key creada. Copia el token ahora; no volverá a mostrarse.", "", token, "")
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
		active := 0
		if r.FormValue("active") != "" {
			active = 1
		}
		if _, err := db.Exec(`
			UPDATE api_keys
			SET name = ?, active = ?, updated_at = ?
			WHERE id = ?
		`, name, active, time.Now().Format(time.RFC3339), keyID); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "unique") {
				redirectWithMessage(w, r, "/configuracion", "", "Ya existe una API key con ese nombre.")
				return
			}
			redirectWithMessage(w, r, "/configuracion", "", "No se pudo actualizar la API key.")
			return
		}
		if err := logAuditEvent(db, userFromContext(r), "api_key_updated", "api_key", strconv.Itoa(keyID), "manual", map[string]any{
			"name":   name,
			"active": active == 1,
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
		if _, err := db.Exec(`
			UPDATE movement_settings
			SET enabled = ?, updated_at = ?
			WHERE movement_type = ?
		`, enabled, time.Now().Format(time.RFC3339), movementType); err != nil {
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
		password := r.FormValue("password")
		role := strings.TrimSpace(r.FormValue("role"))
		isActive := r.FormValue("is_active") != ""

		if username == "" {
			redirectWithMessage(w, r, "/admin/users", "", "Usuario obligatorio.")
			return
		}
		if password == "" {
			redirectWithMessage(w, r, "/admin/users", "", "Contraseña obligatoria.")
			return
		}
		if role != "admin" && role != "empleado" {
			redirectWithMessage(w, r, "/admin/users", "", "Rol inválido.")
			return
		}

		if usersCols["name"] && name == "" {
			name = username
		}
		if usersCols["email"] && email == "" {
			// If username already looks like an email, reuse it.
			if strings.Contains(username, "@") {
				email = username
			} else {
				email = username + "@local"
			}
		}
		if usersCols["email"] {
			var emailExists int
			if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE email = ?`, email).Scan(&emailExists); err == nil && emailExists > 0 {
				redirectWithMessage(w, r, "/admin/users", "", "El email ya existe.")
				return
			}
		}

		var exists int
		if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE username = ?`, username).Scan(&exists); err == nil && exists > 0 {
			redirectWithMessage(w, r, "/admin/users", "", "El usuario ya existe.")
			return
		}

		hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if err != nil {
			redirectWithMessage(w, r, "/admin/users", "", "No se pudo procesar la contraseña.")
			return
		}

		activeInt := 0
		if isActive {
			activeInt = 1
		}

		cols := []string{"username", "password_hash", "role"}
		args := []any{username, string(hashed), role}
		if usersCols["name"] {
			cols = append(cols, "name")
			args = append(args, name)
		}
		if usersCols["email"] {
			cols = append(cols, "email")
			args = append(args, email)
		}
		if usersCols["password_salt"] {
			cols = append(cols, "password_salt")
			args = append(args, "bcrypt")
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

		_, err = db.Exec(
			fmt.Sprintf("INSERT INTO users (%s) VALUES (%s)", strings.Join(cols, ", "), strings.Join(placeholders, ", ")),
			args...,
		)
		if err != nil {
			log.Printf("admin/users/create: insert failed username=%q err=%v", username, err)
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
		role := strings.TrimSpace(r.FormValue("role"))
		isActive := r.FormValue("is_active") != ""

		if username == "" {
			redirectWithMessage(w, r, "/admin/users", "", "Usuario obligatorio.")
			return
		}
		if role != "admin" && role != "empleado" {
			redirectWithMessage(w, r, "/admin/users", "", "Rol inválido.")
			return
		}
		if usersCols["name"] && name == "" {
			name = username
		}
		if usersCols["email"] && email == "" {
			if strings.Contains(username, "@") {
				email = username
			} else {
				email = username + "@local"
			}
		}

		// Prevent leaving the system without any active admin.
		var currentRole string
		var currentActive int
		if err := db.QueryRow(`SELECT role, is_active FROM users WHERE id = ?`, userID).Scan(&currentRole, &currentActive); err != nil {
			redirectWithMessage(w, r, "/admin/users", "", "Usuario no encontrado.")
			return
		}
		willBeActive := 0
		if isActive {
			willBeActive = 1
		}
		isDemotingAdmin := currentRole == "admin" && role != "admin" && currentActive == 1
		isDeactivatingAdmin := currentRole == "admin" && currentActive == 1 && willBeActive == 0
		if isDemotingAdmin || isDeactivatingAdmin {
			var otherActiveAdmins int
			if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE role = 'admin' AND is_active = 1 AND id != ?`, userID).Scan(&otherActiveAdmins); err == nil {
				if otherActiveAdmins == 0 {
					redirectWithMessage(w, r, "/admin/users", "", "Debe existir al menos un admin activo.")
					return
				}
			}
		}

		setCols := []string{"username = ?", "role = ?"}
		args := []any{username, role}
		if usersCols["name"] {
			setCols = append(setCols, "name = ?")
			args = append(args, name)
		}
		if usersCols["email"] {
			setCols = append(setCols, "email = ?")
			args = append(args, email)
		}
		if usersCols["is_active"] {
			setCols = append(setCols, "is_active = ?")
			args = append(args, willBeActive)
		}
		if usersCols["active"] {
			setCols = append(setCols, "active = ?")
			args = append(args, willBeActive)
		}
		args = append(args, userID)

		_, err = db.Exec(fmt.Sprintf("UPDATE users SET %s WHERE id = ?", strings.Join(setCols, ", ")), args...)
		if err != nil {
			redirectWithMessage(w, r, "/admin/users", "", "No se pudo actualizar el usuario.")
			return
		}

		// If deactivated, invalidate sessions.
		if willBeActive == 0 {
			_, _ = db.Exec(`DELETE FROM sessions WHERE user_id = ?`, userID)
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
		if _, err := db.Exec(fmt.Sprintf("UPDATE users SET %s WHERE id = ?", strings.Join(setCols, ", ")), args...); err != nil {
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

		var role string
		var isActive int
		if err := db.QueryRow(`SELECT role, is_active FROM users WHERE id = ?`, userID).Scan(&role, &isActive); err != nil {
			redirectWithMessage(w, r, "/admin/users", "", "Usuario no encontrado.")
			return
		}
		if role == "admin" && isActive == 1 {
			var otherActiveAdmins int
			if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE role = 'admin' AND is_active = 1 AND id != ?`, userID).Scan(&otherActiveAdmins); err == nil {
				if otherActiveAdmins == 0 {
					redirectWithMessage(w, r, "/admin/users", "", "No puedes eliminar el último admin activo.")
					return
				}
			}
		}

		_, _ = db.Exec(`DELETE FROM sessions WHERE user_id = ?`, userID)
		if _, err := db.Exec(`DELETE FROM users WHERE id = ?`, userID); err != nil {
			redirectWithMessage(w, r, "/admin/users", "", "No se pudo eliminar el usuario.")
			return
		}

		redirectWithMessage(w, r, "/admin/users", "Usuario eliminado.", "")
	}))

	mux.HandleFunc("/productos/new", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		activeLines, err := loadBusinessLines(db, true)
		if err != nil {
			http.Error(w, "No se pudieron cargar las líneas de negocio", http.StatusInternalServerError)
			return
		}
		nextSKU, err := generateNextProductSKU(db)
		if err != nil {
			http.Error(w, "No se pudo generar el SKU", http.StatusInternalServerError)
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
			SKU:             nextSKU,
			Cantidad:        1,
			Lineas:          businessLineNames(activeLines),
			HasLineas:       len(activeLines) > 0,
			AssignableUsers: assignableUsers,
			CurrentUser:     userFromContext(r),
		}
		if err := tmpl.ExecuteTemplate(w, "product_new.html", data); err != nil {
			http.Error(w, "Error al renderizar productos", http.StatusInternalServerError)
		}
	}))

	mux.HandleFunc("/productos", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Redirect(w, r, "/productos/new", http.StatusSeeOther)
			return
		}

		activeLines, err := loadBusinessLines(db, true)
		if err != nil {
			http.Error(w, "No se pudieron cargar las líneas de negocio", http.StatusInternalServerError)
			return
		}

		if err := r.ParseForm(); err != nil {
			http.Error(w, "No se pudo leer el formulario", http.StatusBadRequest)
			return
		}

		nombre := strings.TrimSpace(r.FormValue("nombre"))
		linea := strings.TrimSpace(r.FormValue("linea"))
		ownerUserIDRaw := strings.TrimSpace(r.FormValue("owner_user_id"))
		cantidadRaw := strings.TrimSpace(r.FormValue("cantidad"))
		precioVentaRaw := strings.TrimSpace(r.FormValue("precio_venta"))
		aplicaCad := r.FormValue("aplica_caducidad") != ""
		caducidad := strings.TrimSpace(r.FormValue("fecha_caducidad"))
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
		cantidad, err := strconv.Atoi(cantidadRaw)
		if err != nil || cantidad <= 0 {
			errors["cantidad"] = "Cantidad debe ser entero mayor a 0."
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
			nextSKU, skuErr := generateNextProductSKU(db)
			if skuErr != nil {
				http.Error(w, "No se pudo generar el SKU", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusBadRequest)
			data := productNewData{
				Title:           "Crear producto",
				Subtitle:        "Acción reservada para administradores.",
				SKU:             nextSKU,
				Nombre:          nombre,
				Linea:           linea,
				OwnerUserID:     ownerUserIDRaw,
				PrecioVenta:     precioVentaRaw,
				Lineas:          ensureLineOption(businessLineNames(activeLines), linea),
				HasLineas:       len(activeLines) > 0,
				AssignableUsers: assignableUsers,
				Cantidad:        cantidad,
				AplicaCad:       aplicaCad,
				Caducidad:       caducidad,
				Errors:          errors,
				CurrentUser:     userFromContext(r),
			}
			if err := tmpl.ExecuteTemplate(w, "product_new.html", data); err != nil {
				http.Error(w, "Error al renderizar productos", http.StatusInternalServerError)
			}
			return
		}

		tx, err := db.Begin()
		if err != nil {
			http.Error(w, "No se pudo iniciar la transacción", http.StatusInternalServerError)
			return
		}
		defer tx.Rollback()

		sku, err := generateNextProductSKU(db)
		if err != nil {
			http.Error(w, "No se pudo generar el SKU", http.StatusInternalServerError)
			return
		}
		now := time.Now().Format(time.RFC3339)
		if err := upsertProducto(tx, sku, nombre, linea, now); err != nil {
			http.Error(w, "No se pudo guardar el producto", http.StatusInternalServerError)
			return
		}
		if _, err := tx.Exec(`UPDATE productos SET precio_venta = ? WHERE sku = ?`, float64(precioVenta), sku); err != nil {
			http.Error(w, "No se pudo guardar el precio del producto", http.StatusInternalServerError)
			return
		}
		if _, err := tx.Exec(`UPDATE productos SET owner_user_id = ? WHERE sku = ?`, ownerUserID, sku); err != nil {
			http.Error(w, "No se pudo guardar la asignación del producto", http.StatusInternalServerError)
			return
		}
		if err := logAuditEvent(tx, userFromContext(r), "product_created", "product", sku, "manual", map[string]any{
			"sku":           sku,
			"name":          nombre,
			"line":          linea,
			"owner_user_id": ownerUserID,
			"cantidad":      cantidad,
		}); err != nil {
			http.Error(w, "No se pudo registrar la auditoría del producto", http.StatusInternalServerError)
			return
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
		for j := 0; j < cantidad; j++ {
			unitID := fmt.Sprintf("U-%s-%d", sku, baseID+int64(j))
			var cad any = nil
			if aplicaCad && caducidad != "" {
				cad = caducidad
			}
			if _, err := tx.Exec(
				`INSERT INTO unidades (id, producto_id, estado, creado_en, caducidad) VALUES (?, ?, ?, ?, ?)`,
				unitID, sku, "Disponible", now, cad,
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
				products[idx].SalePrice = float64(precioVenta)
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
				ID:           sku,
				Name:         nombre,
				Line:         linea,
				FechaIngreso: time.Now().Format("2006-01-02"),
				SalePrice:    float64(precioVenta),
			}
			if ownerUserID.Valid {
				createdProduct.HasOwner = true
				createdProduct.OwnerUserID = int(ownerUserID.Int64)
			}
			products = append(products, createdProduct)
		}
		productsMu.Unlock()

		http.Redirect(w, r, "/inventario?mensaje=Producto+agregado", http.StatusSeeOther)
	}))

	mux.HandleFunc("/api/products", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			productsSnapshot, err := loadProductos(db)
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
				items = append(items, map[string]any{
					"id":            product.ID,
					"name":          product.Name,
					"line":          product.Line,
					"fecha_ingreso": formatDateWithSettings(product.FechaIngreso),
					"sale_price":    product.SalePrice,
					"owner_user_id": owner,
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
		if currentUser == nil || currentUser.Role != "admin" {
			writeAPIError(w, http.StatusForbidden, "Solo administrador puede crear productos vía API.", nil)
			return
		}
		var payload struct {
			Name           string `json:"name"`
			Line           string `json:"line"`
			OwnerUserID    *int   `json:"owner_user_id"`
			Quantity       int    `json:"quantity"`
			SalePrice      int    `json:"sale_price"`
			AplicaCad      bool   `json:"aplica_caducidad"`
			FechaCaducidad string `json:"fecha_caducidad"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeAPIError(w, http.StatusBadRequest, "JSON inválido.", nil)
			return
		}
		payload.Name = strings.TrimSpace(payload.Name)
		payload.Line = strings.TrimSpace(payload.Line)
		payload.FechaCaducidad = strings.TrimSpace(payload.FechaCaducidad)
		activeLines, err := loadBusinessLines(db, true)
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
			writeAPIError(w, http.StatusInternalServerError, "No se pudo generar el SKU.", nil)
			return
		}
		now := time.Now().Format(time.RFC3339)
		if err := upsertProducto(tx, sku, payload.Name, payload.Line, now); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo guardar el producto.", nil)
			return
		}
		if _, err := tx.Exec(`UPDATE productos SET precio_venta = ?, owner_user_id = ? WHERE sku = ?`, float64(payload.SalePrice), ownerUserID, sku); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo guardar el producto.", nil)
			return
		}
		for j := 0; j < payload.Quantity; j++ {
			unitID := fmt.Sprintf("U-%s-%d", sku, time.Now().UnixNano()+int64(j))
			var cad any = nil
			if payload.AplicaCad && payload.FechaCaducidad != "" {
				cad = payload.FechaCaducidad
			}
			if _, err := tx.Exec(`INSERT INTO unidades (id, producto_id, estado, creado_en, caducidad) VALUES (?, ?, ?, ?, ?)`, unitID, sku, "Disponible", now, cad); err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudieron crear las unidades.", nil)
				return
			}
		}
		if err := logAuditEvent(tx, currentUser, "product_created", "product", sku, "api", withAPIAuditMetadata(r, map[string]any{
			"sku":           sku,
			"name":          payload.Name,
			"line":          payload.Line,
			"owner_user_id": ownerUserID,
			"cantidad":      payload.Quantity,
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
		writeAPIJSON(w, http.StatusCreated, map[string]any{"ok": true, "id": sku, "message": "Producto creado correctamente."})
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
			Sales:           salesData.Sales,
			CurrentUser:     currentUser,
			RangeStart:      startStr,
			RangeEnd:        endStr,
			RangeTotal:      salesData.RangeTotal,
			RangeCount:      salesData.RangeCount,
		}

		if err := tmpl.ExecuteTemplate(w, "dashboard.html", data); err != nil {
			http.Error(w, "Error al renderizar el dashboard", http.StatusInternalServerError)
		}
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
		if currentUser == nil || currentUser.Role != "admin" {
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
		salesDateExpr := "substr(v.fecha, 1, 10)"
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
		activePaymentMethods, err := loadPaymentMethods(db, true)
		if err != nil {
			http.Error(w, "Error al cargar métodos de pago", http.StatusInternalServerError)
			return
		}
		productsSnapshot, err := loadProductos(db)
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

		inventoryProducts := make([]inventoryProduct, 0, len(productsSnapshot))
		for _, product := range productsSnapshot {
			rows, err := db.Query(`
					SELECT id, estado, creado_en, caducidad
					FROM unidades
					WHERE producto_id = ?
					ORDER BY creado_en, id`, product.ID)
			if err != nil {
				http.Error(w, "Error al consultar unidades", http.StatusInternalServerError)
				return
			}

			units := []inventoryUnit{}
			availableCount := 0
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
				if reservedCount > 0 {
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
				ID:                product.ID,
				Name:              product.Name,
				Line:              product.Line,
				EstadoLabel:       estadoLabel,
				EstadoClass:       estadoClass,
				Disponible:        availableCount,
				Unidades:          units,
				DisabledSale:      availableCount == 0,
				FechaIngreso:      formatDateWithSettings(fechaIngresoISO),
				MesesEnStock:      mesesEnStock,
				AlertaPermanencia: alertaPermanencia,
				SalePrice:         product.SalePrice,
			})
		}
		_, movementEnabledMap, err := loadMovementSettings(db)
		if err != nil {
			http.Error(w, "Error al cargar tipos de movimiento", http.StatusInternalServerError)
			return
		}
		data := inventoryPageData{
			Title:       "Seguimiento de existencias",
			Subtitle:    "",
			RoutePrefix: "",
			Flash:       flash,
			MetodoPagos: paymentMethodNames(activePaymentMethods),
			Products:    inventoryProducts,
			CanSell:     movementEnabled(movementEnabledMap, "venta"),
			CanSwap:     movementEnabled(movementEnabledMap, "cambio"),
			CanRetoma:   movementEnabled(movementEnabledMap, "retoma"),
			CurrentUser: currentUser,
		}
		if err := tmpl.ExecuteTemplate(w, "inventario.html", data); err != nil {
			http.Error(w, "Error al renderizar el template", http.StatusInternalServerError)
		}
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

		unitIDs, err := selectAndMarkUnitsByStatus(tx, productID, qty, "Reservada")
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

		unitIDs, err := selectAndMarkUnitsByStatus(tx, productID, qty, "Danada")
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

	mux.HandleFunc("/inventario/stock", func(w http.ResponseWriter, r *http.Request) {
		writeJSONError := func(status int, message string) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
		}

		currentUser := userFromContext(r)
		if currentUser == nil || (currentUser.Role != "admin" && currentUser.Role != "empleado") {
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
		targetValue := strings.TrimSpace(r.FormValue("cantidad"))
		nota := strings.TrimSpace(r.FormValue("nota"))
		priceValue := strings.TrimSpace(r.FormValue("precio_venta"))
		nameValue := strings.TrimSpace(r.FormValue("nombre"))
		target, err := strconv.Atoi(targetValue)
		if productID == "" || err != nil || target < 0 {
			writeJSONError(http.StatusBadRequest, "Cantidad objetivo inválida.")
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
		updatePrice := priceValue != ""
		newPrice := 0.0
		if updatePrice {
			parsed, err := parseCOPInteger(priceValue)
			if err != nil || parsed < 0 {
				writeJSONError(http.StatusBadRequest, "Precio de venta inválido.")
				return
			}
			newPrice = float64(parsed)
		}

		tx, err := db.Begin()
		if err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo iniciar la transacción.")
			return
		}
		defer tx.Rollback()

		rows, err := tx.Query(`
			SELECT id
			FROM unidades
			WHERE producto_id = ? AND estado IN ('Disponible', 'available')
			ORDER BY creado_en DESC, id DESC
		`, productID)
		if err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo consultar el stock actual.")
			return
		}
		availableIDs := make([]string, 0, 64)
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				writeJSONError(http.StatusInternalServerError, "No se pudo leer el stock actual.")
				return
			}
			availableIDs = append(availableIDs, id)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			writeJSONError(http.StatusInternalServerError, "No se pudo procesar el stock actual.")
			return
		}
		rows.Close()

		current := len(availableIDs)
		delta := target - current
		now := time.Now().Format(time.RFC3339)
		if delta > 0 {
			createdIDs := make([]string, 0, delta)
			baseID := time.Now().UnixNano()
			for i := 0; i < delta; i++ {
				unitID := fmt.Sprintf("U-%s-AJ-%d-%d", productID, baseID, i)
				if _, err := tx.Exec(
					`INSERT INTO unidades (id, producto_id, estado, creado_en, caducidad) VALUES (?, ?, ?, ?, ?)`,
					unitID, productID, "Disponible", now, nil,
				); err != nil {
					writeJSONError(http.StatusInternalServerError, "No se pudo incrementar el stock.")
					return
				}
				createdIDs = append(createdIDs, unitID)
			}
			logNote := nota
			if logNote == "" {
				logNote = fmt.Sprintf("Ajuste manual de stock: %d -> %d", current, target)
			}
			if err := logMovimientos(tx, productID, createdIDs, "ajuste_stock_entrada", logNote, currentUser, now); err != nil {
				writeJSONError(http.StatusInternalServerError, "No se pudo registrar el ajuste.")
				return
			}
		} else if delta < 0 {
			removeCount := -delta
			if removeCount > len(availableIDs) {
				writeJSONError(http.StatusBadRequest, "No hay stock suficiente para reducir a ese valor.")
				return
			}
			removeIDs := availableIDs[:removeCount]
			placeholders := make([]string, len(removeIDs))
			args := make([]any, 0, len(removeIDs)+1)
			for i, id := range removeIDs {
				placeholders[i] = "?"
				args = append(args, id)
			}
			args = append(args, productID)
			query := fmt.Sprintf(
				"DELETE FROM unidades WHERE id IN (%s) AND producto_id = ? AND estado IN ('Disponible', 'available')",
				strings.Join(placeholders, ","),
			)
			res, err := tx.Exec(query, args...)
			if err != nil {
				writeJSONError(http.StatusInternalServerError, "No se pudo reducir el stock.")
				return
			}
			affected, err := res.RowsAffected()
			if err != nil || int(affected) != removeCount {
				writeJSONError(http.StatusInternalServerError, "No se pudo confirmar el ajuste de stock.")
				return
			}
			logNote := nota
			if logNote == "" {
				logNote = fmt.Sprintf("Ajuste manual de stock: %d -> %d", current, target)
			}
			if err := logMovimientos(tx, productID, removeIDs, "ajuste_stock_salida", logNote, currentUser, now); err != nil {
				writeJSONError(http.StatusInternalServerError, "No se pudo registrar el ajuste.")
				return
			}
		}
		if updatePrice {
			res, err := tx.Exec(`UPDATE productos SET precio_venta = ? WHERE sku = ?`, newPrice, productID)
			if err != nil {
				writeJSONError(http.StatusInternalServerError, "No se pudo actualizar el precio de venta.")
				return
			}
			affected, err := res.RowsAffected()
			if err != nil || affected == 0 {
				writeJSONError(http.StatusBadRequest, "Producto inválido para actualizar precio.")
				return
			}
		}
		updateName := nameValue != ""
		if updateName {
			res, err := tx.Exec(`UPDATE productos SET nombre = ? WHERE sku = ?`, nameValue, productID)
			if err != nil {
				writeJSONError(http.StatusInternalServerError, "No se pudo actualizar el nombre del producto.")
				return
			}
			affected, err := res.RowsAffected()
			if err != nil || affected == 0 {
				writeJSONError(http.StatusBadRequest, "Producto inválido para actualizar nombre.")
				return
			}
		}
		if updatePrice || updateName {
			payload := map[string]any{
				"product_id": productID,
			}
			if updatePrice {
				payload["precio_venta"] = newPrice
			}
			if updateName {
				payload["nombre"] = nameValue
			}
			if err := logAuditEvent(tx, currentUser, "product_updated", "product", productID, "manual", payload); err != nil {
				writeJSONError(http.StatusInternalServerError, "No se pudo registrar la auditoría del producto.")
				return
			}
		}

		if err := tx.Commit(); err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo confirmar la transacción.")
			return
		}
		if updatePrice || updateName {
			productsMu.Lock()
			for idx := range products {
				if products[idx].ID == productID {
					if updatePrice {
						products[idx].SalePrice = newPrice
					}
					if updateName {
						products[idx].Name = nameValue
					}
					break
				}
			}
			productsMu.Unlock()
		}
		message := "Stock ajustado correctamente."
		if delta == 0 && !updatePrice {
			message = "Stock sin cambios."
		} else if delta == 0 && updatePrice {
			message = "Precio de venta actualizado correctamente."
		} else if delta != 0 && updatePrice {
			message = "Stock y precio de venta actualizados correctamente."
		}
		if updateName && delta == 0 && !updatePrice {
			message = "Nombre del producto actualizado correctamente."
		} else if updateName && delta == 0 && updatePrice {
			message = "Nombre y precio de venta actualizados correctamente."
		} else if updateName && delta != 0 && !updatePrice {
			message = "Stock y nombre del producto actualizados correctamente."
		} else if updateName && delta != 0 && updatePrice {
			message = "Stock, nombre y precio de venta actualizados correctamente."
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":          true,
			"producto_id": productID,
			"actual":      target,
			"objetivo":    target,
			"delta":       delta,
			"mensaje":     message,
		})
	})

	mux.HandleFunc("/inventario/producto/eliminar", func(w http.ResponseWriter, r *http.Request) {
		writeJSONError := func(status int, message string) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
		}

		currentUser := userFromContext(r)
		if currentUser == nil || currentUser.Role != "admin" {
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

		// Legacy compatibility: if the table exists, clear price history rows before deleting the product.
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
		sku := strings.TrimSpace(r.URL.Query().Get("sku"))
		if sku == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "Falta sku."})
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
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "sku": sku, "precio_venta": precioVenta})
	})

	mux.HandleFunc("/api/settings/business", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		settings := currentBusinessSettings()
		writeAPIJSON(w, http.StatusOK, map[string]any{
			"ok": true,
			"settings": map[string]any{
				"business_name": settings.BusinessName,
				"logo_path":     settings.LogoPath,
				"primary_color": settings.PrimaryColor,
				"currency":      settings.Currency,
				"date_format":   settings.DateFormat,
			},
		})
	})

	mux.HandleFunc("/api/products/search", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		q := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("q")))
		productsSnapshot, err := loadProductos(db)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar los productos.", nil)
			return
		}
		productsSnapshot = filterProductsForUser(productsSnapshot, userFromContext(r))
		items := make([]map[string]any, 0, len(productsSnapshot))
		for _, product := range productsSnapshot {
			if q != "" {
				haystack := strings.ToLower(product.ID + " " + product.Name + " " + product.Line)
				if !strings.Contains(haystack, q) {
					continue
				}
			}
			var owner any = nil
			if product.HasOwner {
				owner = product.OwnerUserID
			}
			items = append(items, map[string]any{
				"id":            product.ID,
				"name":          product.Name,
				"line":          product.Line,
				"fecha_ingreso": formatDateWithSettings(product.FechaIngreso),
				"sale_price":    product.SalePrice,
				"owner_user_id": owner,
			})
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "items": items, "count": len(items)})
	})

	mux.HandleFunc("/api/inventory", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		productsSnapshot, err := loadProductos(db)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo cargar el inventario.", nil)
			return
		}
		productsSnapshot = filterProductsForUser(productsSnapshot, userFromContext(r))
		items := make([]map[string]any, 0, len(productsSnapshot))
		for _, product := range productsSnapshot {
			var available, reserved, swapped, damaged int
			rows, err := db.Query(`SELECT estado, COUNT(*) FROM unidades WHERE producto_id = ? GROUP BY estado`, product.ID)
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
			items = append(items, map[string]any{
				"id":            product.ID,
				"name":          product.Name,
				"line":          product.Line,
				"available":     available,
				"reserved":      reserved,
				"swapped":       swapped,
				"damaged":       damaged,
				"sale_price":    product.SalePrice,
				"owner_user_id": owner,
			})
		}
		writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "items": items, "count": len(items)})
	})

	mux.HandleFunc("/api/sales/recent", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		visibilitySQL, visibilityArgs := productVisibilityPredicate("p", userFromContext(r))
		rows, err := db.Query(`
			SELECT v.id, v.fecha, v.producto_id, COALESCE(p.nombre, v.producto_id), v.cantidad, v.precio_final, v.metodo_pago
			FROM ventas v
			LEFT JOIN productos p ON p.sku = v.producto_id
			WHERE `+visibilitySQL+`
			ORDER BY v.fecha DESC, v.id DESC
			LIMIT 50
		`, visibilityArgs...)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar las ventas.", nil)
			return
		}
		defer rows.Close()
		items := make([]map[string]any, 0, 50)
		for rows.Next() {
			var id int
			var fecha, productoID, producto, metodo string
			var cantidad int
			var precioFinal float64
			if err := rows.Scan(&id, &fecha, &productoID, &producto, &cantidad, &precioFinal, &metodo); err != nil {
				writeAPIError(w, http.StatusInternalServerError, "No se pudieron leer las ventas.", nil)
				return
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
		writeAPIJSON(w, http.StatusOK, map[string]any{"ok": true, "items": items, "count": len(items)})
	})

	mux.HandleFunc("/venta/new", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		_, movementEnabledMap, err := loadMovementSettings(db)
		if err != nil {
			http.Error(w, "Error al cargar tipos de movimiento", http.StatusInternalServerError)
			return
		}
		if !movementEnabled(movementEnabledMap, "venta") {
			redirectWithMessage(w, r, "/inventario", "", "La venta está deshabilitada en Configuración.")
			return
		}
		activePaymentMethods, err := loadPaymentMethods(db, true)
		if err != nil {
			http.Error(w, "Error al cargar métodos de pago", http.StatusInternalServerError)
			return
		}
		paymentMethodNamesActive := paymentMethodNames(activePaymentMethods)

		productsMu.RLock()
		productsSnapshot := make([]productOption, len(products))
		copy(productsSnapshot, products)
		productsMu.RUnlock()
		productsSnapshot = filterProductsForUser(productsSnapshot, currentUser)

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

		stockByProd, err := availableCountsByProduct(db)
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

		if err := tmpl.ExecuteTemplate(w, "venta_new.html", data); err != nil {
			http.Error(w, "Error al renderizar el template", http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("/cambio/new", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		_, movementEnabledMap, err := loadMovementSettings(db)
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

		availableUnits, err := availableUnitsByProduct(db, productID)
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

		if err := tmpl.ExecuteTemplate(w, "cambio_new.html", data); err != nil {
			http.Error(w, "Error al renderizar el template", http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("/retoma/new", func(w http.ResponseWriter, r *http.Request) {
		_, movementEnabledMap, err := loadMovementSettings(db)
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

	mux.HandleFunc("/api/sales", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		currentUser := userFromContext(r)
		_, movementEnabledMap, err := loadMovementSettings(db)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "Error al cargar tipos de movimiento.", nil)
			return
		}
		if !movementEnabled(movementEnabledMap, "venta") {
			writeAPIError(w, http.StatusForbidden, "La venta está deshabilitada en Configuración.", nil)
			return
		}
		var payload struct {
			ProductID     string  `json:"product_id"`
			Quantity      int     `json:"quantity"`
			PaymentMethod string  `json:"payment_method"`
			UnitPrice     float64 `json:"unit_price"`
			Total         float64 `json:"total"`
			Notes         string  `json:"notes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeAPIError(w, http.StatusBadRequest, "JSON inválido.", nil)
			return
		}
		payload.ProductID = strings.TrimSpace(payload.ProductID)
		payload.PaymentMethod = strings.TrimSpace(payload.PaymentMethod)
		payload.Notes = strings.TrimSpace(payload.Notes)
		activePaymentMethods, err := loadPaymentMethods(db, true)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudieron cargar los métodos de pago.", nil)
			return
		}
		paymentMethodOptions := paymentMethodNames(activePaymentMethods)
		productsMu.RLock()
		productsSnapshot := make([]productOption, len(products))
		copy(productsSnapshot, products)
		productsMu.RUnlock()
		productsSnapshot = filterProductsForUser(productsSnapshot, currentUser)
		stockByProd, err := availableCountsByProduct(db)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "Error al consultar stock.", nil)
			return
		}
		fields := map[string]string{}
		if payload.ProductID == "" {
			fields["product_id"] = "Selecciona un producto válido."
		}
		if payload.Quantity <= 0 {
			fields["quantity"] = "La cantidad debe ser un número positivo."
		}
		if allowed, err := productAccessibleByID(db, currentUser, payload.ProductID); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "No se pudo validar acceso al producto.", nil)
			return
		} else if !allowed {
			fields["product_id"] = "No tienes acceso a este producto."
		}
		selectedProduct, ok := findProduct(productsSnapshot, payload.ProductID)
		if !ok && len(productsSnapshot) > 0 {
			selectedProduct = productsSnapshot[0]
		}
		validMethod := false
		for _, method := range paymentMethodOptions {
			if payload.PaymentMethod == method {
				validMethod = true
				break
			}
		}
		if !validMethod {
			fields["payment_method"] = "Selecciona un método de pago válido."
		}
		if payload.ProductID != "" && payload.Quantity > 0 {
			if available := stockByProd[payload.ProductID]; available > 0 && payload.Quantity > available {
				fields["quantity"] = "No hay stock disponible suficiente para completar la venta."
			}
		}
		unitPrice := payload.UnitPrice
		if payload.Total > 0 && payload.Quantity > 0 {
			unitPrice = payload.Total / float64(payload.Quantity)
		}
		if unitPrice <= 0 {
			fields["unit_price"] = "Ingresa unit_price o total mayor a 0."
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
		soldUnitIDs, err := selectAndMarkUnitsSold(tx, payload.ProductID, payload.Quantity)
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
		if _, err := tx.Exec(`INSERT INTO ventas (producto_id, cantidad, precio_final, metodo_pago, notas, fecha) VALUES (?, ?, ?, ?, ?, ?)`, payload.ProductID, payload.Quantity, unitPrice, payload.PaymentMethod, payload.Notes, now); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "Error al registrar la venta.", nil)
			return
		}
		if err := logAuditEvent(tx, currentUser, "sale_registered", "sale", payload.ProductID, "api", withAPIAuditMetadata(r, map[string]any{
			"producto_id": payload.ProductID,
			"producto":    selectedProduct.Name,
			"cantidad":    payload.Quantity,
			"metodo_pago": payload.PaymentMethod,
			"total":       unitPrice * float64(payload.Quantity),
		})); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "Error al registrar la auditoría de la venta.", nil)
			return
		}
		if err := tx.Commit(); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "Error al confirmar la venta.", nil)
			return
		}
		writeAPIJSON(w, http.StatusCreated, map[string]any{"ok": true, "product_id": payload.ProductID, "product_name": selectedProduct.Name, "quantity": payload.Quantity, "message": "Venta registrada correctamente."})
	})

	mux.HandleFunc("/api/swaps", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeAPIError(w, http.StatusMethodNotAllowed, "Método no permitido.", nil)
			return
		}
		currentUser := userFromContext(r)
		_, movementEnabledMap, err := loadMovementSettings(db)
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
		productsMu.RLock()
		productsSnapshot := make([]productOption, len(products))
		copy(productsSnapshot, products)
		productsMu.RUnlock()
		productsSnapshot = filterProductsForUser(productsSnapshot, currentUser)
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
			selectedProduct = productsSnapshot[0]
			payload.ProductID = selectedProduct.ID
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
		salientesMarcadas, err := selectAndMarkUnitsByStatus(tx, payload.ProductID, payload.Quantity, "Cambio")
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
			if _, err := tx.Exec(`INSERT INTO unidades (id, producto_id, estado, creado_en, caducidad) VALUES (?, ?, ?, ?, ?)`, unitID, incomingProductID, "Disponible", now, nil); err != nil {
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
	})

	mux.HandleFunc("/venta", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		wantsJSON := strings.Contains(r.Header.Get("Accept"), "application/json") || r.Header.Get("X-Requested-With") == "XMLHttpRequest"
		_, movementEnabledMap, err := loadMovementSettings(db)
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
		activePaymentMethods, err := loadPaymentMethods(db, true)
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

		productsMu.RLock()
		productsSnapshot := make([]productOption, len(products))
		copy(productsSnapshot, products)
		productsMu.RUnlock()
		productsSnapshot = filterProductsForUser(productsSnapshot, currentUser)

		stockByProd, err := availableCountsByProduct(db)
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
		qtyValue := r.FormValue("cantidad")
		precioValue := r.FormValue("precio_final_venta")
		valorVentaFinalValue := r.FormValue("valor_venta_final")
		metodoPago := r.FormValue("metodo_pago")
		notas := r.FormValue("notas")
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

		selectedProduct, ok := findProduct(productsSnapshot, productID)
		if !ok && len(productsSnapshot) > 0 {
			selectedProduct = productsSnapshot[0]
		}

		errors := make(map[string]string)
		cantidad, err := strconv.Atoi(qtyValue)
		if err != nil || cantidad <= 0 {
			errors["cantidad"] = "La cantidad debe ser un número positivo."
		}
		if productID == "" {
			errors["producto_id"] = "Selecciona un producto válido."
		}
		precioParsed := 0.0
		precioOk := false
		if strings.TrimSpace(precioValue) != "" {
			if parsed, err := strconv.ParseFloat(strings.TrimSpace(precioValue), 64); err == nil && parsed > 0 {
				precioParsed = parsed
				precioOk = true
			} else {
				errors["precio_final_venta"] = "El precio debe ser un número mayor a 0."
			}
		}

		valorFinalParsed := 0.0
		valorFinalOk := false
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

		if productID != "" && cantidad > 0 {
			if available := stockByProd[productID]; available > 0 && cantidad > available {
				errors["cantidad"] = "No hay stock disponible suficiente para completar la venta."
			}
		}

		if len(errors) > 0 {
			if wantsJSON {
				message := "Datos inválidos."
				// Pick the first field error as a message for the modal.
				for _, key := range []string{"producto_id", "cantidad", "valor_venta_final", "precio_final_venta", "metodo_pago"} {
					if msg, ok := errors[key]; ok && msg != "" {
						message = msg
						break
					}
				}
				writeJSONError(http.StatusBadRequest, message, errors)
				return
			}
			data := ventaFormData{
				Title:           "Registrar venta",
				ProductoID:      productID,
				ProductoNom:     selectedProduct.Name,
				Productos:       productsSnapshot,
				StockByProd:     stockByProd,
				Cantidad:        cantidad,
				PrecioFinal:     precioValue,
				ValorVentaFinal: valorVentaFinalValue,
				MetodoPago:      metodoPago,
				Notas:           notas,
				Errors:          errors,
				MetodoPagos:     paymentMethodOptions,
				CurrentUser:     currentUser,
			}
			w.WriteHeader(http.StatusBadRequest)
			if err := tmpl.ExecuteTemplate(w, "venta_new.html", data); err != nil {
				http.Error(w, "Error al renderizar el template", http.StatusInternalServerError)
			}
			return
		}

		precioFinal := precioParsed
		precioFinalText := precioValue
		if valorFinalOk && cantidad > 0 {
			precioFinal = valorFinalParsed / float64(cantidad)
			precioFinalText = fmt.Sprintf("%.2f", precioFinal)
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

		soldUnitIDs, err := selectAndMarkUnitsSold(tx, productID, cantidad)
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
				if err := tmpl.ExecuteTemplate(w, "venta_new.html", data); err != nil {
					http.Error(w, "Error al renderizar el template", http.StatusInternalServerError)
				}
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
		if err := logMovimientos(tx, productID, soldUnitIDs, "venta", notas, currentUser, now); err != nil {
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

		if _, err := tx.Exec(
			`INSERT INTO ventas (producto_id, cantidad, precio_final, metodo_pago, notas, fecha)
			VALUES (?, ?, ?, ?, ?, ?)`,
			productID, cantidad, precioFinal, metodoPago, notas, now,
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
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":           true,
				"producto_id":  productID,
				"producto_nom": selectedProduct.Name,
				"cantidad":     cantidad,
				"mensaje":      "Venta registrada correctamente.",
			})
			return
		}

		confirmData := ventaConfirmData{
			Title:           "Venta registrada",
			ProductoID:      productID,
			ProductoNom:     selectedProduct.Name,
			Cantidad:        cantidad,
			PrecioFinal:     precioFinalText,
			ValorVentaFinal: valorVentaFinalValue,
			MetodoPago:      metodoPago,
			Notas:           notas,
			CurrentUser:     currentUser,
		}
		if err := tmpl.ExecuteTemplate(w, "venta_confirm.html", confirmData); err != nil {
			http.Error(w, "Error al renderizar el template", http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("/cambio", func(w http.ResponseWriter, r *http.Request) {
		currentUser := userFromContext(r)
		_, movementEnabledMap, err := loadMovementSettings(db)
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

		availableUnits, err := availableUnitsByProduct(db, productID)
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
			if err := tmpl.ExecuteTemplate(w, "cambio_new.html", data); err != nil {
				http.Error(w, "Error al renderizar el template", http.StatusInternalServerError)
			}
			return
		}

		tx, err := db.Begin()
		if err != nil {
			http.Error(w, "Error al iniciar el cambio", http.StatusInternalServerError)
			return
		}

		outgoingQty := len(salientes)
		salientesMarcadas, err := selectAndMarkUnitsByStatus(tx, productID, outgoingQty, "Cambio")
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
				if err := tmpl.ExecuteTemplate(w, "cambio_new.html", data); err != nil {
					http.Error(w, "Error al renderizar el template", http.StatusInternalServerError)
				}
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

		if err := tmpl.ExecuteTemplate(w, "cambio_confirm.html", confirmData); err != nil {
			http.Error(w, "Error al renderizar el template", http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("/csv/template", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if err := tmpl.ExecuteTemplate(w, "csv_template.html", struct {
			Title       string
			Subtitle    string
			CurrentUser *User
		}{
			Title:       "Plantilla CSV - Carga masiva",
			Subtitle:    "",
			CurrentUser: userFromContext(r),
		}); err != nil {
			http.Error(w, "Error al renderizar plantilla CSV", http.StatusInternalServerError)
		}
	}))

	mux.HandleFunc("/csv/export", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		if err := tmpl.ExecuteTemplate(w, "csv_export.html", struct {
			Title       string
			Subtitle    string
			CurrentUser *User
		}{
			Title:       "Exportaciones CSV",
			Subtitle:    "",
			CurrentUser: userFromContext(r),
		}); err != nil {
			http.Error(w, "Error al renderizar exportaciones CSV", http.StatusInternalServerError)
		}
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
		required := []string{"sku", "linea", "nombre", "cantidad", "precio_base", "precio_venta", "precio_consultora"}
		for _, col := range required {
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
		tx, err := db.Begin()
		if err != nil {
			writeJSONError(http.StatusInternalServerError, "No se pudo iniciar la transacción.")
			return
		}

		now := time.Now().Format(time.RFC3339)
		for i, row := range records[1:] {
			rowIndex := i + 1 // matches the UI preview index (1-based excluding header)
			sku := get(row, "sku")
			linea := get(row, "linea")
			nombre := get(row, "nombre")
			cantidadRaw := get(row, "cantidad")
			if cantidadRaw == "-" {
				cantidadRaw = "0"
			}

			if sku == "" || linea == "" || nombre == "" {
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: sku, Error: "SKU, línea y nombre son obligatorios."})
				continue
			}

			cantidad, err := parseCSVInt(cantidadRaw)
			if err != nil || cantidad < 0 {
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: sku, Error: "Cantidad inválida (debe ser 0 o mayor)."})
				continue
			}

			// Validate numeric columns.
			if _, err := parseCSVFloat(get(row, "precio_base")); err != nil {
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: sku, Error: "Precio base inválido."})
				continue
			}
			precioVenta, err := parseCSVFloat(get(row, "precio_venta"))
			if err != nil {
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: sku, Error: "Precio venta inválido."})
				continue
			}
			if _, err := parseCSVFloat(get(row, "precio_consultora")); err != nil {
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: sku, Error: "Precio consultora inválido."})
				continue
			}

			fechaCaducidad := get(row, "fecha_caducidad")
			aplicaCadRaw := get(row, "aplica_caducidad")
			aplicaCad := false
			if aplicaCadRaw != "" {
				parsed, err := parseCSVBool(aplicaCadRaw)
				if err != nil {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: sku, Error: "Aplica caducidad debe ser true/false."})
					continue
				}
				aplicaCad = parsed
			}
			if aplicaCad && fechaCaducidad == "" {
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: sku, Error: "Fecha caducidad requerida si aplica."})
				continue
			}
			if fechaCaducidad != "" {
				if _, err := time.Parse("2006-01-02", fechaCaducidad); err != nil {
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: sku, Error: "Fecha caducidad debe ser YYYY-MM-DD."})
					continue
				}
			}

			if _, err := tx.Exec("SAVEPOINT csv_row"); err != nil {
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: sku, Error: "Error al preparar la fila."})
				continue
			}

			// Persist catalog.
			if err := upsertProducto(tx, sku, nombre, linea, now); err != nil {
				_, _ = tx.Exec("ROLLBACK TO csv_row")
				_, _ = tx.Exec("RELEASE csv_row")
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: sku, Error: "Error al guardar producto."})
				continue
			}
			if _, err := tx.Exec(`UPDATE productos SET precio_venta = ? WHERE sku = ?`, precioVenta, sku); err != nil {
				_, _ = tx.Exec("ROLLBACK TO csv_row")
				_, _ = tx.Exec("RELEASE csv_row")
				resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: sku, Error: "Error al guardar precio de venta."})
				continue
			}

			// Update in-memory catalog (used by inventario/cambio screens).
			productsMu.Lock()
			found := false
			for idx := range products {
				if products[idx].ID == sku {
					products[idx].Name = nombre
					products[idx].Line = linea
					products[idx].SalePrice = precioVenta
					found = true
					break
				}
			}
			if !found {
				products = append(products, productOption{
					ID:           sku,
					Name:         nombre,
					Line:         linea,
					FechaIngreso: time.Now().Format("2006-01-02"),
					SalePrice:    precioVenta,
				})
				resp.CreatedProducts++
			} else {
				resp.UpdatedProducts++
			}
			productsMu.Unlock()

			// Insert units into DB (inventory source of truth).
			baseID := time.Now().UnixNano()
			rowFailed := false
			for j := 0; j < cantidad; j++ {
				unitID := fmt.Sprintf("U-%s-%d", sku, baseID+int64(j))
				var caducidad any = nil
				if aplicaCad && fechaCaducidad != "" {
					caducidad = fechaCaducidad
				}
				if _, err := tx.Exec(
					`INSERT INTO unidades (id, producto_id, estado, creado_en, caducidad) VALUES (?, ?, ?, ?, ?)`,
					unitID, sku, "Disponible", now, caducidad,
				); err != nil {
					_, _ = tx.Exec("ROLLBACK TO csv_row")
					_, _ = tx.Exec("RELEASE csv_row")
					resp.FailedRows = append(resp.FailedRows, csvFailedRow{Row: rowIndex, SKU: sku, Error: "Error al crear unidades."})
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
