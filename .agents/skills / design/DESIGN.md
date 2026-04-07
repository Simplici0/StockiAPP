DESIGN
# Design System Specification: Obsidian Core for StockiAPP

## 1. Overview and Creative North Star
**Creative North Star: "The Digital Obsidian"**

Obsidian Core is the visual system for StockiAPP: a server-rendered operational workspace built in Go, with shared HTML templates and CSS variables. The goal is to make dense business data feel precise, calm, and high-contrast without adding runtime cost or visual clutter.

### Core Principles
- Depth and dimension through layered surfaces, subtle borders, and restrained shadows.
- Luminous data for critical metrics and charts, using controlled accent color and clear contrast.
- Functional minimalism: remove non-essential chrome, decorative noise, and explanatory copy that does not help operation.
- Performance first: prefer shared CSS, stable template partials, and static states over JS-heavy effects.

---

## 2. Implementation Constraints for StockiAPP

### Rendering Model
- The app is SSR-first with Go templates.
- Use shared partials and global CSS tokens before introducing local overrides.
- Avoid UI patterns that require hydration, component frameworks, or layout thrash.
- Keep interactive behavior simple enough to run with plain JavaScript and native browser primitives.

### Visual Cost Rules
- Prefer CSS variables, borders, opacity, and color shifts over heavy blur or layered effects.
- Use glow only where it improves data emphasis, mainly on charts and key metrics in dark mode.
- Avoid large animated backgrounds, parallax, or expensive paint effects.
- Keep theme switching and hover/focus states cheap and predictable.

### Source of Truth
- Shared theme tokens live in the app shell and are reused across templates.
- Local overrides are allowed only when a screen has a documented exception.
- Templates should inherit the system by default instead of redefining it.

---

## 3. Visual Foundation

### Dark Mode
- Primary background: `#060e20`
- Surface level 1: `#0f1930`
- Surface level 2: `#16213e`
- Accent/action: `#39b8fd`
- Secondary accent: `#dee5ff`
- Borders: subtle, cool, and low-opacity

### Light Mode
- Primary background: `#f8fafc`
- Surface level 1: `#ffffff`
- Surface level 2: `#f1f5f9`
- Accent/action: `#0284c7`
- Secondary accent: `#1e293b`
- Borders: `#e2e8f0`

### Typography
- Primary font: `Inter`
- Hero metrics: 32px, bold
- Section headers: 18px, semibold
- Body text: 14px, regular
- Labels and subtext: 12px, medium

### Typographic Rules
- Keep brand and page title dominant.
- Prefer short labels over explanatory paragraphs.
- Use muted text only for secondary guidance, not for critical values.

---

## 4. UI Components and Layout

### Navigation
- Side navigation should be visually stable and compact.
- Active states must be obvious without adding noise.
- Use one accent color for active items and selected states.

### Surfaces
- Cards, panels, tables, modals, and dropdowns must share the same surface logic.
- Use consistent radius, borders, and shadow depth across the app.
- Avoid decorative cards when plain layout is enough.

### Data Displays
- Charts should be crisp and legible, with slightly stronger strokes in light mode.
- In dark mode, use subtle glow only for the most important data series.
- Keep table rows, badges, and status chips readable at a glance.

### Forms and Controls
- Inputs, selects, buttons, toggles, and accordions must work in both themes without relying on browser defaults.
- Focus, hover, active, disabled, success, warning, and error states must remain readable.

---

## 5. Operational UI Rules

### Product Surfaces
- This design system is for dashboards, admin tools, and operational screens, not marketing pages.
- Prioritize orientation, status, and action over mood, promise, or storytelling.
- Each screen should answer: what is this, what can I do here, and what changed.

### Copy Rules
- Keep copy short and functional.
- Remove redundant subtitles and demo-like explanatory text.
- Let headings, counts, tables, and actions carry the meaning.

### Motion Rules
- Use only light motion for presence and affordance.
- Allowed motion: theme transitions, hover states, accordion open/close, and soft page entry.
- Avoid motion that changes layout structure or demands GPU-heavy effects.

---

## 6. Theme Switching
- Theme switching should be global and minimal.
- Use a `dark` class or `data-theme` attribute on `html` or `body`.
- Persist the preference simply, with `localStorage` or equivalent lightweight state.
- Respect the OS preference on first load.
- The switch must be compact, readable, and not dominate the header.

---

## 7. Exceptions
- Printable or document-like flows may use a paper-first treatment where needed, but they must still inherit the theme safely.
- Manual, receipt, invoice, and preview surfaces may override shell styling only when necessary for legibility.
- If a template needs a special treatment, document the exception and keep it local.

---

## 8. Implementation Notes
- Preferred approach: shared CSS variables, reusable partials, and small template-specific overrides.
- Avoid introducing Tailwind or a parallel design framework unless the app architecture is explicitly changed.
- Keep shadows soft in light mode and controlled in dark mode.
- Prefer stable, maintainable CSS over clever but expensive effects.
