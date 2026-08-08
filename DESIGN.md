# StockiAPP Visual System

## North Star

StockiAPP is a calm operational workspace: dense business information should feel precise, legible, and dependable. The interface uses structure and alignment to create presence instead of decorative depth.

## Visual Language

- Restraint: neutral surfaces, thin borders, and one tenant-configurable accent.
- Light theme: warm bone page and work surfaces with muted taupe secondary surfaces.
- Dark theme: charcoal page and surfaces with a luminous emerald accent for clear separation.
- Typography: Inter for interface content; Roboto Mono only for code, IDs, tokens, and measurements.
- Radius: 8px controls and small components, 10px standard panels, 12px larger surfaces, pills only for statuses.
- Elevation: no shadow on routine cards, tables, filters, or navigation; shadows are reserved for menus, drawers, and modals.
- Gradients: not used as routine surface decoration. Printable and document-like flows may retain their own paper treatment.

## Layout

- The desktop shell has an integrated sidebar and a quiet topbar separated by 1px borders.
- The expanded sidebar is 248px; the collapsed sidebar is 72px.
- Content starts below the topbar and keeps a 24px desktop gutter.
- Mobile keeps a 60px top bar and a drawer with 44px touch targets.
- Operational screens use a shared `.page-heading` with one title, short context, and at most one primary action.

## Components

- Primary buttons use the business accent and are reserved for the next important action.
- Secondary actions use neutral surfaces and borders.
- Destructive actions use semantic danger colors and remain contextual or confirmed.
- Tables use compact rows, quiet uppercase headers, and thin separators.
- Status chips use text plus semantic color; meaning never depends on color alone.
- Modals and menus are the only routinely elevated surfaces.

## Accessibility And Behavior

- Preserve minimum 44x44px touch targets on tablet and mobile.
- Preserve visible focus rings, keyboard navigation, reduced-motion behavior, and modal focus restoration.
- Keep the existing `data-theme` and localStorage theme preference.
- Keep tenant branding through the existing `businessPrimary*` template functions in both themes; dark mode uses an accessible luminous variant for custom accents.

## Implementation Rules

- `templates/partials/app_styles.html` is the source of truth for tokens and shared primitives.
- `templates/partials/header.html` is the source of truth for the app shell and navigation.
- Template-local CSS is allowed only for behavior or composition specific to one screen.
- Do not change backend concepts, routes, API contracts, ownership, tenant isolation, or audit behavior for visual work.
- Printable receipts, invoices, labels, and the standalone manual are exceptions when paper/document readability requires a different treatment.
