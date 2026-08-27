(function () {
  const root = document.querySelector("[data-sale-cart]");
  if (!root) return;

  const storageKey = root.dataset.saleCartKey || "stocki-sale-cart";
  let memoryCart = [];

  function readCart() {
    try {
      const parsed = JSON.parse(window.sessionStorage.getItem(storageKey) || "[]");
      return Array.isArray(parsed) ? parsed : [];
    } catch (_) {
      return memoryCart;
    }
  }

  function writeCart(cart) {
    memoryCart = cart;
    try {
      window.sessionStorage.setItem(storageKey, JSON.stringify(cart));
    } catch (_) {}
    document.dispatchEvent(new CustomEvent("stocki:cart-changed", { detail: cart }));
  }

  function number(value) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function money(value) {
    return new Intl.NumberFormat("es-CO", {
      maximumFractionDigits: 0,
    }).format(Math.round(number(value)));
  }

  const locationCache = new Map();
  const locationRequests = new Map();
  let renderGeneration = 0;

  function locationKey(value) {
    return String(value || "").trim().toLowerCase();
  }

  function locationInputValue(value) {
    return String(value || "").trim() || "__unassigned__";
  }

  function locationValue(value) {
    return value === "__unassigned__" ? "" : String(value || "").trim();
  }

  function persistCart(cart) {
    memoryCart = cart;
    try {
      window.sessionStorage.setItem(storageKey, JSON.stringify(cart));
    } catch (_) {}
  }

  async function fetchLocations(productID) {
    const key = String(productID || "").trim();
    if (!key) return [];
    if (locationCache.has(key)) return locationCache.get(key);
    if (locationRequests.has(key)) return locationRequests.get(key);
    const request = fetch(`/api/inventory/locations?product_id=${encodeURIComponent(key)}`, {
      headers: { Accept: "application/json" },
      credentials: "same-origin",
    })
      .then(async (response) => {
        const data = await response.json().catch(() => ({}));
        if (!response.ok || !data.ok || !Array.isArray(data.locations)) {
          throw new Error(data.error || "No se pudieron cargar las ubicaciones.");
        }
        locationCache.set(key, data.locations);
        return data.locations;
      })
      .finally(() => locationRequests.delete(key));
    locationRequests.set(key, request);
    return request;
  }

  function availableLocations(locations) {
    return (Array.isArray(locations) ? locations : [])
      .filter((location) => number(location.available) > 0);
  }

  function suggestedAllocations(locations, item) {
    const quantity = Math.max(0, Number(item.quantity) || 0);
    const entries = availableLocations(locations);
    const initial = new Map();
    let initialTotal = 0;
    (Array.isArray(item.allocations) ? item.allocations : []).forEach((allocation) => {
      const amount = Math.max(0, Number(allocation?.quantity) || 0);
      if (amount <= 0) return;
      const key = locationKey(allocation.location);
      if (initial.has(key)) return;
      const entry = entries.find((location) => locationKey(location.location) === key);
      if (!entry || amount > number(entry.available)) return;
      initial.set(key, amount);
      initialTotal += amount;
    });
    if (initialTotal === quantity && initial.size > 0) {
      return entries
        .map((entry) => ({
          location: String(entry.location || "").trim(),
          quantity: number(initial.get(locationKey(entry.location))),
        }))
        .filter((allocation) => allocation.quantity > 0);
    }

    let remaining = quantity;
    return entries.reduce((result, entry) => {
      if (remaining <= 0) return result;
      const amount = Math.min(number(entry.available), remaining);
      if (amount > 0) {
        result.push({ location: String(entry.location || "").trim(), quantity: amount });
        remaining -= amount;
      }
      return result;
    }, []);
  }

  function allocationTotal(container) {
    return Array.from(container?.querySelectorAll("[data-cart-allocation-input]") || [])
      .reduce((total, input) => total + Math.max(0, Number(input.value) || 0), 0);
  }

  function updateAllocationTotal(container, quantity) {
    const totalNode = container?.querySelector("[data-cart-allocation-total]");
    if (!totalNode) return;
    const assigned = allocationTotal(container);
    const target = Math.max(0, Number(quantity) || 0);
    totalNode.textContent = `Asignado: ${assigned} de ${target}`;
    totalNode.dataset.invalid = assigned === target ? "false" : "true";
  }

  function persistLineAllocations(productID, container) {
    const allocations = [];
    container?.querySelectorAll("[data-cart-allocation-input]").forEach((input) => {
      const quantity = Math.max(0, Number(input.value) || 0);
      if (quantity <= 0) return;
      allocations.push({
        location: locationValue(input.dataset.locationValue),
        quantity,
      });
    });
    const cart = readCart();
    const item = cart.find((entry) => String(entry.product_id) === String(productID));
    if (!item) return;
    item.allocations = allocations;
    persistCart(cart);
    renderCheckoutSummary(cart);
    updateAllocationTotal(container, item.quantity);
  }

  async function renderLineAllocations(item, container, generation) {
    if (!container) return;
    const token = String(Number(container.dataset.renderToken || 0) + 1);
    container.dataset.renderToken = token;
    container.hidden = false;
    const rows = container.querySelector("[data-cart-allocation-rows]");
    if (!rows) return;
    rows.replaceChildren();
    const loading = document.createElement("span");
    loading.className = "sale-location-allocation-loading";
    loading.textContent = "Cargando stock por ubicación...";
    rows.appendChild(loading);
    try {
      const locations = await fetchLocations(item.product_id);
      if (generation !== renderGeneration || container.dataset.renderToken !== token) return;
      const entries = availableLocations(locations);
      rows.replaceChildren();
      if (!entries.length) {
        const empty = document.createElement("span");
        empty.className = "sale-location-allocation-error";
        empty.textContent = "No hay stock disponible por ubicación.";
        rows.appendChild(empty);
        updateAllocationTotal(container, item.quantity);
        return;
      }
      const suggested = new Map(
        suggestedAllocations(locations, item).map((allocation) => [
          locationKey(allocation.location),
          allocation.quantity,
        ]),
      );
      entries.forEach((entry) => {
        const row = document.createElement("div");
        row.className = "sale-location-allocation-row";
        const label = document.createElement("label");
        label.textContent = entry.label || "Sin ubicación";
        const available = document.createElement("small");
        available.textContent = `${number(entry.available)} disponibles`;
        label.appendChild(available);
        const input = document.createElement("input");
        input.type = "number";
        input.min = "0";
        input.max = String(number(entry.available));
        input.step = "1";
        input.value = String(suggested.get(locationKey(entry.location)) || 0);
        input.dataset.cartAllocationInput = "true";
        input.dataset.locationValue = locationInputValue(entry.location);
        input.setAttribute("aria-label", `${entry.label || "Sin ubicación"} para ${item.product_name || item.product_id}`);
        input.addEventListener("input", () => {
          persistLineAllocations(item.product_id, container);
        });
        row.append(label, input);
        rows.appendChild(row);
      });
      updateAllocationTotal(container, item.quantity);
      persistLineAllocations(item.product_id, container);
    } catch (error) {
      if (container.dataset.renderToken !== token) return;
      rows.replaceChildren();
      const failure = document.createElement("span");
      failure.className = "sale-location-allocation-error";
      failure.textContent = error?.message || "No se pudieron cargar las ubicaciones.";
      rows.appendChild(failure);
      updateAllocationTotal(container, item.quantity);
    }
  }

  function totals(cart) {
    return cart.reduce(
      (result, item) => {
        result.itemCount += 1;
        result.quantity += Math.max(0, Number(item.quantity) || 0);
        result.total += (Number(item.quantity) || 0) * (Number(item.unit_price) || 0);
        return result;
      },
      { itemCount: 0, quantity: 0, total: 0 },
    );
  }

  function updateBadges(cart) {
    const summary = totals(cart);
    document.querySelectorAll("[data-cart-count]").forEach((node) => {
      node.textContent = String(summary.quantity);
    });
    document.querySelectorAll("[data-cart-total]").forEach((node) => {
      node.textContent = `$ ${money(summary.total)}`;
    });
  }

  function updateCatalog(cart) {
    const byID = new Map(cart.map((item) => [String(item.product_id), item]));
    document.querySelectorAll("[data-product-card]").forEach((card) => {
      const item = byID.get(String(card.dataset.productId));
      const badge = card.querySelector("[data-product-cart-quantity]");
      if (badge) {
        badge.textContent = item ? `En carrito: ${item.quantity}` : "";
        badge.hidden = !item;
      }
    });
  }

  function updateInventory(cart) {
    const byID = new Map(cart.map((item) => [String(item.product_id), item]));
    document.querySelectorAll("[data-cart-badge]").forEach((badge) => {
      const item = byID.get(String(badge.dataset.cartBadge || ""));
      const quantity = Math.max(0, Number(item?.quantity) || 0);
      const visible = quantity > 0;
      badge.textContent = visible ? String(quantity) : "";
      badge.hidden = !visible;
      badge.setAttribute(
        "aria-label",
        visible
          ? `${quantity} unidades de ${badge.dataset.cartProductName || item.product_name || item.product_id} en el carrito`
          : "",
      );
    });
    document.querySelectorAll("[data-add-to-cart]").forEach((button) => {
      const item = byID.get(String(button.dataset.product || ""));
      button.textContent = item ? "Agregar mas" : "Agregar al carrito";
    });
  }

  function renderCartLines(cart, generation) {
    const container = document.querySelector("[data-cart-lines]");
    if (!container) return;
    container.replaceChildren();

    if (!cart.length) {
      const empty = document.createElement("div");
      empty.className = "sale-empty-state";
      empty.textContent = "El carrito está vacío. Agrega productos para continuar.";
      container.appendChild(empty);
      return;
    }

    cart.forEach((item, index) => {
      const line = document.createElement("article");
      line.className = "sale-cart-line";

      const info = document.createElement("div");
      info.className = "sale-cart-line-info";
      const title = document.createElement("strong");
      title.textContent = item.product_name || item.product_id;
      const id = document.createElement("span");
      id.textContent = `${item.product_id} · Disponibles: ${item.stock}`;
      info.append(title, id);

      const quantity = document.createElement("input");
      quantity.type = "number";
      quantity.min = "1";
      quantity.max = String(Math.max(1, Number(item.stock) || 1));
      quantity.value = String(item.quantity);
      quantity.setAttribute("aria-label", `Cantidad de ${item.product_name || item.product_id}`);
      quantity.addEventListener("change", () => {
        const next = Math.max(1, Math.min(Number(item.stock) || 1, Number(quantity.value) || 1));
        const updated = readCart();
        if (updated[index]) {
          updated[index].quantity = next;
          updated[index].allocations = [];
        }
        writeCart(updated);
        render();
      });

      const price = document.createElement("input");
      price.type = "number";
      price.min = "0.01";
      price.step = "0.01";
      price.value = String(item.unit_price);
      price.setAttribute("aria-label", `Precio de ${item.product_name || item.product_id}`);
      price.addEventListener("change", () => {
        const next = Number(price.value);
        if (!(next > 0)) {
          price.value = String(item.unit_price);
          return;
        }
        const updated = readCart();
        if (updated[index]) updated[index].unit_price = next;
        writeCart(updated);
        render();
      });

      const subtotal = document.createElement("strong");
      subtotal.className = "sale-cart-line-total";
      subtotal.textContent = `$ ${money(item.quantity * item.unit_price)}`;

      const remove = document.createElement("button");
      remove.type = "button";
      remove.className = "button secondary sale-remove-line";
      remove.textContent = "Quitar";
      remove.addEventListener("click", () => {
        const updated = readCart();
        updated.splice(index, 1);
        writeCart(updated);
        render();
      });

      const controls = document.createElement("div");
      controls.className = "sale-cart-line-controls";
      controls.append(quantity, price, subtotal, remove);
      const allocation = document.createElement("section");
      allocation.className = "sale-location-allocation";
      allocation.dataset.productId = String(item.product_id);
      const allocationTitle = document.createElement("h3");
      allocationTitle.className = "sale-location-allocation-title";
      allocationTitle.textContent = "Distribución por ubicación";
      const allocationHelp = document.createElement("p");
      allocationHelp.className = "sale-location-allocation-help";
      allocationHelp.textContent = "Confirma de qué ubicaciones saldrán las unidades.";
      const allocationRows = document.createElement("div");
      allocationRows.className = "sale-location-allocation-rows";
      allocationRows.dataset.cartAllocationRows = "true";
      const allocationTotal = document.createElement("p");
      allocationTotal.className = "sale-location-allocation-total";
      allocationTotal.dataset.cartAllocationTotal = "true";
      allocation.append(allocationTitle, allocationHelp, allocationRows, allocationTotal);
      line.append(info, controls, allocation);
      container.appendChild(line);
      renderLineAllocations(item, allocation, generation);
    });
  }

  function renderCheckoutSummary(cart) {
    const container = document.querySelector("[data-checkout-lines]");
    if (!container) return;
    container.replaceChildren();
    cart.forEach((item) => {
      const row = document.createElement("div");
      row.className = "checkout-line";
      const label = document.createElement("span");
      label.textContent = `${item.quantity} × ${item.product_name || item.product_id}`;
      const allocations = Array.isArray(item.allocations)
        ? item.allocations.filter((allocation) => Number(allocation.quantity) > 0)
        : [];
      if (allocations.length) {
        label.textContent += ` · ${allocations.map((allocation) => `${allocation.quantity} en ${allocation.location || "Sin ubicación"}`).join(", ")}`;
      }
      const value = document.createElement("strong");
      value.textContent = `$ ${money(item.quantity * item.unit_price)}`;
      row.append(label, value);
      container.appendChild(row);
    });
  }

  function updateCheckout(cart) {
    const summary = totals(cart);
    renderCheckoutSummary(cart);
    const total = document.querySelector("[data-checkout-total]");
    if (total) total.textContent = `$ ${money(summary.total)}`;
    const totalInput = document.querySelector("[name='total_value']");
    if (totalInput) totalInput.value = String(summary.total);
    updateInstallment(summary.total);
  }

  function updateInstallment(total) {
    const installments = Math.max(1, Number(document.querySelector("[name='installments_total']")?.value) || 1);
    const interest = Math.max(0, Number(document.querySelector("[name='interest_percent']")?.value) || 0);
    const financed = total + total * interest / 100;
    const value = Math.round((financed / installments) * 100) / 100;
    const target = document.querySelector("[data-installment-value]");
    if (target) target.textContent = `$ ${money(value)}`;
    const hidden = document.querySelector("[name='installment_value']");
    if (hidden) hidden.value = String(value);
  }

  function updateCheckoutMode() {
    const mode = document.querySelector("[name='sale_mode']:checked")?.value || "normal";
    const creditFields = document.querySelector("[data-credit-fields]");
    const paymentFields = document.querySelector("[data-payment-fields]");
    if (creditFields) creditFields.hidden = mode !== "credit";
    if (paymentFields) paymentFields.hidden = mode === "credit";

    const customerID = String(document.querySelector("[name='customer_id']")?.value || "").trim();
    const customerName = String(document.querySelector("[name='customer_name']")?.value || "").trim();
    const hasCustomer = !!(customerID || customerName);
    const anonymousBox = document.querySelector("[data-anonymous-confirm-wrap]");
    const anonymousConfirm = document.querySelector("[name='anonymous_confirm']");
    const warning = document.querySelector("[data-anonymous-warning]");
    if (anonymousBox) anonymousBox.hidden = mode === "credit" || hasCustomer;
    if (warning) warning.hidden = mode === "credit" || hasCustomer;
    if (anonymousConfirm && hasCustomer) anonymousConfirm.checked = false;

    const button = document.querySelector("[data-confirm-sale]");
    if (button) {
      button.textContent = mode === "credit" ? "Confirmar crédito" : "Confirmar venta";
    }
    updateInstallment(totals(readCart()).total);
  }

  function addItem(item) {
    const productID = String(item?.product_id || "").trim();
    const stock = Math.max(0, Number(item?.stock) || 0);
    const quantity = Math.max(1, Number(item?.quantity) || 1);
    if (!productID || quantity > stock) {
      return { ok: false, error: `Solo hay ${stock} unidades disponibles.` };
    }

    const updated = readCart();
    const existing = updated.find((entry) => String(entry.product_id) === productID);
    const nextQuantity = (existing?.quantity || 0) + quantity;
    if (nextQuantity > stock) {
      return { ok: false, error: `Solo hay ${stock} unidades disponibles.` };
    }
    if (existing) {
      existing.quantity = nextQuantity;
      existing.stock = stock;
      existing.allocations = [];
    } else {
      updated.push({
        product_id: productID,
        product_name: String(item.product_name || productID),
        quantity,
         unit_price: Number(item.unit_price) || 0,
         stock,
         allocations: [],
       });
    }
    writeCart(updated);
    render();
    return { ok: true, quantity: nextQuantity };
  }

  function bindCatalog(cart) {
    const search = document.querySelector("[data-sale-search]");
    if (search) {
      search.addEventListener("input", () => {
        const query = search.value.trim().toLowerCase();
        document.querySelectorAll("[data-product-card]").forEach((card) => {
          card.hidden = query !== "" && !String(card.dataset.search || "").includes(query);
        });
      });
    }
    document.querySelectorAll("[data-add-product]").forEach((button) => {
      button.addEventListener("click", () => {
        const card = button.closest("[data-product-card]");
        if (!card) return;
        const productID = String(card.dataset.productId || "").trim();
        const stock = Number(card.dataset.stock || 0);
        const quantityInput = card.querySelector("[data-product-quantity]");
        const quantity = Math.max(1, Number(quantityInput?.value) || 1);
        const result = addItem({
          product_id: productID,
          product_name: card.dataset.productName || productID,
          quantity,
          unit_price: Number(card.dataset.price || 0),
          stock,
        });
        const status = card.querySelector("[data-product-status]");
        if (status) status.textContent = result.ok ? "Producto agregado al carrito." : result.error;
      });
    });
  }

  function bindInventoryCatalog() {
    root.addEventListener("click", (event) => {
      const button = event.target.closest("[data-add-to-cart]");
      if (!button || !root.contains(button)) return;
      const row = button.closest("tr");
      const quantityInput = row?.querySelector("[data-qty]");
      const result = addItem({
        product_id: button.dataset.product,
        product_name: button.dataset.productName,
        quantity: Number(quantityInput?.value) || 1,
        unit_price: Number(button.dataset.unitPrice) || 0,
        stock: Number(button.dataset.stock) || 0,
      });
    const status = row?.querySelector("[data-cart-error]");
    if (status) {
      status.hidden = result.ok;
      status.textContent = result.ok ? "" : result.error;
    }
    });
  }

  function bindCheckout() {
    const form = document.querySelector("[data-sale-checkout-form]");
    if (!form) return;
    document.querySelectorAll("[name='sale_mode'], [name='installments_total'], [name='interest_percent']").forEach((input) => {
      input.addEventListener("change", updateCheckoutMode);
      input.addEventListener("input", updateCheckoutMode);
    });
    document.querySelectorAll("[name='customer_id'], [name='customer_name']").forEach((input) => {
      input.addEventListener("change", updateCheckoutMode);
      input.addEventListener("input", updateCheckoutMode);
    });
    form.addEventListener("submit", (event) => {
      const cart = readCart();
      const mode = document.querySelector("[name='sale_mode']:checked")?.value || "normal";
      const customerID = String(document.querySelector("[name='customer_id']")?.value || "").trim();
      const customerName = String(document.querySelector("[name='customer_name']")?.value || "").trim();
      const anonymousConfirm = document.querySelector("[name='anonymous_confirm']")?.checked;
      if (!cart.length) {
        event.preventDefault();
        window.alert("Agrega al menos un producto al carrito.");
        return;
      }
      if (mode === "credit" && !customerID && !customerName) {
        event.preventDefault();
        window.alert("Selecciona o registra un cliente para continuar con el crédito.");
        return;
      }
      if (mode !== "credit" && !customerID && !customerName && !anonymousConfirm) {
        event.preventDefault();
        window.alert("Confirma que deseas registrar la venta sin cliente.");
        return;
      }
      const itemsInput = document.querySelector("[name='items_json']");
      if (itemsInput) itemsInput.value = JSON.stringify(cart.map((item) => ({
        product_id: item.product_id,
        quantity: item.quantity,
        unit_price: item.unit_price,
        allocations: Array.isArray(item.allocations) ? item.allocations : [],
      })));
      const button = document.querySelector("[data-confirm-sale]");
      if (button) {
        button.disabled = true;
        button.textContent = "Procesando...";
      }
    });
  }

  function render() {
    const generation = ++renderGeneration;
    const cart = readCart();
    updateBadges(cart);
    updateCatalog(cart);
    updateInventory(cart);
    renderCartLines(cart, generation);
    updateCheckout(cart);
    updateCheckoutMode();
  }

  document.addEventListener("stocki:cart-changed", render);
  window.StockiSaleCart = { add: addItem, refresh: render };
  bindCatalog(readCart());
  bindInventoryCatalog();
  bindCheckout();
  render();
})();
