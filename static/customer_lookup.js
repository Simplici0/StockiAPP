(function () {
  function debounce(fn, wait) {
    let timeout = null;
    return function (...args) {
      window.clearTimeout(timeout);
      timeout = window.setTimeout(() => fn.apply(this, args), wait);
    };
  }

  function customerLabel(item) {
    const name = String(item.name || "").trim();
    const document = String(item.document_number || "").trim();
    const phone = String(item.phone || "").trim();
    const tail = document || phone;
    return tail ? `${name} · ${tail}` : name;
  }

  function safeText(value) {
    return value == null ? "" : String(value);
  }

  function setValue(input, value) {
    if (!input) return;
    input.value = value == null ? "" : String(value);
    input.dispatchEvent(new Event("change", { bubbles: true }));
  }

  function attachLookup(root) {
    if (!root) return;
    const searchInput = root.querySelector("[data-customer-search]");
    const idInput = root.querySelector("[data-customer-id]");
    if (!searchInput || !idInput) return;

    const status = root.querySelector("[data-customer-status]");
    const nameInput = root.querySelector("[data-customer-name]");
    const phoneInput = root.querySelector("[data-customer-phone]");
    const documentTypeInput = root.querySelector("[data-customer-document-type]");
    const documentNumberInput = root.querySelector("[data-customer-document-number]");
    const cityInput = root.querySelector("[data-customer-city]");
    const addressInput = root.querySelector("[data-customer-address]");
    const notesInput = root.querySelector("[data-customer-notes]");

    const results = document.createElement("div");
    results.className = "customer-lookup-results";
    results.hidden = true;
    searchInput.parentElement.appendChild(results);

    let selected = null;
    let controller = null;

    function hideResults() {
      results.hidden = true;
      results.innerHTML = "";
    }

    function setStatus(text, active) {
      if (!status) return;
      status.textContent = text || "";
      status.classList.toggle("active", !!active);
    }

    function clearSelection() {
      selected = null;
      idInput.value = "";
      setStatus("Busca por nombre, documento o teléfono para reutilizar un cliente.", false);
    }

    function applyCustomer(item) {
      selected = item;
      idInput.value = String(item.id || "");
      setValue(nameInput, item.name || "");
      setValue(phoneInput, item.phone || "");
      setValue(documentTypeInput, item.document_type || "");
      setValue(documentNumberInput, item.document_number || "");
      setValue(cityInput, item.city || "");
      setValue(addressInput, item.address || "");
      if (notesInput && item.notes) setValue(notesInput, item.notes || "");
      searchInput.value = customerLabel(item);
      setStatus(`Reutilizando cliente #${item.id} · ${customerLabel(item)}`, true);
      hideResults();
    }

    function renderResults(items) {
      if (!items.length) {
        hideResults();
        return;
      }
      results.innerHTML = "";
      items.forEach((item) => {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "customer-lookup-result";
        const strong = document.createElement("strong");
        strong.textContent = safeText(item.name || "Sin nombre");
        const span = document.createElement("span");
        const parts = [item.document_type, item.document_number].filter(Boolean);
        span.textContent =
          `${parts.join(" ")}${item.city ? ` · ${item.city}` : ""}${item.phone ? ` · ${item.phone}` : ""}`;
        button.appendChild(strong);
        button.appendChild(span);
        button.addEventListener("click", () => applyCustomer(item));
        results.appendChild(button);
      });
      results.hidden = false;
    }

    const fetchCustomers = debounce(async (query) => {
      const trimmed = String(query || "").trim();
      if (trimmed.length < 2) {
        hideResults();
        return;
      }
      if (controller) controller.abort();
      controller = new AbortController();
      try {
        const resp = await fetch(`/api/customers?${new URLSearchParams({ q: trimmed, limit: "8" }).toString()}`, {
          headers: { Accept: "application/json" },
          credentials: "same-origin",
          signal: controller.signal,
        });
        const data = await resp.json().catch(() => ({}));
        if (!resp.ok || !data.ok || !Array.isArray(data.items)) {
          hideResults();
          return;
        }
        renderResults(data.items);
      } catch (error) {
        if (error && error.name === "AbortError") return;
        hideResults();
      }
    }, 220);

    searchInput.addEventListener("input", () => {
      const query = searchInput.value;
      if (selected && query.trim() !== customerLabel(selected)) {
        clearSelection();
      }
      fetchCustomers(query);
    });

    [nameInput, phoneInput, documentNumberInput].forEach((input) => {
      if (!input) return;
      input.addEventListener("input", () => {
        if (!selected) return;
        const matchesName = !nameInput || String(nameInput.value || "").trim() === String(selected.name || "").trim();
        const matchesPhone = !phoneInput || String(phoneInput.value || "").trim() === String(selected.phone || "").trim();
        const matchesDocument = !documentNumberInput || String(documentNumberInput.value || "").trim() === String(selected.document_number || "").trim();
        if (!matchesName || !matchesPhone || !matchesDocument) {
          clearSelection();
        }
      });
    });

    document.addEventListener("click", (event) => {
      if (!root.contains(event.target)) hideResults();
    });

    if (idInput.value && nameInput && nameInput.value) {
      selected = {
        id: idInput.value,
        name: nameInput.value,
        phone: phoneInput ? phoneInput.value : "",
        document_type: documentTypeInput ? documentTypeInput.value : "",
        document_number: documentNumberInput ? documentNumberInput.value : "",
        city: cityInput ? cityInput.value : "",
      };
      searchInput.value = customerLabel(selected);
      setStatus(`Reutilizando cliente #${selected.id} · ${customerLabel(selected)}`, true);
    } else {
      setStatus("Busca por nombre, documento o teléfono para reutilizar un cliente.", false);
    }
  }

  function init() {
    document.querySelectorAll("[data-customer-lookup]").forEach(attachLookup);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init, { once: true });
  } else {
    init();
  }
})();
