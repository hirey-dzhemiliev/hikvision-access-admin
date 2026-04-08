document.addEventListener("DOMContentLoaded", function () {
  if (window.M) {
    window.M.Sidenav.init(document.querySelectorAll(".sidenav"));
  }

  initFlashDismiss();
  initConfirmActions();
  initLoginForm();
  initTimeSettingsForms();
  initEmployeePhotoFields();
  initEmployeesPage();
  initPanelsPage();
  initSyncPage();
});

function initFlashDismiss() {
  document.querySelectorAll("[data-dismiss='flash']").forEach(function (button) {
    button.addEventListener("click", function () {
      const target = button.closest("#flash-notice");
      if (target) {
        target.remove();
      }
    });
  });
}

function initLoginForm() {
  const form = document.getElementById("login-form");
  if (!form) {
    return;
  }
  form.querySelectorAll("input").forEach(function (input) {
    input.addEventListener("keydown", function (event) {
      if (event.key !== "Enter") {
        return;
      }
      event.preventDefault();
      if (typeof form.requestSubmit === "function") {
        form.requestSubmit();
      } else {
        form.submit();
      }
    });
  });
}

function initConfirmActions() {
  document.querySelectorAll("form[data-confirm]").forEach(function (form) {
    form.addEventListener("submit", function (event) {
      const message = form.getAttribute("data-confirm");
      if (message && !window.confirm(message)) {
        event.preventDefault();
      }
    });
  });

  document.querySelectorAll("[data-confirm]:not(form)").forEach(function (element) {
    element.addEventListener("click", function (event) {
      const message = element.getAttribute("data-confirm");
      if (message && !window.confirm(message)) {
        event.preventDefault();
        event.stopPropagation();
      }
    });
  });
}

function initTimeSettingsForms() {
  document.querySelectorAll("[data-time-settings-form]").forEach(function (form) {
    const modeSelect = form.querySelector("[data-time-mode-select]");
    const manualRows = form.querySelectorAll("[data-manual-time-row]");
    const ntpRows = form.querySelectorAll("[data-ntp-row]");
    const manualSections = form.querySelectorAll("[data-manual-time-section]");
    const ntpSections = form.querySelectorAll("[data-ntp-section]");
    if (!modeSelect) {
      return;
    }
    const syncTimeModeVisibility = function () {
      const isManual = modeSelect.value === "manual";
      manualSections.forEach(function (section) {
        section.classList.toggle("is-hidden", !isManual);
      });
      ntpSections.forEach(function (section) {
        section.classList.toggle("is-hidden", isManual);
      });
      manualRows.forEach(function (row) {
        row.classList.toggle("is-hidden", !isManual);
        row.querySelectorAll("input, select, textarea").forEach(function (field) {
          if (field.name === "manual_time") {
            field.disabled = !isManual;
          }
        });
      });
      ntpRows.forEach(function (row) {
        row.classList.toggle("is-hidden", isManual);
        row.querySelectorAll("input, select, textarea").forEach(function (field) {
          if (["ntp_server", "ntp_port", "ntp_interval"].includes(field.name)) {
            field.disabled = isManual;
          }
        });
      });
    };
    modeSelect.addEventListener("change", syncTimeModeVisibility);
    syncTimeModeVisibility();
  });
}

function initEmployeesPage() {
  const input = document.getElementById("employee-live-search");
  const searchable = Array.from(document.querySelectorAll("[data-search]"));
  const bulkForm = document.getElementById("employees-bulk-form");
  const hidden = document.getElementById("selected-employee-ids");
  const selectAll = document.getElementById("employees-select-all");
  const selectedCount = document.getElementById("employees-selected-count");
  const bulkButtons = Array.from(document.querySelectorAll("[data-bulk-submit]"));
  if (!bulkForm && !input && !selectAll) {
    return;
  }

  function visibleCheckboxes() {
    return Array.from(document.querySelectorAll(".employee-select")).filter(function (el) {
      const container = el.closest("[data-search]");
      return container && container.style.display !== "none";
    });
  }

  function syncSelection() {
    const selected = Array.from(document.querySelectorAll(".employee-select:checked")).map(function (el) {
      return el.value;
    });
    if (hidden) {
      hidden.value = selected.join(",");
    }
    if (selectedCount) {
      selectedCount.textContent = "Выбрано: " + selected.length;
    }
    bulkButtons.forEach(function (button) {
      button.disabled = selected.length === 0;
    });
    if (selectAll) {
      const visible = visibleCheckboxes();
      selectAll.checked = visible.length > 0 && visible.every(function (el) { return el.checked; });
      selectAll.indeterminate = visible.some(function (el) { return el.checked; }) && !selectAll.checked;
    }
  }

  if (input) {
    input.addEventListener("input", function () {
      const needle = input.value.trim().toLowerCase();
      searchable.forEach(function (row) {
        row.style.display = row.dataset.search.includes(needle) ? "" : "none";
      });
      syncSelection();
    });
  }

  document.querySelectorAll(".employee-select").forEach(function (checkbox) {
    checkbox.addEventListener("change", syncSelection);
  });

  if (selectAll) {
    selectAll.addEventListener("change", function () {
      visibleCheckboxes().forEach(function (checkbox) {
        checkbox.checked = selectAll.checked;
      });
      syncSelection();
    });
  }

  if (bulkForm) {
    bulkForm.addEventListener("submit", syncSelection);
  }
  syncSelection();
}

function initEmployeePhotoFields() {
  document.querySelectorAll("[data-photo-input]").forEach(function (input) {
    const meta = input.closest(".form-row")?.querySelector("[data-photo-meta]");
    if (!meta) {
      return;
    }
    input.addEventListener("change", function () {
      const file = input.files && input.files[0];
      if (!file) {
        meta.textContent = "Выбранный файл пока не загружен.";
        return;
      }
      const sizeKb = Math.max(1, Math.round(file.size / 1024));
      const reader = new FileReader();
      reader.onload = function (event) {
        const image = new Image();
        image.onload = function () {
          meta.textContent = "Файл: " + file.name + " · " + image.width + "×" + image.height + " px · " + sizeKb + " KB";
        };
        image.onerror = function () {
          meta.textContent = "Файл: " + file.name + " · " + sizeKb + " KB";
        };
        image.src = String(event.target?.result || "");
      };
      reader.onerror = function () {
        meta.textContent = "Файл: " + file.name + " · " + sizeKb + " KB";
      };
      reader.readAsDataURL(file);
    });
  });
}

function initPanelsPage() {
  const hidden = document.getElementById("selected-panel-ids");
  const selectAll = document.getElementById("panels-select-all");
  const selectedCount = document.getElementById("panels-selected-count");
  const submit = document.getElementById("panels-bulk-submit");
  if (!hidden && !selectAll && !submit) {
    return;
  }

  function allPanelCheckboxes() {
    return Array.from(document.querySelectorAll(".panel-select")).filter(function (el) {
      return !el.disabled;
    });
  }

  function syncPanelsSelection() {
    const all = allPanelCheckboxes();
    const selected = all.filter(function (el) { return el.checked; });
    if (hidden) {
      hidden.value = selected.map(function (el) { return el.value; }).join(",");
    }
    if (selectedCount) {
      selectedCount.textContent = "Выбрано: " + selected.length;
    }
    if (submit) {
      submit.disabled = selected.length === 0;
    }
    if (selectAll) {
      selectAll.checked = all.length > 0 && selected.length === all.length;
      selectAll.indeterminate = selected.length > 0 && selected.length < all.length;
    }
  }

  document.querySelectorAll(".panel-select").forEach(function (checkbox) {
    checkbox.addEventListener("change", syncPanelsSelection);
  });

  if (selectAll) {
    selectAll.addEventListener("change", function () {
      allPanelCheckboxes().forEach(function (checkbox) {
        checkbox.checked = selectAll.checked;
      });
      syncPanelsSelection();
    });
  }

  syncPanelsSelection();
}

function initSyncPage() {
  const panelCheckboxes = Array.from(document.querySelectorAll(".sync-panel-checkbox"));
  const syncForm = document.querySelector('form[action="/sync/preview"]');
  if (!syncForm) {
    return;
  }
  const selectAllPanels = document.getElementById("sync-panels-select-all");
  const selectedPanelsCount = document.getElementById("sync-panels-selected-count");
  const employeeSelect = document.getElementById("sync-employee-select");
  const panelHidden = document.getElementById("sync-panel-ids");
  const employeeHidden = document.getElementById("sync-employee-ids");
  const submitButton = syncForm.querySelector('button[type="submit"]');

  function syncPanelSelection() {
    const selected = panelCheckboxes.filter(function (checkbox) { return checkbox.checked; });
    if (panelHidden) {
      panelHidden.value = selected.length ? selected.map(function (checkbox) { return checkbox.value; }).join(",") : "0";
    }
    if (selectedPanelsCount) {
      selectedPanelsCount.textContent = "Выбрано: " + selected.length;
    }
    if (submitButton) {
      submitButton.disabled = selected.length === 0;
    }
    if (selectAllPanels) {
      selectAllPanels.checked = panelCheckboxes.length > 0 && selected.length === panelCheckboxes.length;
      selectAllPanels.indeterminate = selected.length > 0 && selected.length < panelCheckboxes.length;
    }
  }

  panelCheckboxes.forEach(function (checkbox) {
    checkbox.addEventListener("change", syncPanelSelection);
  });

  if (selectAllPanels) {
    selectAllPanels.addEventListener("change", function () {
      panelCheckboxes.forEach(function (checkbox) {
        checkbox.checked = selectAllPanels.checked;
      });
      syncPanelSelection();
    });
  }

  syncForm.addEventListener("submit", function () {
    syncPanelSelection();
    if (employeeSelect && employeeHidden) {
      employeeHidden.value = employeeSelect.value ? employeeSelect.value : "";
    }
  });

  syncPanelSelection();
}
