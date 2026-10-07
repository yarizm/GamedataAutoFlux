// Core: schema-driven collector target form for the no-build (static) page.
//
// Mirrors src/web/src/core/targetForm.js. Fields come from the collector's
// declared target_schema, so no collector is hardcoded here. Depends on the
// globals escapeHtml (core/api.js) and t (i18n.js).

function targetFormSchemaFields(metadata) {
    const schema = (metadata && metadata.target_schema) || {};
    return Array.isArray(schema.fields) ? schema.fields : [];
}

function targetFormNameField(fields) {
    return fields.find((field) => field.location === "name") || null;
}

function targetFormFieldId(prefix, field) {
    const safeKey = String((field && field.key) || "").replace(/[^a-zA-Z0-9_-]/g, "-");
    return `${prefix}-metadata-${safeKey}`;
}

function targetFormRenderInput(prefix, field) {
    const id = targetFormFieldId(prefix, field);
    const common = [
        `id="${escapeHtml(id)}"`,
        `data-target-key="${escapeHtml(field.key)}"`,
        `data-target-location="${escapeHtml(field.location || "params")}"`,
        `data-target-input="${escapeHtml(field.input_type || "text")}"`,
        field.required ? "required" : "",
    ].filter(Boolean).join(" ");
    const placeholder = field.placeholder
        ? ` placeholder="${escapeHtml(field.placeholder)}"`
        : "";
    const defaultValue = field.default === undefined || field.default === null ? "" : field.default;

    if (field.input_type === "boolean") {
        return `<label class="metadata-target-checkbox">
      <input type="checkbox" ${common} ${defaultValue ? "checked" : ""}>
      <span>${escapeHtml(field.label)}</span>
    </label>`;
    }
    if (field.input_type === "select") {
        return `<select ${common}>
      ${(field.options || []).map((option) => {
            const value = String(option.value === undefined || option.value === null ? "" : option.value);
            const label = String(option.label === undefined || option.label === null ? value : option.label);
            return `<option value="${escapeHtml(value)}" ${value === String(defaultValue) ? "selected" : ""}>${escapeHtml(label)}</option>`;
        }).join("")}
    </select>`;
    }
    if (field.input_type === "textarea" || field.input_type === "textarea_lines") {
        const rows = field.input_type === "textarea_lines" ? 5 : 3;
        return `<textarea ${common} rows="${rows}"${placeholder}>${escapeHtml(defaultValue)}</textarea>`;
    }
    const type = ["url", "number", "date"].indexOf(field.input_type) >= 0 ? field.input_type : "text";
    const bounds = [
        field.minimum !== undefined && field.minimum !== null ? `min="${escapeHtml(String(field.minimum))}"` : "",
        field.maximum !== undefined && field.maximum !== null ? `max="${escapeHtml(String(field.maximum))}"` : "",
    ].filter(Boolean).join(" ");
    return `<input type="${type}" ${common} ${bounds}${placeholder} value="${escapeHtml(defaultValue)}">`;
}

function renderMetadataTargetForm(prefix, metadata) {
    const container = document.getElementById(`${prefix}-metadata-target-fields`);
    if (!container) return false;
    const fields = targetFormSchemaFields(metadata);
    const commonLabel = document.querySelector(`label[for="${prefix}-target-name"]`);
    const commonInput = document.getElementById(`${prefix}-target-name`);
    const commonHelp = document.getElementById(`${prefix}-target-name-help`);

    if (!fields.length) {
        container.innerHTML = "";
        container.hidden = true;
        if (commonLabel) commonLabel.textContent = t("tasks.targetName");
        if (commonInput) commonInput.placeholder = t("tasks.targetName");
        if (commonHelp) {
            commonHelp.textContent = "";
            commonHelp.hidden = true;
        }
        return false;
    }

    const nameField = targetFormNameField(fields);
    if (commonLabel && nameField && nameField.label) {
        commonLabel.innerHTML = `${escapeHtml(nameField.label)}${nameField.required ? ' <span class="target-required">*</span>' : ""}`;
    } else if (commonLabel) {
        commonLabel.textContent = t("tasks.targetName");
    }
    if (commonInput) {
        commonInput.placeholder = (nameField && nameField.placeholder) || t("tasks.targetName");
    }
    if (commonHelp) {
        commonHelp.textContent = (nameField && nameField.description) || "";
        commonHelp.hidden = !(nameField && nameField.description);
    }

    const paramFields = fields.filter((field) => field.location !== "name");
    container.hidden = false;
    container.innerHTML = paramFields.map((field) => `
    <div class="form-group metadata-target-field">
      ${field.input_type === "boolean"
            ? targetFormRenderInput(prefix, field)
            : `<label for="${escapeHtml(targetFormFieldId(prefix, field))}">${escapeHtml(field.label)}${field.required ? ' <span class="target-required">*</span>' : ""}</label>
          ${targetFormRenderInput(prefix, field)}`}
      <small class="metadata-target-help">${escapeHtml(field.description || "")}</small>
    </div>
  `).join("");
    return true;
}

function targetFormReadField(prefix, field) {
    if (field.location === "name") {
        return document.getElementById(`${prefix}-target-name`)?.value?.trim?.() || "";
    }
    const element = document.getElementById(targetFormFieldId(prefix, field));
    if (!element) return "";
    if (field.input_type === "boolean") return Boolean(element.checked);
    const raw = element.value?.trim?.() ?? element.value ?? "";
    if (raw === "") return "";
    if (field.input_type === "number") return Number(raw);
    return raw;
}

function buildTargetsFromMetadata(prefix, metadata) {
    const fields = targetFormSchemaFields(metadata);
    if (!fields.length) return null;

    const schema = metadata.target_schema || {};
    const nameField = targetFormNameField(fields);
    const targetName = String(
        nameField
            ? targetFormReadField(prefix, nameField)
            : document.getElementById(`${prefix}-target-name`)?.value?.trim?.() || "",
    );
    const params = Object.assign({}, schema.default_params || {});
    let identityValue = targetName;
    let repeatedField = null;
    let repeatedValues = [];

    for (const field of fields) {
        if (field.location === "name") continue;
        const value = targetFormReadField(prefix, field);
        if (field.input_type === "textarea_lines" || field.multiple) {
            repeatedField = field;
            repeatedValues = String(value || "")
                .split(/\r?\n/)
                .map((item) => item.trim())
                .filter(Boolean);
            if (repeatedValues.length) identityValue = identityValue || repeatedValues[0];
            continue;
        }
        if (value === "" || value === null || value === undefined) continue;
        params[field.key] = value;
        if (field.default === undefined || field.default === null) identityValue = identityValue || String(value);
    }

    if (repeatedField) {
        if (!repeatedValues.length) return [];
        return repeatedValues.map((value) => {
            const itemParams = Object.assign({}, params);
            itemParams[repeatedField.key] = value;
            return { name: value, target_type: schema.target_type || "game", params: itemParams };
        });
    }
    if (!identityValue) return [];
    return [{
        name: targetName || identityValue,
        target_type: schema.target_type || "game",
        params,
    }];
}

function applyTargetsToMetadataForm(prefix, metadata, targets) {
    const fields = targetFormSchemaFields(metadata);
    const list = Array.isArray(targets) ? targets : [];
    if (!fields.length || !list.length) return false;

    const nameField = targetFormNameField(fields);
    const nameInput = document.getElementById(`${prefix}-target-name`);
    if (nameInput) {
        const first = list[0] || {};
        const explicit = nameField ? (first.params || {})[nameField.key] : "";
        nameInput.value = String(explicit === undefined || explicit === null ? (first.name || "") : explicit);
    }

    for (const field of fields) {
        if (field.location === "name") continue;
        const element = document.getElementById(targetFormFieldId(prefix, field));
        if (!element) continue;
        if (field.input_type === "textarea_lines" || field.multiple) {
            const lines = list
                .map((target) => (target.params || {})[field.key])
                .filter((value) => value !== undefined && value !== null && value !== "")
                .map((value) => String(value));
            element.value = lines.length ? lines.join("\n") : String(field.default === undefined || field.default === null ? "" : field.default);
            continue;
        }
        const value = (list[0].params || {})[field.key];
        if (field.input_type === "boolean") {
            element.checked = value === undefined || value === null ? Boolean(field.default) : Boolean(value);
        } else if (value !== undefined && value !== null && value !== "") {
            element.value = String(value);
        } else if (field.default !== undefined && field.default !== null) {
            element.value = String(field.default);
        }
    }
    return true;
}

function buildFallbackTarget(prefix, targetType) {
    const name = document.getElementById(`${prefix}-target-name`)?.value?.trim?.() || "";
    if (!name) return [];
    return [{ name, target_type: targetType || "game", params: {} }];
}

function updateCollectorLabels(prefix, metadata) {
    const collector = (metadata && metadata.collector_id) || "";
    const badge = document.getElementById(`${prefix}-collector-badge`);
    if (badge) {
        badge.textContent = t("cron.collectorBadge", {
            name: (metadata && metadata.display_name) || collector || "—",
        });
    }
    const helper = document.getElementById(`${prefix}-target-helper`);
    if (helper) {
        helper.textContent = (metadata && metadata.description)
            || (collector ? t("cron.collectorActive", { collector }) : t("cron.targetHelper"));
    }
}

function getCollectorMetadata(collectorId) {
    const id = String(collectorId || "").trim();
    if (!id) return null;
    return (window.collectorMetadata && window.collectorMetadata[id]) || null;
}

async function loadCollectorMetadata() {
    try {
        const payload = await api("/components/metadata");
        window.collectorMetadata = (payload && payload.collectors) || {};
    } catch (err) {
        window.collectorMetadata = window.collectorMetadata || {};
    }
    return window.collectorMetadata;
}
