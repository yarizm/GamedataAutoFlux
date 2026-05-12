"use strict";

function useDataRecordForReport(key) {
    const record = currentDataRecords.find((item) => item.key === key);
    addReportRecordSelection(key, record);
    syncSelectedReportRecordKeys();
    setValue("report-data-source", record?.data_source || "");
    setValue("report-prompt", `基于所选原始 JSON 数据，生成${record?.game_name || "该游戏"}的数据分析报告。`);
    activateTab("reports");
    toast("已添加 1 条原始 JSON 用于报告", "success");
}

function useCurrentDataForReport() {
    if (!currentDataRecords.length) {
        toast("当前没有可用于报告的记录", "error");
        return;
    }
    for (const record of currentDataRecords) {
        addReportRecordSelection(record.key, record);
    }
    syncSelectedReportRecordKeys();
    setValue("report-data-source", selectedDataGame?.game_name || "");
    setValue("report-prompt", `基于${selectedDataGame?.game_name || "所选游戏"}当前筛选出的原始 JSON 数据，生成综合分析报告。`);
    activateTab("reports");
    toast(`已选择 ${selectedReportRecordKeys.length} 条原始 JSON 用于报告`, "success");
}

function createFillTaskFromPrecheck(collector) {
    const gameName = (selectedDataGame && selectedDataGame.game_name) || "";
    const pipelineMap = {
        steam: "steam_steamdb", steam_discussions: "steam_discussions",
        taptap: "taptap_basic", gtrends: "gtrends_weekly",
        monitor: "monitor_basic", events: "events",
        official_site: "official_site", qimai: "qimai",
    };
    const pipeline = pipelineMap[collector] || collector;
    setValue("task-name", gameName ? `${gameName} - ${labelCollector(collector)} 补采` : `${labelCollector(collector)} 补采`);
    setValue("task-target-name", gameName || "");
    setValue("task-pipeline", pipeline);
    if (collector === "steam" || collector === "steam_discussions") {
        const appId = selectedDataGame?.app_id || "";
        setValue("task-app-id", appId);
        if (collector === "steam_discussions") setValue("task-steam-discussions-app-id", appId);
    }
    if (collector === "taptap" && selectedDataGame?.app_id) {
        setValue("task-app-id", selectedDataGame.app_id);
    }
    if (collector === "monitor" || collector === "qimai") {
        setValue("task-app-id", selectedDataGame?.app_id || "");
    }
    updateTaskTargetFields();
    openModal("modal-create-task");
    currentWizardStep = 1;
    updateWizardUI();
}

function clearSelectedReportRecords() {
    selectedReportRecordKeys = [];
    selectedReportRecordMeta = {};
    syncSelectedReportRecordKeys();
}

function syncSelectedReportRecordKeys() {
    const el = document.getElementById("report-record-keys");
    if (el) {
        el.value = selectedReportRecordKeys.join("\n");
    }
    renderSelectedReportRecords();
    updateReportTemplateHelp();
}

function syncReportRecordKeysFromTextarea() {
    const raw = document.getElementById("report-record-keys")?.value.trim() || "";
    const keys = raw ? raw.split(/\s+/).map((item) => item.trim()).filter(Boolean) : [];
    selectedReportRecordKeys = [...new Set(keys)];
    for (const key of Object.keys(selectedReportRecordMeta)) {
        if (!selectedReportRecordKeys.includes(key)) {
            delete selectedReportRecordMeta[key];
        }
    }
    renderSelectedReportRecords();
    updateReportTemplateHelp();
}

function addReportRecordSelection(key, meta = null) {
    if (!selectedReportRecordKeys.includes(key)) {
        selectedReportRecordKeys.push(key);
    }
    if (meta) {
        selectedReportRecordMeta[key] = {
            key,
            collector: meta.collector || "",
            data_source: meta.data_source || meta.collector || "",
            game_name: meta.game_name || "",
            app_id: meta.app_id || "",
        };
    }
}

function renderSelectedReportRecords() {
    const container = document.getElementById("report-selected-records");
    if (!container) return;
    if (!selectedReportRecordKeys.length) {
        container.innerHTML = '<p class="text-muted">尚未添加 JSON 数据源</p>';
        return;
    }
    container.innerHTML = selectedReportRecordKeys.map((key) => {
        const meta = selectedReportRecordMeta[key] || {};
        const label = meta.data_source || meta.collector || "手工输入";
        const title = meta.game_name ? `${meta.game_name} / ${label}` : label;
        return `
            <div class="selected-source-chip">
                <span>
                    <strong>${escapeHtml(title)}</strong>
                    <code>${escapeHtml(key)}</code>
                </span>
                <button class="btn btn-ghost btn-sm" type="button" onclick="removeReportRecordSelection('${escapeJs(key)}')">移除</button>
            </div>
        `;
    }).join("");
}

function renderReportPrecheck(precheck) {
    const container = document.getElementById("report-precheck");
    if (!container || !precheck) return;
    const status = precheck.status || "unchecked";
    const missing = precheck.missing_collectors || [];
    const available = precheck.available_collectors || [];
    const sourceCounts = precheck.source_counts || {};
    const recommendations = precheck.recommendations || [];
    const missingText = missing.length ? missing.map(labelCollector).join(" / ") : "None";
    const availableText = available.length ? available.map((collector) => {
        const count = sourceCounts[collector] || 0;
        return `${labelCollector(collector)}${count ? ` (${count})` : ""}`;
    }).join(" / ") : "None";
    container.style.display = "block";
    container.className = `report-precheck report-precheck-${status}`;
    const fillTaskButtons = missing.length ? missing.map((collector) => {
        const label = labelCollector(collector);
        return `<button class="btn btn-primary btn-sm" onclick="createFillTaskFromPrecheck('${escapeJs(collector)}')" style="margin:2px;">补采 ${escapeHtml(label)}</button>`;
    }).join("") : "";
    container.innerHTML = `
        <div class="report-precheck-title">${escapeHtml(precheck.message || "Report precheck finished")}</div>
        <div class="report-precheck-grid">
            <span>Records</span><strong>${precheck.usable_records || 0}/${precheck.selected_records || 0}</strong>
            <span>Available</span><strong>${escapeHtml(availableText)}</strong>
            <span>Missing</span><strong>${escapeHtml(missingText)}</strong>
        </div>
        ${fillTaskButtons ? `<div class="report-precheck-actions">${fillTaskButtons}</div>` : ""}
        ${recommendations.length ? `<ul>${recommendations.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>` : ""}
    `;
}

function removeReportRecordSelection(key) {
    selectedReportRecordKeys = selectedReportRecordKeys.filter((item) => item !== key);
    delete selectedReportRecordMeta[key];
    syncSelectedReportRecordKeys();
}

async function loadReportTemplates() {
    try {
        reportTemplates = await api("/reports/templates");
        const select = document.getElementById("report-template");
        if (!select) return;
        const current = select.value;
        select.innerHTML = reportTemplates.map((template) => (
            `<option value="${escapeHtml(template.id)}">${escapeHtml(template.name)}</option>`
        )).join("");
        if ([...select.options].some((option) => option.value === current)) {
            select.value = current;
        }
        updateReportTemplateHelp();
    } catch (err) {
        console.error("Load report templates failed:", err);
    }
}

function updateReportTemplateHelp() {
    const templateId = document.getElementById("report-template")?.value || "";
    const help = document.getElementById("report-template-help");
    if (!help) return;
    const template = reportTemplates.find((item) => item.id === templateId);
    if (!template) {
        help.textContent = "";
        return;
    }
    const knownCollectors = new Set(
        Object.values(selectedReportRecordMeta)
            .map((meta) => normalizeCollector(meta.collector))
            .filter(Boolean)
    );
    const missing = (template.required_collectors || []).filter((collector) => !knownCollectors.has(collector));
    const requirements = (template.required_collectors || []).map(labelCollector).join(" / ");
    const manualCount = selectedReportRecordKeys.filter((key) => !selectedReportRecordMeta[key]).length;
    const status = missing.length
        ? `缺少：${missing.map(labelCollector).join(" / ")}`
        : "已满足已知数据源要求";
    help.innerHTML = `
        <span>${escapeHtml(template.description)}</span><br>
        <span>必需数据源：${escapeHtml(requirements || "-")}；${escapeHtml(status)}</span>
        ${manualCount ? `<br><span>包含 ${manualCount} 个手工 key，前端无法识别来源，后端生成时会再次校验。</span>` : ""}
    `;
}

function normalizeCollector(value) {
    const normalized = String(value || "").toLowerCase();
    const aliases = {
        google_trends: "gtrends",
        pytrends: "gtrends",
        steam_api: "steam",
        steamdb: "steam",
        firecrawl: "steam",
    };
    return aliases[normalized] || normalized;
}

function labelCollector(value) {
    const labels = {
        steam: "Steam",
        taptap: "TapTap",
        gtrends: "Google Trends",
        monitor: "Monitor",
        events: "事件数据",
        steam_discussions: "Steam Community Discussions",
    };
    return labels[value] || value;
}

async function uploadReportJsonFiles() {
    const input = document.getElementById("report-json-files");
    const files = [...(input?.files || [])];
    if (!files.length) {
        toast("请选择 JSON 文件", "error");
        return;
    }
    const formData = new FormData();
    for (const file of files) {
        formData.append("files", file);
    }
    try {
        const resp = await fetch("/api/reports/upload-json", {
            method: "POST",
            body: formData,
        });
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({ detail: resp.statusText }));
            throw new Error(err.detail || `HTTP ${resp.status}`);
        }
        const uploaded = await resp.json();
        for (const item of uploaded) {
            addReportRecordSelection(item.key, {
                collector: item.collector,
                data_source: labelCollector(normalizeCollector(item.collector)),
                game_name: item.game_name,
                app_id: item.app_id,
            });
        }
        syncSelectedReportRecordKeys();
        if (input) input.value = "";
        loadDataGames();
        toast(`已导入 ${uploaded.length} 个 JSON 数据源`, "success");
    } catch (err) {
        toast(`Upload failed: ${err.message}`, "error");
    }
}

async function importReportGroupRecords() {
    const groupId = document.getElementById("report-group-select")?.value || "";
    if (!groupId) {
        toast("Choose a data group", "error");
        return;
    }
    try {
        const records = await api(`/reports/group-records?group_id=${encodeURIComponent(groupId)}`);
        for (const record of records) {
            addReportRecordSelection(record.key, record);
        }
        syncSelectedReportRecordKeys();
        toast(`Imported ${records.length} records`, "success");
    } catch (err) {
        toast(`Import failed: ${err.message}`, "error");
    }
}

async function loadReports() {
    try {
        const reports = await api("/reports");
        const container = document.getElementById("reports-list");
        if (!container) return;

        if (!reports.length) {
            container.innerHTML = '<p class="text-muted">No reports</p>';
            return;
        }

        container.innerHTML = reports.map((report) => `
            <div class="report-item">
                <button class="report-item-main" onclick="viewReport('${report.id}')">
                    <span class="report-item-title">${escapeHtml(report.title)}</span>
                    <span class="report-item-meta">${formatTime(report.generated_at)} | ${escapeHtml(report.template)} | ${report.matched_records} records</span>
                </button>
                <div class="inline-actions">
                    <button class="btn btn-ghost btn-sm" onclick="editReport('${report.id}')">Edit</button>
                    <button class="btn btn-danger btn-sm" onclick="deleteReport('${report.id}')">Delete</button>
                </div>
            </div>
        `).join("");
    } catch (err) {
        toast(`Load failed: ${err.message}`, "error");
    }
}

function renderReport(report) {
    const container = document.getElementById("report-content");
    if (container) {
        let contentHtml = `<pre>${escapeHtml(report.content || "")}</pre>`;
        
        // Add Excel download button if it's an Excel report
        const isExcel = report.metadata?.format === "excel" || report.metadata?.excel_path;
        if (isExcel) {
            contentHtml = `
                <div style="margin-bottom: 1rem; padding: 1rem; background: var(--bg-card); border-radius: 4px; border: 1px solid var(--border);">
                    <h4 style="margin: 0 0 0.5rem 0; color: var(--success);">📊 Excel 报告已生成</h4>
                    <p style="margin: 0 0 1rem 0; color: var(--text-muted);">
                        该报告包含了清洗好的表格行、多个工作表以及统计图表。
                    </p>
                    <a href="/api/reports/${report.id}/download" class="btn btn-primary" target="_blank" download>
                        ⬇️ 下载 Excel 文件
                    </a>
                </div>
            ` + contentHtml;
        }
        
        container.innerHTML = contentHtml;
    }
}

async function generateReport() {
    syncReportRecordKeysFromTextarea();
    const prompt = document.getElementById("report-prompt")?.value.trim() || "";
    const dataSource = document.getElementById("report-data-source")?.value.trim() || "";
    const template = document.getElementById("report-template")?.value || "default";
    const recordKeysRaw = document.getElementById("report-record-keys")?.value.trim() || "";
    const recordKeys = recordKeysRaw
        ? recordKeysRaw.split(/\s+/).map((item) => item.trim()).filter(Boolean)
        : [];

    if (!prompt) {
        toast("Prompt is required", "error");
        return;
    }

    const requestPayload = {
        prompt,
        data_source: dataSource,
        template,
        record_keys: recordKeys,
        params: {},
    };

    currentReportProgressId = `report_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    resetReportProgress();
    const button = document.getElementById("btn-generate-report");
    if (button) {
        button.disabled = true;
        button.textContent = "Generating...";
    }

    try {
        setReportProgress(0.04, "precheck", "Checking report data coverage");
        const precheck = await api("/reports/precheck", {
            method: "POST",
            body: JSON.stringify(requestPayload),
        });
        renderReportPrecheck(precheck);
        if (precheck.status === "empty") {
            throw new Error(precheck.message || "No usable report data");
        }
        if (precheck.status === "partial") {
            const missing = (precheck.missing_collectors || []).map(labelCollector).join(" / ");
            const proceed = confirm(`Missing data sources: ${missing}. Generate report anyway?`);
            if (!proceed) {
                setReportProgress(0, "cancelled", "Report generation cancelled");
                return;
            }
        }
        setReportProgress(0.08, "requesting", "Sending report request");
        requestPayload.params = { progress_id: currentReportProgressId };
        const report = await api("/reports/generate-excel", {
            method: "POST",
            body: JSON.stringify(requestPayload),
        });
        setReportProgress(1, "completed", "Report generated");
        renderReport(report);
        loadReports();
        toast("Report generated", "success");
    } catch (err) {
        setReportProgress(1, "failed", err.message);
        toast(`Generate failed: ${err.message}`, "error");
    } finally {
        if (button) {
            button.disabled = false;
            setTimeout(() => { button.textContent = "生成报告"; }, 0);
            button.textContent = "生成报告";
        }
    }
}

async function viewReport(id) {
    try {
        const report = await api(`/reports/${id}`);
        renderReport(report);
    } catch (err) {
        toast(`Load failed: ${err.message}`, "error");
    }
}

async function editReport(id) {
    try {
        const report = await api(`/reports/${id}`);
        const title = prompt("Report title", report.title || "");
        if (title === null) return;
        const notes = prompt("Notes", report.metadata?.notes || "");
        if (notes === null) return;
        const updated = await api(`/reports/${id}`, {
            method: "PATCH",
            body: JSON.stringify({ title: title.trim(), notes: notes.trim() }),
        });
        renderReport(updated);
        loadReports();
        toast("Report updated", "success");
    } catch (err) {
        toast(`Edit failed: ${err.message}`, "error");
    }
}

async function deleteReport(id) {
    if (!confirm(`Delete report ${id}?`)) return;
    try {
        await api(`/reports/${encodeURIComponent(id)}?confirm=true`, { method: "DELETE" });
        toast("Report deleted", "success");
        loadReports();
        const container = document.getElementById("report-content");
        if (container) container.textContent = "No report selected";
    } catch (err) {
        toast(`Delete failed: ${err.message}`, "error");
    }
}
