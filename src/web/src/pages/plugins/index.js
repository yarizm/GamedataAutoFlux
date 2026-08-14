import './style.css';
import { api, escapeHtml, toast } from '../../core/api.js';
import { t } from '../../core/i18n.js';
import { renderEmptyState, renderErrorState } from '../../core/uiState.js';
import {
  inventoryRequiresRestart,
  operationIsActive,
  pluginActivationPending,
  pluginMatchesFilters,
  pluginStateTranslationKey,
} from './model.js';

function badgeClass(value) {
  if (['active', 'managed', 'succeeded'].includes(value)) return 'plugins-badge-success';
  if (value === 'failed') return 'plugins-badge-danger';
  if (['development', 'running', 'queued', 'restart_pending'].includes(value)) return 'plugins-badge-development';
  return 'plugins-badge-neutral';
}

function initials(name) {
  const parts = String(name || '').trim().split(/\s+/).filter(Boolean);
  if (!parts.length) return '?';
  return parts.slice(0, 2).map((part) => part[0]).join('').toUpperCase();
}

function sourceLabel(source) {
  return t(`plugins.source.${source}`);
}

function stateLabel(state) {
  return t(pluginStateTranslationKey(state));
}

function environmentItem(label, value, { code = false } = {}) {
  const displayValue = value || '—';
  const content = code
    ? `<code title="${escapeHtml(displayValue)}">${escapeHtml(displayValue)}</code>`
    : `<span>${escapeHtml(displayValue)}</span>`;
  return `<div class="plugins-environment-item">
    <span class="plugins-environment-label">${escapeHtml(label)}</span>
    ${content}
  </div>`;
}

function operationLabel(state) {
  return t(`plugins.operations.state.${state}`);
}

export default {
  init(container, store) {
    this.container = container;
    this.store = store;
    this.payload = { plugins: [], summary: {} };
    this.catalog = { plugins: [] };
    this.operations = { operations: [], active: null };
    this.environment = null;
    this.selectedWheel = null;
    this.filters = { search: '', source: '', state: '' };
    this._pollTimer = null;
    this._bindEvents();
    this._unsub = store.subscribe((key) => {
      if (key === 'refresh' && store.get('activeTab') === 'plugins') this.refresh(true);
    });
    this.refresh();
    return this;
  },

  destroy() {
    if (this._unsub) this._unsub();
    if (this._pollTimer) window.clearTimeout(this._pollTimer);
  },

  _bindEvents() {
    this.container.querySelector('#btn-refresh-plugins')?.addEventListener('click', () => this.refresh());
    this.container.querySelector('#plugins-search')?.addEventListener('input', (event) => {
      this.filters.search = event.target.value.trim().toLowerCase();
      this._renderPlugins();
    });
    this.container.querySelector('#plugins-source-filter')?.addEventListener('change', (event) => {
      this.filters.source = event.target.value;
      this._renderPlugins();
    });
    this.container.querySelector('#plugins-state-filter')?.addEventListener('change', (event) => {
      this.filters.state = event.target.value;
      this._renderPlugins();
    });
    this.container.querySelector('#plugins-catalog-list')?.addEventListener('click', (event) => {
      const upgradeButton = event.target.closest('[data-plugin-upgrade]');
      if (upgradeButton && !upgradeButton.disabled) {
        this._upgradeCatalogPlugin(upgradeButton.dataset.pluginUpgrade);
        return;
      }
      const button = event.target.closest('[data-plugin-install]');
      if (button && !button.disabled) this._installCatalogPlugin(button.dataset.pluginInstall);
    });
    this.container.querySelector('#plugins-list')?.addEventListener('click', (event) => {
      const button = event.target.closest('[data-plugin-action]');
      if (button && !button.disabled) {
        this._runPluginAction(button.dataset.pluginId, button.dataset.pluginAction);
      }
    });
    this.container.querySelector('#plugins-operations')?.addEventListener('click', (event) => {
      const button = event.target.closest('[data-operation-delete]');
      if (button && !button.disabled) this._deleteOperation(button.dataset.operationDelete);
    });
    this.container.querySelector('#plugins-wheel-input')?.addEventListener('change', (event) => {
      this.selectedWheel = event.target.files?.[0] || null;
      const name = this.container.querySelector('#plugins-wheel-name');
      if (name) name.textContent = this.selectedWheel?.name || t('plugins.upload.none');
      this._syncUploadButton();
    });
    this.container.querySelector('#btn-upload-plugin')?.addEventListener('click', () => this._installUploadedWheel());
    this.container.querySelector('#btn-rollback-plugin-generation')?.addEventListener('click', () => this._rollbackGeneration());
  },

  async refresh(silent = false) {
    const list = this.container.querySelector('#plugins-list');
    if (!silent && list) {
      list.innerHTML = `<div class="plugins-loading">${escapeHtml(t('common.loading'))}</div>`;
    }
    try {
      const [payload, environment, catalog, operations] = await Promise.all([
        api('/plugin-manager/plugins'),
        api('/plugin-manager/environment'),
        api('/plugin-manager/catalog'),
        api('/plugin-manager/operations'),
      ]);
      this.payload = payload || { plugins: [], summary: {} };
      this.environment = environment || null;
      this.catalog = catalog || { plugins: [] };
      this.operations = operations || { operations: [], active: null };
      this._renderSummary();
      this._renderPlugins();
      this._renderCatalog();
      this._renderOperations();
      this._renderEnvironment();
      this._renderRestartBanner();
      this._schedulePolling();
    } catch (error) {
      if (list) {
        list.innerHTML = renderErrorState({
          message: t('plugins.error.title'),
          detail: error.message,
          escapeHtml,
        });
      }
      if (!silent) toast(t('plugins.error.toast', { error: error.message }), 'error');
    }
  },

  _schedulePolling() {
    if (this._pollTimer) window.clearTimeout(this._pollTimer);
    this._pollTimer = null;
    if (operationIsActive(this.operations.active)) {
      this._pollTimer = window.setTimeout(() => this.refresh(true), 800);
    }
  },

  _renderSummary() {
    const summary = this.payload.summary || {};
    const values = {
      'plugins-stat-total': summary.total ?? 0,
      'plugins-stat-active': summary.active ?? 0,
      'plugins-stat-failed': summary.failed ?? 0,
      'plugins-stat-development': summary.by_source?.development ?? 0,
    };
    for (const [id, value] of Object.entries(values)) {
      const element = this.container.querySelector(`#${id}`);
      if (element) element.textContent = value;
    }
  },

  _filteredPlugins() {
    return (this.payload.plugins || []).filter((plugin) => pluginMatchesFilters(plugin, this.filters));
  },

  _renderPlugins() {
    const list = this.container.querySelector('#plugins-list');
    if (!list) return;
    const plugins = this._filteredPlugins();
    if (!plugins.length) {
      list.innerHTML = renderEmptyState({
        title: t('plugins.empty.title'),
        hint: t('plugins.empty.hint'),
        escapeHtml,
      });
      return;
    }

    list.innerHTML = plugins.map((plugin) => {
      const activeOperation = operationIsActive(this.operations.active);
      const version = plugin.installed_version || t('plugins.version.unknown');
      const collectors = (plugin.collectors || []).length
        ? plugin.collectors.map((collector) => `<span class="plugins-collector-chip">${escapeHtml(collector)}</span>`).join('')
        : `<span class="plugins-no-collectors">${escapeHtml(t('plugins.collectors.none'))}</span>`;
      const capabilityCounts = (plugin.capabilities || []).reduce((counts, capability) => {
        const kind = capability.kind || 'unknown';
        counts[kind] = (counts[kind] || 0) + 1;
        return counts;
      }, {});
      const capabilities = Object.entries(capabilityCounts)
        .map(([kind, count]) => `<span class="plugins-collector-chip" title="${escapeHtml(kind)}">${escapeHtml(`${kind} · ${count}`)}</span>`)
        .join('');
      const error = plugin.last_error
        ? `<div class="plugins-error-message"><strong>${escapeHtml(t('common.error'))}:</strong> ${escapeHtml(plugin.last_error)}</div>`
        : '';
      const reason = plugin.management?.reason || t('plugins.management.readOnly');
      const managementLabel = plugin.management?.managed
        ? t('plugins.management.managed')
        : t('plugins.management.readOnly');
      const activationPending = pluginActivationPending(plugin);
      const pending = plugin.restart_required && !activationPending
        ? `<span class="plugins-badge plugins-badge-development">${escapeHtml(t('plugins.restart.pending'))}</span>`
        : '';
      const displayedState = activationPending ? 'restart_pending' : plugin.runtime_state;
      const displayedStateLabel = activationPending
        ? t('plugins.restart.pending')
        : stateLabel(plugin.runtime_state);
      const managed = Boolean(plugin.management?.managed);
      const desiredState = plugin.desired_state === 'disabled' ? 'disabled' : 'enabled';
      const toggleAction = desiredState === 'enabled' ? 'disable' : 'enable';
      const toggleAllowed = Boolean(plugin.management?.[`can_${toggleAction}`]) && !activeOperation;
      const upgradeAllowed = Boolean(plugin.management?.can_upgrade) && !activeOperation;
      const uninstallAllowed = Boolean(plugin.management?.can_uninstall) && !activeOperation;
      const lifecycleHint = managed
        ? t('plugins.lifecycle.hint')
        : reason;

      return `<article class="plugin-card" data-plugin-id="${escapeHtml(plugin.id)}">
        <div class="plugin-card-head">
          <div class="plugin-avatar" aria-hidden="true">${escapeHtml(initials(plugin.display_name))}</div>
          <div class="plugin-heading">
            <div class="plugin-title-row">
              <h2>${escapeHtml(plugin.display_name || plugin.id)}</h2>
              <span class="plugin-version">v${escapeHtml(version)}</span>
              ${pending}
            </div>
            <code>${escapeHtml(plugin.distribution || plugin.id)}</code>
          </div>
          <span class="plugins-badge ${badgeClass(displayedState)}">${escapeHtml(displayedStateLabel)}</span>
        </div>
        <p class="plugin-description">${escapeHtml(plugin.description || t('plugins.description.empty'))}</p>
        <div class="plugin-meta-row">
          <span class="plugins-badge ${badgeClass(plugin.source_type)}">${escapeHtml(sourceLabel(plugin.source_type))}</span>
          <span class="plugins-source-value" title="${escapeHtml(plugin.source)}">${escapeHtml(plugin.source)}</span>
          <span class="plugins-badge plugins-badge-neutral">${escapeHtml(t(`plugins.desired.${desiredState}`))}</span>
        </div>
        <div class="plugin-contributions">
          <span class="plugin-subheading">${escapeHtml(t('plugins.collectors.title'))}</span>
          <div class="plugins-collector-list">${collectors}</div>
          ${capabilities ? `<span class="plugin-subheading">${escapeHtml(t('plugins.capabilities.validated'))}</span><div class="plugins-collector-list">${capabilities}</div>` : ''}
        </div>
        ${error}
        <div class="plugin-card-actions">
          <span title="${escapeHtml(reason)}">${escapeHtml(managementLabel)}</span>
          <div>
            <button type="button" class="btn btn-sm" data-plugin-action="${toggleAction}" data-plugin-id="${escapeHtml(plugin.id)}" ${toggleAllowed ? '' : 'disabled'} title="${escapeHtml(lifecycleHint)}">${escapeHtml(t(`plugins.action.${toggleAction}`))}</button>
            ${plugin.latest_version ? `<button type="button" class="btn btn-sm" data-plugin-action="upgrade" data-plugin-id="${escapeHtml(plugin.id)}" ${upgradeAllowed ? '' : 'disabled'} title="${escapeHtml(lifecycleHint)}">${escapeHtml(t('plugins.action.upgrade'))}</button>` : ''}
            <button type="button" class="btn btn-sm btn-danger" data-plugin-action="uninstall" data-plugin-id="${escapeHtml(plugin.id)}" ${uninstallAllowed ? '' : 'disabled'} title="${escapeHtml(lifecycleHint)}">${escapeHtml(t('plugins.action.uninstall'))}</button>
          </div>
        </div>
      </article>`;
    }).join('');
  },

  _renderCatalog() {
    const target = this.container.querySelector('#plugins-catalog-list');
    if (!target) return;
    const active = operationIsActive(this.operations.active);
    const mutable = Boolean(this.environment?.mutable);
    const plugins = this.catalog.plugins || [];
    target.innerHTML = plugins.map((plugin) => {
      const updateAvailable = Boolean(plugin.update_available);
      const compatibilityReasons = plugin.compatibility?.reasons || [];
      const incompatible = plugin.compatibility?.compatible === false;
      const disabled = (plugin.installed && !updateAvailable) || !plugin.available || active || !mutable;
      let actionKey = 'plugins.catalog.install';
      if (incompatible) actionKey = 'plugins.catalog.incompatible';
      else if (updateAvailable) actionKey = 'plugins.catalog.upgrade';
      else if (plugin.installed) actionKey = 'plugins.catalog.installed';
      else if (active) actionKey = 'plugins.catalog.busy';
      else if (!plugin.available) actionKey = 'plugins.catalog.unavailable';
      else if (!mutable) actionKey = 'plugins.catalog.readOnly';
      const collectors = (plugin.collectors || [])
        .map((collector) => `<span class="plugins-collector-chip">${escapeHtml(collector)}</span>`)
        .join('');
      const compatibilityMessage = compatibilityReasons.length
        ? `<p class="plugins-compatibility-error"><strong>${escapeHtml(t('plugins.catalog.compatibility'))}:</strong> ${escapeHtml(compatibilityReasons.join(' '))}</p>`
        : '';
      const actionTitle = compatibilityReasons.join(' ') || this.environment?.reason || '';
      return `<article class="plugin-catalog-item" data-catalog-plugin="${escapeHtml(plugin.id)}">
        <span class="plugin-catalog-icon" aria-hidden="true">${escapeHtml(plugin.icon || initials(plugin.display_name))}</span>
        <div class="plugin-catalog-copy">
          <div class="plugin-title-row">
            <h3>${escapeHtml(plugin.display_name)}</h3>
            <span class="plugin-version">v${escapeHtml(plugin.version)}</span>
            <span class="plugins-badge plugins-badge-success">${escapeHtml(t('plugins.trust.official'))}</span>
          </div>
          <p>${escapeHtml(plugin.description)}</p>
          <small>${escapeHtml(`${plugin.publisher || ''}${plugin.license ? ` · ${plugin.license}` : ''}`)}</small>
          <div class="plugins-collector-list">${collectors}</div>
          ${compatibilityMessage}
        </div>
        <button type="button" class="btn btn-sm btn-primary" title="${escapeHtml(actionTitle)}" ${updateAvailable ? `data-plugin-upgrade="${escapeHtml(plugin.id)}"` : `data-plugin-install="${escapeHtml(plugin.id)}"`} ${disabled ? 'disabled' : ''}>${escapeHtml(t(actionKey))}</button>
      </article>`;
    }).join('');
    this._syncUploadButton();
  },

  _renderOperations() {
    const target = this.container.querySelector('#plugins-operations');
    if (!target) return;
    const items = this.operations.operations || [];
    if (!items.length) {
      target.innerHTML = `<p class="plugins-operations-empty">${escapeHtml(t('plugins.operations.empty'))}</p>`;
      return;
    }
    target.innerHTML = items.slice(0, 6).map((operation) => {
      const progress = Math.max(0, Math.min(100, Number(operation.progress) || 0));
      const error = operation.error_message
        ? `<p class="plugins-operation-error">${escapeHtml(operation.error_code || '')}: ${escapeHtml(operation.error_message)}</p>`
        : '';
      const deleteButton = operationIsActive(operation)
        ? ''
        : `<button type="button" class="btn btn-sm plugins-operation-delete" data-operation-delete="${escapeHtml(operation.id)}">${escapeHtml(t('plugins.operations.delete'))}</button>`;
      return `<article class="plugins-operation-item" data-operation-id="${escapeHtml(operation.id)}">
        <div class="plugins-operation-main">
          <div class="plugins-operation-identity">
            <strong>${escapeHtml(operation.plugin_id)}</strong>
            <span>${escapeHtml(operation.stage || operation.kind)}</span>
          </div>
          <div class="plugins-operation-actions">
            <span class="plugins-badge ${badgeClass(operation.state)}">${escapeHtml(operationLabel(operation.state))}</span>
            ${deleteButton}
          </div>
        </div>
        <div class="plugins-operation-progress" aria-label="${progress}%"><span style="width:${progress}%"></span></div>
        ${error}
      </article>`;
    }).join('');
  },

  async _deleteOperation(operationId) {
    const operation = (this.operations.operations || []).find((item) => item.id === operationId);
    if (!operation || operationIsActive(operation)) return;
    if (!window.confirm(t('plugins.operations.deleteConfirm', { name: operation.plugin_id }))) return;
    try {
      await api(`/plugin-manager/operations/${encodeURIComponent(operationId)}?confirm=true`, {
        method: 'DELETE',
      });
      await this.refresh(true);
      toast(t('plugins.operations.deleted'), 'success');
    } catch (error) {
      toast(t('plugins.operations.deleteFailed', { error: error.message }), 'error');
    }
  },

  _renderRestartBanner() {
    const banner = this.container.querySelector('#plugins-restart-banner');
    if (!banner) return;
    // Successful operations remain in history after the generation has been
    // loaded.  The reconciled inventory summary is therefore the authoritative
    // source for whether the current process still needs a restart.
    const pending = inventoryRequiresRestart(this.payload.summary);
    banner.hidden = !pending;
    const instructions = banner.querySelector('#plugins-restart-instructions');
    if (instructions) {
      instructions.textContent = this.environment?.restart_controller?.name === 'manual'
        ? t('plugins.restart.manualInstructions')
        : (this.environment?.restart_controller?.instructions || '');
    }
  },

  _renderEnvironment() {
    const target = this.container.querySelector('#plugins-environment');
    if (!target || !this.environment) return;
    const environment = this.environment;
    const mode = environment.mode || 'read_only';
    const modeBadge = this.container.querySelector('#plugins-mode-badge');
    if (modeBadge) modeBadge.textContent = t(`plugins.mode.${mode}`);
    const modeTitle = this.container.querySelector('#plugins-mode-title');
    const modeBody = this.container.querySelector('#plugins-mode-body');
    if (modeTitle) modeTitle.textContent = t(environment.mutable ? 'plugins.managed.title' : 'plugins.readOnly.title');
    if (modeBody) modeBody.textContent = t(environment.mutable ? 'plugins.managed.body' : 'plugins.readOnly.body');
    target.innerHTML = [
      environmentItem(t('plugins.environment.mode'), t(`plugins.mode.${mode}`)),
      environmentItem(t('plugins.environment.coreVersion'), environment.core_version),
      environmentItem(t('plugins.environment.pythonVersion'), environment.python_version),
      environmentItem(t('plugins.environment.platform'), `${environment.platform?.system || ''} ${environment.platform?.machine || ''}`.trim()),
      environmentItem(t('plugins.environment.capabilities'), (environment.runtime_capabilities || []).join(', ') || t('plugins.environment.noCapabilities'), { code: true }),
      environmentItem(t('plugins.environment.entryPoint'), environment.entry_point_group, { code: true }),
      environmentItem(t('plugins.environment.runtimeDir'), environment.runtime_dir, { code: true }),
      environmentItem(t('plugins.environment.generation'), environment.current_generation?.site_packages || t('plugins.environment.none'), { code: true }),
      environmentItem(t('plugins.environment.restart'), environment.restart_controller?.available ? environment.restart_controller.name : t('plugins.environment.manualRestart')),
      ...(!environment.mutable && environment.reason
        ? [environmentItem(t('plugins.environment.readOnlyReason'), environment.reason)]
        : []),
    ].join('');
    const rollback = this.container.querySelector('#btn-rollback-plugin-generation');
    if (rollback) {
      rollback.disabled = !environment.rollback?.available
        || operationIsActive(this.operations.active)
        || !environment.mutable;
      rollback.hidden = !environment.rollback?.available;
    }
  },

  _syncUploadButton() {
    const button = this.container.querySelector('#btn-upload-plugin');
    if (!button) return;
    button.disabled = !this.selectedWheel
      || operationIsActive(this.operations.active)
      || !this.environment?.mutable;
  },

  async _installCatalogPlugin(pluginId) {
    const plugin = (this.catalog.plugins || []).find((item) => item.id === pluginId);
    if (!plugin) return;
    if (!window.confirm(t('plugins.catalog.confirm', { name: plugin.display_name }))) return;
    try {
      const operation = await api('/plugin-manager/operations/install?confirm=true', {
        method: 'POST',
        body: JSON.stringify({ plugin_id: plugin.id, version: plugin.version }),
      });
      this.operations.active = operation;
      this.operations.operations = [operation, ...(this.operations.operations || [])];
      this._renderCatalog();
      this._renderOperations();
      this._schedulePolling();
      toast(t('plugins.catalog.accepted', { name: plugin.display_name }), 'success');
    } catch (error) {
      toast(t('plugins.operation.failed', { error: error.message }), 'error');
    }
  },

  async _upgradeCatalogPlugin(pluginId) {
    const plugin = (this.catalog.plugins || []).find((item) => item.id === pluginId);
    if (!plugin) return;
    if (!window.confirm(t('plugins.upgrade.confirm', { name: plugin.display_name, version: plugin.version }))) return;
    try {
      const operation = await api('/plugin-manager/operations/upgrade?confirm=true', {
        method: 'POST',
        body: JSON.stringify({ plugin_id: plugin.id, version: plugin.version }),
      });
      this._acceptOperation(operation);
      toast(t('plugins.upgrade.accepted', { name: plugin.display_name }), 'success');
    } catch (error) {
      toast(t('plugins.operation.failed', { error: error.message }), 'error');
    }
  },

  async _runPluginAction(pluginId, action) {
    const plugin = (this.payload.plugins || []).find((item) => item.id === pluginId);
    if (!plugin) return;
    if (action === 'enable' || action === 'disable') {
      const desiredState = action === 'enable' ? 'enabled' : 'disabled';
      if (!window.confirm(t(`plugins.${action}.confirm`, { name: plugin.display_name }))) return;
      try {
        await api(`/plugin-manager/plugins/${encodeURIComponent(pluginId)}/desired-state?confirm=true`, {
          method: 'PUT',
          body: JSON.stringify({ desired_state: desiredState }),
        });
        await this.refresh(true);
        toast(t(`plugins.${action}.accepted`, { name: plugin.display_name }), 'success');
      } catch (error) {
        toast(t('plugins.operation.failed', { error: error.message }), 'error');
      }
      return;
    }
    if (action === 'upgrade') {
      await this._upgradeCatalogPlugin(pluginId);
      return;
    }
    if (action === 'uninstall') await this._uninstallPlugin(plugin);
  },

  async _uninstallPlugin(plugin) {
    try {
      const detail = await api(`/plugin-manager/plugins/${encodeURIComponent(plugin.id)}`);
      const references = detail.references || [];
      if (references.length) {
        const names = references.slice(0, 6).map((item) => `${item.kind}: ${item.name}`).join(', ');
        toast(t('plugins.uninstall.referenced', { references: names }), 'error');
        return;
      }
      if (!window.confirm(t('plugins.uninstall.confirm', { name: plugin.display_name }))) return;
      const operation = await api('/plugin-manager/operations/uninstall?confirm=true', {
        method: 'POST',
        body: JSON.stringify({ plugin_id: plugin.id }),
      });
      this._acceptOperation(operation);
      toast(t('plugins.uninstall.accepted', { name: plugin.display_name }), 'success');
    } catch (error) {
      toast(t('plugins.operation.failed', { error: error.message }), 'error');
    }
  },

  async _rollbackGeneration() {
    if (!window.confirm(t('plugins.rollback.confirm'))) return;
    try {
      const operation = await api('/plugin-manager/operations/rollback?confirm=true', {
        method: 'POST',
      });
      this._acceptOperation(operation);
      toast(t('plugins.rollback.accepted'), 'success');
    } catch (error) {
      toast(t('plugins.operation.failed', { error: error.message }), 'error');
    }
  },

  _acceptOperation(operation) {
    this.operations.active = operation;
    this.operations.operations = [operation, ...(this.operations.operations || [])];
    this._renderPlugins();
    this._renderCatalog();
    this._renderOperations();
    this._renderEnvironment();
    this._schedulePolling();
  },

  async _installUploadedWheel() {
    if (!this.selectedWheel) return;
    if (!window.confirm(t('plugins.upload.confirm', { name: this.selectedWheel.name }))) return;
    const formData = new FormData();
    formData.append('file', this.selectedWheel);
    try {
      const operation = await api('/plugin-manager/operations/upload?confirm=true', {
        method: 'POST',
        body: formData,
      });
      this.operations.active = operation;
      this.operations.operations = [operation, ...(this.operations.operations || [])];
      this.selectedWheel = null;
      const input = this.container.querySelector('#plugins-wheel-input');
      if (input) input.value = '';
      const name = this.container.querySelector('#plugins-wheel-name');
      if (name) name.textContent = t('plugins.upload.none');
      this._renderCatalog();
      this._renderOperations();
      this._schedulePolling();
      toast(t('plugins.upload.accepted'), 'success');
    } catch (error) {
      toast(t('plugins.operation.failed', { error: error.message }), 'error');
    }
  },

  handleRoute(params) {
    if (!this.container) return;
    let shouldRerender = false;
    if (params?.search !== undefined) {
      this.filters.search = params.search;
      const input = this.container.querySelector('#plugins-search-input');
      if (input) input.value = params.search;
      shouldRerender = true;
    }
    if (params?.source !== undefined) {
      this.filters.source = params.source;
      const select = this.container.querySelector('#plugins-filter-source');
      if (select) select.value = params.source;
      shouldRerender = true;
    }
    if (params?.state !== undefined) {
      this.filters.state = params.state;
      const select = this.container.querySelector('#plugins-filter-state');
      if (select) select.value = params.state;
      shouldRerender = true;
    }
    if (shouldRerender) {
      this._renderPlugins();
    }
  },
};
