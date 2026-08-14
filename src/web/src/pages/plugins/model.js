/** Pure helpers for Plugin Center filtering and translation keys. */

export function pluginMatchesFilters(plugin, filters = {}) {
  if (filters.source && plugin.source_type !== filters.source) return false;
  if (filters.state && plugin.runtime_state !== filters.state) return false;
  const search = String(filters.search || '').trim().toLowerCase();
  if (!search) return true;
  const haystack = [
    plugin.display_name,
    plugin.distribution,
    plugin.description,
    ...(plugin.collectors || []),
  ].join(' ').toLowerCase();
  return haystack.includes(search);
}

export function pluginStateTranslationKey(state) {
  return `plugins.state.${state === 'not_loaded' ? 'notLoaded' : state}`;
}

export function operationIsActive(operation) {
  return Boolean(operation && ['queued', 'running'].includes(operation.state));
}

export function pluginActivationPending(plugin = {}) {
  return Boolean(
    plugin.restart_required
    && plugin.desired_state !== 'disabled'
    && ['inactive', 'not_loaded'].includes(plugin.runtime_state),
  );
}

export function inventoryRequiresRestart(summary = {}) {
  return Number(summary.restart_required || 0) > 0;
}
