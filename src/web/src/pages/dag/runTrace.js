/**
 * DAG run trace: fold task events into per-node run state.
 * Mirrors src/core/dag_trace.py so WebSocket live events and the
 * /tasks/{id}/dag-trace snapshot produce identical states.
 */
import { t } from '../../core/i18n.js';

const STATUS_FROM_EVENT = {
  started: 'running',
  succeeded: 'succeeded',
  failed: 'failed',
  retrying: 'retrying',
  skipped: 'skipped',
};

export const TERMINAL_TASK_STATUSES = new Set(['success', 'failed', 'cancelled']);

/**
 * Apply one task event (as stored / broadcast) to a states map. Returns the
 * same map for chaining; events without payload.node_id are ignored.
 * @param {Record<string, object>} states
 * @param {object} event
 */
export function applyNodeEvent(states, event) {
  const payload = event?.payload && typeof event.payload === 'object' ? event.payload : {};
  const nodeId = payload.node_id;
  if (typeof nodeId !== 'string' || !nodeId) return states;
  const entry = states[nodeId] || { node_id: nodeId };
  const snapshot = payload.node;
  if (snapshot && typeof snapshot === 'object') {
    states[nodeId] = { ...entry, ...snapshot };
    return states;
  }
  const status = STATUS_FROM_EVENT[String(payload.status || '')];
  if (!status) return states; // progress events do not change node status
  states[nodeId] = {
    ...entry,
    status,
    ...(payload.error ? { error: payload.error } : {}),
    ...(payload.component && !entry.component ? { component: payload.component } : {}),
  };
  return states;
}

/** @param {object[]} events in seq order */
export function foldNodeEvents(events) {
  const states = {};
  for (const event of events || []) applyNodeEvent(states, event);
  return states;
}

export function formatElapsed(ms) {
  const value = Number(ms);
  if (!Number.isFinite(value) || value < 0) return '';
  if (value < 1000) return `${Math.round(value)}ms`;
  const seconds = value / 1000;
  if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 1 : 0)}s`;
  const minutes = Math.floor(seconds / 60);
  const rest = Math.round(seconds % 60);
  return `${minutes}m ${String(rest).padStart(2, '0')}s`;
}

function countsText(state) {
  const counts = state.output_counts || {};
  if (state.node_type === 'storage') {
    return t('dag.run.stored', { count: counts.stored ?? 0 });
  }
  if (state.node_type === 'logic' && state.branch) {
    return t('dag.run.branch', { branch: state.branch });
  }
  if ('rejected' in counts) {
    return t('dag.run.filtered', { kept: counts.records ?? 0, rejected: counts.rejected ?? 0 });
  }
  if ('records' in counts) {
    return t('dag.run.records', { count: counts.records });
  }
  return '';
}

/**
 * Human readable node badge for the canvas and run panel.
 * @param {object} state NodeRun snapshot
 * @returns {{ status: string, icon: string, text: string, title: string }}
 */
export function describeRunState(state) {
  const status = String(state?.status || 'pending');
  const elapsed = formatElapsed(state?.elapsed_ms);
  const attempts = Number(state?.attempts || 0);
  const maxAttempts = Number(state?.max_attempts || 1);
  const parts = [];
  let icon = '•';
  if (status === 'running') {
    icon = '⟳';
    parts.push(t('dag.run.status.running'));
    if (attempts > 1) parts.push(t('dag.run.attempt', { attempt: attempts, max: maxAttempts }));
  } else if (status === 'retrying') {
    icon = '⟳';
    parts.push(t('dag.run.attempt', { attempt: attempts, max: maxAttempts }));
  } else if (status === 'succeeded') {
    icon = '✓';
    const counts = countsText(state);
    if (counts) parts.push(counts);
    if (attempts > 1) parts.push(t('dag.run.attempt', { attempt: attempts, max: maxAttempts }));
  } else if (status === 'failed') {
    icon = '✗';
    parts.push(t('dag.run.status.failed'));
  } else if (status === 'exception') {
    icon = '!';
    parts.push(t('dag.run.status.exception'));
    const counts = countsText(state);
    if (counts && state.branch) parts.push(counts);
  } else if (status === 'skipped') {
    icon = '↷';
    parts.push(t('dag.run.status.skipped'));
  } else if (status === 'not_run') {
    icon = '–';
    parts.push(t('dag.run.status.not_run'));
  } else {
    parts.push(t('dag.run.status.pending'));
  }
  if (elapsed && !['skipped', 'not_run', 'pending'].includes(status)) parts.push(elapsed);
  const title = [parts.join(' · '), state?.error || ''].filter(Boolean).join('\n');
  return { status, icon, text: parts.join(' · '), title };
}

/** Count node states by status (for the run bar). */
export function summarizeRun(states) {
  const summary = { total: 0 };
  for (const state of Object.values(states || {})) {
    const status = String(state?.status || 'pending');
    summary[status] = (summary[status] || 0) + 1;
    summary.total += 1;
  }
  return summary;
}
