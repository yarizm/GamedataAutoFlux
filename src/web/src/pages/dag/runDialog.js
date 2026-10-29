/**
 * "Run DAG" dialog: collects targets with the entry collector's target_schema
 * form (same contract as the task wizard) and hands them to the page.
 */
import { toast } from '../../core/api.js';
import { t } from '../../core/i18n.js';
import {
  buildFallbackTarget,
  buildTargetsFromMetadata,
  parseAdvancedTargetsJson,
  renderMetadataTargetForm,
} from '../../core/targetForm.js';
import { getCachedCollectorMeta } from './schemas.js';

const MODAL_ID = 'modal-dag-run';
const PREFIX = 'dagrun';

/**
 * Collector whose target_schema drives the form: the one fed by the input
 * node's params port, else the first collector that is not chained upstream.
 * @param {{ nodes: object[], edges: object[] }} editor
 */
export function pickEntryCollector(editor) {
  const nodes = editor?.nodes || [];
  const edges = editor?.edges || [];
  const collectors = nodes.filter((n) => n.type === 'collector');
  const fedByInput = edges
    .filter((e) => (e.in || '') === 'params')
    .map((e) => collectors.find((n) => n.id === e.to))
    .find(Boolean);
  return fedByInput
    || collectors.find((n) => !n.config?.from_upstream)
    || collectors[0]
    || null;
}

function ensureModal() {
  let modal = document.getElementById(MODAL_ID);
  if (modal) return modal;
  modal = document.createElement('div');
  modal.className = 'modal-overlay';
  modal.id = MODAL_ID;
  modal.innerHTML = `
    <div class="modal dag-run-modal">
      <div class="modal-header">
        <h2 data-run-title></h2>
        <button class="btn-close" type="button" data-run-close>&times;</button>
      </div>
      <div class="modal-body">
        <div class="form-group">
          <label for="${PREFIX}-task-name" data-run-label-name></label>
          <input type="text" id="${PREFIX}-task-name">
        </div>
        <p class="text-muted text-[11px] mb-2" data-run-collector></p>
        <div class="form-group">
          <label for="${PREFIX}-target-name"></label>
          <input type="text" id="${PREFIX}-target-name">
          <small id="${PREFIX}-target-name-help" class="metadata-target-help" hidden></small>
        </div>
        <div id="${PREFIX}-metadata-target-fields" hidden></div>
        <details class="mt-2">
          <summary class="text-[11px] text-muted cursor-pointer" data-run-advanced></summary>
          <textarea id="${PREFIX}-targets-json" rows="4" class="mt-2 w-full font-mono text-xs" placeholder='[{"name": "Deadlock", "target_type": "game", "params": {}}]'></textarea>
        </details>
        <p class="text-muted text-[10px] mt-3" data-run-hint></p>
      </div>
      <div class="modal-footer">
        <button class="btn btn-ghost" type="button" data-run-cancel></button>
        <button class="btn btn-primary" type="button" data-run-submit></button>
      </div>
    </div>`;
  document.body.appendChild(modal);
  const close = () => window.closeModal?.(MODAL_ID);
  modal.querySelector('[data-run-close]')?.addEventListener('click', close);
  modal.querySelector('[data-run-cancel]')?.addEventListener('click', close);
  modal.addEventListener('mousedown', (ev) => {
    if (ev.target === modal) close();
  });
  return modal;
}

function defaultTaskName(dagName) {
  const now = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  return `${dagName} · ${t('dag.run.trial')} ${pad(now.getMonth() + 1)}${pad(now.getDate())}-${pad(now.getHours())}${pad(now.getMinutes())}`;
}

/**
 * @param {{
 *   editor: object,
 *   dagName: string,
 *   onSubmit: (payload: { name: string, targets: object[] }) => Promise<boolean>,
 * }} options
 */
export function openRunDialog({ editor, dagName, onSubmit }) {
  const modal = ensureModal();
  const entry = pickEntryCollector(editor);
  const metadata = entry ? getCachedCollectorMeta(entry.component) : null;

  modal.querySelector('[data-run-title]').textContent = t('dag.run.dialogTitle', { name: dagName });
  modal.querySelector('[data-run-label-name]').textContent = t('dag.run.taskName');
  modal.querySelector('[data-run-advanced]').textContent = t('dag.run.advancedTargets');
  modal.querySelector('[data-run-hint]').textContent = t('dag.run.dialogHint');
  modal.querySelector('[data-run-cancel]').textContent = t('common.cancel');
  const submitBtn = modal.querySelector('[data-run-submit]');
  submitBtn.textContent = t('dag.run.start');
  modal.querySelector('[data-run-collector]').textContent = entry
    ? t('dag.run.formFrom', { collector: metadata?.display_name || entry.component })
    : t('dag.run.noCollector');

  const nameInput = modal.querySelector(`#${PREFIX}-task-name`);
  nameInput.value = defaultTaskName(dagName);
  const targetName = modal.querySelector(`#${PREFIX}-target-name`);
  targetName.value = '';
  modal.querySelector(`#${PREFIX}-targets-json`).value = '';
  renderMetadataTargetForm(PREFIX, metadata);

  submitBtn.onclick = async () => {
    let targets = buildTargetsFromMetadata(PREFIX, metadata);
    if (!targets || !targets.length) {
      targets = buildFallbackTarget(PREFIX, metadata?.target_schema?.target_type || 'game');
    }
    const raw = modal.querySelector(`#${PREFIX}-targets-json`).value;
    try {
      const parsed = parseAdvancedTargetsJson(raw, targetName.value.trim() || 'Target');
      if (parsed) targets = parsed;
    } catch {
      toast(t('message.targetsJsonInvalid'), 'error');
      return;
    }
    if (!targets.length) {
      toast(t('message.targetRequired'), 'error');
      return;
    }
    submitBtn.disabled = true;
    try {
      const ok = await onSubmit({ name: nameInput.value.trim() || defaultTaskName(dagName), targets });
      if (ok) window.closeModal?.(MODAL_ID);
    } finally {
      submitBtn.disabled = false;
    }
  };

  window.openModal?.(MODAL_ID);
  setTimeout(() => targetName.focus(), 30);
}
