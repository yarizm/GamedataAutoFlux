// Spotlight tour engine: step highlight + bubble over page targets.
import { t } from '../i18n.js';
import { toast, escapeHtml } from '../api.js';
import { getTour } from './content.js';
import { markTourCompleted } from './storage.js';

const PAD = 8;
const BUBBLE_MARGIN = 12;

function nextFrame() {
  return new Promise((resolve) => {
    if (typeof requestAnimationFrame === 'function') {
      requestAnimationFrame(() => resolve());
    } else {
      setTimeout(resolve, 16);
    }
  });
}

/**
 * @param {{
 *   ensurePage?: (tab: string) => Promise<any>,
 *   activateTab?: (tab: string) => void,
 *   onComplete?: (tourId?: string) => void,
 *   onStop?: () => void,
 * }} deps
 * @returns {{ start: (tourId: string) => void, stop: () => void, isActive: () => boolean }}
 */
export function createSpotlight(deps = {}) {
  const ensurePage = deps.ensurePage;
  const activateTab = deps.activateTab;
  const onComplete = typeof deps.onComplete === 'function' ? deps.onComplete : null;
  const onStop = typeof deps.onStop === 'function' ? deps.onStop : null;

  let active = false;
  let tourId = null;
  let steps = [];
  let index = 0;
  let root = null;
  let holeEl = null;
  let bubbleEl = null;
  /** Bumps to cancel in-flight async showStep work. */
  let showToken = 0;

  function ensureDom() {
    if (root) return root;

    root = document.createElement('div');
    root.id = 'help-spotlight-root';
    root.className = 'help-spot-layer';
    root.setAttribute('aria-hidden', 'true');

    holeEl = document.createElement('div');
    holeEl.className = 'help-spot-hole';
    holeEl.setAttribute('aria-hidden', 'true');

    bubbleEl = document.createElement('div');
    bubbleEl.className = 'help-spot-bubble';
    bubbleEl.setAttribute('role', 'dialog');
    bubbleEl.setAttribute('aria-modal', 'true');

    root.appendChild(holeEl);
    root.appendChild(bubbleEl);
    document.body.appendChild(root);

    bubbleEl.addEventListener('click', onBubbleClick);
    return root;
  }

  function onBubbleClick(e) {
    const btn = e.target instanceof Element ? e.target.closest('[data-spot-action]') : null;
    if (!btn || !bubbleEl?.contains(btn)) return;

    const action = btn.getAttribute('data-spot-action');
    if (action === 'back') {
      if (index > 0) {
        index -= 1;
        void showStep();
      }
      return;
    }
    if (action === 'next' || action === 'done') {
      index += 1;
      void showStep();
      return;
    }
    if (action === 'skip') {
      // Skip entire tour without marking completed
      stop();
    }
  }

  function measureHole(el) {
    const rect = el.getBoundingClientRect();
    return {
      top: Math.max(0, rect.top - PAD),
      left: Math.max(0, rect.left - PAD),
      width: Math.max(0, rect.width + PAD * 2),
      height: Math.max(0, rect.height + PAD * 2),
    };
  }

  function applyHole(hole) {
    if (!holeEl) return;
    holeEl.style.top = `${hole.top}px`;
    holeEl.style.left = `${hole.left}px`;
    holeEl.style.width = `${hole.width}px`;
    holeEl.style.height = `${hole.height}px`;
  }

  function placeBubble(hole) {
    if (!bubbleEl) return;
    const bw = bubbleEl.offsetWidth || 320;
    const bh = bubbleEl.offsetHeight || 120;
    const vw = window.innerWidth || 1024;
    const vh = window.innerHeight || 768;

    let top = hole.top + hole.height + BUBBLE_MARGIN;
    if (top + bh > vh - BUBBLE_MARGIN) {
      top = hole.top - bh - BUBBLE_MARGIN;
    }
    if (top < BUBBLE_MARGIN) top = BUBBLE_MARGIN;

    let left = hole.left;
    if (left + bw > vw - BUBBLE_MARGIN) {
      left = vw - bw - BUBBLE_MARGIN;
    }
    if (left < BUBBLE_MARGIN) left = BUBBLE_MARGIN;

    bubbleEl.style.top = `${top}px`;
    bubbleEl.style.left = `${left}px`;
  }

  function renderBubble(step, isLast) {
    const title = escapeHtml(t(step.titleKey));
    const body = escapeHtml(t(step.bodyKey));
    const progress = escapeHtml(`${index + 1} / ${steps.length}`);
    const backLabel = escapeHtml(t('help.tour.back'));
    const nextLabel = escapeHtml(t('help.tour.next'));
    const skipLabel = escapeHtml(t('help.tour.skip'));
    const doneLabel = escapeHtml(t('help.tour.done'));

    const backBtn =
      index > 0
        ? `<button type="button" class="btn btn-ghost btn-sm" data-spot-action="back">${backLabel}</button>`
        : `<span class="help-spot-back-spacer"></span>`;

    const primary = isLast
      ? `<button type="button" class="btn btn-primary btn-sm" data-spot-action="done">${doneLabel}</button>`
      : `<button type="button" class="btn btn-primary btn-sm" data-spot-action="next">${nextLabel}</button>`;

    bubbleEl.innerHTML =
      `<div class="help-spot-bubble-head">` +
      `<strong class="help-spot-title">${title}</strong>` +
      `<span class="help-spot-progress">${progress}</span>` +
      `</div>` +
      `<p class="help-spot-body">${body}</p>` +
      `<div class="help-spot-actions">` +
      backBtn +
      `<div class="help-spot-actions-right">` +
      `<button type="button" class="btn btn-ghost btn-sm" data-spot-action="skip">${skipLabel}</button>` +
      primary +
      `</div>` +
      `</div>`;

    bubbleEl.setAttribute('aria-label', t(step.titleKey));
  }

  async function runBefore(step) {
    const before = step?.before;
    if (!before || typeof before !== 'string') return;
    if (!before.startsWith('ensure-tab:')) return;

    const tab = before.slice('ensure-tab:'.length).trim();
    if (!tab) return;

    try {
      if (typeof ensurePage === 'function') await ensurePage(tab);
    } catch (err) {
      console.error('spotlight ensurePage failed:', err);
    }
    if (typeof activateTab === 'function') activateTab(tab);
    await nextFrame();
    await nextFrame();
  }

  async function showStep() {
    const token = ++showToken;

    if (index >= steps.length) {
      const completedId = tourId;
      if (completedId) markTourCompleted(completedId);
      stop({ completed: true });
      try {
        onComplete?.(completedId);
      } catch (err) {
        console.error('spotlight onComplete failed:', err);
      }
      return;
    }

    const step = steps[index];
    await runBefore(step);
    if (token !== showToken || !active) return;

    const el = step.target ? document.querySelector(step.target) : null;
    if (!el) {
      toast(t('help.tour.missingTarget'));
      index += 1;
      await showStep();
      return;
    }

    ensureDom();
    root.classList.add('is-open');
    root.setAttribute('aria-hidden', 'false');

    try {
      el.scrollIntoView({ block: 'center', behavior: 'smooth' });
    } catch {
      try {
        el.scrollIntoView(true);
      } catch {
        /* ignore */
      }
    }
    await nextFrame();
    await nextFrame();
    if (token !== showToken || !active) return;

    const hole = measureHole(el);
    applyHole(hole);
    renderBubble(step, index === steps.length - 1);
    placeBubble(hole);

    queueMicrotask(() => {
      bubbleEl
        ?.querySelector('[data-spot-action="done"], [data-spot-action="next"]')
        ?.focus?.({ preventScroll: true });
    });
  }

  function setOpenChrome(open) {
    ensureDom();
    if (open) {
      root.classList.add('is-open');
      root.setAttribute('aria-hidden', 'false');
    } else {
      root.classList.remove('is-open');
      root.setAttribute('aria-hidden', 'true');
      if (holeEl) {
        holeEl.style.top = '';
        holeEl.style.left = '';
        holeEl.style.width = '';
        holeEl.style.height = '';
      }
      if (bubbleEl) {
        bubbleEl.innerHTML = '';
        bubbleEl.style.top = '';
        bubbleEl.style.left = '';
      }
    }
  }

  /**
   * @param {string} id
   */
  function start(id) {
    const tour = getTour(id);
    if (!tour) return;

    // Clear any existing run without mark / onStop (restart)
    stop({ silent: true });

    tourId = tour.id;
    steps = Array.isArray(tour.steps) ? tour.steps.slice() : [];
    index = 0;
    active = true;
    setOpenChrome(true);
    void showStep();
  }

  /**
   * @param {{ completed?: boolean, silent?: boolean }} [opts]
   */
  function stop(opts = {}) {
    const wasActive = active;
    showToken += 1;
    active = false;
    tourId = null;
    steps = [];
    index = 0;

    if (root) setOpenChrome(false);

    if (wasActive && !opts.completed && !opts.silent) {
      try {
        onStop?.();
      } catch (err) {
        console.error('spotlight onStop failed:', err);
      }
    }
  }

  function isActive() {
    return active;
  }

  function repositionActive() {
    if (!active || !steps[index]) return;
    const step = steps[index];
    const el = step.target ? document.querySelector(step.target) : null;
    if (!el) return;
    const hole = measureHole(el);
    applyHole(hole);
    placeBubble(hole);
  }

  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Escape') return;
    if (!active) return;
    e.preventDefault();
    // stop mid-way: no markTourCompleted
    stop();
  });

  window.addEventListener('resize', repositionActive);
  window.addEventListener('scroll', repositionActive, true);

  return { start, stop, isActive };
}
