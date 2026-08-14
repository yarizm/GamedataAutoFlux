import { t } from '../i18n.js';
import {
  isInlineGuideCollapsed,
  setInlineGuideCollapsed,
} from './storage.js';

const DEFAULT_VERSION = 'v1';

function applyState(root, collapsed) {
  const body = root.querySelector('[data-inline-guide-body]');
  const toggle = root.querySelector('[data-inline-guide-toggle]');
  root.classList.toggle('is-collapsed', collapsed);
  if (body) body.hidden = collapsed;
  if (toggle) {
    toggle.setAttribute('aria-expanded', String(!collapsed));
    toggle.textContent = t(collapsed ? 'help.inline.expand' : 'help.inline.collapse');
  }
}

export function initInlineGuides(scope = document) {
  const roots = Array.from(scope.querySelectorAll('[data-inline-guide]'));

  for (const root of roots) {
    if (root.dataset.inlineGuideBound === '1') continue;
    const guideId = String(root.dataset.inlineGuide || '').trim();
    const version = String(root.dataset.inlineGuideVersion || DEFAULT_VERSION).trim();
    if (!guideId) continue;

    root.dataset.inlineGuideBound = '1';
    applyState(root, isInlineGuideCollapsed(guideId, version));
    root.querySelector('[data-inline-guide-toggle]')?.addEventListener('click', () => {
      const collapsed = !root.classList.contains('is-collapsed');
      setInlineGuideCollapsed(guideId, collapsed, version);
      applyState(root, collapsed);
    });
  }

  return {
    refresh() {
      for (const root of roots) {
        const guideId = String(root.dataset.inlineGuide || '').trim();
        const version = String(root.dataset.inlineGuideVersion || DEFAULT_VERSION).trim();
        applyState(root, isInlineGuideCollapsed(guideId, version));
      }
    },
  };
}
