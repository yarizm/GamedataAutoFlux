// Help drawer shell: feature map + page help + tour list (spotlight is Task 5).
import { t } from '../i18n.js';
import { escapeHtml } from '../api.js';
import { mapCards, getPageHelp, tours, getTour } from './content.js';
import { isTourCompleted } from './storage.js';

/**
 * @param {{
 *   store: { get: (k: string) => any, subscribe: (fn: Function) => Function },
 *   activateTab: (tab: string) => void,
 *   ensurePage: (tab: string) => Promise<any>,
 *   onStartTour?: (tourId: string) => void,
 *   onDrawerClose?: () => void,
 * }} deps
 * @returns {{ open: Function, close: Function, toggle: Function, isOpen: Function, setTourHandler: Function, refresh: Function }}
 */
export function initHelp(deps) {
  const store = deps.store;
  const activateTab = deps.activateTab;
  const ensurePage = deps.ensurePage;

  const state = {
    open: false,
    section: 'map', // 'map' | 'page' | 'tours'
    onStartTour: typeof deps.onStartTour === 'function' ? deps.onStartTour : () => {},
    onDrawerClose: typeof deps.onDrawerClose === 'function' ? deps.onDrawerClose : null,
    root: null,
    titleEl: null,
    closeBtn: null,
    tabsEl: null,
    bodyEl: null,
  };

  function setTourHandler(fn) {
    state.onStartTour = typeof fn === 'function' ? fn : () => {};
  }

  function setSection(section) {
    const next = section === 'page' || section === 'tours' ? section : 'map';
    state.section = next;
    if (state.tabsEl) {
      state.tabsEl.querySelectorAll('[data-help-section]').forEach((btn) => {
        const active = btn.getAttribute('data-help-section') === next;
        btn.classList.toggle('is-active', active);
        btn.setAttribute('aria-selected', active ? 'true' : 'false');
      });
    }
  }

  function ensureDom() {
    if (state.root) return state.root;

    const root = document.createElement('div');
    root.id = 'help-drawer-root';
    root.setAttribute('aria-hidden', 'true');

    root.innerHTML = [
      '<div class="help-backdrop" data-help-action="close"></div>',
      '<aside class="help-panel" role="dialog" aria-modal="true" aria-labelledby="help-drawer-title">',
      '  <div class="help-panel-header">',
      '    <h2 id="help-drawer-title" class="help-panel-title"></h2>',
      '    <button type="button" class="btn btn-ghost btn-sm help-close-btn" data-help-action="close" aria-label="Close">×</button>',
      '  </div>',
      '  <div class="help-section-tabs" role="tablist"></div>',
      '  <div class="help-panel-body" role="tabpanel"></div>',
      '</aside>',
    ].join('');

    document.body.appendChild(root);
    state.root = root;
    state.titleEl = root.querySelector('#help-drawer-title');
    state.closeBtn = root.querySelector('.help-close-btn');
    state.tabsEl = root.querySelector('.help-section-tabs');
    state.bodyEl = root.querySelector('.help-panel-body');

    root.addEventListener('click', onRootClick);
    return root;
  }

  function onRootClick(e) {
    const target = e.target instanceof Element ? e.target.closest('[data-help-action]') : null;
    if (!target || !state.root?.contains(target)) return;

    const action = target.getAttribute('data-help-action');
    if (action === 'close') {
      close();
      return;
    }
    if (action === 'section') {
      const section = target.getAttribute('data-help-section') || 'map';
      setSection(section);
      renderAll();
      return;
    }
    if (action === 'open-tab') {
      const tab = target.getAttribute('data-tab');
      if (tab) void navigateToTab(tab);
      return;
    }
    if (action === 'start-tour') {
      const tourId = target.getAttribute('data-tour-id');
      if (tourId) startTour(tourId);
    }
  }

  function startTour(tourId) {
    if (!getTour(tourId)) return;
    state.onStartTour(tourId);
  }

  async function navigateToTab(tab) {
    try {
      if (typeof ensurePage === 'function') await ensurePage(tab);
    } catch (err) {
      console.error('help ensurePage failed:', err);
    }
    if (typeof activateTab === 'function') activateTab(tab);
    setSection('page');
    renderPage();
    // Keep map/tours chrome in sync (tabs active state + title)
    renderChrome();
  }

  function renderChrome() {
    ensureDom();
    if (state.titleEl) {
      state.titleEl.textContent = t('help.entry');
    }
    // Close control: keep a neutral "Close" label (not help.entry)
    if (state.closeBtn) {
      state.closeBtn.setAttribute('aria-label', 'Close');
      state.closeBtn.removeAttribute('title');
    }
    if (state.tabsEl) {
      const sections = [
        { id: 'map', key: 'help.section.map' },
        { id: 'page', key: 'help.section.page' },
        { id: 'tours', key: 'help.section.tours' },
      ];
      state.tabsEl.innerHTML = sections
        .map((s) => {
          const active = state.section === s.id;
          return (
            `<button type="button" role="tab" class="help-section-tab${active ? ' is-active' : ''}"` +
            ` data-help-action="section" data-help-section="${s.id}"` +
            ` aria-selected="${active ? 'true' : 'false'}">${escapeHtml(t(s.key))}</button>`
          );
        })
        .join('');
    }
  }

  function renderMap() {
    if (!state.bodyEl) return;
    const cards = mapCards
      .map((card) => {
        const title = escapeHtml(t(card.titleKey));
        const blurb = escapeHtml(t(card.blurbKey));
        const badge =
          card.badge === 'advanced'
            ? `<span class="badge-advanced">${escapeHtml(t('help.badge.advanced'))}</span>`
            : '';
        const openLabel = escapeHtml(t('help.openPage'));
        return (
          `<article class="help-map-card" data-card-id="${escapeHtml(card.id)}">` +
          `<div class="help-map-card-head">` +
          `<h3 class="help-map-card-title">${title}</h3>${badge}` +
          `</div>` +
          `<p class="help-map-card-blurb">${blurb}</p>` +
          `<button type="button" class="btn btn-sm help-map-open" data-help-action="open-tab" data-tab="${escapeHtml(card.tab)}">${openLabel}</button>` +
          `</article>`
        );
      })
      .join('');
    state.bodyEl.innerHTML = `<div class="help-map-list">${cards}</div>`;
  }

  function renderPage() {
    if (!state.bodyEl) return;
    const tab = store?.get?.('activeTab') || '';
    const help = getPageHelp(tab);
    if (!help) {
      state.bodyEl.innerHTML =
        `<div class="help-empty">${escapeHtml(t('help.empty.page'))}</div>`;
      return;
    }

    const summary = escapeHtml(t(help.summaryKey));
    const points = (help.points || [])
      .map((key) => `<li>${escapeHtml(t(key))}</li>`)
      .join('');
    const nextSteps = (help.nextSteps || [])
      .map((step) => {
        const label = escapeHtml(t(step.labelKey));
        if (step.action?.type === 'tab' && step.action.tab) {
          return (
            `<button type="button" class="btn btn-sm help-next-step"` +
            ` data-help-action="open-tab" data-tab="${escapeHtml(step.action.tab)}">${label}</button>`
          );
        }
        return `<span class="help-next-step-label">${label}</span>`;
      })
      .join('');

    let tourBtn = '';
    if (help.tourId && getTour(help.tourId)) {
      tourBtn =
        `<button type="button" class="btn btn-primary btn-sm help-start-page-tour"` +
        ` data-help-action="start-tour" data-tour-id="${escapeHtml(help.tourId)}">` +
        `${escapeHtml(t('help.startPageTour'))}</button>`;
    }

    state.bodyEl.innerHTML =
      `<div class="help-page">` +
      `<p class="help-page-summary">${summary}</p>` +
      (points ? `<ul class="help-page-points">${points}</ul>` : '') +
      (nextSteps ? `<div class="help-page-next">${nextSteps}</div>` : '') +
      (tourBtn ? `<div class="help-page-tour">${tourBtn}</div>` : '') +
      `</div>`;
  }

  function renderTours() {
    if (!state.bodyEl) return;
    const items = Object.values(tours)
      .map((tour) => {
        const id = tour.id;
        const title = escapeHtml(t(tour.titleKey));
        const completed = isTourCompleted(id);
        const badge = completed
          ? `<span class="help-tour-badge">${escapeHtml(t('help.tour.completed'))}</span>`
          : '';
        const actionLabel = escapeHtml(
          completed ? t('help.tour.replay') : t('help.startPageTour'),
        );
        return (
          `<div class="help-tour-row" data-tour-id="${escapeHtml(id)}">` +
          `<div class="help-tour-meta">` +
          `<span class="help-tour-title">${title}</span>${badge}` +
          `</div>` +
          `<button type="button" class="btn btn-sm${completed ? '' : ' btn-primary'}"` +
          ` data-help-action="start-tour" data-tour-id="${escapeHtml(id)}">${actionLabel}</button>` +
          `</div>`
        );
      })
      .join('');
    state.bodyEl.innerHTML = `<div class="help-tour-list">${items || ''}</div>`;
  }

  function renderAll() {
    ensureDom();
    renderChrome();
    if (state.section === 'page') renderPage();
    else if (state.section === 'tours') renderTours();
    else renderMap();
  }

  function open(opts = {}) {
    ensureDom();
    if (opts.section === 'map' || opts.section === 'page' || opts.section === 'tours') {
      setSection(opts.section);
    }
    state.open = true;
    state.root.classList.add('is-open');
    state.root.setAttribute('aria-hidden', 'false');
    document.body.classList.add('help-drawer-open');
    renderAll();
    // Focus close control for keyboard users (non-interruptive: only when opening)
    queueMicrotask(() => {
      state.closeBtn?.focus?.({ preventScroll: true });
    });
  }

  function close() {
    if (!state.root) return;
    state.open = false;
    state.root.classList.remove('is-open');
    state.root.setAttribute('aria-hidden', 'true');
    document.body.classList.remove('help-drawer-open');
    try {
      state.onDrawerClose?.();
    } catch (err) {
      console.error('help onDrawerClose failed:', err);
    }
  }

  function toggle() {
    if (state.open) close();
    else open();
  }

  function isOpen() {
    return state.open;
  }

  function refresh() {
    if (state.open) renderAll();
  }

  // Subscribe once: refresh page section when tab changes while open
  if (store && typeof store.subscribe === 'function') {
    store.subscribe((key) => {
      if (key === 'activeTab' && state.open) {
        if (state.section === 'page') renderPage();
      }
    });
  }

  // Esc closes drawer (spotlight handles Esc first when Task 5 is wired)
  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Escape') return;
    if (!state.open) return;
    // If spotlight root is active, let Task 5 own Esc
    if (document.getElementById('help-spotlight-root')?.classList.contains('is-open')) return;
    e.preventDefault();
    close();
  });

  // Build DOM lazily on first open — never auto-open at init
  return {
    open,
    close,
    toggle,
    isOpen,
    setTourHandler,
    refresh,
  };
}
