const STORAGE_KEY = 'gamedata-autoflux.theme';
const PREFERENCES = new Set(['dark', 'light', 'system']);
const DEFAULT_PREFERENCE = 'dark';

let mediaQuery = null;
let mediaHandler = null;

export function normalizePreference(value) {
  return PREFERENCES.has(value) ? value : DEFAULT_PREFERENCE;
}

export function getThemePreference() {
  try {
    return normalizePreference(localStorage.getItem(STORAGE_KEY));
  } catch {
    return DEFAULT_PREFERENCE;
  }
}

/** @param {string} preference @param {boolean} [systemDark] */
export function resolveTheme(preference, systemDark) {
  const pref = normalizePreference(preference);
  if (pref === 'light') return 'light';
  if (pref === 'dark') return 'dark';
  const dark =
    typeof systemDark === 'boolean'
      ? systemDark
      : typeof window !== 'undefined' &&
        window.matchMedia?.('(prefers-color-scheme: dark)').matches;
  return dark ? 'dark' : 'light';
}

export function applyResolvedTheme(theme) {
  const resolved = theme === 'light' ? 'light' : 'dark';
  document.documentElement.dataset.theme = resolved;
  document.documentElement.style.colorScheme = resolved;
  return resolved;
}

export function applyThemePreference(preference, options = {}) {
  const pref = normalizePreference(preference ?? getThemePreference());
  try {
    localStorage.setItem(STORAGE_KEY, pref);
  } catch {
    /* ignore */
  }
  document.documentElement.dataset.themePreference = pref;
  const theme = applyResolvedTheme(resolveTheme(pref));
  if (!options.skipEvent) {
    window.dispatchEvent(
      new CustomEvent('themechange', { detail: { preference: pref, theme } }),
    );
  }
  refreshThemeControls();
  return theme;
}

export function setThemePreference(preference, options = {}) {
  return applyThemePreference(preference, options);
}

export function initTheme() {
  const theme = applyThemePreference(getThemePreference(), { skipEvent: true });
  if (typeof window !== 'undefined' && window.matchMedia) {
    mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
    mediaHandler = () => {
      if (getThemePreference() === 'system') {
        applyThemePreference('system');
      }
    };
    mediaQuery.addEventListener?.('change', mediaHandler);
  }
  return theme;
}

export function bindThemeControls(root = document) {
  root.querySelectorAll('[data-theme-pref]').forEach((button) => {
    if (button.dataset.themeBound === 'true') return;
    button.dataset.themeBound = 'true';
    button.addEventListener('click', () => {
      setThemePreference(button.dataset.themePref);
    });
  });
  refreshThemeControls(root);
}

function refreshThemeControls(root = document) {
  const pref = getThemePreference();
  root.querySelectorAll('[data-theme-pref]').forEach((button) => {
    const selected = button.dataset.themePref === pref;
    button.classList.toggle('active', selected);
    button.setAttribute('aria-pressed', selected ? 'true' : 'false');
  });
}

export { STORAGE_KEY, DEFAULT_PREFERENCE };
