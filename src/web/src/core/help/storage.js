export const TOUR_STORAGE_PREFIX = 'gamedata-autoflux.help.tour.';
export const INLINE_GUIDE_STORAGE_PREFIX = 'gamedata-autoflux.help.inline.';

function keyFor(tourId) {
  return `${TOUR_STORAGE_PREFIX}${String(tourId || '').trim()}`;
}

function inlineKeyFor(guideId, version) {
  const id = String(guideId || '').trim();
  const normalizedVersion = String(version || '').trim() || 'v1';
  return `${INLINE_GUIDE_STORAGE_PREFIX}${id}.${normalizedVersion}`;
}

export function isTourCompleted(tourId) {
  const id = String(tourId || '').trim();
  if (!id) return false;
  try {
    return localStorage.getItem(keyFor(id)) === '1';
  } catch {
    return false;
  }
}

export function markTourCompleted(tourId) {
  const id = String(tourId || '').trim();
  if (!id) return;
  try {
    localStorage.setItem(keyFor(id), '1');
  } catch {
    /* ignore quota / private mode */
  }
}

export function clearTourCompleted(tourId) {
  const id = String(tourId || '').trim();
  if (!id) return;
  try {
    localStorage.removeItem(keyFor(id));
  } catch {
    /* ignore */
  }
}

export function isInlineGuideCollapsed(guideId, version = 'v1') {
  const id = String(guideId || '').trim();
  if (!id) return false;
  try {
    return localStorage.getItem(inlineKeyFor(id, version)) === '1';
  } catch {
    return false;
  }
}

export function setInlineGuideCollapsed(guideId, collapsed, version = 'v1') {
  const id = String(guideId || '').trim();
  if (!id) return;
  try {
    const key = inlineKeyFor(id, version);
    if (collapsed) localStorage.setItem(key, '1');
    else localStorage.removeItem(key);
  } catch {
    /* ignore quota / private mode */
  }
}
