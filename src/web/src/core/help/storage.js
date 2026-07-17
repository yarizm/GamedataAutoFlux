export const TOUR_STORAGE_PREFIX = 'gamedata-autoflux.help.tour.';

function keyFor(tourId) {
  return `${TOUR_STORAGE_PREFIX}${String(tourId || '').trim()}`;
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
