import assert from 'node:assert/strict';
import {
  renderEmptyState,
  renderLoadingState,
  renderErrorState,
  renderTableSkeleton,
  renderCardSkeleton,
  renderDetailSkeleton,
} from './uiState.js';

// 1. Table skeleton generates expected rows & cols
const tableHtml = renderTableSkeleton({ cols: 5, rows: 4 });
assert.match(tableHtml, /<tr class="border-b border-theme-subtle animate-pulse">/);
assert.match(tableHtml, /<div class="skeleton-shimmer h-4 rounded"/);
const rowMatches = tableHtml.match(/<tr/g) || [];
assert.equal(rowMatches.length, 4);

// 2. Card skeleton generates expected count
const cardHtml = renderCardSkeleton({ count: 3 });
const cardMatches = cardHtml.match(/skeleton-shimmer/g) || [];
assert.ok(cardMatches.length >= 9);

// 3. Detail skeleton generates valid markup
const detailHtml = renderDetailSkeleton();
assert.match(detailHtml, /grid-cols-1 md:grid-cols-2/);
assert.match(detailHtml, /skeleton-shimmer/);

// 4. Backward compatibility
assert.match(renderLoadingState({ label: 'Loading...' }), /ui-loading-label/);
assert.match(renderEmptyState({ title: 'No Data' }), /ui-empty-title/);
assert.match(renderErrorState({ message: 'Failed' }), /ui-error-title/);

console.log('UI_STATE_SELFTEST_OK');
