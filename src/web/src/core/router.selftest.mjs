import assert from 'node:assert/strict';
import { parseRoute, buildRoute, VALID_TABS } from './router.js';

function testValidTabs() {
  assert.ok(Array.isArray(VALID_TABS), 'VALID_TABS must be an array');
  assert.ok(VALID_TABS.includes('dashboard'), 'VALID_TABS must include dashboard');
  assert.ok(VALID_TABS.includes('tasks'), 'VALID_TABS must include tasks');
  assert.ok(VALID_TABS.includes('dag'), 'VALID_TABS must include dag');
  assert.ok(VALID_TABS.includes('plugins'), 'VALID_TABS must include plugins');
  assert.ok(VALID_TABS.includes('agent'), 'VALID_TABS must include agent');
}

function testParseRoute() {
  // 1. Empty / null / undefined
  assert.deepEqual(parseRoute(''), { tab: 'dashboard', params: {} });
  assert.deepEqual(parseRoute(null), { tab: 'dashboard', params: {} });
  assert.deepEqual(parseRoute('#'), { tab: 'dashboard', params: {} });
  assert.deepEqual(parseRoute('#/'), { tab: 'dashboard', params: {} });

  // 2. Simple tab
  assert.deepEqual(parseRoute('#/tasks'), { tab: 'tasks', params: {} });
  assert.deepEqual(parseRoute('#tasks'), { tab: 'tasks', params: {} });
  assert.deepEqual(parseRoute('/plugins'), { tab: 'plugins', params: {} });

  // 3. Tab with query parameters
  const routeWithParams = parseRoute('#/tasks?id=task_123&status=running');
  assert.equal(routeWithParams.tab, 'tasks');
  assert.equal(routeWithParams.params.id, 'task_123');
  assert.equal(routeWithParams.params.status, 'running');

  // 4. DAG query parameters
  const dagRoute = parseRoute('#/dag?name=steam_basic');
  assert.equal(dagRoute.tab, 'dag');
  assert.equal(dagRoute.params.name, 'steam_basic');

  // 5. Invalid tab fallback
  const invalidRoute = parseRoute('#/nonexistent_tab?foo=bar');
  assert.equal(invalidRoute.tab, 'dashboard');
  assert.equal(invalidRoute.params.foo, 'bar');

  // 6. Encoded characters
  const encodedRoute = parseRoute('#/plugins?search=%E6%B8%B8%E6%88%8F');
  assert.equal(encodedRoute.tab, 'plugins');
  assert.equal(encodedRoute.params.search, '游戏');
}

function testBuildRoute() {
  // 1. Simple tab without params
  assert.equal(buildRoute('tasks'), '#/tasks');
  assert.equal(buildRoute('dag'), '#/dag');

  // 2. Invalid tab fallback
  assert.equal(buildRoute('invalid'), '#/dashboard');

  // 3. Tab with params
  const withParams = buildRoute('tasks', { id: 'task_123', status: 'running' });
  assert.ok(withParams.startsWith('#/tasks?'), `Expected start with #/tasks?, got: ${withParams}`);
  assert.ok(withParams.includes('id=task_123'));
  assert.ok(withParams.includes('status=running'));

  // 4. Tab with null / undefined / empty params filtered
  const filtered = buildRoute('dag', { name: 'steam_basic', empty: '', nul: null, undef: undefined });
  assert.equal(filtered, '#/dag?name=steam_basic');

  // 5. Encoded characters
  const encoded = buildRoute('plugins', { search: '游戏' });
  assert.ok(encoded.includes('search=%E6%B8%B8%E6%88%8F') || encoded.includes('search=%EF%BF%BD') || encoded.includes('search='));
  assert.equal(parseRoute(encoded).params.search, '游戏');
}

function runAll() {
  testValidTabs();
  testParseRoute();
  testBuildRoute();
  console.log('ROUTER_SELFTEST_OK');
}

runAll();
