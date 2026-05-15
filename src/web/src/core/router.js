const AUTO_REFRESH_INTERVAL_MS = 30000;
let autoRefreshHandle = null;

export function activateTab(tab, store) {
  store.set('activeTab', tab);

  document.querySelectorAll('.nav-link').forEach((item) => {
    item.classList.toggle('active', item.dataset.tab === tab);
  });

  document.querySelectorAll('.tab-content').forEach((panel) => panel.classList.remove('active'));
  const target = document.getElementById(`tab-${tab}`);
  if (target) target.classList.add('active');
}

export function bindNavigation(store) {
  document.querySelectorAll('.nav-link').forEach((link) => {
    link.addEventListener('click', (e) => {
      e.preventDefault();
      activateTab(link.dataset.tab, store);
    });
  });
}

export function bindModalOverlayClose() {
  document.querySelectorAll('.modal-overlay').forEach((overlay) => {
    overlay.addEventListener('click', (e) => {
      if (e.target === overlay) {
        overlay.classList.remove('show');
      }
    });
  });
}

export function restartAutoRefresh(store) {
  if (autoRefreshHandle) {
    clearInterval(autoRefreshHandle);
    autoRefreshHandle = null;
  }

  autoRefreshHandle = setInterval(() => {
    const tab = store.get('activeTab');
    if (tab === 'dashboard') { store.set('refresh', 'dashboard'); }
    else if (tab === 'tasks') { store.set('refresh', 'tasks'); }
    else if (tab === 'data') { store.set('refresh', 'data'); }
    else if (tab === 'cron') { store.set('refresh', 'cron'); }
    else if (tab === 'system') { store.set('refresh', 'system'); }
  }, AUTO_REFRESH_INTERVAL_MS);
}

export function openModal(id) {
  const modal = document.getElementById(id);
  if (modal) {
    modal.classList.add('show');
    setTimeout(() => {
      if (id === 'modal-create-task' && window.taskTargetsEditor) {
        window.taskTargetsEditor.refresh();
      }
      if (id === 'modal-create-pipeline' && window.pipelineStepsEditor) {
        window.pipelineStepsEditor.refresh();
      }
    }, 10);
  }
}

export function closeModal(id) {
  const modal = document.getElementById(id);
  if (modal) modal.classList.remove('show');
}

// Backward compat
window.activateTab = activateTab;
window.bindNavigation = bindNavigation;
window.bindModalOverlayClose = bindModalOverlayClose;
window.restartAutoRefresh = restartAutoRefresh;
window.openModal = openModal;
window.closeModal = closeModal;
