import CodeMirror from 'codemirror';
import 'codemirror/mode/javascript/javascript.js';
import 'codemirror/lib/codemirror.css';
import 'codemirror/theme/dracula.css';
import 'codemirror/theme/eclipse.css'; // light

let editorsInitialized = false;

function cmThemeName() {
  return document.documentElement.dataset.theme === 'light' ? 'eclipse' : 'dracula';
}

function createJsonEditor(textareaId, options = {}) {
  const textarea = document.getElementById(textareaId);
  if (!textarea) return null;
  const existing = textarea.nextElementSibling?.CodeMirror;
  if (existing) return existing;

  const editor = CodeMirror.fromTextArea(textarea, {
    mode: { name: 'javascript', json: true },
    theme: cmThemeName(),
    lineNumbers: true,
    lineWrapping: true,
    tabSize: 2,
    ...options,
  });
  return editor;
}

export function applyEditorsTheme() {
  const name = cmThemeName();
  [window.taskTargetsEditor, window.pipelineStepsEditor].forEach((ed) => {
    if (ed) ed.setOption('theme', name);
  });
}

export function initEditors() {
  if (editorsInitialized) return;
  editorsInitialized = true;
  window.CodeMirror = CodeMirror;
  if (!window.taskTargetsEditor) {
    window.taskTargetsEditor = createJsonEditor('task-targets', { viewportMargin: Infinity });
    if (window.taskTargetsEditor) window.taskTargetsEditor.setSize(null, 150);
  }
  if (!window.pipelineStepsEditor) {
    window.pipelineStepsEditor = createJsonEditor('pipeline-steps', { viewportMargin: Infinity });
    if (window.pipelineStepsEditor) window.pipelineStepsEditor.setSize(null, 200);
  }
}

export function refreshEditorForModal(id) {
  if (id === 'modal-create-task' && window.taskTargetsEditor) {
    window.taskTargetsEditor.refresh();
  }
  if (id === 'modal-create-pipeline' && window.pipelineStepsEditor) {
    window.pipelineStepsEditor.refresh();
  }
}

if (typeof window !== 'undefined') {
  window.addEventListener('themechange', applyEditorsTheme);
}
