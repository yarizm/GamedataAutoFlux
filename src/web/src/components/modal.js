export class Modal {
  constructor({ id, title, wide, onClose } = {}) {
    this.id = id;
    this.title = title || '';
    this.wide = !!wide;
    this.onClose = onClose || null;
  }

  open() {
    const el = document.getElementById(this.id);
    if (el) {
      el.classList.add('show');
      setTimeout(() => {
        const cm = el.querySelector('.CodeMirror');
        if (cm && cm.CodeMirror) cm.CodeMirror.refresh();
      }, 10);
    }
  }

  close() {
    const el = document.getElementById(this.id);
    if (el) el.classList.remove('show');
    if (this.onClose) this.onClose();
  }

  setContent(html) {
    const body = document.querySelector(`#${this.id} .modal-body`);
    if (body) body.innerHTML = html;
  }

  setTitle(text) {
    const titleEl = document.querySelector(`#${this.id} .modal-header h2`);
    if (titleEl) titleEl.textContent = text;
  }

  static bindOverlayClose() {
    document.querySelectorAll('.modal-overlay').forEach((overlay) => {
      overlay.addEventListener('click', (e) => {
        if (e.target === overlay) overlay.classList.remove('show');
      });
    });
  }
}
