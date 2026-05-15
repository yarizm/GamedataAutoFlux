export function renderPagination({ page, pageSize, total }) {
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const hasPrev = page > 1;
  const hasNext = page < totalPages;

  let html = '<div style="display:flex;align-items:center;justify-content:space-between;margin-top:1rem">';
  html += `<span class="text-muted text-sm">共 ${total} 条，第 ${page}/${totalPages} 页</span>`;
  html += '<div class="btn-group">';

  html += `<button class="btn btn-sm" data-page="prev" ${hasPrev ? '' : 'disabled'}>上一页</button>`;

  const start = Math.max(1, page - 2);
  const end = Math.min(totalPages, page + 2);
  for (let i = start; i <= end; i++) {
    html += `<button class="btn btn-sm ${i === page ? 'btn-primary' : ''}" data-page="${i}">${i}</button>`;
  }

  html += `<button class="btn btn-sm" data-page="next" ${hasNext ? '' : 'disabled'}>下一页</button>`;
  html += '</div></div>';

  return {
    html,
    bind(container, onChange) {
      container.querySelectorAll('[data-page]').forEach(btn => {
        btn.addEventListener('click', () => {
          const p = btn.dataset.page;
          if (p === 'prev') onChange(page - 1);
          else if (p === 'next') onChange(page + 1);
          else onChange(parseInt(p, 10));
        });
      });
    },
  };
}
