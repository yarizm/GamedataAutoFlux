FROM node:22-slim AS web-builder

WORKDIR /web
COPY src/web/package.json src/web/package-lock.json ./
RUN npm ci
COPY src/web/postcss.config.js src/web/vite.config.js ./
COPY src/web/src/ ./src/
RUN npm run build


FROM python:3.12-slim

# Core-only images do not install a browser. Playwright system dependencies are
# added later only when a selected collector plugin needs them.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app
ENV AUTOFLUX_PROJECT_ROOT=/app

# Copy project source (needed before pip install)
COPY pyproject.toml ./
COPY src/ ./src/
COPY config/ ./config/
COPY plugins/ ./plugins/
COPY --from=web-builder /web/static/dist/ ./src/web/static/dist/

# Space-separated plugin directory names, for example:
#   --build-arg AUTOFLUX_PLUGINS="steam youtube"
ARG AUTOFLUX_PLUGINS=""
RUN set -eux; \
    pip install --no-cache-dir .; \
    case " ${AUTOFLUX_PLUGINS} " in \
        *" official_site "*|*" monitor "*) pip install --no-cache-dir ./plugins/smart_web ;; \
    esac; \
    for plugin in ${AUTOFLUX_PLUGINS}; do \
        test -f "./plugins/${plugin}/pyproject.toml"; \
        pip install --no-cache-dir "./plugins/${plugin}"; \
    done; \
    case " ${AUTOFLUX_PLUGINS} " in \
        *" steam "*|*" taptap "*|*" qimai "*|*" official_site "*|*" dynamic_playwright "*) \
            playwright install chromium --with-deps ;; \
    esac

# Ensure runtime directories exist
RUN mkdir -p data logs tmp

# Expose port
EXPOSE 8000

# Start the application
CMD ["autoflux"]
