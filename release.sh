#!/usr/bin/env bash
set -euo pipefail

# ==================================================
# Configuración
# ==================================================

RELEASE_BRANCH="1.1.x.sigic.whl"
BASE_VERSION="1.1.0"

TOKEN="${GITHUB_TOKEN_CI:-}"

REPO_FULL=$(git config --get remote.origin.url | sed -E 's#.*github.com[:/](.+)/(.+)\.git#\1/\2#')
OWNER="${REPO_FULL%/*}"
REPO="${REPO_FULL#*/}"

# ==================================================
# Checks de seguridad
# ==================================================

BRANCH=$(git branch --show-current)
HEAD=$(git rev-parse HEAD)

echo "🔐 Rama actual: $BRANCH"
[ "$BRANCH" = "$RELEASE_BRANCH" ] || {
  echo "❌ Debe ejecutarse desde $RELEASE_BRANCH"
  exit 1
}

[ -n "$TOKEN" ] || {
  echo "❌ GITHUB_TOKEN_CI no definido"
  exit 1
}

command -v jq >/dev/null || { echo "❌ Falta jq"; exit 1; }

python - <<'EOF'
import tomli_w
EOF

if ! git diff-index --quiet HEAD --; then
  echo "❌ Working tree sucio"
  exit 1
fi

echo "🔎 Verificando CI para commit $HEAD"

RAW=$(curl -s \
  -H "Authorization: token $TOKEN" \
  -H "Accept: application/vnd.github+json" \
  "https://api.github.com/repos/$OWNER/$REPO/commits/$HEAD/check-runs")

TOTAL=$(echo "$RAW" | jq '.total_count')
[ "$TOTAL" -gt 0 ] || {
  echo "❌ No hay checks reportados"
  exit 1
}

FAILED=$(echo "$RAW" | jq '[.check_runs[] | select(.conclusion != "success")] | length')
[ "$FAILED" -eq 0 ] || {
  echo "❌ CI no está verde"
  exit 1
}

echo "✅ CI verificado"

# ==================================================
# Cálculo de versión postN (PEP 440)
# ==================================================

LAST_TAG=$(git tag --list "v${BASE_VERSION}.post*" --sort=-v:refname | head -n1)

if [ -z "$LAST_TAG" ]; then
  POST=1
else
  POST="${LAST_TAG##*.post}"
  POST=$((POST + 1))
fi

VERSION="${BASE_VERSION}.post${POST}"
TAG="v${VERSION}"

echo "📦 Nueva versión: $VERSION"

# ==================================================
# Actualizar versión en pyproject.toml
# ==================================================

echo "✏️  Actualizando pyproject.toml a versión $VERSION"

python - <<EOF
from pathlib import Path
import tomllib
import tomli_w

path = Path("pyproject.toml")
data = tomllib.loads(path.read_text())

data["project"]["version"] = "$VERSION"

path.write_text(tomli_w.dumps(data))
EOF

# ==================================================
# Actualizar __version__ en código
# ==================================================

VERSION_FILE="src/sigic_geonode_importer/__init__.py"

if [ -f "$VERSION_FILE" ]; then
  echo "✏️  Actualizando __version__ a $VERSION"

  python - <<EOF
import re, pathlib

path = pathlib.Path("$VERSION_FILE")
text = path.read_text()

text = re.sub(
    r'__version__\s*=\s*"[^"]+"',
    '__version__ = "$VERSION"',
    text,
)

path.write_text(text)
EOF
fi

# ==================================================
# Validar __version__ si existe
# ==================================================

VERSION_FILE="src/sigic_geonode_importer/__init__.py"

if [ -f "$VERSION_FILE" ]; then
  CODE_VERSION=$(python - <<EOF
import re, pathlib
text = pathlib.Path("$VERSION_FILE").read_text()
m = re.search(r'__version__\s*=\s*"([^"]+)"', text)
print(m.group(1) if m else "")
EOF
)

  if [ -n "$CODE_VERSION" ] && [ "$CODE_VERSION" != "$VERSION" ]; then
    echo "❌ __version__ NO coincide"
    echo "   código         : $CODE_VERSION"
    echo "   release        : $VERSION"
    exit 1
  fi

  echo "✅ __version__ validado"
fi

# ==================================================
# Crear y subir tag
# ==================================================

git add pyproject.toml src/sigic_geonode_importer/__init__.py
git commit -m "chore(release): $VERSION"

git push origin "$RELEASE_BRANCH"

git tag "$TAG"
git push origin "$TAG"

echo "🚀 Tag creado: $TAG"
echo "📦 El workflow publicará el wheel automáticamente"
