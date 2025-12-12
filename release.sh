#!/usr/bin/env bash
set -euo pipefail

# ------------------------------
# Configuración
# ------------------------------
RELEASE_BRANCH="1.1.x.sigic.whl"
BASE_VERSION="1.1.0"

# ------------------------------
# Checks básicos
# ------------------------------
BRANCH=$(git branch --show-current)
if [ "$BRANCH" != "$RELEASE_BRANCH" ]; then
  echo "❌ Debes estar en la rama $RELEASE_BRANCH (actual: $BRANCH)"
  exit 1
fi

if ! git diff-index --quiet HEAD --; then
  echo "❌ Working tree sucio"
  exit 1
fi

# ------------------------------
# Calcular postN
# ------------------------------
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

# ------------------------------
# Actualizar pyproject.toml
# ------------------------------
python - <<EOF
from pathlib import Path
import tomllib, tomli_w

path = Path("pyproject.toml")
data = tomllib.loads(path.read_text())
data["project"]["version"] = "$VERSION"
path.write_text(tomli_w.dumps(data))
EOF

git add pyproject.toml
git commit -m "chore(release): $VERSION"

# ------------------------------
# Crear tag y push
# ------------------------------
git tag "$TAG"
git push origin "$RELEASE_BRANCH"
git push origin "$TAG"

echo "🚀 Tag creado y enviado: $TAG"
echo "➡️  GitHub Actions hará el build y el release"
