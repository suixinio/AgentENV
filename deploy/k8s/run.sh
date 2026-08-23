#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <render|apply|delete> [kubectl args...]" >&2
  exit 1
fi

MODE="$1"
shift
KUBECTL_BIN="${KUBECTL:-kubectl}"
OVERLAY_NAME="${K8S_OVERLAY:-default}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TEMP_DIR}"' EXIT

sed_in_place() {
  local expression="$1"
  local file="$2"
  if sed --version >/dev/null 2>&1; then
    sed -i "${expression}" "${file}"
  else
    sed -i '' "${expression}" "${file}"
  fi
}

cp -R "${SCRIPT_DIR}" "${TEMP_DIR}/k8s"
cp "${REPO_ROOT}/config/default.toml" "${TEMP_DIR}/k8s/base/config/agentenv.toml"

# 🔴 The images the render points at, injected rather than hard-coded.
#
# `kustomization.yaml` names the three images with no registry and the tag
# `latest`, which is right for a laptop and wrong for every real cluster: an
# apply there pulls `agentenv-runtime:latest` from Docker Hub and every Pod sits
# in ImagePullBackOff. That is the one drift point in this tree whose failure is
# *loud* (`_sd-impl-phase3-role.md` §10.2 ①) — and being loud, it drowns the
# first screen of output after an apply, which is exactly when somebody is
# looking for the quiet ones. So it is fixed first and separately.
#
# Both are optional and independent: IMAGE_REGISTRY prefixes all three names,
# IMAGE_TAG replaces all three tags. One tag for the three because they are built
# together from one commit; an image built from a different commit than the other
# two is not a case this should make easy to express.
#
# Each substitution verifies itself. A silently-unapplied rewrite here produces
# exactly the ImagePullBackOff it was meant to prevent, one step further from its
# cause.
if [[ -n "${IMAGE_REGISTRY:-}" ]]; then
  ESCAPED_IMAGE_REGISTRY="${IMAGE_REGISTRY%/}"
  ESCAPED_IMAGE_REGISTRY="${ESCAPED_IMAGE_REGISTRY//\\/\\\\}"
  ESCAPED_IMAGE_REGISTRY="${ESCAPED_IMAGE_REGISTRY//&/\\&}"
  sed_in_place "s#^\( *\)newName: \(agentenv-[a-z-]*\)\$#\1newName: ${ESCAPED_IMAGE_REGISTRY}/\2#" "${TEMP_DIR}/k8s/base/kustomization.yaml"
  if ! grep -q "newName: ${IMAGE_REGISTRY%/}/agentenv-" "${TEMP_DIR}/k8s/base/kustomization.yaml"; then
    echo "failed to apply IMAGE_REGISTRY=${IMAGE_REGISTRY} to the images: block" >&2
    exit 1
  fi
fi

if [[ -n "${IMAGE_TAG:-}" ]]; then
  ESCAPED_IMAGE_TAG="${IMAGE_TAG//\\/\\\\}"
  ESCAPED_IMAGE_TAG="${ESCAPED_IMAGE_TAG//&/\\&}"
  sed_in_place "s#^\( *\)newTag: .*\$#\1newTag: ${ESCAPED_IMAGE_TAG}#" "${TEMP_DIR}/k8s/base/kustomization.yaml"
  if [[ "${IMAGE_TAG}" != "latest" ]] && grep -q "newTag: latest" "${TEMP_DIR}/k8s/base/kustomization.yaml"; then
    echo "failed to apply IMAGE_TAG=${IMAGE_TAG}; some image is still on latest" >&2
    exit 1
  fi
fi

if [[ "${SANDBOX_PROXY_DOMAINS+x}" == "x" ]]; then
  ESCAPED_SANDBOX_PROXY_DOMAINS="${SANDBOX_PROXY_DOMAINS//\\/\\\\}"
  ESCAPED_SANDBOX_PROXY_DOMAINS="${ESCAPED_SANDBOX_PROXY_DOMAINS//&/\\&}"
  ESCAPED_SANDBOX_PROXY_DOMAINS="${ESCAPED_SANDBOX_PROXY_DOMAINS//#/\\#}"
  sed_in_place "s#- SANDBOX_PROXY_DOMAINS=.*#- SANDBOX_PROXY_DOMAINS=${ESCAPED_SANDBOX_PROXY_DOMAINS}#" "${TEMP_DIR}/k8s/base/kustomization.yaml"
fi

OVERLAY_PATH="${TEMP_DIR}/k8s/overlays/${OVERLAY_NAME}"
if [[ ! -d "${OVERLAY_PATH}" ]]; then
  echo "unknown overlay: ${OVERLAY_NAME}" >&2
  exit 1
fi

if [[ "${OVERLAY_NAME}" == "local-dev" ]]; then
  REPO_ENV_PATH="${AENV_LOCAL_REPO_ENV_PATH:-${REPO_ROOT}/env}"
  if [[ ! -d "${REPO_ENV_PATH}" ]]; then
    echo "local-dev overlay requires a readable env directory at ${REPO_ENV_PATH}" >&2
    exit 1
  fi

  ESCAPED_REPO_ENV_PATH="${REPO_ENV_PATH//\\/\\\\}"
  ESCAPED_REPO_ENV_PATH="${ESCAPED_REPO_ENV_PATH//&/\\&}"
  sed_in_place "s#path: \"\"#path: \"${ESCAPED_REPO_ENV_PATH}\"#" "${OVERLAY_PATH}/kustomization.yaml"

  if grep -q 'path: ""' "${OVERLAY_PATH}/kustomization.yaml"; then
    echo "failed to render local-dev repo env hostPath; path is still empty" >&2
    exit 1
  fi
fi

case "${MODE}" in
  render)
    "${KUBECTL_BIN}" kustomize "${OVERLAY_PATH}" "$@"
    ;;
  apply)
    "${KUBECTL_BIN}" apply -k "${OVERLAY_PATH}" "$@"
    ;;
  delete)
    "${KUBECTL_BIN}" delete --ignore-not-found -k "${OVERLAY_PATH}" "$@"
    ;;
  *)
    echo "unsupported mode: ${MODE}" >&2
    exit 1
    ;;
esac
