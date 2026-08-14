"""Inventory, catalog, and managed plugin installation API."""

from __future__ import annotations

import uuid
from pathlib import Path

from fastapi import APIRouter, File, HTTPException, Query, Request, UploadFile, status
from packaging.utils import InvalidWheelFilename, parse_wheel_filename
from pydantic import BaseModel, Field

from src.plugin_manager import build_plugin_inventory, get_environment_inventory
from src.plugin_manager.catalog import plugin_catalog
from src.plugin_manager.compatibility import evaluate_catalog_plugin, evaluate_wheel
from src.plugin_manager.operations import (
    OperationConflictError,
    PluginOperationService,
)
from src.plugin_manager.package_reader import normalize_distribution, read_wheel_metadata
from src.plugin_manager.references import PluginReferenceScanError
from src.plugin_manager.restart import RestartUnavailableError, restart_controller
from src.plugin_manager.store import plugin_state_store
from src.web.safety import require_explicit_confirmation


router = APIRouter(prefix="/plugin-manager", tags=["plugin-manager"])
MAX_UPLOAD_SIZE = 200 * 1024 * 1024


class CatalogInstallRequest(BaseModel):
    plugin_id: str = Field(min_length=1, max_length=200)
    version: str | None = Field(default=None, max_length=100)


class DesiredStateRequest(BaseModel):
    desired_state: str = Field(pattern="^(enabled|disabled)$")


def _error(
    status_code: int,
    code: str,
    message: str,
    **details,
) -> HTTPException:
    return HTTPException(
        status_code=status_code,
        detail={"code": code, "message": message, **details},
    )


def _operation_service(request: Request) -> PluginOperationService:
    service = getattr(request.app.state, "plugin_operations", None)
    if service is None:
        raise _error(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "PLUGIN_MANAGER_UNAVAILABLE",
            "The plugin operation service is not running.",
        )
    return service


def _require_mutable_environment() -> None:
    environment = get_environment_inventory()
    if not environment["mutable"]:
        raise _error(
            status.HTTP_409_CONFLICT,
            "PLUGIN_ENV_READ_ONLY",
            environment["reason"] or "The plugin environment is read-only.",
        )


@router.get("/plugins")
async def list_plugin_inventory():
    """Merge runtime diagnostics with managed installation state."""

    return build_plugin_inventory()


@router.get("/plugins/{plugin_id}")
async def get_plugin_detail(plugin_id: str, request: Request):
    payload = build_plugin_inventory()
    plugin = next(
        (item for item in payload["plugins"] if item["id"] == plugin_id),
        None,
    )
    if plugin is None:
        raise _error(
            status.HTTP_404_NOT_FOUND,
            "PLUGIN_NOT_FOUND",
            "Plugin was not found.",
        )
    references = []
    service = _operation_service(request)
    scanner = service.reference_scanner
    if plugin["management"]["managed"] and scanner is not None:
        try:
            references = await scanner.scan(plugin.get("collectors") or [])
        except PluginReferenceScanError as exc:
            raise _error(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                exc.code,
                str(exc),
            ) from exc
    return {
        "plugin": plugin,
        "references": [item.to_dict() for item in references],
    }


@router.get("/catalog")
async def list_plugin_catalog():
    """Return trusted bundled plugins and their installation state."""

    return plugin_catalog.payload()


@router.get("/operations")
async def list_plugin_operations(limit: int = Query(50, ge=1, le=200)):
    operations = plugin_state_store.list_operations(limit=limit)
    return {
        "operations": [item.to_dict() for item in operations],
        "total": len(operations),
        "active": next(
            (
                item.to_dict()
                for item in operations
                if item.state in {"queued", "running"}
            ),
            None,
        ),
    }


@router.get("/operations/{operation_id}")
async def get_plugin_operation(operation_id: str):
    operation = plugin_state_store.get_operation(operation_id)
    if operation is None:
        raise _error(
            status.HTTP_404_NOT_FOUND,
            "PLUGIN_OPERATION_NOT_FOUND",
            "Plugin operation was not found.",
        )
    return operation.to_dict()


@router.delete("/operations/{operation_id}")
async def delete_plugin_operation(
    operation_id: str,
    confirm: bool = Query(False),
):
    """Delete a completed operation from visible history."""

    require_explicit_confirmation(confirm, "plugin operation history deletion")
    _require_mutable_environment()
    try:
        operation = plugin_state_store.delete_operation(operation_id)
    except KeyError as exc:
        raise _error(
            status.HTTP_404_NOT_FOUND,
            "PLUGIN_OPERATION_NOT_FOUND",
            "Plugin operation was not found.",
        ) from exc
    except ValueError as exc:
        raise _error(
            status.HTTP_409_CONFLICT,
            "PLUGIN_OPERATION_ACTIVE",
            "An active plugin operation cannot be deleted.",
        ) from exc
    return {"deleted": True, "operation": operation.to_dict()}


@router.get("/environment")
async def get_plugin_environment():
    """Describe the current managed plugin environment."""

    return get_environment_inventory()


@router.post("/operations/install", status_code=status.HTTP_202_ACCEPTED)
async def install_catalog_plugin(
    payload: CatalogInstallRequest,
    request: Request,
    confirm: bool = Query(False),
):
    require_explicit_confirmation(confirm, "plugin installation")
    _require_mutable_environment()
    catalog_entry = plugin_catalog.get(payload.plugin_id)
    if catalog_entry is None:
        raise _error(
            status.HTTP_404_NOT_FOUND,
            "PLUGIN_NOT_FOUND",
            "The requested plugin is not present in a trusted catalog.",
        )
    compatibility = evaluate_catalog_plugin(catalog_entry)
    if not compatibility.compatible:
        raise _error(
            status.HTTP_409_CONFLICT,
            "PLUGIN_INCOMPATIBLE",
            " ".join(compatibility.reasons),
            reasons=list(compatibility.reasons),
        )
    try:
        operation = await _operation_service(request).submit_catalog_install(
            payload.plugin_id,
            requested_version=payload.version,
        )
    except OperationConflictError as exc:
        raise _error(status.HTTP_409_CONFLICT, exc.code, str(exc)) from exc
    return operation.to_dict()


@router.post("/operations/upgrade", status_code=status.HTTP_202_ACCEPTED)
async def upgrade_catalog_plugin(
    payload: CatalogInstallRequest,
    request: Request,
    confirm: bool = Query(False),
):
    require_explicit_confirmation(confirm, "plugin upgrade")
    _require_mutable_environment()
    managed = plugin_state_store.get_managed_plugin(payload.plugin_id)
    if managed is None:
        raise _error(
            status.HTTP_404_NOT_FOUND,
            "PLUGIN_NOT_INSTALLED",
            "Managed plugin is not installed.",
        )
    catalog_entry = plugin_catalog.get(payload.plugin_id)
    if catalog_entry is None:
        raise _error(
            status.HTTP_409_CONFLICT,
            "PLUGIN_UPGRADE_UNAVAILABLE",
            "The plugin is not backed by a trusted catalog entry.",
        )
    compatibility = evaluate_catalog_plugin(catalog_entry)
    if not compatibility.compatible:
        raise _error(
            status.HTTP_409_CONFLICT,
            "PLUGIN_INCOMPATIBLE",
            " ".join(compatibility.reasons),
            reasons=list(compatibility.reasons),
        )
    try:
        operation = await _operation_service(request).submit_catalog_upgrade(
            payload.plugin_id,
            requested_version=payload.version,
        )
    except OperationConflictError as exc:
        raise _error(status.HTTP_409_CONFLICT, exc.code, str(exc)) from exc
    return operation.to_dict()


@router.put("/plugins/{plugin_id}/desired-state")
async def set_plugin_desired_state(
    plugin_id: str,
    payload: DesiredStateRequest,
    confirm: bool = Query(False),
):
    require_explicit_confirmation(confirm, "plugin desired state change")
    _require_mutable_environment()
    try:
        plugin_state_store.set_desired_state(plugin_id, payload.desired_state)
    except KeyError as exc:
        raise _error(
            status.HTTP_404_NOT_FOUND,
            "PLUGIN_NOT_INSTALLED",
            "Managed plugin is not installed.",
        ) from exc
    inventory = build_plugin_inventory()
    return next(item for item in inventory["plugins"] if item["id"] == plugin_id)


@router.post("/operations/uninstall", status_code=status.HTTP_202_ACCEPTED)
async def uninstall_managed_plugin(
    payload: CatalogInstallRequest,
    request: Request,
    confirm: bool = Query(False),
):
    require_explicit_confirmation(confirm, "plugin uninstall")
    _require_mutable_environment()
    managed = plugin_state_store.get_managed_plugin(payload.plugin_id)
    if managed is None:
        raise _error(
            status.HTTP_404_NOT_FOUND,
            "PLUGIN_NOT_INSTALLED",
            "Managed plugin is not installed.",
        )
    if managed.desired_state != "disabled":
        raise _error(
            status.HTTP_409_CONFLICT,
            "PLUGIN_MUST_BE_DISABLED",
            "Disable the plugin before uninstalling it.",
        )
    if managed.restart_required:
        raise _error(
            status.HTTP_409_CONFLICT,
            "PLUGIN_RESTART_REQUIRED",
            "Restart the service to finish disabling the plugin before uninstalling it.",
        )
    service = _operation_service(request)
    if service.reference_scanner is not None:
        try:
            references = await service.reference_scanner.scan(managed.collectors)
        except PluginReferenceScanError as exc:
            raise _error(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                exc.code,
                str(exc),
            ) from exc
        if references:
            raise _error(
                status.HTTP_409_CONFLICT,
                "PLUGIN_REFERENCED",
                "Plugin is still referenced by configured workflows or active tasks.",
                references=[item.to_dict() for item in references],
            )
    try:
        operation = await service.submit_uninstall(payload.plugin_id)
    except OperationConflictError as exc:
        raise _error(status.HTTP_409_CONFLICT, exc.code, str(exc)) from exc
    return operation.to_dict()


@router.post("/operations/rollback", status_code=status.HTTP_202_ACCEPTED)
async def rollback_managed_generation(
    request: Request,
    confirm: bool = Query(False),
):
    require_explicit_confirmation(confirm, "plugin generation rollback")
    _require_mutable_environment()
    active = plugin_state_store.get_active_generation()
    if not active or not active.get("previous_generation_id"):
        raise _error(
            status.HTTP_409_CONFLICT,
            "PLUGIN_ROLLBACK_UNAVAILABLE",
            "No retained previous generation is available.",
        )
    try:
        operation = await _operation_service(request).submit_rollback()
    except OperationConflictError as exc:
        raise _error(status.HTTP_409_CONFLICT, exc.code, str(exc)) from exc
    return operation.to_dict()


@router.post("/apply-restart")
async def apply_plugin_restart(confirm: bool = Query(False)):
    require_explicit_confirmation(confirm, "service restart")
    try:
        await restart_controller.request_restart()
    except RestartUnavailableError as exc:
        raise _error(
            status.HTTP_409_CONFLICT,
            exc.code,
            str(exc),
        ) from exc
    return {"accepted": True, "controller": restart_controller.name}


@router.post("/operations/upload", status_code=status.HTTP_202_ACCEPTED)
async def install_uploaded_wheel(
    request: Request,
    file: UploadFile = File(...),
    confirm: bool = Query(False),
):
    require_explicit_confirmation(confirm, "local wheel installation")
    _require_mutable_environment()
    original_filename = Path(file.filename or "").name
    if len(original_filename) > 200 or not original_filename.lower().endswith(".whl"):
        raise _error(
            status.HTTP_400_BAD_REQUEST,
            "PLUGIN_PACKAGE_INVALID",
            "The uploaded package must be a .whl file.",
        )
    try:
        filename_distribution, filename_version, _build, _tags = parse_wheel_filename(
            original_filename
        )
    except InvalidWheelFilename as exc:
        raise _error(
            status.HTTP_400_BAD_REQUEST,
            "PLUGIN_PACKAGE_INVALID",
            "The uploaded wheel filename is not valid.",
        ) from exc

    uploads_dir = plugin_state_store.manager_dir / "uploads" / uuid.uuid4().hex
    uploads_dir.mkdir(parents=True, exist_ok=True)
    upload_path = uploads_dir / original_filename
    total = 0
    try:
        with upload_path.open("xb") as destination:
            while chunk := await file.read(1024 * 1024):
                total += len(chunk)
                if total > MAX_UPLOAD_SIZE:
                    raise _error(
                        status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        "PLUGIN_PACKAGE_TOO_LARGE",
                        "The uploaded wheel exceeds the 200 MiB limit.",
                    )
                destination.write(chunk)
        try:
            metadata = read_wheel_metadata(upload_path, max_size=MAX_UPLOAD_SIZE)
            if (
                normalize_distribution(str(filename_distribution))
                != normalize_distribution(metadata.distribution)
                or str(filename_version) != metadata.version
            ):
                raise ValueError("wheel filename does not match package metadata")
            compatibility = evaluate_wheel(metadata, filename=original_filename)
            if not compatibility.compatible:
                raise _error(
                    status.HTTP_409_CONFLICT,
                    "PLUGIN_INCOMPATIBLE",
                    " ".join(compatibility.reasons),
                    reasons=list(compatibility.reasons),
                )
        except ValueError as exc:
            raise _error(
                status.HTTP_400_BAD_REQUEST,
                "PLUGIN_PACKAGE_INVALID",
                str(exc),
            ) from exc
        try:
            operation = await _operation_service(request).submit_wheel_install(
                upload_path,
                original_filename=original_filename,
            )
        except OperationConflictError as exc:
            raise _error(status.HTTP_409_CONFLICT, exc.code, str(exc)) from exc
    except Exception:
        if upload_path.exists():
            upload_path.unlink()
        raise
    finally:
        await file.close()
    return operation.to_dict()
