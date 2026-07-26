"""Operations catalog — registry, search, describe, execute.

Implements the Speakeasy Dynamic Toolsets pattern:
    tg_search_ops -> tg_describe_op -> tg_execute

Operations are registered via the @operation() decorator.
The catalog stores metadata extracted from type hints and docstrings.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from typing import Any, Callable, get_type_hints

from tg_mcp.config import logger

# Valid categories
VALID_CATEGORIES = frozenset({
    "channels", "messages", "interact", "folders", "analytics",
})


# ---------------------------------------------------------------------------
# Structured error
# ---------------------------------------------------------------------------

class OperationError(Exception):
    """4-part structured error for catalog operations."""

    def __init__(
        self,
        what: str,
        expected: str,
        example: str,
        recovery: str,
    ) -> None:
        self.what = what
        self.expected = expected
        self.example = example
        self.recovery = recovery
        super().__init__(what)

    def format(self) -> str:
        return (
            f"Error: {self.what}\n"
            f"Expected: {self.expected}\n"
            f"Example: {self.example}\n"
            f"\u2192 {self.recovery}"
        )


# ---------------------------------------------------------------------------
# Operation metadata
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ParamInfo:
    """Metadata for one operation parameter."""

    name: str
    type_str: str
    required: bool
    default: Any
    description: str


@dataclass(frozen=True, slots=True)
class OperationEntry:
    """Complete metadata for a registered operation."""

    name: str
    category: str
    description: str
    destructive: bool
    idempotent: bool
    func: Callable[..., Any]
    params: list[ParamInfo] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Type introspection
# ---------------------------------------------------------------------------

_TYPE_MAP = {
    str: "string",
    int: "int",
    float: "float",
    bool: "bool",
}


def _type_to_str(annotation: Any) -> str:
    """Convert a type annotation to a human-readable string."""
    if annotation is inspect.Parameter.empty:
        return "any"

    origin = getattr(annotation, "__origin__", None)

    # Handle Union types (including Optional)
    if origin is not None:
        args = getattr(annotation, "__args__", ())
        # list[str] -> "string[]"
        if origin is list and args:
            inner = _TYPE_MAP.get(args[0], getattr(args[0], "__name__", "any"))
            return f"{inner}[]"
        # str | None -> "string"
        non_none = [a for a in args if a is not type(None)]
        if non_none:
            return _type_to_str(non_none[0])

    return _TYPE_MAP.get(annotation, "any")


def _extract_params(func: Callable[..., Any]) -> list[ParamInfo]:
    """Extract parameter info from function signature + type hints.

    Skips 'self', 'client', and 'cache' parameters — those are injected.
    """
    sig = inspect.signature(func)
    try:
        hints = get_type_hints(func)
    except Exception:
        hints = {}

    skip_names = {"self", "client", "cache", "return"}
    params = []

    for pname, param in sig.parameters.items():
        if pname in skip_names:
            continue

        annotation = hints.get(pname, param.annotation)
        type_str = _type_to_str(annotation)

        required = param.default is inspect.Parameter.empty
        default = param.default if not required else inspect.Parameter.empty

        params.append(ParamInfo(
            name=pname,
            type_str=type_str,
            required=required,
            default=default,
            description="",
        ))

    return params


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_registry: dict[str, OperationEntry] = {}


def operation(
    *,
    name: str,
    category: str,
    description: str,
    destructive: bool = False,
    idempotent: bool = True,
) -> Callable:
    """Decorator to register an async function as a catalog operation.

    Validates at import time — fail fast if decorator is misused.
    """
    if not name or not isinstance(name, str):
        raise ValueError(f"@operation requires a non-empty string name, got: {name!r}")

    if category not in VALID_CATEGORIES:
        raise ValueError(
            f"@operation category must be one of {sorted(VALID_CATEGORIES)}, "
            f"got: {category!r}"
        )

    if not description:
        raise ValueError(f"@operation({name!r}) requires a non-empty description")

    if name in _registry:
        raise ValueError(
            f"Duplicate operation name: {name!r}. "
            f"Already registered by {_registry[name].func.__module__}"
        )

    def decorator(func: Callable) -> Callable:
        if not inspect.iscoroutinefunction(func):
            raise TypeError(
                f"@operation({name!r}) must decorate an async function, "
                f"got: {type(func).__name__}"
            )

        params = _extract_params(func)

        entry = OperationEntry(
            name=name,
            category=category,
            description=description,
            destructive=destructive,
            idempotent=idempotent,
            func=func,
            params=params,
        )

        _registry[name] = entry
        logger.debug("catalog.registered", extra={"op": name, "category": category})
        return func

    return decorator


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


def search(
    query: str | None = None,
    category: str | None = None,
) -> list[OperationEntry]:
    """Search operations by keyword and/or category.

    Multi-word queries require ALL terms to match (name or description).
    """
    if category and category not in VALID_CATEGORIES:
        raise ValueError(
            f"Unknown category: {category!r}. "
            f"Valid: {', '.join(sorted(VALID_CATEGORIES))}"
        )

    results = list(_registry.values())

    if category:
        results = [op for op in results if op.category == category]

    if query:
        terms = query.lower().split()
        results = [
            op for op in results
            if all(
                term in op.name.lower() or term in op.description.lower()
                for term in terms
            )
        ]

    return sorted(results, key=lambda op: op.name)


def get(name: str) -> OperationEntry:
    """Get an operation by exact name. Raises OperationError if not found."""
    entry = _registry.get(name)
    if entry is None:
        suggestions = [
            n for n in _registry
            if name.lower() in n.lower() or n.lower() in name.lower()
        ]
        msg = f"Operation {name!r} not found."
        if suggestions:
            msg += f" Did you mean: {', '.join(suggestions)}?"
        raise OperationError(
            what=msg,
            expected="A valid operation name from the catalog.",
            example='tg_search_ops query="channels"',
            recovery="Use tg_search_ops to discover available operations.",
        )
    return entry


def describe(name: str) -> str:
    """Generate a human-readable schema for an operation.

    Raises OperationError if not found.
    """
    entry = get(name)

    lines = [
        f"op: {entry.name}",
        f"description: {entry.description}",
        f"category: {entry.category}",
        f"destructive: {'true' if entry.destructive else 'false'}",
        f"idempotent: {'true' if entry.idempotent else 'false'}",
        "",
        "params:",
    ]

    if not entry.params:
        lines.append("  (none)")
    else:
        for p in entry.params:
            req = "required" if p.required else "optional"
            default_str = ""
            if not p.required and p.default is not inspect.Parameter.empty:
                default_str = f", default={p.default!r}"
            desc = f" \u2014 {p.description}" if p.description else ""
            lines.append(f"  {p.name} ({p.type_str}, {req}{default_str}){desc}")

    # Example invocation
    example_params = {}
    for p in entry.params:
        if p.required:
            if "str" in p.type_str:
                example_params[p.name] = f"<{p.name}>"
            elif "int" in p.type_str:
                example_params[p.name] = 1
            elif "bool" in p.type_str:
                example_params[p.name] = True
            else:
                example_params[p.name] = f"<{p.name}>"

    if example_params:
        lines.append("")
        lines.append(
            f'example: tg_execute op="{entry.name}" params={example_params}'
        )

    return "\n".join(lines)


async def _build_confirm_preview(
    entry: OperationEntry,
    client: Any,
    params: dict[str, Any],
) -> str:
    """Render an operation-specific preview of what confirm=true would do.

    An operation opts in by setting a ``confirm_preview`` attribute on its
    function: ``async def preview(client, params) -> str``. Failures are
    swallowed — a missing preview must never mask the confirmation guard.
    """
    preview_fn = getattr(entry.func, "confirm_preview", None)
    if preview_fn is None or client is None:
        return ""

    try:
        return await preview_fn(client, params) or ""
    except OperationError as exc:
        # Resolution failed — surfacing why is more useful than a bare guard.
        return exc.format()
    except Exception:
        logger.exception("catalog.confirm_preview_error", extra={"op": entry.name})
        return ""


async def execute(
    name: str,
    *,
    client: Any = None,
    cache: Any = None,
    params: dict[str, Any] | None = None,
    confirm: bool = False,
) -> Any:
    """Execute a registered operation with parameter validation.

    Injects client and cache into the operation if its signature accepts them.
    """
    entry = get(name)
    params = params or {}

    # Destructive guard
    if entry.destructive and not confirm:
        param_summary = ", ".join(f"{k}={v!r}" for k, v in params.items())
        preview = await _build_confirm_preview(entry, client, params)
        raise OperationError(
            what=(
                f"Operation {name!r} is destructive and requires confirmation."
                + (f"\n\n{preview}" if preview else "")
            ),
            expected="Pass confirm=true to proceed.",
            example=f'tg_execute op="{name}" params={{{param_summary}}} confirm=true',
            recovery="Review the operation description with tg_describe_op first.",
        )

    # Validate required parameters
    missing = []
    for p in entry.params:
        if p.required and p.name not in params:
            missing.append(p.name)

    if missing:
        raise OperationError(
            what=f"Missing required parameters for {entry.name}: {', '.join(missing)}",
            expected=', '.join(f'{p.name} ({p.type_str})' for p in entry.params if p.required),
            example=f'tg_describe_op name="{entry.name}"',
            recovery="Check the operation schema for required parameters.",
        )

    # Validate no unknown parameters
    valid_names = {p.name for p in entry.params}
    unknown = set(params.keys()) - valid_names
    if unknown:
        raise OperationError(
            what=f"Unknown parameters for {entry.name}: {', '.join(sorted(unknown))}",
            expected=f"Valid parameters: {', '.join(sorted(valid_names))}",
            example=f'tg_describe_op name="{entry.name}"',
            recovery="Check the operation schema for valid parameters.",
        )

    # Type coercion for common MCP param types (arrive as strings)
    coerced_params = {}
    for p in entry.params:
        if p.name in params:
            coerced_params[p.name] = _coerce_param(p, params[p.name])
        elif not p.required and p.default is not inspect.Parameter.empty:
            coerced_params[p.name] = p.default

    # Build call kwargs — inject client and cache if the function accepts them
    sig = inspect.signature(entry.func)
    call_kwargs: dict[str, Any] = {}

    for pname in sig.parameters:
        if pname == "client" and client is not None:
            call_kwargs["client"] = client
        elif pname == "cache" and cache is not None:
            call_kwargs["cache"] = cache
        elif pname in coerced_params:
            call_kwargs[pname] = coerced_params[pname]

    logger.info(
        "catalog.execute",
        extra={"op": name, "params": {k: str(v)[:100] for k, v in coerced_params.items()}},
    )

    return await entry.func(**call_kwargs)


def _coerce_param(param: ParamInfo, value: Any) -> Any:
    """Attempt basic type coercion for str->int, str->bool, str->float."""
    if value is None:
        return value

    type_lower = param.type_str.lower()

    if "int" in type_lower and not isinstance(value, int):
        try:
            return int(value)
        except (ValueError, TypeError):
            raise OperationError(
                what=f"Parameter {param.name!r} must be an integer, got: {value!r}",
                expected="integer value",
                example=f"{param.name}=42",
                recovery="provide a numeric value",
            )

    if "float" in type_lower and not isinstance(value, (int, float)):
        try:
            return float(value)
        except (ValueError, TypeError):
            raise OperationError(
                what=f"Parameter {param.name!r} must be a number, got: {value!r}",
                expected="numeric value",
                example=f"{param.name}=3.14",
                recovery="provide a numeric value",
            )

    if "bool" in type_lower and not isinstance(value, bool):
        if isinstance(value, str):
            if value.lower() in ("true", "1", "yes"):
                return True
            if value.lower() in ("false", "0", "no"):
                return False
        raise OperationError(
            what=f"Parameter {param.name!r} must be a boolean, got: {value!r}",
            expected="true or false",
            example=f"{param.name}=true",
            recovery="use true/false",
        )

    return value


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def list_categories() -> list[str]:
    """Return sorted list of categories that have at least one operation."""
    return sorted({op.category for op in _registry.values()})


def count() -> int:
    """Return total number of registered operations."""
    return len(_registry)
