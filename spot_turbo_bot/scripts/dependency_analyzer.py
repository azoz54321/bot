from __future__ import annotations

import ast
import json
import sys
from argparse import ArgumentParser
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


class ModuleInfo:
    def __init__(self, name: str, path: Path) -> None:
        self.name = name
        self.path = path
        self.imports: Set[str] = set()
        self.from_imports: Dict[str, Set[Optional[str]]] = defaultdict(set)
        self.defined_functions: Set[str] = set()
        self.defined_async_functions: Set[str] = set()
        self.defined_classes: Set[str] = set()
        self.name_usages: Set[str] = set()
        self.attribute_usages: Set[Tuple[str, str]] = set()
        self.relative_aliases: Dict[str, str] = {}
        self.absolute_aliases: Dict[str, str] = {}


def iter_python_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        yield path


def module_name_from_path(package_root: Path, path: Path) -> str:
    rel_parts = path.relative_to(package_root).parts
    if rel_parts[-1] == "__init__.py":
        rel_parts = rel_parts[:-1]
    else:
        rel_parts = rel_parts[:-1] + (rel_parts[-1][:-3],)
    return ".".join((package_root.name, *rel_parts)) if rel_parts else package_root.name


def resolve_relative_import(current: str, level: int, module: Optional[str]) -> Optional[str]:
    parts = current.split(".")
    if level > len(parts):
        return None
    base = parts[:-level]
    if module:
        base.extend(module.split("."))
    if not base:
        return None
    return ".".join(base)


class Analyzer(ast.NodeVisitor):
    def __init__(self, module: ModuleInfo, modules: Dict[str, ModuleInfo]):
        self.module = module
        self.modules = modules

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            target = alias.name
            self.module.imports.add(target)
            if alias.asname:
                self.module.absolute_aliases[alias.asname] = target
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module_name: Optional[str]
        if node.level:
            module_name = resolve_relative_import(self.module.name, node.level, node.module)
        else:
            module_name = node.module
        for alias in node.names:
            self.module.from_imports[module_name].add(alias.name)
            if alias.asname and module_name:
                self.module.relative_aliases[alias.asname] = f"{module_name}.{alias.name}"
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        if isinstance(getattr(node, "ctx", None), ast.Store) or node.col_offset > 0:
            # Skip nested definitions – we only care about module-level ones.
            pass
        else:
            self.module.defined_functions.add(node.name)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        if node.col_offset > 0:
            pass
        else:
            self.module.defined_async_functions.add(node.name)
        self.generic_visit(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        if node.col_offset > 0:
            pass
        else:
            self.module.defined_classes.add(node.name)
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Load):
            self.module.name_usages.add(node.id)
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        value = node.value
        if isinstance(value, ast.Name):
            self.module.attribute_usages.add((value.id, node.attr))
        self.generic_visit(node)


def build_module_graph(root: Path, package: str = "spot_turbo_bot") -> Dict[str, ModuleInfo]:
    package_root = root / package
    modules: Dict[str, ModuleInfo] = {}
    for path in iter_python_files(package_root):
        name = module_name_from_path(package_root, path)
        modules[name] = ModuleInfo(name, path)

    for module in modules.values():
        source = module.path.read_text(encoding="utf-8-sig")
        tree = ast.parse(source, filename=str(module.path))
        Analyzer(module, modules).visit(tree)
    return modules


def render_markdown(data: dict) -> str:
    modules = data["modules"]
    lines: List[str] = []
    lines.append("# Module Map")
    lines.append("")
    unreferenced = [
        name for name, info in modules.items() if not info["imported_by"]
    ]
    if unreferenced:
        lines.append("## Unreferenced Modules")
        for name in unreferenced:
            lines.append(f"- `{name}` ({modules[name]['file']})")
        lines.append("")
    lines.append("## Module Details")
    for name, info in modules.items():
        lines.append(f"### `{name}`")
        lines.append(f"- Path: `{info['file']}`")
        if info["imports"]:
            lines.append(f"- Imports: {', '.join(f'`{imp}`' for imp in info['imports'])}")
        else:
            lines.append("- Imports: _none_")
        if info["imported_by"]:
            lines.append(
                f"- Imported by: {', '.join(f'`{imp}`' for imp in info['imported_by'])}"
            )
        else:
            lines.append("- Imported by: _none_")
        symbols: List[str] = []
        if info["classes"]:
            symbols.append(f"classes={', '.join(f'`{c}`' for c in info['classes'])}")
        if info["functions"]:
            symbols.append(f"functions={', '.join(f'`{f}`' for f in info['functions'])}")
        if info["async_functions"]:
            symbols.append(
                f"async={', '.join(f'`{f}`' for f in info['async_functions'])}"
            )
        if symbols:
            lines.append(f"- Defines: {', '.join(symbols)}")
        else:
            lines.append("- Defines: _none_")
        if info["possibly_unused"]:
            lines.append(
                f"- Possibly unused: {', '.join(f'`{n}`' for n in info['possibly_unused'])}"
            )
        else:
            lines.append("- Possibly unused: _none_")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def main() -> None:
    parser = ArgumentParser(description="Analyze module dependencies.")
    parser.add_argument("--markdown", type=Path, help="Write markdown report to path.")
    parser.add_argument(
        "--json", type=Path, help="Write JSON output to path (defaults to stdout)."
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    modules = build_module_graph(project_root)
    internal_modules = set(modules.keys())

    imports_graph: Dict[str, Set[str]] = defaultdict(set)
    imported_by: Dict[str, Set[str]] = defaultdict(set)

    for mod, info in modules.items():
        for target in info.imports:
            if target in internal_modules or any(target.startswith(f"{m}.") for m in internal_modules):
                imports_graph[mod].add(target)
                imported_by[target].add(mod)
        for target, names in info.from_imports.items():
            if target:
                imports_graph[mod].add(target)
                imported_by[target].add(mod)

    defined_symbols: Dict[str, Set[str]] = defaultdict(set)
    symbol_usage: Dict[str, Set[str]] = defaultdict(set)

    for mod, info in modules.items():
        all_defined = (
            {f"{mod}.{name}" for name in info.defined_functions}
            | {f"{mod}.{name}" for name in info.defined_async_functions}
            | {f"{mod}.{name}" for name in info.defined_classes}
        )
        defined_symbols[mod] = all_defined

    for mod, info in modules.items():
        # Direct name usage
        for name in info.name_usages:
            symbol_usage[name].add(mod)
        # Attribute usage (alias.name)
        for alias, attr in info.attribute_usages:
            target = info.absolute_aliases.get(alias) or info.relative_aliases.get(alias)
            if target:
                symbol_usage[target.split(".")[-1]].add(mod)

    unused_candidates: Dict[str, List[str]] = defaultdict(list)
    for mod, symbols in defined_symbols.items():
        info = modules[mod]
        for symbol in symbols:
            simple = symbol.split(".")[-1]
            if simple not in info.name_usages and simple not in symbol_usage:
                unused_candidates[mod].append(simple)

    data: Dict[str, Dict[str, Dict[str, object]]] = {
        "modules": {
            mod: {
                "file": str(info.path.relative_to(project_root)),
                "imports": sorted(imports_graph.get(mod, [])),
                "imported_by": sorted(imported_by.get(mod, [])),
                "functions": sorted(info.defined_functions),
                "async_functions": sorted(info.defined_async_functions),
                "classes": sorted(info.defined_classes),
                "possibly_unused": sorted(unused_candidates.get(mod, [])),
            }
            for mod, info in sorted(modules.items())
        }
    }
    if args.json:
        args.json.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    else:
        json.dump(data, sys.stdout, indent=2, sort_keys=True)

    if args.markdown:
        args.markdown.write_text(render_markdown(data), encoding="utf-8")


if __name__ == "__main__":
    main()
