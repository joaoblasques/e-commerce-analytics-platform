#!/usr/bin/env python3
"""
Dependency Audit Script

Analyzes current project dependencies and generates reports for the
Simplified Dependencies - Incremental Addition Strategy.

This script helps identify:
1. Currently used vs unused dependencies
2. Dependency categories and their usage
3. Security vulnerabilities in dependencies
4. Optimization opportunities for dependency reduction
"""

import ast
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Tuple

import toml


class DependencyAuditor:
    """Analyzes project dependencies and usage patterns."""

    def __init__(self, project_root: str = "."):
        self.project_root = Path(project_root)
        self.pyproject_path = self.project_root / "pyproject.toml"
        self.src_path = self.project_root / "src"
        self.tests_path = self.project_root / "tests"

        # Load current dependencies
        self.dependencies = self._load_dependencies()

        # Track imports found in codebase
        self.imports_used = set()
        self.file_imports = defaultdict(set)

    def _load_dependencies(self) -> Dict[str, Dict[str, str]]:
        """Load dependencies from pyproject.toml."""
        if not self.pyproject_path.exists():
            raise FileNotFoundError(
                f"pyproject.toml not found at {self.pyproject_path}"
            )

        with open(self.pyproject_path, "r") as f:
            pyproject = toml.load(f)

        dependencies = {}

        # Production dependencies
        if "tool" in pyproject and "poetry" in pyproject["tool"]:
            poetry_config = pyproject["tool"]["poetry"]
            if "dependencies" in poetry_config:
                dependencies["production"] = poetry_config["dependencies"]

            # Development dependencies
            if "group" in poetry_config and "dev" in poetry_config["group"]:
                dependencies["development"] = poetry_config["group"]["dev"].get(
                    "dependencies", {}
                )

        return dependencies

    def _normalize_package_name(self, name: str) -> str:
        """Normalize package names for comparison."""
        # Convert underscores to hyphens, lowercase
        return name.replace("_", "-").lower()

    def _extract_imports_from_file(self, file_path: Path) -> Set[str]:
        """Extract import statements from a Python file."""
        imports = set()

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            # Parse AST to extract imports
            try:
                tree = ast.parse(content)
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        for alias in node.names:
                            imports.add(alias.name.split(".")[0])
                    elif isinstance(node, ast.ImportFrom):
                        if node.module:
                            imports.add(node.module.split(".")[0])
            except SyntaxError:
                # If AST parsing fails, use regex as fallback
                import_patterns = [
                    r"^import\s+([a-zA-Z_][a-zA-Z0-9_]*)",
                    r"^from\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+import",
                ]

                for line in content.split("\n"):
                    line = line.strip()
                    for pattern in import_patterns:
                        match = re.match(pattern, line)
                        if match:
                            imports.add(match.group(1))

        except Exception as e:
            print(f"Warning: Could not analyze {file_path}: {e}")

        return imports

    def analyze_codebase_imports(self) -> None:
        """Analyze all Python files to find used imports."""
        python_files = []

        # Find all Python files in src and tests
        for directory in [self.src_path, self.tests_path]:
            if directory.exists():
                python_files.extend(directory.rglob("*.py"))

        print(f"Analyzing {len(python_files)} Python files...")

        for file_path in python_files:
            imports = self._extract_imports_from_file(file_path)
            self.file_imports[str(file_path)] = imports
            self.imports_used.update(imports)

    def _map_import_to_package(self, import_name: str) -> str:
        """Map import name to package name."""
        # Common mappings for packages with different import names
        import_to_package = {
            "pyspark": "pyspark",
            "kafka": "kafka-python",
            "fastapi": "fastapi",
            "uvicorn": "uvicorn",
            "pydantic": "pydantic",
            "pandas": "pandas",
            "numpy": "numpy",
            "sqlalchemy": "sqlalchemy",
            "psycopg2": "psycopg2-binary",
            "redis": "redis",
            "httpx": "httpx",
            "click": "click",
            "pytest": "pytest",
            "requests": "requests",
            "yaml": "pyyaml",
            "dateutil": "python-dateutil",
            "jose": "python-jose",
            "passlib": "passlib",
            "prometheus_client": "prometheus-client",
            "opentelemetry": "opentelemetry-api",
            "elasticsearch": "elasticsearch",
            "streamlit": "streamlit",
            "plotly": "plotly",
            "boto3": "boto3",
            "s3fs": "s3fs",
            "minio": "minio",
            "pyarrow": "pyarrow",
            "delta": "delta-spark",
            "hypothesis": "hypothesis",
            "faker": "faker",
            "locust": "locust",
            "docker": "docker",
            "testcontainers": "testcontainers",
            "kubernetes": "kubernetes",
            "hvac": "hvac",
            "cryptography": "cryptography",
            "schedule": "schedule",
            "aiohttp": "aiohttp",
            "loguru": "loguru",
            "psutil": "psutil",
            "pytz": "pytz",
        }

        return import_to_package.get(import_name, import_name)

    def categorize_dependencies(self) -> Dict[str, List[str]]:
        """Categorize dependencies by functional area."""
        categories = {
            "core_framework": ["fastapi", "uvicorn", "pydantic", "pydantic-settings"],
            "database": ["sqlalchemy", "psycopg2-binary", "alembic", "redis"],
            "data_processing": ["pandas", "numpy", "pyspark", "pyarrow"],
            "streaming": ["kafka-python", "delta-spark"],
            "storage": ["minio", "boto3", "s3fs"],
            "authentication": ["python-jose", "passlib", "python-multipart"],
            "monitoring": [
                "prometheus-client",
                "opentelemetry-api",
                "opentelemetry-sdk",
                "opentelemetry-instrumentation-fastapi",
                "elasticsearch",
                "elastic-apm",
            ],
            "visualization": ["streamlit", "plotly"],
            "configuration": [
                "python-dotenv",
                "pyyaml",
                "hvac",
                "kubernetes",
                "cryptography",
            ],
            "http_client": ["httpx", "requests", "aiohttp"],
            "cli": ["click"],
            "utilities": [
                "python-dateutil",
                "pytz",
                "schedule",
                "psutil",
                "faker",
                "watchdog",
            ],
            "testing": [
                "pytest",
                "pytest-cov",
                "pytest-mock",
                "pytest-asyncio",
                "pytest-xdist",
                "hypothesis",
                "hypothesis-jsonschema",
                "testcontainers",
                "locust",
            ],
            "code_quality": ["black", "flake8", "mypy", "isort", "pre-commit"],
            "security": ["bandit", "safety"],
            "documentation": [
                "sphinx",
                "sphinx-rtd-theme",
                "jupyter",
                "notebook",
                "ipykernel",
            ],
            "type_stubs": ["types-redis", "types-requests", "types-pyyaml"],
            "development": ["docker", "scipy", "pyjwt"],
        }

        # Categorize actual dependencies
        categorized = defaultdict(list)
        all_deps = {}

        if "production" in self.dependencies:
            all_deps.update(self.dependencies["production"])
        if "development" in self.dependencies:
            all_deps.update(self.dependencies["development"])

        for dep_name in all_deps.keys():
            if dep_name == "python":
                continue

            categorized_flag = False
            for category, deps in categories.items():
                if self._normalize_package_name(dep_name) in deps:
                    categorized[category].append(dep_name)
                    categorized_flag = True
                    break

            if not categorized_flag:
                categorized["uncategorized"].append(dep_name)

        return dict(categorized)

    def find_unused_dependencies(self) -> Tuple[List[str], List[str]]:
        """Find dependencies that are not imported in the codebase."""
        all_deps = {}
        if "production" in self.dependencies:
            all_deps.update(self.dependencies["production"])
        if "development" in self.dependencies:
            all_deps.update(self.dependencies["development"])

        # Map dependencies to their import names
        used_packages = set()
        unused_packages = []

        for import_name in self.imports_used:
            package_name = self._map_import_to_package(import_name)
            normalized_package = self._normalize_package_name(package_name)
            used_packages.add(normalized_package)

        for dep_name in all_deps.keys():
            if dep_name == "python":
                continue

            normalized_dep = self._normalize_package_name(dep_name)
            if normalized_dep not in used_packages:
                # Check if it's a development tool that might not have direct imports
                dev_tools = {
                    "black",
                    "flake8",
                    "mypy",
                    "isort",
                    "pre-commit",
                    "bandit",
                    "safety",
                    "pytest-cov",
                    "pytest-mock",
                    "pytest-asyncio",
                    "pytest-xdist",
                    "sphinx",
                    "sphinx-rtd-theme",
                    "jupyter",
                    "notebook",
                    "ipykernel",
                    "locust",
                    "docker",
                    "types-redis",
                    "types-requests",
                    "types-pyyaml",
                }

                if normalized_dep not in dev_tools:
                    unused_packages.append(dep_name)

        return list(used_packages), unused_packages

    def run_security_audit(self) -> Dict[str, any]:
        """Run security audit using safety."""
        try:
            result = subprocess.run(
                ["poetry", "run", "safety", "check", "--json"],
                capture_output=True,
                text=True,
                cwd=self.project_root,
            )

            if result.returncode == 0:
                return {"vulnerabilities": [], "status": "clean"}
            else:
                try:
                    vulnerabilities = json.loads(result.stdout)
                    return {
                        "vulnerabilities": vulnerabilities,
                        "status": "issues_found",
                    }
                except json.JSONDecodeError:
                    return {
                        "vulnerabilities": [],
                        "status": "error",
                        "message": result.stderr,
                    }

        except FileNotFoundError:
            return {"vulnerabilities": [], "status": "safety_not_found"}
        except Exception as e:
            return {"vulnerabilities": [], "status": "error", "message": str(e)}

    def generate_audit_report(self) -> Dict[str, any]:
        """Generate comprehensive audit report."""
        print("Starting dependency audit...")

        # Analyze codebase imports
        self.analyze_codebase_imports()

        # Categorize dependencies
        categories = self.categorize_dependencies()

        # Find unused dependencies
        used_packages, unused_packages = self.find_unused_dependencies()

        # Run security audit
        security_audit = self.run_security_audit()

        # Count dependencies
        total_deps = 0
        if "production" in self.dependencies:
            total_deps += len(self.dependencies["production"]) - 1  # Exclude python
        if "development" in self.dependencies:
            total_deps += len(self.dependencies["development"])

        report = {
            "summary": {
                "total_dependencies": total_deps,
                "production_dependencies": len(self.dependencies.get("production", {}))
                - 1,
                "development_dependencies": len(
                    self.dependencies.get("development", {})
                ),
                "imports_found": len(self.imports_used),
                "used_packages": len(used_packages),
                "unused_packages": len(unused_packages),
                "categories": len(categories),
            },
            "categories": categories,
            "used_packages": sorted(used_packages),
            "unused_packages": sorted(unused_packages),
            "security_audit": security_audit,
            "imports_by_file": dict(self.file_imports),
            "recommendations": self._generate_recommendations(
                categories, unused_packages
            ),
        }

        return report

    def _generate_recommendations(
        self, categories: Dict[str, List[str]], unused_packages: List[str]
    ) -> List[str]:
        """Generate recommendations for dependency optimization."""
        recommendations = []

        if unused_packages:
            recommendations.append(
                f"Consider removing {len(unused_packages)} unused dependencies: "
                f"{', '.join(unused_packages[:5])}"
                f"{'...' if len(unused_packages) > 5 else ''}"
            )

        # Check for heavy categories
        heavy_categories = {k: v for k, v in categories.items() if len(v) > 8}
        if heavy_categories:
            recommendations.append(
                f"Consider reviewing heavy categories with 8+ dependencies: "
                f"{', '.join(heavy_categories.keys())}"
            )

        # Core framework recommendations
        if len(categories.get("core_framework", [])) > 4:
            recommendations.append(
                "Core framework has many dependencies - consider if all are essential"
            )

        # Development tool recommendations
        dev_tools = len(categories.get("testing", [])) + len(
            categories.get("code_quality", [])
        )
        if dev_tools > 15:
            recommendations.append(
                f"Development tools ({dev_tools} packages) could be streamlined"
            )

        if not recommendations:
            recommendations.append("Dependency usage looks reasonable")

        return recommendations


def main():
    """Main function to run dependency audit."""
    auditor = DependencyAuditor()

    try:
        report = auditor.generate_audit_report()

        # Print summary
        print("\n" + "=" * 60)
        print("DEPENDENCY AUDIT REPORT")
        print("=" * 60)

        summary = report["summary"]
        print(f"Total Dependencies: {summary['total_dependencies']}")
        print(f"  Production: {summary['production_dependencies']}")
        print(f"  Development: {summary['development_dependencies']}")
        print(f"Imports Found in Code: {summary['imports_found']}")
        print(f"Used Packages: {summary['used_packages']}")
        print(f"Unused Packages: {summary['unused_packages']}")

        print(f"\nDependency Categories ({summary['categories']}):")
        for category, deps in report["categories"].items():
            if deps:
                print(f"  {category}: {len(deps)} packages")

        if report["unused_packages"]:
            print(f"\nUnused Dependencies ({len(report['unused_packages'])}):")
            for dep in report["unused_packages"]:
                print(f"  - {dep}")

        print(f"\nSecurity Audit Status: {report['security_audit']['status']}")
        if report["security_audit"]["vulnerabilities"]:
            print(
                f"Vulnerabilities Found: {len(report['security_audit']['vulnerabilities'])}"
            )

        print("\nRecommendations:")
        for rec in report["recommendations"]:
            print(f"  • {rec}")

        # Save detailed report to file
        output_file = Path("dependency_audit_report.json")
        with open(output_file, "w") as f:
            json.dump(report, f, indent=2, default=str)

        print(f"\nDetailed report saved to: {output_file}")

    except Exception as e:
        print(f"Error running audit: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
