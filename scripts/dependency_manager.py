"""
Dependency version management for testing.
Generates requirements files for min, max, and default dependency versions.
"""

import toml
import sys
import argparse
from packaging.specifiers import SpecifierSet
from pathlib import Path

class DependencyManager:
    def __init__(self, pyproject_path="pyproject.toml"):
        self.pyproject_path = Path(pyproject_path)
        self.dependencies = self._load_dependencies()
    
    def _load_dependencies(self):
        """Load dependencies from pyproject.toml"""
        with open(self.pyproject_path, 'r') as f:
            pyproject = toml.load(f)
        return pyproject['tool']['poetry']['dependencies']
    
    def _parse_constraint(self, name, constraint):
        """Parse a dependency constraint into version info"""
        if isinstance(constraint, str):
            return constraint, False  # version_constraint, is_optional
        elif isinstance(constraint, list):
            # Handle complex constraints like pandas
            return constraint[0]['version'], False
        elif isinstance(constraint, dict):
            if 'version' in constraint:
                return constraint['version'], constraint.get('optional', False)
        return None, False
    
    def _extract_versions_from_specifier(self, spec_set_str):
        """Extract minimum version from a specifier set"""
        try:
            # Handle caret (^) and tilde (~) constraints that packaging doesn't support
            if spec_set_str.startswith('^'):
                # ^1.2.3 means >=1.2.3, <2.0.0
                min_version = spec_set_str[1:]  # Remove ^
                return min_version, None
            elif spec_set_str.startswith('~'):
                # ~1.2.3 means >=1.2.3, <1.3.0
                min_version = spec_set_str[1:]  # Remove ~
                return min_version, None
            
            spec_set = SpecifierSet(spec_set_str)
            min_version = None
            
            for spec in spec_set:
                if spec.operator in ('>=', '=='):
                    min_version = spec.version
                    break
            
            return min_version, None
        except Exception as e:
            print(f"Warning: Could not parse constraint '{spec_set_str}': {e}", file=sys.stderr)
            return None, None
    
    def generate_requirements(self, version_type="min", include_optional=False):
        """
        Generate requirements for specified version type.
        
        Args:
            version_type: "min" or "default"
            include_optional: Whether to include optional dependencies
        """
        requirements = []
        
        for name, constraint in self.dependencies.items():
            if name == 'python':
                continue
                
            version_constraint, is_optional = self._parse_constraint(name, constraint)
            if not version_constraint:
                continue
                
            if is_optional and not include_optional:
                continue
            
            if version_type == "default":
                # For default, just use the constraint as-is (let poetry resolve)
                requirements.append(f"{name}{version_constraint}")
            elif version_type == "min":
                min_version, _ = self._extract_versions_from_specifier(version_constraint)
                if min_version:
                    requirements.append(f"{name}=={min_version}")
        
        return requirements
    

    def write_requirements_file(self, filename, version_type="min", include_optional=False):
        """Write requirements to a file"""
        requirements = self.generate_requirements(version_type, include_optional)
        
        with open(filename, 'w') as f:
            f.write(f"# {version_type.title()} dependency versions generated from pyproject.toml\n")
            for req in sorted(requirements):
                f.write(f"{req}\n")
        
        print(f"Generated {filename} with {len(requirements)} dependencies")
        return requirements

def main():
    parser = argparse.ArgumentParser(description="Manage dependency versions for testing")
    parser.add_argument("version_type", choices=["min", "default"], 
                       help="Type of versions to generate")
    parser.add_argument("--output", "-o", default=None,
                       help="Output requirements file (default: requirements-{version_type}.txt)")
    parser.add_argument("--include-optional", action="store_true",
                       help="Include optional dependencies")
    parser.add_argument("--pyproject", default="pyproject.toml",
                       help="Path to pyproject.toml file")
    
    args = parser.parse_args()
    
    if args.output is None:
        args.output = f"requirements-{args.version_type}.txt"
    
    manager = DependencyManager(args.pyproject)
    requirements = manager.write_requirements_file(
        args.output, 
        args.version_type, 
        args.include_optional
    )
    
    # Also print to stdout for GitHub Actions
    for req in requirements:
        print(req)

if __name__ == "__main__":
    main()
