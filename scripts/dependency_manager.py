"""
Dependency version management for testing.
Generates requirements files for min and default dependency versions.
For min versions, creates flexible constraints (e.g., >=1.2.5,<1.3.0) to allow 
compatible patch updates instead of pinning exact versions.
"""

import toml
import sys
import argparse
from packaging.specifiers import SpecifierSet
from packaging.requirements import Requirement
from pathlib import Path

class DependencyManager:
    def __init__(self, pyproject_path="pyproject.toml"):
        self.pyproject_path = Path(pyproject_path)
        self.dependencies = self._load_dependencies()
        
        # Map of packages that need specific transitive dependency constraints when downgraded
        self.transitive_dependencies = {
            'pandas': {
                # When pandas is downgraded to 1.x, ensure numpy compatibility
                'numpy': {
                    'min_constraint': '>=1.16.5,<2.0.0',  # pandas 1.x works with numpy 1.x
                    'applies_when': lambda version: version.startswith('1.')
                }
            }
        }
    
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
            # Handle complex constraints like pandas/pyarrow
            first_constraint = constraint[0]
            version = first_constraint['version']
            is_optional = first_constraint.get('optional', False)
            return version, is_optional
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
    
    def _create_flexible_minimum_constraint(self, package_name, min_version):
        """Create a flexible minimum constraint that allows compatible updates"""
        try:
            # Split version into parts
            version_parts = min_version.split('.')
            
            if len(version_parts) >= 2:
                major = version_parts[0]
                minor = version_parts[1]
                
                # Special handling for packages that commonly have conflicts
                # For these packages, use wider constraints to allow more compatibility
                if package_name in ['requests', 'urllib3', 'pandas']:
                    # Use wider constraint: >=min_version,<next_major
                    # e.g., 2.18.1 becomes >=2.18.1,<3.0.0
                    next_major = int(major) + 1
                    upper_bound = f"{next_major}.0.0"
                    return f"{package_name}>={min_version},<{upper_bound}"
                else:
                    # For other packages, use minor version constraint
                    # e.g., 1.2.5 becomes >=1.2.5,<1.3.0
                    next_minor = int(minor) + 1
                    upper_bound = f"{major}.{next_minor}.0"
                    return f"{package_name}>={min_version},<{upper_bound}"
            else:
                # If version doesn't have minor version, just use exact version
                return f"{package_name}=={min_version}"
                
        except (ValueError, IndexError) as e:
            print(f"Warning: Could not create flexible constraint for {package_name}=={min_version}: {e}", file=sys.stderr)
            # Fallback to exact version
            return f"{package_name}=={min_version}"
    
    def _get_transitive_dependencies(self, package_name, version, version_type):
        """Get transitive dependencies that need specific constraints based on the main package version"""
        transitive_reqs = []
        
        if package_name in self.transitive_dependencies:
            transitive_deps = self.transitive_dependencies[package_name]
            
            for dep_name, dep_config in transitive_deps.items():
                # Check if this transitive dependency applies for this version
                if dep_config['applies_when'](version):
                    if version_type == "min":
                        # Use the predefined constraint for minimum versions
                        constraint = dep_config['min_constraint']
                        transitive_reqs.append(f"{dep_name}{constraint}")
                    # For default version_type, we don't add transitive deps as Poetry handles them
        
        return transitive_reqs
    
    def generate_requirements(self, version_type="min", include_optional=False):
        """
        Generate requirements for specified version type.
        
        Args:
            version_type: "min" or "default"
            include_optional: Whether to include optional dependencies
        """
        requirements = []
        transitive_requirements = []
        
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
                    # Create flexible constraint that allows patch updates for compatibility
                    flexible_constraint = self._create_flexible_minimum_constraint(name, min_version)
                    requirements.append(flexible_constraint)
                    
                    # Check if this package needs specific transitive dependencies
                    transitive_deps = self._get_transitive_dependencies(name, min_version, version_type)
                    transitive_requirements.extend(transitive_deps)
        
        # Combine main requirements with transitive requirements
        all_requirements = requirements + transitive_requirements
        
        # Remove duplicates (prefer main requirements over transitive ones)
        seen_packages = set()
        final_requirements = []
        
        # First add main requirements
        for req in requirements:
            package_name = Requirement(req).name
            seen_packages.add(package_name)
            final_requirements.append(req)
        
        # Then add transitive requirements that don't conflict
        for req in transitive_requirements:
            package_name = Requirement(req).name
            if package_name not in seen_packages:
                final_requirements.append(req)
        
        return final_requirements
    

    def write_requirements_file(self, filename, version_type="min", include_optional=False):
        """Write requirements to a file"""
        requirements = self.generate_requirements(version_type, include_optional)
        
        with open(filename, 'w') as f:
            if version_type == "min":
                f.write(f"# Minimum compatible dependency versions generated from pyproject.toml\n")
                f.write(f"# Uses flexible constraints to resolve compatibility conflicts:\n")
                f.write(f"# - Common packages (requests, urllib3, pandas): >=min,<next_major\n") 
                f.write(f"# - Other packages: >=min,<next_minor\n")
                f.write(f"# - Includes transitive dependencies (e.g., numpy for pandas)\n")
            else:
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
