import functools
from packaging.version import parse as parse_version
import unittest

MAJOR_DBR_V_KEY = "major_dbr_version"
MINOR_DBR_V_KEY = "minor_dbr_version"
ENDPOINT_TEST_KEY = "is_endpoint_test"


def pysql_supports_arrow():
    """Import databricks.sql and test whether Cursor has fetchall_arrow."""
    from databricks.sql.client import Cursor
    return hasattr(Cursor, 'fetchall_arrow')


def pysql_has_version(compare, version):
    """Import databricks.sql, and return compare_module_version(...).

    Expected use:
        from common.predicates import pysql_has_version
        from databricks import sql as pysql
        ...
        @unittest.skipIf(pysql_has_version('<', '2'))
        def test_some_pyhive_v1_stuff():
            ...
    """
    from databricks import sql
    return compare_module_version(sql, compare, version)


def is_endpoint_test(cli_args=None):
    
    # Currently only supporting tests against DBSQL Endpoints
    # So we don't read `is_endpoint_test` from the CLI args
    return True 


def compare_dbr_versions(cli_args, compare, major_version, minor_version):
    if MAJOR_DBR_V_KEY in cli_args and MINOR_DBR_V_KEY in cli_args:
        if cli_args[MINOR_DBR_V_KEY] == "x":
            actual_minor_v = float('inf')
        else:
            actual_minor_v = int(cli_args[MINOR_DBR_V_KEY])
        dbr_version = (int(cli_args[MAJOR_DBR_V_KEY]), actual_minor_v)
        req_version = (major_version, minor_version)
        return compare_versions(compare, dbr_version, req_version)

    if not is_endpoint_test():
        raise ValueError(
            "DBR version not provided for non-endpoint test. Please pass the {} and {} params".
            format(MAJOR_DBR_V_KEY, MINOR_DBR_V_KEY))


def is_thrift_v5_plus(cli_args):
    return compare_dbr_versions(cli_args, ">=", 10, 2) or is_endpoint_test(cli_args)


_compare_fns = {
    '<': '__lt__',
    '<=': '__le__',
    '>': '__gt__',
    '>=': '__ge__',
    '==': '__eq__',
    '!=': '__ne__',
}


def compare_versions(compare, v1_tuple, v2_tuple):
    compare_fn_name = _compare_fns.get(compare)
    assert compare_fn_name, 'Received invalid compare string: ' + compare
    return getattr(v1_tuple, compare_fn_name)(v2_tuple)


def compare_module_version(module, compare, version):
    """Compare `module`'s version as specified, returning True/False.

    @unittest.skipIf(compare_module_version(sql, '<', '2'))
    def test_some_pyhive_v1_stuff():
        ...

    `module`: the module whose version will be compared
    `compare`: one of '<', '<=', '>', '>=', '==', '!='
    `version`: a version string, of the form 'x[.y[.z]]

    Asserts module and compare to be truthy, and casts version to string.

    NOTE: This comparison leverages packaging.version.parse, and compares _release_ versions,
    thus ignoring pre/post release tags (eg -rc1, -dev, etc).
    """
    assert module, 'Received invalid module: ' + module
    assert getattr(module, '__version__'), 'Received module with no version: ' + module

    def validate_version(version):
        v = parse_version(str(version))
        # assert that we get a PEP-440 Version back -- LegacyVersion doesn't have major/minor.
        assert hasattr(v, 'major'), 'Module has incompatible "Legacy" version: ' + version
        return (v.major, v.minor, v.micro)

    mod_version = validate_version(module.__version__)
    req_version = validate_version(version)
    return compare_versions(compare, mod_version, req_version)
