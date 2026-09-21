from databricks.sdk.runtime import dbutils

def param(name: str, allowed: set[str] | None = None) -> str:
    """Read a REQUIRED job parameter.

    No defaults. A default that is usually wrong is worse than no default: an
    interactive run would silently write to the wrong catalog with no error.
    Values flow databricks.yml -> resources/jobs.yml base_parameters -> here.
    """
    dbutils.widgets.text(name, "")
    value = dbutils.widgets.get(name).strip()
    if not value:
        raise ValueError(
            f"Required parameter '{name}' was not supplied. Set it in "
            f"resources/jobs.yml under base_parameters, or type it into the "
            f"widget above if you are running this notebook by hand."
        )
    if allowed and value not in allowed:
        raise ValueError(f"Parameter '{name}' must be one of {sorted(allowed)}, got '{value}'")
    return value
