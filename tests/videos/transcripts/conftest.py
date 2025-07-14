from pytest import fixture


@fixture
def tables_to_truncate() -> list[str]:
    return ["videos"]
