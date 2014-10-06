
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres


class SimpleFDW(ForeignDataWrapper):
    """Simple FDW that just demonstrates how to return data to a select
    statement.

    CREATE SERVER simple_srv foreign data wrapper multicorn options (wrapper 'pygoth.simple_fdw.SimpleFDW' );

    CREATE FOREIGN TABLE simple (col1 text, col2 int) server simple_srv;

    """

    def execute(self, quals, columns):
        log_to_postgres("executing simple select")

        for q in quals:
            log_to_postgres("qual: " + str(q))

        for c in columns:
            log_to_postgres("col: " + str(c))

        yield {
            'col1': 'hello',
            'col2': 42,
        }

        yield {
            'col1': 'world',
            'col2': 43,
        }


class NotSoSimpleFDW(ForeignDataWrapper):
    """Simple FDW that demonstrates returning lists of ints and strings.

    CREATE SERVER not_so_simple_srv foreign data wrapper multicorn options (wrapper 'pygoth.simple_fdw.NotSoSimpleFDW' );

    CREATE FOREIGN TABLE not_so_simple (id int, col1 int[], col2 text[]) server not_so_simple_srv;

    """

    def execute(self, quals, columns):
        log_to_postgres("executing not_so_simple select")

        yield {
            'id': 0,
            'col1': [1, 2, 3, 4],
            'col2': ['one', 'two', 'three', 'four'],
        }

        yield {
            'id': 1,
            'col1': [2, 4],
            'col2': ['two', 'four'],
        }

        yield {
            'id': 2,
            'col1': [1, 3],
            'col2': ['one', 'three'],
        }
