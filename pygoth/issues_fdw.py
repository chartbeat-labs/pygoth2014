
import requests
from BeautifulSoup import BeautifulSoup

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres


def get_issue_summary(issue):
    resp = requests.get('http://bugs.python.org/issue%d' % issue)
    if resp.status_code == 200:
        soup = BeautifulSoup(resp.text)
        return soup.head.title.contents[0].strip()
    else:
        return None


class IssuesFDW(ForeignDataWrapper):
    """
    FDW that queries Python issue tracker.

    CREATE SERVER issues_srv foreign data wrapper multicorn options (wrapper 'pygoth.issues_fdw.IssuesFDW' );

    CREATE FOREIGN TABLE issues (id int, summary text) server issues_srv;

    """

    def execute(self, quals, cols):
        for q in quals:
            log_to_postgres(str(q))

            if q.field_name == 'id' and q.operator == '=':
                summary = get_issue_summary(int(q.value))
                if summary:
                    yield {
                        'id': q.value,
                        'summary': summary,
                    }


if __name__ == '__main__':
    print get_issue_summary(21880)
