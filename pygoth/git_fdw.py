
import re
from datetime import datetime

import git

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres


def to_psql_timestamp(unix_ts):
    """Converts a Unix epoch timestamp to a postgresql formatted
    timestamp.

    """

    dt = datetime.utcfromtimestamp(unix_ts)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def extract_issues(summary):
    """Attempts to extract issue numbers from the summary.

    If the line contains the words "issue", "fix", or "close", we
    extract out any number that's preceded by a #.

    """

    summary = summary.lower()
    if 'issue' in summary or \
       'fix' in summary or \
       'close' in summary:
        return [int(m) for m in re.findall('#(\d+)', summary)]
    else:
        return []

def get_changed_paths(c_old, c_new):
    """Returns a list of filenames that were modified between c_new and
    c_old.

    If c_old is None then returns empty list. Er, which I realize is
    not the right thing to do but hey this is just a demo.

    I had grand aspirations on doing this:

      http://google-engtools.blogspot.com/2011/12/bug-prediction-at-google.html

    Except that generating the file change info is just so damn slow
    in git. Mercurial should theoretically be faster at it since it
    stores commit deltas.

    """

    if c_old is None:
        return []

    changed = set()
    for diff in c_new.diff(c_old):
        if diff.a_blob:
            changed.add(diff.a_blob.path)

        if diff.b_blob:
            changed.add(diff.b_blob.path)

    return list(changed)

def get_git_commits(repo_path):
    """Returns commits for the given repo."""

    repo = git.Repo(repo_path)
    commits = repo.iter_commits('master')
    previous_commit = None

    for c in commits:
        row =  {
            'githash': c.hexsha[:7],
            'author': unicode(c.author),
            'time': to_psql_timestamp(c.authored_date),
            'issues': extract_issues(c.summary),
            #'files': get_changed_paths(previous_commit, c),
            'summary': c.summary,
        }

        previous_commit = c
        yield row


class GitFDW(ForeignDataWrapper):
    """
    FDW that wraps git repo data.

    CREATE SERVER git_srv foreign data wrapper multicorn options (wrapper 'pygoth.git_fdw.GitFDW' );

    CREATE FOREIGN TABLE cpython_git (githash text, author text, time timestamp without time zone, issues integer[], summary text) server git_srv;
    """

    def __init__(self, options, columns):
        super(GitFDW, self).__init__(options, columns)
        self._commits = list(get_git_commits('/home/vagrant/cpython'))

    def execute(self, quals, cols):
        for c in self._commits:
            yield c


if __name__ == '__main__':
    for c in get_git_commits('/home/vagrant/cpython'):
        print c
