Bend PostgreSQL to Your Pythonic Will
====

by Wes Chow, CTO @ Chartbeat, @weschow

I imagine it's difficult for those who have never given a talk to
realize just how nerve racking and all consuming preparation can
be. I'd given an internal company talk about a neat PostgreSQL feature
called Foreign Data Wrappers, but when I set about presenting it at
PyGotham 2014 I all of a sudden felt an urge to put together something
much more epic and with a Big Insight.

What resulted instead was a week of solid work, a minor panic about
not having a Big Idea, the euphoria of discovering a Big Idea, and
then a major panic on the train ride into NYC an hour before I was to
speak when I realized my Big Idea was partially wrong.

What follows are fleshed out talk notes. During the talk I decided to
live code SQL queries, which I think was my undoing. We started 5
minutes late, and I needed 5 minutes more than I was slotted, which
meant that the last meaningful 10 minutes of the talk was lost in the
shuffle.

So!

We abandon SQL too quickly. Why? NoSQL folk believe:

  1. It's inflexible.
  2. It's slow.
  3. It doesn't scale to large amounts of data.

But it's important to separate out issues of SQL-the-language with
SQL-the-implementation. If you do not have at least a cursory
understanding of how data is organized on disk in a SQL server, you
can't really make any of the above claims.

So what we're going to do with PostgreSQL and Python is separate out
the storage details using a module called Multicorn and look at how
expressive SQL can be.

What is Multicorn? PostgreSQL provides a Foreign Data Wrapper API,
which is C based and allows you to plug in arbitrary
backends. Multicorn is a Foreign Data Wrapper implementation that
embeds the Python interpreter and calls Python code. This allows you,
then, to write your data backend in Python.

The basic Multicorn interface expects you to define functions for
responding to selects, inserts, updates, and deletes. In addition to
this it provides an API for dealing with query optimizing, but we're
going to ignore all of that and look only at the select API.

I'll be skipping over the details of how to set up FDWs and
Multicorn. You can find good documentation on that here:
http://multicorn.org/

Simple
----

This is a very basic Python FDW:

```python
class SimpleFDW(ForeignDataWrapper):
    """Simple FDW that just demonstrates how to return data to a select
    statement.

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
```

Running a query on it looks like this:

```sql
pygoth=> select * from simple;
NOTICE:  executing simple select
NOTICE:  col: col2
NOTICE:  col: col1
 col1  | col2 
-------+------
 hello |   42
 world |   43
(2 rows)
```

So let's step through this. `SELECT` statements are implmented by the
`execute` method on the FDW class. `execute` is passed two arguments:
qualifiers and columns. Qualifiers are conditions in select
statements, and `columns` is a list of columns that are required to
satisfy the select. For example:

```sql
select foo from bar where qux = 5
```

results in a call to `execute` with a qualifiers list of: `[qux = 5]`,
and a columns list of: `[foo, qux]`. (That's pseudo-code, not actual
output.)

The `NOTICE` lines are the output from the `log_to_postgres` calls.

The return of `execute` is an iterable that produces all of the rows,
where each element is a map of column name to value. `SimpleFDW`
always returns two rows, regardless of what goes into the select
statement.

```sql
pygoth=> select * from simple where col2 = 42;
NOTICE:  executing simple select
NOTICE:  qual: col2 = 42
NOTICE:  col: col2
NOTICE:  col: col1
 col1  | col2 
-------+------
 hello |   42
(1 row)

pygoth=> select * from simple where col2 = 42 and col1 = 'world';
NOTICE:  executing simple select
NOTICE:  qual: col2 = 42
NOTICE:  qual: col1 = world
NOTICE:  col: col2
NOTICE:  col: col1
 col1 | col2 
------+------
(0 rows)
```

Note that `execute` is allowed to return *more* data than what might
be suggested by its qualifiers. PostgreSQL filters down data received
from the FDW to ensure that the output is correct.


Not So Simple
----

PostgreSQL has an array data type, and Multicorn lets you push Python
lists in as arrays.

```python
class NotSoSimpleFDW(ForeignDataWrapper):
    """Simple FDW that demonstrates returning lists of ints and strings.

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
```

So we can:

```sql
pygoth=> select * from not_so_simple ;
NOTICE:  executing not_so_simple select
 id |   col1    |         col2         
----+-----------+----------------------
  0 | {1,2,3,4} | {one,two,three,four}
  1 | {2,4}     | {two,four}
  2 | {1,3}     | {one,three}
(3 rows)
```

You can also search within an array:

```sql
pygoth=> select * from not_so_simple where 2 = ANY(col1);
NOTICE:  executing not_so_simple select
 id |   col1    |         col2         
----+-----------+----------------------
  0 | {1,2,3,4} | {one,two,three,four}
  1 | {2,4}     | {two,four}
(2 rows)
```

A useful function for arrays is `unnest`, which "explodes" the array:

```sql
pygoth=> select *, unnest(col1) from not_so_simple;
NOTICE:  executing not_so_simple select
 id |   col1    |         col2         | unnest 
----+-----------+----------------------+--------
  0 | {1,2,3,4} | {one,two,three,four} |      1
  0 | {1,2,3,4} | {one,two,three,four} |      2
  0 | {1,2,3,4} | {one,two,three,four} |      3
  0 | {1,2,3,4} | {one,two,three,four} |      4
  1 | {2,4}     | {two,four}           |      2
  1 | {2,4}     | {two,four}           |      4
  2 | {1,3}     | {one,three}          |      1
  2 | {1,3}     | {one,three}          |      3
(8 rows)
```

One classic example of where NoSQL document stores shine is in
modeling tags. In traditional SQL, you have to maintain a many-to-many
relation to properly model tags, but in a document store you can cheat
and just maintain a list of tags per document. Well, turns out you can
do this in PostgreSQL with simple arrays.


Log Parsing
----

Let's do something a little more involved. I pulled from our web
servers 500,000 lines of an access log in the basic Nginx format. An
example line would be:

```
184.152.-.- - - - [01/Jul/2014:06:54:59 -0400] GET /publishing/dashboard/?url=-------- HTTP/1.1 "302" 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36" "0.213" 127.0.0.1:8080 302 0.088
```

(I've masked the IP addresses and customer IDs for privacy reasons.)

A very simple and admittedly brittle method for parsing the line is
just to break it into components and extract the parts we care about:

```python
def get_rows(path):
    """Iterates through compressed access log and yields dicts that
    contain ip, time, and error code info.

    """

    with gzip.open(path) as finp:
        for line in finp:
            try:
                parts = line.split()

                # clean time, chop off timezone info
                time = parts[4].replace('[', '')

                # clean path and truncate for demo purposes
                path = parts[7].split('?')[0].rstrip('/')
                path = path[:60]
                if not path:
                    continue

                yield {
                    'path': path,
                    'ip': parts[0],
                    'time': _log_time_to_psql(time),
                    'error': int(parts[-2]),
                    'elapsed': float(parts[-1]),
                }
            except Exception:
                # Some simple error handling because log file ain't
                # clean. Of *course* I would never do this in
                # production...
                pass
```

Where `_log_time_to_psql` formats the access log time string in a form
that
[PostgreSQL understands](http://www.postgresql.org/docs/9.3/static/datatype-datetime.html):

```python
def _log_time_to_psql(timestr):
    """Converts time format from '01/Jul/2014:06:31:24' to '2014-07-01 06:31:24'."""

    dt = datetime.strptime(timestr, '%d/%b/%Y:%H:%M:%S')
    return dt.strftime("%Y-%m-%d %H:%M:%S")
```

The access log `execute` function is simple. It merely passes through
the iterator `get_rows` returns.

```python
def execute(self, quals, columns):
    for row in get_rows(self._access_log_path):
        yield row
```

It's slow though:

```sql
pygoth=> select count(*) from access_log;
 count  
--------
 466560
(1 row)

Time: 12232.053 ms
```

Also, note that the query essentially does a table scan regardless of
any qualifiers we add:

```sql
pygoth=> select count(*) from access_log where error = 404;
 count 
-------
   152
(1 row)

Time: 12267.970 ms
```

The practical programmer's go-to strategy for making something faster
is to cache it. We can do that in the FDW constructor:

```python
def __init__(self, options, columns):
    super(AccessLogFDW, self).__init__(options, columns)
    self._access_log_path = '/home/vagrant/access_log.gz'

    log_to_postgres("caching row data")
    self._rows = list(get_rows(self._access_log_path))
```

And then a slight modification to the `execute` function:

```python
def execute(self, quals, columns):
    return self._rows
```

The first time we run a select, PostgreSQL instantiates the class and
we load in all the data:

```sql
pygoth=> select count(*) from access_log ;
NOTICE:  caching row data
 count  
--------
 466560
(1 row)

Time: 11103.197 ms
```

But since the PostgreSQL already has an AccessLogFDW instance for this
connection, the second query comes back much faster. But note that
we're still doing a full scan on every query.

```sql
pygoth=> select count(*) from access_log ;
 count  
--------
 466560
(1 row)

Time: 592.792 ms

pygoth=> select count(*) from access_log where error = 404;
 count 
-------
   152
(1 row)

Time: 578.662 ms
```

So now that the query isn't excruciatingly slow, we're ready to start
exploring a bit. What are the top ten paths by count?

```sql
pygoth=> select path, count(1) as c from access_log group by path order by c desc limit 10;
                    path                    |   c    
--------------------------------------------+--------
 /api/live/referrer_urls                    | 169155
 /link_api/click_through_rates              | 114044
 /action_api/history                        |  57068
 /action_api/summary                        |  16698
 /event_api/events                          |  14063
 /historical/traffic/velocities             |  12603
 /link_api/links2                           |  11607
 /publishing/hud                            |   8627
 /historical/traffic/values                 |   7443
 /wrap/labs/map/images/labs/map/red-pin.png |   6825
(10 rows)

Time: 653.187 ms
```

What are the top ten paths by average elapsed time?

```sql
pygoth=> select path, count(1) as c, avg(elapsed) as e from access_log group by path order by e desc limit 10;
                   path                    | c |   e    
-------------------------------------------+---+--------
 /add_domain                               | 1 |  2.658
 /reset/-----------------------------      | 1 |  2.402
 /publishing/dashboard/------------------- | 1 |  2.259
 /publishing/dashboard/---------------     | 1 |  2.239
 /command/account/-----                    | 1 |  2.144
 /publishing/dashboard/-----------------   | 1 |  1.852
 /publishing/dashboard/------              | 1 |  1.806
 /publishing/dashboard/-------             | 1 |  1.542
 /admin/log                                | 2 | 1.5155
 /dashboard/----                           | 2 |  1.371
(10 rows)

Time: 667.511 ms
```

(Note: I've masked out customer identifying information.)

So we notice that all of the slowest requests didn't happen all that
often. Maybe these are outliers we don't care about at the moment. If
that's the case, we can drop requests that didn't happen at least 100
times.

```sql
pygoth=> select * from (select path, count(1) as c, avg(elapsed) as e from access_log group by path) as temp where c > 100 order by e desc limit 10;
                   path                    |  c   |         e         
-------------------------------------------+------+-------------------
 /publishing/dashboard/---------------     |  110 | 0.505909090909091
 /twitter_api/search                       |  224 | 0.442973214285714
 /labs/rising/topterms                     |  545 | 0.413379816513762
 /publishing/perspectives/weekly           |  141 | 0.372673758865248
 /twitter_api/favorites                    |  111 | 0.351387387387387
 /signin                                   | 2788 | 0.304692611190817
 /publishing/hud/details                   | 3406 | 0.283876688197298
 /labs/publishing/bigboard/--------------- |  144 | 0.280597222222222
 /dashboard                                | 1193 | 0.271636211232188
 /publishing/hud                           | 8627 | 0.269752984815116
(10 rows)

Time: 700.455 ms
```

Once we have the power of SQL attached to the logs, we can ask all
sorts of interesting questions, like what are the paths with the
highest variance in elapsed time? Which paths contain the most errors?

Each one of these queries is doing a full scan of all the data, which
is reasonably fast at under 1 second for exploratory purposes. But
let's say that we want to optimize for queries revolving around paths
with errors.

We can build a simple index in the constructor:

```python
self._rows_by_error = defaultdict(list)
for row in self._rows:
    self._rows_by_error[row['error']].append(row)
```

And then modify execute to look for qualifiers that involve error
codes:

```python
def execute(self, quals, columns):
    results = self._rows
    for q in quals:
        if q.field_name == 'error' and q.operator == '=':
            log_to_postgres("filtering query on error to %d" % q.value)
            results = self._rows_by_error[q.value]

    return results
```

Now queries involving errors are fast:

```sql
pygoth=> select count(1) from access_log where error = 404;
NOTICE:  filtering query on error to 404
 count 
-------
   152
(1 row)

Time: 1.058 ms
```

What are the paths that produce the most 404s?

```sql
pygoth=> select path, count(*) as c from (select * from access_log where error = 404) as errors group by path order by c desc limit 10;
NOTICE:  filtering query on error to 404
                             path                             | c  
--------------------------------------------------------------+----
 /demo/http:%5C/%5C/lp.chartbeat.com%5C/chartbeat-publishing- | 54
 /publishing/demo/http:%5C/%5C/lp.chartbeat.com%5C/chartbeat- | 54
 /labs/bigboard/undefined                                     |  9
 /command/account                                             |  8
 /crossdomain.xml                                             |  4
 /dashboard/246                                               |  2
 /404                                                         |  2
 /dashboard/rgb(246                                           |  2
 /elk4kbr.js                                                  |  2
 /dashboard/250                                               |  2
(10 rows)

Time: 2.980 ms
```

Not surprising, since most of those are invalid paths.


Python Issues
----

Ok, so let's push the boundary a little. We can run arbitrary code, so
there's nothing that says that we're limited to running *local*
code. Let's make some network calls!

Let's take a look at the
[Python Issue tracker](http://bugs.python.org/) and check out its HTML
structure. For example, bug 17620 is located at
http://bugs.python.org/issue17620 and contains the title tag "Issue
17620: Python interactive console doesn't use sys.stdin for input -
Python tracker". With the power of BeautifulSoup and requests, parsing
this out is easy:

```python
def get_issue_summary(issue):
    resp = requests.get('http://bugs.python.org/issue%d' % issue)
    if resp.status_code == 200:
        soup = BeautifulSoup(resp.text)
        return soup.head.title.contents[0].strip()
    else:
        return None
```

And we can accompany this with a really simple FDW:

```python
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
```

Let's look up 17620:

```sql
pygoth=> select * from issues where id = 17620;
NOTICE:  id = 17620
  id   |                                         summary                                          
-------+------------------------------------------------------------------------------------------
 17620 | Issue 17620: Python interactive console doesn't use sys.stdin for input - Python tracker
(1 row)

Time: 5997.037 ms
```

One curiosity:

```sql
pygoth=> select * from issues ;
 id | summary 
----+---------
(0 rows)

Time: 0.389 ms
```

That's because we didn't teach the FDW how to get an exhaustive
listing of all issues.


Git
----

Let's hook PostgreSQL up to a git repo, because why not?

This one's a bit too complicated to be pasting code snippets into a
blog post, so I'll just direct you to the source repo containing the
full FDW. What we do is use the gitpython module for extracting commit
info from a git mirror of the CPython source.

How many commits are there?

```sql
pygoth=> select count(*) from cpython_git ;
 count 
-------
 86229
(1 row)
```

Who commits the most often?

```sql
pygoth=> select author, count(*) as c from cpython_git  group by author order by c desc limit 10;
      author       |   c   
-------------------+-------
 Guido van Rossum  | 10866
 Fred Drake        |  5465
 Georg Brandl      |  5456
 Benjamin Peterson |  4698
 Antoine Pitrou    |  3541
 Raymond Hettinger |  3485
 Victor Stinner    |  3247
 Jack Jansen       |  2978
 Martin v. Löwis   |  2760
 Tim Peters        |  2507
(10 rows)
```

Predictable!

How many commits in the last year?

```sql
pygoth=> select count(*) from cpython_git where time > timestamp 'now' - interval '1 year';
 count 
-------
  5778
(1 row)
```

And who committed in the last year?

```sql
pygoth=> select author, count(*) as c from (select * from cpython_git where time > timestamp 'now' - interval '1 year') as last_year group by author order by c desc limit 10;
      author       |  c  
-------------------+-----
 Victor Stinner    | 934
 Serhiy Storchaka  | 624
 Benjamin Peterson | 427
 R David Murray    | 364
 Georg Brandl      | 306
 Antoine Pitrou    | 293
 Zachary Ware      | 244
 Terry Jan Reedy   | 237
 Raymond Hettinger | 180
 Christian Heimes  | 177
(10 rows)
```

Tsk, tsk, Guido, what a slacker.

One thing this FDW does is extract out anything that looks like an
issue number:

```sql
pygoth=> select * from cpython_git limit 10;
 githash |      author      |        time         | issues  |                                  summary                                   
---------+------------------+---------------------+---------+----------------------------------------------------------------------------
 1fcc2fb | Victor Stinner   | 2014-08-28 09:19:46 | {}      | (Merge 3.4) asyncio, Tulip issue 201: Fix a race condition in wait_for()
 f3ceacc | Victor Stinner   | 2014-08-28 09:19:25 | {}      | asyncio, Tulip issue 201: Fix a race condition in wait_for()
 a2a1095 | Gregory P. Smith | 2014-08-27 16:41:05 | {}      | The webbrowser module now uses subprocess's start_new_session=True rather
 e72804e | Gregory P. Smith | 2014-08-27 16:34:38 | {}      | The webbrowser module now uses subprocess's start_new_session=True rather
 9cea894 | Victor Stinner   | 2014-08-27 12:02:36 | {22042} | Issue #22042: Fix test_signal on Windows
 b34fd12 | Victor Stinner   | 2014-08-27 10:59:44 | {22042} | Issue #22042: signal.set_wakeup_fd(fd) now raises an exception if the file
 2d52f8f | Terry Jan Reedy  | 2014-08-27 05:58:57 | {}      | Merge with 3.4
 df9de57 | Terry Jan Reedy  | 2014-08-27 05:58:40 | {22065} | Issue #22065: Remove the now unsed configGUI menu parameter and arguments.
 d95c79d | Terry Jan Reedy  | 2014-08-27 05:44:13 | {}      | Merge with 3.4
 587b91a | Terry Jan Reedy  | 2014-08-27 05:43:50 | {22065} | Issue #22065: Menus, unlike Menubottons, do not have a state option.
(10 rows)

Time: 0.296 ms
```

So now we can ask a questions about issues. Which commits deal with
the most issues?

```sql
pygoth=> select * from (select githash, array_length(issues, 1) as c from cpython_git) as counts where c > 0 order by c desc limit 10;
 githash | c 
---------+---
 9f61fab | 5
 70a3984 | 4
 6c12149 | 4
 6ee516a | 4
 97e77e5 | 3
 e53cc2c | 3
 d183e28 | 3
 5f33c01 | 3
 a700e70 | 3
 faeac0c | 3
(10 rows)
```

Or better yet, which issues require the most commits? This could be a
sign that an issue is particularly difficult to resolve.

```sql
pygoth=> select issue, count(*) as c from (select *, unnest(issues) as issue from cpython_git) as by_issue group by issue order by c desc limit 10;
 issue | c  
-------+----
 18408 | 93
  3080 | 45
 19437 | 41
  9566 | 27
 12400 | 24
 13959 | 22
 12451 | 20
 19512 | 19
  6972 | 19
 17047 | 18
(10 rows)
```

Issue 18408 looks to be the worst. What is it?

```sql
pygoth=> select * from issues where id = 18408;
NOTICE:  id = 18408
  id   |                              summary                              
-------+-------------------------------------------------------------------
 18408 | Issue 18408: Fixes crashes found by pyfailmalloc - Python tracker
(1 row)
```

Hm, sounds pretty serious to me.

What I really wanted to do is apply this number-of-commits logic to
files. [Google suggests](http://google-engtools.blogspot.com/2011/12/bug-prediction-at-google.html)
that constant commits to a file is a sign that the file is buggy, has
poor abstractions, or is difficult to understand. I wasn't able to do
this for the demo because it looks like calculating file diffs in git
is *really* expensive. I simply lost patience. Mercurial might have
done better because it stores deltas between changesets (and the
Python project already uses it). That said, I wrote the code and kept
it around in the repo, so the especially curious can try it out.


Click Data
----

So now let's tackle some "big data." I've pulled about 1.5 months of
*sampled* click data for Gawker. We'll set up a really simple disk
format using LevelDB to store all of this. For this demo, we'll ingest
it in a big batch, but you could imagine that in production you might
receive this data in realtime.

What is LevelDB? It's a sorted key-value store that does a periodic
background flush of data to a set of immutable tables in sorted
order. It has a strong probabilistic guarantee that lookups take no
more than a single disk seek, though its iteration performance is
respectable but not great.

We define click data to be a tuple of uid, path, and
[engaged time on that page](https://chartbeat.com/publishing/for-editorial/understanding-engaged-time). We
organize the data in the clicks db by timestamp, and do a little bit
of precalculation on timestamp format generation for Multicorn. The
key is:

```
[ts]:[uid]
```

Where ts is a 10 digit padded int, and the value is:

```
[ts, postgres timestamp formatted ts, uid, path, engaged time]
```

Such as:

```
[
    1401071519,
    '2014-05-26 02:31:59',
    'CvPevCKoAUTabvOa',
    '/high-elf-high-on-acid-attacks-womans-bmw-with-a-sword-1579493237',
    '16',
]
```

How fast can we write to the system? Loading 7 million records takes a
little over 2 minutes (around 50k writes/sec), and is completely CPU
bound, so actual performance of this test would vary quite a bit from
machine to machine. The Gawker data is about 300 MB, which makes for a
transfer rate of around 2 MB/sec, well within the bounds of the network
card. This is fine for demo purposes, but for a production system
you'd probably want something that isn't quite as CPU intensive.

Much of the CPU time is spent generating the PostgreSQL formatted
timestamp. This is in general an issue that would need to be addressed
in a production system -- the amount of time converting between
different time formats. One trivial way to solve this problem is to
use Unix epoch timestamps as ints instead, and thus skirt around
conversion performance issues.

You can find the code to load data in `click_db.py` in the
repository. Also of note is `db_dump.py` which will give you the
contents of a click database, and of course the FDW definition in
`click_fdw.py`.

So let's ask some questions. How many clicks?

```sql
pygoth=> select count(1) from clicks;
  count  
---------
 7183805
(1 row)
```

(Again, keep in mind this is sampled data -- you shouldn't infer
anything about real Gawker data from these numbers except relative
path performance.)

What about the number of clicks in the last month?

```sql
pygoth=> select count(1) from clicks where time > timestamp 'now' - interval '1 month';
 count 
-------
 22877
(1 row)
```

What are the top pages over the last month by page views?

```sql
pygoth=> select path, count(1) as c from (select * from clicks where time > timestamp 'now' - interval '2 weeks') as temp group by path order by c desc limit 10;                         
                                path                                 |   c    
---------------------------------------------------------------------+--------
 /                                                                   | 242608
 /man-buys-every-pie-at-local-burger-king-to-spite-shitty-1617088150 |  69070
 /oklahoma-teacher-shows-up-drunk-and-pantsless-to-her-fi-1617231453 |  21576
 /smiling-young-white-people-make-app-for-avoiding-black-1617775138  |  17207
 /how-i-became-thousands-of-nerds-worst-enemy-by-tweeting-1618323233 |  13859
 /woman-reunites-with-long-lost-mom-learns-she-married-h-1617604033  |  10033
 /james-franco-is-living-with-a-man-1616908548                       |   8090
 /government-worker-suspended-for-tweeting-amateur-porn-s-1617054646 |   7863
 /two-trains-taken-out-of-service-as-bedbug-infestation-h-1616852605 |   7159
 /taylor-swift-visits-young-cancer-patient-drowns-you-in-1615713252  |   7028
(10 rows)
```

Ah, but how does this differ by uniques? One person may visit a page multiple times.

```sql
pygoth=> select path, count(distinct(uid)) as c from (select * from clicks where time > timestamp 'now' - interval '2 weeks') as temp group by path order by c desc limit 10;             
                                path                                 |   c   
---------------------------------------------------------------------+-------
 /man-buys-every-pie-at-local-burger-king-to-spite-shitty-1617088150 | 65241
 /                                                                   | 34341
 /oklahoma-teacher-shows-up-drunk-and-pantsless-to-her-fi-1617231453 | 20190
 /smiling-young-white-people-make-app-for-avoiding-black-1617775138  | 15850
 /how-i-became-thousands-of-nerds-worst-enemy-by-tweeting-1618323233 | 13038
 /woman-reunites-with-long-lost-mom-learns-she-married-h-1617604033  |  9421
 /james-franco-is-living-with-a-man-1616908548                       |  7444
 /government-worker-suspended-for-tweeting-amateur-porn-s-1617054646 |  6795
 /two-trains-taken-out-of-service-as-bedbug-infestation-h-1616852605 |  6648
 /is-steve-jobs-alive-in-brazil-1617504682                           |  6573
(10 rows)
```

You can see that the top pages are roughly the same but become more
varied the further down in the rankings you go. But what if we ordered
by total engaged time?

```sql
pygoth=> select path, sum(et) as c from (select * from clicks where time > timestamp 'now' - interval '2 weeks') as temp group by path order by c desc limit 10;                          
                                path                                 |    c    
---------------------------------------------------------------------+---------
 /                                                                   | 5537080
 /man-buys-every-pie-at-local-burger-king-to-spite-shitty-1617088150 | 2233591
 /how-i-became-thousands-of-nerds-worst-enemy-by-tweeting-1618323233 | 1358990
 /smiling-young-white-people-make-app-for-avoiding-black-1617775138  |  900873
 /oklahoma-teacher-shows-up-drunk-and-pantsless-to-her-fi-1617231453 |  501631
 /woman-reunites-with-long-lost-mom-learns-she-married-h-1617604033  |  408282
 /james-franco-is-living-with-a-man-1616908548                       |  352322
 /anonymous-declares-cyber-war-on-israel-downs-mossad-si-1615500861  |  332352
 /night-at-the-boozeum-handjobs-and-spiders-at-nycs-best-1615043426  |  314230
 /manson-girl-patricia-krenwinkel-gives-prison-interview-1616329478  |  251289
(10 rows)
```

And here you can see there's a lot more variation in the results. This
data set is too small to draw the conclusion that engaged time doesn't
correlate well with page views or uniques, but we've found that to be
the case for the larger Internet.

For our final trick, we'll plug our custom FDW with the PostgreSQL HLL
extension. First, notice that counting uniques is very expensive:

```sql
pygoth=> select count(distinct(uid)) from clicks;
  count  
---------
 1821470
(1 row)

Time: 94295.837 ms
```

In order to calcuate this number, PostgreSQL has to build up a
structure containing all the uid strings and then take that
structure's cardinatlity. A naive implementation would simply be a map
or a dictionary.

HLLs give us a way of calculating sizes of unique sets in a
probablistic way, using far less space, but at the expense of
accuracy. A property of the HLL calculation is that it doesn't have to
compare strings, unlike a map or dictionary structure. Thus insertions
into the HLL tend to be much faster.

HLLs are not built into PostgreSQL, however Aggregate Knowledge
[released a robust extension](https://github.com/aggregateknowledge/postgresql-hll).

Calculating the total distinct uids works like this:

```sql
pygoth=> select #hll_add_agg(hll_hash_text(uid)) from clicks;                                                                                                                             
     ?column?     
------------------
 1864561.51728971
(1 row)

Time: 16113.052 ms
```

94 seconds versus 16 seconds with about 2% accuracy. The true power of
HLLs is beyond the scope of this already mammoth post, but the point
of this exercise is to show that FDWs are a first class citizen in
PostgreSQL. Whatever user defined functions or modules PostgreSQL
supports on native tables also works with FDWs.


Summary
----

At the start of the post, I mentioned that I'd come up with a Big
Idea, but then discovered that it was wrong. My Big Idea at the time
was that using knowledge of how data is stored on disk and some fancy
libraries (LevelDB), you could put together an analytics system with
PostgreSQL that could beat PostgreSQL itself. This is, in fact, the
message of
[AdRoll's talk](http://tuulos.github.io/sf-python-meetup-sep-2013/#/)
that inspired my explorations into FDWs, in which they were able to
cobble together a system that was more performant than Redshift
(Amazon's very fast column oriented store). I believe this to be true,
however my example with LevelDB didn't pan out. It turns out that
ingesting the click data as a native PostgreSQL table results in some
really quick queries on its own, much faster than going through
Multicorn. This, I believe, has to do with some Python overhead, but
I'm not entirely sure and have not had time to really delve into
it. On the other hand, a really simple piece of Python code *was* able
to ingest 50,000 writes/sec in a VM on a laptop, so that's not so
shabby either.

We at Chartbeat have not yet put FDWs into production, but it's
something we're seriously considering. What we are doing, though, is
leaning just slightly more heavily on SQL than we did before and
thinking more deeply about our data stores. In most startups, it makes
sense to minimize tool divergence -- a smaller set of engineers can
keep the entire system in their heads. This is what we've done in the
past. We've traditionally stuck with Python and Mongo until it really
hurt, and we've put in place some bizarre and terrible hacks around
Mongo to handle our increasing data load. But as the team and data
scales up, seemingly small differences in technology choices compounds
and can become serious bottlenecks for performance and pain points for
cost. We're learning to be more nuanced about our tools.

And PostgreSQL FTW.
