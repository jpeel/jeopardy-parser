#!/usr/bin/env python -OO
# -*- coding: utf-8 -*-

# parses much faster than using BeautifulSoup with lxml.
# a multiprocess option parse in parallel using all cores
# a daily_double column for whether a clue is a daily double
# some database indices created to greatly speed up look-up

# all values are reported starting with $200 base value in
# the jeopardy round and $400 base value in double jeopardy
# this makes it easier to create a trainer that uses the values

from __future__ import with_statement
from glob import glob
import argparse, re, os, sys, sqlite3
import lxml.html

class SqlQueue(object):
    def __init__(self, sql):
        self.sql = sql

    def put(self, item):
        insert(self.sql, item)

def get_eligible_filenames(directory):
    return glob(os.path.join(directory, "*.html"))

def main(args):
    """Loop thru all the games and parse them."""
    if not os.path.isdir(args.dir):
        print "The specified folder is not a directory."
        sys.exit(1)
    NUMBER_OF_FILES = len(os.listdir(args.dir))
    if args.num_of_files:
        NUMBER_OF_FILES = args.num_of_files
    print "Parsing", NUMBER_OF_FILES, "files"
    sql = None
    if not args.stdout:
        sql = sqlite3.connect(args.database)
        sql.execute("""PRAGMA foreign_keys = ON;""")
        sql.execute("""CREATE TABLE airdates(
            game INTEGER PRIMARY KEY,
            airdate TEXT
        );""")
        sql.execute("""CREATE TABLE documents(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            clue TEXT,
            answer TEXT
        );""")
        sql.execute("""CREATE TABLE categories(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT UNIQUE
        );""")
        sql.execute("""CREATE TABLE clues(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game INTEGER,
            round INTEGER,
            value INTEGER,
            daily_double INTEGER,
            FOREIGN KEY(id) REFERENCES documents(id),
            FOREIGN KEY(game) REFERENCES airdates(game)
        );""")
        sql.execute("""CREATE TABLE classifications(
            clue_id INTEGER,
            category_id INTEGER,
            FOREIGN KEY(clue_id) REFERENCES clues(id),
            FOREIGN KEY(category_id) REFERENCES categories(id)
        );""")
    import re
    gid_pat = re.compile(r'(\d+)\.html')
    if not args.multiprocess:
        # this option sends the found clues into a SqlQueue which simply puts the clues directly into the SQL database
        # the reason for the SqlQueue abstraction is so to enable the multiprocessing option below
        sqlqueue = SqlQueue(sql)
        for file_name in get_eligible_filenames(args.dir):
            gid = int(gid_pat.findall(file_name)[0])
            parse_game_filename(file_name, sqlqueue, gid)
    else:
        # this option processes the files in parallel.
        # a pool of processes is created and fed the filenames to be processed.
        # the processes put the clues found into a queue to be processed by the main thread.
        import multiprocessing, Queue, time
        num_cpus = multiprocessing.cpu_count()
        # create the items to process
        items_to_process = [(file_name, int(gid_pat.findall(file_name)[0])) for file_name in get_eligible_filenames(args.dir)]
        insert_queue = multiprocessing.Queue()
        items_to_process_counter = multiprocessing.Value('i')
        items_to_process_counter.value = len(items_to_process)
        if num_cpus > 1:
            num_cpus -= 1
        pool = multiprocessing.Pool(num_cpus, f_init, [insert_queue, items_to_process_counter])
        results = [pool.apply_async(f, item_to_process) for item_to_process in items_to_process]

        # process results from the sql insert_queue and put them into the sql database.
        # when items_to_process_counter reaches zero, all of the html files have been processed.
        while items_to_process_counter.value > 0:
            if not insert_queue.empty():
                while not insert_queue.empty():
                    item = insert_queue.get_nowait()
                    if not args.stdout:
                        insert(sql, item)
                    else:
                        print item
            else:
                time.sleep(.001)
        pool.close()

        # process any remaining results
        while not insert_queue.empty():
            item = insert_queue.get()
            insert(sql, item)

    if not args.stdout:
        # create indices for faster look-ups.
        sql.execute("CREATE INDEX index_clues_game_round on clues(game, round);")
        sql.execute("CREATE INDEX index_classifications on classifications(clue_id, category_id);")
        sql.commit()
    print "All done"


def parse_game_filename_multi(file_name, gid, counter, queue):
    parse_game_filename(file_name, queue, gid)
    with counter.get_lock():
        counter.value -= 1

# entrance function for multiprocessing the html files.
def f(file_name, gid):
    parse_game_filename_multi(file_name, gid, f.counter, f.q)

# the queue and counter need to be put into the pool processes during initialization
def f_init(q, counter):
    f.q = q
    f.counter = counter

def parse_game_filename(file_name, queue, gid):
    with open(os.path.abspath(file_name)) as f:
        parse_game(f, queue, gid)

def parse_game(f, queue, gid):
    """Parses an entire Jeopardy! game and extract individual clues."""
    print "Parsing game:", gid
    txt = f.read().decode('cp1252')
    html = lxml.html.fromstring(txt)
    # the title is in the format:
    # J! Archive - Show #XXXX, aired 2004-09-16
    # the last part is all that is required
    airdate_search = html.xpath('/html/head/title')
    if airdate_search:
        airdate = airdate_search[0].text.split()[-1]
    else:
        print "no airdate. skipping."
        return

    jeopardy_round_search = html.xpath('//div[@id = "jeopardy_round"]')
    if not jeopardy_round_search:
        print "no jeopardy round. skipping."
        return
    parse_round(jeopardy_round_search[0], queue, 1, gid, airdate)

    double_jeopardy_round_search = html.xpath('//div[@id = "double_jeopardy_round"]')
    if not double_jeopardy_round_search:
        print "no double jeopardy round. skipping."
        return
    parse_round(double_jeopardy_round_search[0], queue, 2, gid, airdate)

    final_jeopardy_round_search = html.xpath('//div[@id = "final_jeopardy_round"]')
    if not final_jeopardy_round_search:
        print "no final jeopardy round. skipping."
        return
    final_jeopardy_round = final_jeopardy_round_search[0]
    category_search = final_jeopardy_round.xpath('.//td[@class = "category_name"]')
    if not category_search:
        print "couldn't find final jeopardy category. skipping."
        return
    category = category_search[0].text_content()
    text_search = final_jeopardy_round.xpath('.//td[@class = "clue_text"]')
    if not text_search:
        print "couldn't find final jeopardy text. skipping."
        return
    text = text_search[0].text_content()

    answer_search = final_jeopardy_round.xpath(".//div[@onmouseover]")
    if not answer_search:
        print "couldn't find final jeopardy answer. skipping."
        return

    onmouseover = answer_search[0].get("onmouseover")
    onmouseover_html = lxml.html.fromstring(onmouseover)
    onmouseover_search = onmouseover_html.xpath('.//em')
    if not onmouseover_search:
        print "couldn't find final jeopardy answer. skipping."
        return
    answer = onmouseover_search[0].text_content()

    queue.put([gid, airdate, 3, category, False, text, answer, 0])

def parse_round(r, queue, rnd, gid, airdate):
    """Parses and inserts the list of clues from a whole round."""
    category_matches = r.xpath('.//td[@class = "category_name"]')
    categories = [c.text_content() for c in category_matches]
    # the x_coord determines which category a clue is in
    # because the categories come before the clues, we will
    # have to match them up with the clues later on
    x = 0

    # assign values consistently across the entire database even if
    # in the past there was a lower starting value
    value_increment = 200 if rnd == 1 else 400
    value = value_increment
    for a in r.xpath('.//td[@class = "clue"]'):
        # check if this clue was revealed
        if a.text_content().strip():
            value_search = a.xpath('.//td[contains(@class, "clue_value")]')
            if not value_search:
                a_html = lxml.html.tostring(a)
                print "no value:", a_html
                continue
            value_found = value_search[0].text_content().strip(' ')
            daily_double = int(value_found.startswith('DD:'))
            text_search = a.xpath('.//td[@class = "clue_text"]')
            if not text_search:
                a_html = lxml.html.tostring(a)
                print "no text:", a_html
                continue
            text = text_search[0].text_content()
            answer_search = a.xpath(".//div[@onmouseover]")
            if not answer_search:
                a_html = lxml.html.tostring(a)
                print "no answer:", a_html
                continue
            onmouseover = answer_search[0].get("onmouseover")
            onmouseover_html = lxml.html.fromstring(onmouseover)
            onmouseover_search = onmouseover_html.xpath('.//em')
            if not onmouseover_search:
                onmouseover_decoded = lxml.html.tostring(lxml.html.fromstring(onmouseover))
                print "no onmouseover:",onmouseover_decoded
                continue
            answer = onmouseover_search[0].text_content()
            queue.put([gid, airdate, rnd, categories[x], value, text, answer, daily_double])

        # always update x, even if we skip
        # a clue, as this keeps things in order. there
        # are 6 categories, so once we reach the end,
        # loop back to the beginning category
        #
        if x == 5:
            x = 0
            value += value_increment
        else:
            x += 1

def insert(sql, clue):
    """Inserts the given clue into the database."""
    # clue is [game, airdate, round, category, value, clue, answer]
    # note that at this point, clue[4] is False if round is 3
    if "\\\'" in clue[6]:
        clue[6] = clue[6].replace("\\\'", "'")
    if "\\\"" in clue[6]:
        clue[6] = clue[6].replace("\\\"", "\"")
    if not sql:
        print clue
        return
    sql.execute("INSERT OR IGNORE INTO airdates VALUES(?, ?);", (clue[0], clue[1], ))
    sql.execute("INSERT OR IGNORE INTO categories(category) VALUES(?);", (clue[3], ))
    category_id = sql.execute("SELECT id FROM categories WHERE category = ?;", (clue[3], )).fetchone()[0]
    clue_id = sql.execute("INSERT INTO documents(clue, answer) VALUES(?, ?);", (clue[5], clue[6], )).lastrowid
    sql.execute("INSERT INTO clues(game, round, value, daily_double) VALUES(?, ?, ?, ?);", (clue[0], clue[2], clue[4], clue[7], ))
    sql.execute("INSERT INTO classifications VALUES(?, ?)", (clue_id, category_id, ))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description = "Parse games from the J! Archive website.",
        add_help = False,
        usage = "%(prog)s [options]"
    )
    parser.add_argument(
        "-d", "--dir",
        dest = "dir",
        metavar = "<folder>",
        help = "the directory containing the game files",
        default = "j-archive"
    )
    parser.add_argument(
        "-n", "--number-of-files",
        dest = "num_of_files",
        metavar = "<number>",
        help = "the number of files to parse",
        type = int
    )
    parser.add_argument(
        "-f", "--filename",
        dest = "database",
        metavar = "<filename>",
        help = "the filename for the SQLite database",
        default = "clues.db"
    )
    parser.add_argument(
        "--stdout",
        help = "output the clues to stdout and not a database",
        action = "store_true"
    )
    parser.add_argument(
        "--multiprocess",
        help = "process the files in parallel",
        action = "store_true"
    )
    parser.add_argument("--help", action = "help", help = "show this help message and exit")
    parser.add_argument("--version", action = "version", version = "2013.07.09")
    main(parser.parse_args())
