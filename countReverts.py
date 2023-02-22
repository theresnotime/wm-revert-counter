import os
import time
from datetime import datetime, timedelta

from termcolor import colored

import SOTime
from eventstreams import EventStreams


def log_wiki_count(change: dict, wiki_counts: dict) -> dict:
    if change["database"] not in wiki_counts:
        wiki_counts[change["database"]] = 1
    else:
        wiki_counts[change["database"]] += 1
    return wiki_counts


def main(
    start_timestamp: str,
    end_timestamp: str,
    debug: bool = False,
    verbose: bool = False,
    delay: float = 0,
):
    print(f"Start: {start_timestamp}")
    print(f"End: {end_timestamp}")

    started_run = datetime.now()
    stream = EventStreams(
        streams=["mediawiki.revision-tags-change"], since=start_timestamp, timeout=1
    )
    wiki_counts = {}
    total_count = 0
    watch_tags = ["mw-rollback", "mw-undo"]

    while stream:
        change = next(iter(stream))
        database = change["database"]
        rev_timestamp = change["rev_timestamp"]
        added_tags = change["tags"]

        if rev_timestamp >= end_timestamp:
            break

        if any(item in watch_tags for item in added_tags):
            # Watched tag
            if debug:
                print(
                    colored(f"{database} at {rev_timestamp} >>> {added_tags}", "green")
                )
            wiki_counts = log_wiki_count(change, wiki_counts)
            total_count += 1
        else:
            # Not a watched tag
            if verbose:
                print(colored(f"{database} at {rev_timestamp} >>> {added_tags}", "red"))
            continue
        time.sleep(delay)

    ended_run = datetime.now()
    run_time = ended_run - started_run
    print(f"Run time: ~{round(run_time.total_seconds() / 60)} minutes")
    print(f"Total count: {total_count}")
    for count in wiki_counts:
        print(f"{count}: {wiki_counts[count]}")


if __name__ == "__main__":
    os.system("color")
    end_timestamp = SOTime.round_time(
        datetime.now(), date_delta=timedelta(minutes=1), to="down"
    ).isoformat()
    start_timestamp = SOTime.round_time(
        (datetime.now() - timedelta(minutes=5)),
        date_delta=timedelta(minutes=1),
        to="down",
    ).isoformat()
    main(
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        debug=False,
        verbose=False,
    )
