import argparse
import datetime
from pathlib import Path
from app.manager import select_strategy_from_list

parser = argparse.ArgumentParser(
    prog="abacus",
    description="Create and calculate indices",
    epilog="Thanks for using %(prog)s! :)",
)


actions = parser.add_argument_group("actions")
actions.add_argument("--list", action="store_true")
actions.add_argument("--strategy", action="store_true")
actions.add_argument("--new", action="store_true")
actions.add_argument("--update", action="store_true")
actions.add_argument("--clone", action="store_true")
actions.add_argument("--delete", action="store_true")

args = parser.parse_args()

target_dir = Path(args.path)

if not target_dir.exists():
    print("The target directory doesn't exist")
    raise SystemExit(1)

def build_output(entry, long=False):
    if long:
        size = entry.stat().st_size
        date = datetime.datetime.fromtimestamp(
            entry.stat().st_mtime).strftime(
            "%b %d %H:%M:%S"
        )
        return f"{size:>6d} {date} {entry.name}"
    return entry.name

for entry in target_dir.iterdir():
    print(build_output(entry, long=args.long))

def main():
    select_strategy_from_list()

if __name__ == '__main__':
    main()
