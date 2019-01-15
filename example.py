#!/usr/bin/env python3
import argparse
import logging
import pprint

from pgdumplib import directory


def parse_cli_args():
    parser = argparse.ArgumentParser(
        description='Convert PostgreSQL pg_dump -Fd backups to Avro')
    parser.add_argument(
        'directory', metavar='DIR', nargs=1,
        help='Path to the directory containing the backup')

    parser.add_argument('-d', '--debug', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')
    return parser.parse_args()


def main():
    args = parse_cli_args()
    level = logging.WARNING
    if args.debug is True:
        level = logging.DEBUG
    elif args.verbose is True:
        level = logging.INFO
    logging.basicConfig(level=level)

    reader = directory.Reader(args.directory[0])
    print('Header: {}'.format(reader.toc.header))
    print('Database: {}'.format(reader.toc.dbname))
    print('Archive Timestamp: {}'.format(reader.timestamp))
    print('Server Version: {}'.format(reader.server_version))
    print('Dump Version: {}'.format(reader.dump_version))

    types = set([])

    for entry in reader.toc.entries.items():
        types.add(entry.desc.lower())

    pprint.pprint(types)


if __name__ == '__main__':
    main()
