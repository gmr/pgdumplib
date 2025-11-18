import argparse
import datetime
import logging
import os
import random
from os import path

import faker
import psycopg
from faker.providers import address, internet, misc, person

LOGGER = logging.getLogger(__name__)
LOGGING_FORMAT = '[%(asctime)-15s] %(levelname)-8s %(message)s'

# Faker Supported Locales
LOCALES = [
    'ar_AA',
    'ar_EG',
    'ar_JO',
    'ar_PS',
    'ar_SA',
    'bg_BG',
    'bs_BA',
    'cs_CZ',
    'de_AT',
    'de_CH',
    'de_DE',
    'dk_DK',
    'el_CY',
    'el_GR',
    'en_AU',
    'en_CA',
    'en_GB',
    'en_IE',
    'en_NZ',
    'en_TH',
    'en_US',
    'es_ES',
    'es_MX',
    'et_EE',
    'fa_IR',
    'fi_FI',
    'fr_CH',
    'fr_FR',
    'he_IL',
    'hi_IN',
    'hr_HR',
    'hu_HU',
    'hy_AM',
    'id_ID',
    'it_IT',
    'ja_JP',
    'ka_GE',
    'ko_KR',
    'lb_LU',
    'lt_LT',
    'lv_LV',
    'mt_MT',
    'ne_NP',
    'nl_BE',
    'nl_NL',
    'no_NO',
    'pl_PL',
    'pt_BR',
    'pt_PT',
    'ro_RO',
    'ru_RU',
    'sk_SK',
    'sl_SI',
    'sv_SE',
    'th_TH',
    'tr_TR',
    'tw_GH',
    'uk_UA',
    'zh_CN',
    'zh_TW',
]

STATES = ['unverified', 'verified', 'suspended']
TYPES = ['billing', 'delivery']

ADDRESS_SQL = """\
INSERT INTO test.addresses
            (created_at, last_modified_at, user_id, "type", address1, address2,
             address3, locality, region, postal_code, country)
     VALUES (%(created_at)s, %(last_modified_at)s, %(user_id)s, %(type)s,
             %(address1)s, %(address2)s, %(address3)s, %(locality)s,
             %(region)s, %(postal_code)s, %(country)s)
  RETURNING id;"""
ICON_SQL = 'SELECT lo_from_bytea(0, %(data)s)'
USER_SQL = """\
INSERT INTO test.users
            (created_at, last_modified_at, state, email, name, surname,
             display_name, locale, password_salt, password, signup_ip, icon)
     VALUES (%(created_at)s, %(last_modified_at)s, %(state)s, %(email)s,
             %(name)s, %(surname)s, %(display_name)s, %(locale)s,
             %(password_salt)s, %(password)s, %(signup_ip)s, %(icon)s)
  RETURNING id, created_at;"""


def add_connection_options_to_parser(parser):
    """Add PostgreSQL connection CLI options to the parser.

    :param argparse.ArgumentParser parser: The parser to add the args to

    """
    conn = parser.add_argument_group('Connection Options')
    conn.add_argument(
        '-d',
        '--dbname',
        action='store',
        default=os.environ.get('PGDATABASE', 'postgres'),
        help='database name to connect to',
    )
    conn.add_argument(
        '-h',
        '--host',
        action='store',
        default=os.environ.get('PGHOST', 'localhost'),
        help='database server host or socket directory',
    )
    conn.add_argument(
        '-p',
        '--port',
        action='store',
        type=int,
        default=int(os.environ.get('PGPORT', 5432)),
        help='database server port number',
    )
    conn.add_argument(
        '-U',
        '--username',
        action='store',
        default=os.environ.get('PGUSER', 'postgres'),
        help='The PostgreSQL username to operate as',
    )


def add_logging_options_to_parser(parser):
    """Add logging options to the parser.

    :param argparse.ArgumentParser parser: The parser to add the args to

    """
    group = parser.add_argument_group(title='Logging Options')
    group.add_argument(
        '-L',
        '--log-file',
        action='store',
        help='Log to the specified filename. If not specified, '
        'log output is sent to STDOUT',
    )
    group.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='Increase output verbosity',
    )
    group.add_argument(
        '--debug', action='store_true', help='Extra verbose debug logging'
    )


def configure_logging(args):
    """Configure Python logging.

    :param argparse.namespace args: The parsed cli arguments

    """
    level = logging.WARNING
    if args.verbose:
        level = logging.INFO
    elif args.debug:
        level = logging.DEBUG
    filename = args.log_file if args.log_file else None
    if filename:
        filename = path.abspath(filename)
        if not path.exists(path.dirname(filename)):
            filename = None
    logging.basicConfig(level=level, filename=filename, format=LOGGING_FORMAT)


def generate_address(fake, user_id, created_at):
    """Generate a street address for the user"""
    last_modified_at = None
    if fake.boolean(chance_of_getting_true=25):
        last_modified_at = created_at + fake.time_delta(
            end_datetime=datetime.datetime.now(tz=datetime.UTC)
        )

    address1 = fake.street_address()
    address2 = None
    if '\n' in address1:
        parts = address1.split('\n')
        address1 = parts[0]
        address2 = parts[1]
    try:
        address3 = fake.secondary_address()
    except AttributeError:
        address3 = None
    try:
        region = fake.state()
    except AttributeError:
        region = None
    addr = {
        'created_at': created_at,
        'last_modified_at': last_modified_at,
        'user_id': user_id,
        'type': fake.random_element(TYPES),
        'address1': address1,
        'address2': address2,
        'address3': address3,
        'locality': fake.city(),
        'region': region,
        'postal_code': fake.postcode(),
        'country': fake.country(),
    }
    LOGGER.debug('Returning %r', addr)
    return addr


def generate_user(fake, locale_fake, locale, icon_oid):
    """Generate a fake user"""
    created_at = fake.date_time_this_year()
    last_modified_at = None
    if fake.boolean(chance_of_getting_true=45):
        last_modified_at = created_at + fake.time_delta(
            end_datetime=datetime.datetime.now(tz=datetime.UTC)
        )
    name = locale_fake.first_name()
    surname = locale_fake.last_name()
    display_name = None
    if fake.boolean(chance_of_getting_true=50):
        if fake.boolean(chance_of_getting_true=50):
            display_name = name
        else:
            display_name = f'{name} {surname}'
    user = {
        'created_at': created_at,
        'last_modified_at': last_modified_at,
        'state': fake.random_element(STATES),
        'email': locale_fake.safe_email(),
        'name': name,
        'surname': surname,
        'display_name': display_name,
        'locale': locale.replace('_', '-'),
        'password_salt': fake.uuid4(),
        'password': fake.password(12),  # It's fake data :-p
        'signup_ip': fake.ipv4_public(),
        'icon': icon_oid,
    }
    LOGGER.debug('Returning user: %r', user)
    return user


def generate_users(args, cursor):
    """Generate fixture data for the user table."""
    LOGGER.info('Creating %i users', args.user_count)
    fake = faker.Faker()
    fake.add_provider(address)
    fake.add_provider(internet)
    fake.add_provider(misc)

    for _offset in range(0, args.user_count):
        # Randomly maybe create a blob
        icon_oid = None
        if fake.boolean(chance_of_getting_true=25):
            cursor.execute(
                ICON_SQL,
                {'data': fake.binary(length=random.randint(10000, 200000))},
            )
            icon_oid = cursor.fetchone()[0]

        locale = fake.random_element(LOCALES)
        locale_fake = get_locale_faker(locale)

        # Create the User
        try:
            cursor.execute(
                USER_SQL, generate_user(fake, locale_fake, locale, icon_oid)
            )
        except psycopg.IntegrityError as err:
            LOGGER.error('Error creating user: %s', err)
            continue

        user = cursor.fetchone()
        LOGGER.info('Created user %s', user[0])
        created_at = user[1]
        for _offset in range(random.randint(0, 2)):
            cursor.execute(
                ADDRESS_SQL, generate_address(locale_fake, user[0], created_at)
            )
            addr = cursor.fetchone()
            LOGGER.info('Created address %s for user %s', addr[0], user[0])
            created_at = created_at + fake.time_delta(
                end_datetime=datetime.datetime.now(tz=datetime.UTC)
            )


def get_locale_faker(locale=None):
    """Return a fake.Faker instance for the specified locale.

    :param str locale: The locale to generate fake data for
    :rtype: faker.Faker

    """
    fake = faker.Faker(locale)
    fake.add_provider(address)
    fake.add_provider(internet)
    fake.add_provider(person)
    return fake


def main(args):
    """Primary script for generating fake data in test database

    :param argparse.namespace args: The parsed cli arguments

    """
    configure_logging(args)
    conn = psycopg.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.username,
        autocommit=True,
    )
    cursor = conn.cursor()
    generate_users(args, cursor)
    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Generates fixture data for tests',
        conflict_handler='resolve',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_connection_options_to_parser(parser)
    add_logging_options_to_parser(parser)
    parser.add_argument(
        '--user-count',
        action='store',
        type=int,
        default=250,
        help='How many users to generate',
    )
    main(parser.parse_args())
