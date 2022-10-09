# -*- coding: utf-8 -*-
import datetime
import argparse
import shutil
import pathlib
import subprocess
import os
import logging
from items.objects import *
from subprocess import PIPE, Popen


def pg_ddl_export():
    arg_parser = argparse.ArgumentParser(conflict_handler='resolve',
                                         description='Очень незамысловато конвертит дамп Postgresql базы в файлики.'
                                                     'Указать или реквизиты для коннекта, или путь к файлу дампа')
    arg_parser.add_argument('-h', '--host', type=str, help='host')
    arg_parser.add_argument('-p', '--port', type=str, help='port')
    arg_parser.add_argument('-U', '--login', type=str, help='login')
    arg_parser.add_argument('-d', '--dbname', type=str, help='dbname')
    arg_parser.add_argument('-W', '--password', type=str, help='Password')
    arg_parser.add_argument('-D', '--directory', type=str, help='output directory')
    arg_parser.add_argument('-f', '--dumpfile', type=str, help='pg_dump -s -O result')
    arg_parser.add_argument('--clean', action='store_true', help='clean out_dir if not empty')

    args = arg_parser.parse_args()
    if args.directory:
        if os.path.exists(args.directory) and not args.clean:
            print('Target directory is not empty.Continue?')
            x = input('Y/n\n')
            if x != 'y':
                exit()

    logs_folder = os.path.join(str(pathlib.Path(__file__).parent.absolute()), '.pg_ddl_export', 'logs')
    os.makedirs(logs_folder, exist_ok=True)
    logging.basicConfig(
        format=u'%(levelname)-8s [%(asctime)s] %(message)s',
        level=logging.DEBUG,
        filename=os.path.join(
            logs_folder, "pg_ddl_export_{0}.log".format(
                            datetime.datetime.now().strftime("%Y-%m-%d")
                        )
            )
        )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)-5s: %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    logger = logging.getLogger(__name__)

    shutil.rmtree(args.directory, ignore_errors=True)
    with open(get_dump(args), encoding='utf-8') as src_dump:
        dump2array(src_dump.read(), args)


def get_dump(args):
    if args.dumpfile:
        return args.dumpfile

    args.dumpfile ="_basedump.dump"
    if args.password:
        os.environ['PGPASSWORD'] = args.password
    logger.info("Dumping database...")
    process = subprocess.Popen(["pg_dump", "-sO", "-h",
                                   args.host, "-p", args.port,
                                   "-U",args.login, "-d", args.dbname,
                                   "-f",args.dumpfile], stdout=subprocess.PIPE)
    result = process.communicate()[0]
    if process.returncode != 0:
        logger.critical('dump error : %s',process.returncode,)
        exit(1)
    else:
        logger.info("dump succesful!")
        return args.dumpfile



def dump2array(dump, args):
    dump = re.split(r'(?=--.*\n-- Name)', dump)
    logger.info("dump contains %s items",len(dump))
    logger.info("parsing...")
    for element in dump[1:]:
        if re.findall(r'.*DEFAULT ACL.*',element).__len__()==0:
            parse(element, args)
    logger.info("Done!")

def parse(element, args):
    item_types = {
        'ACL': Acl,
        'AGGREGATE': Aggregate,
        'CAST': Cast,
        'COMMENT': Comment,
        'CONSTRAINT': Constraint,
        'DOMAIN': Domain,
        'EXTENSION': Extension,
        'FK CONSTRAINT': FkConstraint,
        'FUNCTION': Function,
        'INDEX': Index,
        'OPERATOR': Operator,
        'SCHEMA': Schema,
        'SEQUENCE': Sequence,
        'TABLE': Table,
        'TRIGGER': Trigger,
        'TYPE': Type,
        'VIEW': View,
        'MATERIALIZED VIEW': MaterializedView,
        'PROCEDURE': Procedure,
        'COLLATION': Collation
    }
    hd = get_attr(element.split(';')[1].replace('-- ', ''))
    var = item_types[hd['Type']](element, args)


def get_attr(header):
    hd = {}
    for att in header.split(';'):
        if att.split(':').__len__() > 1:
            try:
                hd[att.split(':')[0].strip()] = att.split(':')[1].strip()
            except UnicodeEncodeError or IndexError as e:
                logger.error(
                      '%s on parse header: %s',str(e.reason), str(header))
    return hd


pg_ddl_export()
