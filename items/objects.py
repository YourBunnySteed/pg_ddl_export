import logging
import re
import os

logger = logging.getLogger(__name__)

class PgObject(object):
    def __init__(self, data, context):

        self.header = data.split('\n')[1].replace('-- ','').split(';')
        self.ddl = '\n'.join(data.split('\n')[1:])
        self.attrs = {}
        self.name = '-'
        self.type = '-'
        self.schema = '-'
        self.owner = '-'
        self.get_attr()
        self.file = self.name
        self.ext = '.pgsql'
        self.path = context.directory
        self.comments = []
        self.data = data

        self.is_child = False
        self.set_path()
        self.find_parent_path()
        self.cut_name()
        self.write()


    def get_attr(self):
        self.attrs['is_broken'] = False
        for att in self.header:
            try:
                self.attrs[att.split(':')[0].strip()] = att.split(':')[1].strip()
            except UnicodeEncodeError or IndexError as e:
                logger.error(
                    '%s on parse header: %s', str(reason), self.header)
        self.type = self.attrs['Type']
        self.schema = self.attrs['Schema']
        self.owner = self.attrs['Owner']
        self.name = self.attrs['Name']


    def cut_name(self):
        self.name = self.name.split(' ')[-1]

    def write(self):
        os.makedirs(self.path, exist_ok=True)
        if self.is_child:
            self.data = '\n'.join(self.data.split('\n')[3:-2])
        try:
            open(os.path.join(self.path, self.name.replace('"','') + self.ext), 'a').write(self.data.replace('\n\n\n', '\n'))
        except UnicodeEncodeError as e:
            logger.error(
                'Error with encoding, Header is: %s ', str(self.header))

    def set_path(self):
        self.path = os.path.join(self.path, self.schema, self.type + 'S')

    def set_name(self):
        pass

    def clear(self):
        pass

    def find_parent_path(self):
        pass


class Extension(PgObject):
    def set_path(self):
        self.path = os.path.join(self.path)

    def cut_name(self):
            self.name = 'EXTENSIONS'


class Cast(PgObject):
    def set_path(self):
        self.path = os.path.join(self.path)
    def cut_name(self):
            self.name = 'CASTS'


class Schema(PgObject):
    def set_path(self):
        self.path = os.path.join(self.path, self.name)


class Operator(PgObject):
    def set_path(self):
        self.path = os.path.join(self.path, self.schema)
    def cut_name(self):
        self.name = 'OPERATORS'


class Collation(PgObject):
    pass


class Type(PgObject):
    pass


class Table(PgObject):
    pass


class Function(PgObject):
    def cut_name(self):
        self.name = re.sub('\(.*', '', self.attrs['Name'])


class Procedure(PgObject):
    def cut_name(self):
        self.name = re.sub('\(.*','',self.attrs['Name'])


class Sequence(PgObject):
    def set_name(self):
        self.name = 'SEQUENCES'


class View(PgObject):
    pass


class Aggregate(PgObject):
    def set_name(self):
        self.name = 'AGGREGATES'


class MaterializedView(PgObject):
    pass


class Comment(PgObject):
    def set_path(self):
        pass

    def find_parent_path(self):
        self.is_child = True
        gr = re.search(r'ON (\S+) (\S+?) IS', self.data.replace('"', '')).groups()

        if gr.__len__()==1:
             logger.warning("Unknown comment type! data: %s", self.data.replace('"', ''))
        else:
            parent_type, parent_fullname = gr

        if parent_type in ['SCHEMA']:
            self.path = os.path.join(self.path, parent_fullname)
            self.name = parent_fullname
        elif parent_type in ['FUNCTION', 'PROCEDURE', 'AGGREGATE', 'TABLE', 'VIEW', 'SEQUENCE', 'MATERIALIZED VIEW']:
            self.path = os.path.join(self.path, self.schema, self.type + 'S')
            self.name = re.sub(r'\(.*', '', parent_fullname).split('.')[-1]
        elif parent_type in ['COLUMN']:
            self.path = os.path.join(self.path, self.schema, 'TABLES')
            self.name = parent_fullname.split('.')[-2]
        elif parent_type in ['EXTENSION', 'CAST']:
            self.path = self.path
            self.name = parent_type + 'S'
        elif parent_type in ['TYPE']:
            self.path = self.path
            self.name = parent_type + 'S'
        else:
            logger.warning("Orphan here! %s data: %s",str(parent_fullname) ,self.data.replace('"', ''))



class Index(PgObject):
    def find_parent_path(self):
        self.is_child = True
        parent_fullname = re.search('ON (.*) USING', self.data.replace('"', '')).group(1)
        self.path = os.path.join(os.path.dirname(self.path), 'TABLES')
        self.name = parent_fullname.split('.')[-1]


class Constraint(PgObject):
    def cut_name(self):
        pass

    def find_parent_path(self):
        self.is_child = True
        parent_fullname = re.search('ONLY (.*)', self.data.replace('"', '')).group(1)
        self.path = os.path.join(os.path.dirname(self.path), 'TABLES')
        self.name = parent_fullname.split('.')[-1]


class FkConstraint(PgObject):
    def cut_name(self):
        pass

    def find_parent_path(self):
        self.is_child = True
        parent_fullname = re.search('ONLY (.*)', self.data.replace('"', '')).group(1)
        self.path = os.path.join(os.path.dirname(self.path), 'TABLES')
        self.name = parent_fullname.split('.')[-1]


class Domain(PgObject):
    def find_parent_path(self):
        self.is_child = True
        parent_fullname = re.search(' AS (.*);', self.data.replace('"', '')).group(1)
        self.path = os.path.join(self.path, parent_fullname.split('.')[0], 'TYPES')
        self.name = parent_fullname.split('.')[-1]


class Trigger(PgObject):
    def find_parent_path(self):
        self.is_child = True
        parent_fullname = re.search(' ON (.*) FOR ', self.data.replace('"', '')).group(1)
        self.path = os.path.join(self.path, self.schema, 'TYPES')
        self.name = parent_fullname.split('.')[-1]

class Acl(PgObject):
    def set_path(self):
        pass

    def find_parent_path(self):
        self.is_child = True
        gr = re.search(r'ON\s([^\(\s]+)\s"?(\S*?)"?\sTO', self.data)
        if gr is None:
            logger.warning("Unknown ACL type! data: %s", self.data.replace('"', ''))
        else:
            parent_type, parent_fullname = gr.groups()
            if parent_type in ['SCHEMA']:
                self.path = os.path.join(self.path, parent_fullname)
                self.name = self.schema.replace('"', '')
            elif parent_type in ['FUNCTION', 'PROCEDURE', 'AGGREGATE', 'TABLE', 'VIEW', 'SEQUENCE', 'MATERIALIZED VIEW']:
                self.path = os.path.join(os.path.dirname(self.path),parent_type)
                self.name = parent_fullname.split('.')[-1]
            elif parent_type in ['COLUMN']:
                self.path = os.path.join(self.path, self.schema, 'TABLES')
                self.name = parent_fullname.split('.')[-2]
            elif parent_type in ['EXTENSION', 'CAST']:
                self.name = parent_type + 'S'
            elif parent_type in ['TYPE']:
                self.name = parent_type + 'S'
            else:
                logger.warning("Orphan here! %s data: %s",str(parent_fullname) ,self.data.replace('"', ''))

            self.name = parent_fullname.split('.')[-1]
