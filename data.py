from pymysql.err import InternalError,IntegrityError
from dateutil.parser import parse
import os
import hashlib
import yaml
import pymysql

# Parsing the emails
class Extract(object):
    def level(self, email):
        store = {}
        to_addr = []
        cc_addr = []
        bcc_addr = []
        from_addr = None
        msg_id = None
        subject = None
        dt = None
        prev = ""

        if self.invalid(email):
            print "INVALID"
            return None
        for line in email:
            line = line.replace("\r", "").replace("\n", "").replace(
                "\t", "").strip()
            if "X-" in line:
                break
            elif "Message-ID:" in line:
                prev = "Message-ID:"
                msg_id = line.replace("Message-ID:", "").strip()
            elif "Date:" in line:
                prev = "Date:"
                dt = line.replace("Date:", "").strip()
            elif "From:" in line:
                prev = "From:"
                from_addr = line.replace("From:", "").strip()
            elif "To:" in line:
                prev = "To:"
                t = [i.strip() for i in
                     line.replace("To:", "").strip().split(",") if
                     len(i) > 1]
                to_addr.extend(t)
            elif "Cc:" in line:
                prev = "Cc:"
                t = [i.strip() for i in
                     line.replace("Cc:", "").strip().split(",") if
                     len(i) > 1]
                cc_addr.extend(t)
            elif "Bcc:" in line:
                prev = "Bcc:"
                t = [i.strip() for i in
                     line.replace("Bcc:", "").strip().split(",") if
                     len(i) > 1]
                bcc_addr.extend(t)
            elif "Subject:" in line:
                prev = "Subject:"
                subject = line.replace("Subject:", "").strip()
            elif "Mime-Version:" in line or "Content-Type:" in line or "Content-Transfer-Encoding:" in line:
                prev = ""
            else:
                if prev == "To:":
                    t = [i.strip() for i in line.strip().split(",") if
                         len(i) > 1]
                    to_addr.extend(t)
                if prev == "Subject:":
                    subject = subject + line
                if prev == 'Cc:':
                    t = [i.strip() for i in line.strip().split(",") if
                         len(i) > 1]
                    cc_addr.extend(t)
                if prev == 'Bcc:':
                    t = [i.strip() for i in line.strip().split(",") if
                         len(i) > 1]
                    bcc_addr.extend(t)
        store['to'] = [i.strip() for i in to_addr if '@' in i]
        store['cc'] = [i.strip() for i in cc_addr if '@' in i]
        store['from'] = from_addr.strip() if from_addr else ""
        store['bcc'] = [i.strip() for i in bcc_addr if '@' in i]
        store['subject'] = subject.strip() if subject else ""
        store['message_id'] = msg_id if msg_id else ""
        store['date'] = dt
        store['sub_md5'] = hashlib.md5(
            subject.lower().replace("re:", "").strip()).hexdigest() if subject else None
        return store



# Creating Database, Inserting Parsed Data, running queries.
class Database(object):
    def __init__(self):
        self.config = os.getcwd() + "/ddl.yaml"
        self.config_yaml = self._get_config()

        self.schema = self.config_yaml.get('config').get(
                'schema')
        self.dbhost = self.config_yaml.get('config').get('host')
        self.dbuser = self.config_yaml.get('config').get('user')
        self.dbpass = self.config_yaml.get('config').get('pass')
        self.dbport = self.config_yaml.get('config').get('port')
        self.conn = pymysql.connect(host=self.dbhost,
                                        port=self.dbport,
                                        user=self.dbuser,
                                        passwd=self.dbpass,
                                        db=self.schema, charset='utf8',
                                        autocommit=True)
        self.cursor = self.conn.cursor()

    def _get_config(self):
        return yaml.load(open(self.config))

    def create_tables(self):
        for name, ddl in self.config_yaml.get('ddl').items():
            if 'drop' in name:
                print("Dropping table {}: ".format(name))
            elif 'create' in name:
                print("Creating table {}: ".format(name))
            try:
                self.cursor.execute(ddl)
            except InternalError as e:
                print e

    def insert(self, table=None, rows=None):
        if rows is None or table is None:
            print "Table Missing?"
            return False
        if table is 'email':
            for r in rows:
                try:
                    self.cursor.execute("INSERT INTO email(message_id, sender, subject, email_date, label, sub_md5) VALUES (%s,%s,%s,%s,%s, %s)",r)
                except IntegrityError as e:
                    print e

        elif table is 'recipient':
            for r in rows:
                try:
                    self.cursor.execute("INSERT INTO recipient(message_id, sender, recipient, is_to, is_cc, is_bcc) VALUES (%s,%s,%s,%s,%s,%s)",r)
                except IntegrityError as e:
                    print e
        else:
            print "FAILURE"
            return False

    def run_query(self, query=None, n=None):
        if query is None:
            return None
        self.cursor.execute(query)
        if n is None:
            return self.cursor.fetchall()
        elif n == 1:
            return self.cursor.fetchone()
        else:
            return self.cursor.fetchmany(5)

    def convert_date_format(self,date=None):
        if date:
            return parse(date).strftime("%Y-%m-%d %H:%M:%S")
        else:
            return None
