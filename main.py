import os
from data import Database, Extract


class Execute(object):
    def __init__(self):
        self.database = Database()
        self.database.create_tables()
        self.message_response = {}
        

    # Parse emails to be inserted into MySQL.
    def extract_data(self):
        parsed = Extract()
        data = []
        files_contain = set()
        for top, d, agg in os.walk(os.getcwd() + "/enron_with_categories"):
            for f in agg:
                if f.endswith(".txt") and f not in files_contain:
                    files_contain.add(f)
                    f = os.path.join(top, f)
                    data.append(parsed.level(open(f, 'r').readlines()))
        return data


    # Insert parsed data into MySQL.
    def insert_into_database(self, list_of_value_dicts):
        for row in list_of_value_dicts:
            erow = row.get('message_id'), row.get('from'), row.get(
                'subject'), self.database.convert_date_format(
                row.get('date')), self.get_label(len(row.get('to'))), row.get('sub_md5')
            erows = [erow]
            rrows = []
            for to in row.get('to'):
                rrow = row.get('message_id'), row.get(
                    'from'), to, 1, 0, 0
                rrows.append(rrow)
            for cc in row.get('cc'):
                rrow = row.get('message_id'), row.get(
                    'from'), cc, 0, 1, 0
                rrows.append(rrow)
            for bcc in row.get('bcc'):
                rrow = row.get('message_id'), row.get(
                    'from'), bcc, 0, 0, 1
                rrows.append(rrow)
            self.database.insert('email', erows)
            self.database.insert('recipient', rrows)
        return True

    # Solve the first question.
    def per_day(self):
        print "Each person recieved this many emails each day: "
        print "EMAIL", '|', "COUNT", '|' ,"DATE"
        print "_________________________________"
        for e, c, date in self.database.run_query("select recipient,count(r.message_id) as cnt, email_date from recipient r, email e where e.message_id = r.message_id  group by recipient,DATE(email_date) order by cnt desc;"):
            print e, '|', c, '|' ,unicode(date)
        print "_________________________________"

    # Solve the third question
    def fast_responses(self):
        prev_sub_md5 = None
        prev_ts = None
        for sub_md5, sender, message_id, email_date in self.database.run_query("SELECT sub_md5,sender, message_id, email_date from email where sub_md5 is NOT NULL order by sub_md5,email_date asc;"):
            if sub_md5 != prev_sub_md5:
                prev_sub_md5 = sub_md5
                prev_ts = email_date
                continue
            else:
                prev_sub_md5 = sub_md5
                self.message_response[message_id] = email_date - prev_ts
                prev_ts = email_date
        print "These people responded the fastest: "
        print [str(unicode(k)) for k,v in (sorted(self.message_response.items(), key=lambda x: x[1])[:6])]
        print "_________________________________"

    # Broadcast part of question 2
    def broadcast(self):
        print "BROADCAST"
        e,c = self.database.run_query("select sender,count(*) as cnt from email where label='broadcast' group by sender order by cnt desc;", 1)
        print "The email " + unicode(e) + " recieved the most broadcast emails with " + c + " emails recieved."
        print "_________________________________"

    # Direct part of question 2
    def direct(self):
        print "DIRECT"
        e,c = self.database.run_query("select recipient,count(e.message_id) as cnt from recipient r, email e where e.message_id = r.message_id  and e.label = 'direct' and r.is_cc = 1 group by recipient,label order by cnt desc;", 1)
        print "The email " + unicode(e) + " sent the most direct emails with " + c + " emails sent."
        print "_________________________________"

    # Make sure the directory of emails can be found.
    def check_for_emails(self):
        if os.path.isdir("enron_with_categories"):
            print "Data Set has been Accessed!"
            return True
        return False

    #  Get Count of how many people email was sent out to!
    def get_label(self, to):
        if to <= 1:
            return 'direct'
        return 'broadcast'

    # Run Full Job
    def execute(self):
        if self.check_for_emails():
            data = self.extract_data()
            if data and self.insert_into_database(data):
                return True
        return False


if __name__ == "__main__":
    ex = Execute()
    if ex.execute():
        ex.per_day()
        ex.direct()
        ex.broadcast()
        ex.fast_responses()
