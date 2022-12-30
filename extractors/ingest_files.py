import imaplib
import email
import os
import re
import traceback
import datetime

# import awswrangler as wr
# import pandas as pd
# from sqlalchemy import create_engine

# sys.path.append('/home/silambarasan.s/Config')
# sys.path.append("D:\\Initial_Version\\Lake_Ingestion_Codes\\")

from config import basic_config as con
from utils.customlogger import basiclogger

file_logger = basiclogger(logname=f"File_Load", section=f"PROCESS")
file_except_logger = basiclogger(logname=f"File_Exceptions", section=f"EXCEPTION")


class IngestFile:
    def __init__(self, bingads_flag=0, googleads_flag=0, discoverads_flag=0):
        self.bing = bingads_flag
        self.google = googleads_flag
        self.discover = discoverads_flag
        self.all = 0
        if self.discover and self.google and self.bing:
            self.all = 1
        # File path to download the files
        self.ppc_file_path = con.attachment_path
        # Connect to IMAP Server (Webmail) and select "Inbox"
        self.mail = imaplib.IMAP4_SSL(con.file_ppc_webmail_url, con.file_ppc_webmail_port)
        self.mail.login(con.zmailid, con.zmailpassword)
        self.mail.select(con.bing_mailfolder)

    def download_xlsx_files(self):
        self.download_bingads_files()
        self.download_discoverads_files()
        self.download_googleads_files()

    @staticmethod
    def get_first_text_block(email_message_instance, poss):
        maintype = email_message_instance.get_content_maintype()

        if maintype == 'multipart':
            for part in email_message_instance.walk():
                print(part.get_content_maintype())

                if part.get_content_maintype() == 'multipart':
                    filename = part.get_payload()[1].get_filename()
                    print(filename)
                    attachment_file = con.attachment_path + filename.split('.')[
                        0] + "_" + datetime.date.today().strftime(
                        "%Y%m%d%H%M%S") + "_" + str(
                        poss).rjust(2, '0') + "." + filename.split('.')[1]
                    print(attachment_file)
                    if not os.path.isfile(attachment_file):
                        fp = open(attachment_file, 'wb')
                        print(part.get_payload(decode=True))
                        # fp.write(part.get_payload(decode=True))
                        fp.write(part.get_payload()[1].get_payload(decode=True))
                        fp.close()
                if part.get_content_maintype() == 'text':
                    continue
                if part.get_content_maintype() == 'application':
                    continue

    def download_bingads_files(self):
        # Retrieve a list of messages sent for Raw and Aggregated BingAds data
        date1 = (datetime.date.today()).strftime("%d-%b-%Y")

        result, data = self.mail.uid('search', None,
                                     '(SENTSINCE {date} HEADER FROM "{mailsender}" Subject "Your scheduled report is '
                                     'ready to '
                                     'view")'.format(
                                         date=date1, mailsender=con.bing_mailfolder))

        email_uid = data[0].split()
        print(f'EMAIL_UID:', email_uid)

        poss = 0
        for latest_email_uid in email_uid:
            poss = poss + 1
            # Fetch the email body (RFC822) for the given uid
            result, data = self.mail.uid('fetch', latest_email_uid, '(RFC822)')
            # Raw text of the whole email
            raw_email = data[0][1]
            # Create EmailMessage object from the raw email
            email_message = email.message_from_string(raw_email)
            # Get the payload from email
            self.get_first_text_block(email_message, poss)

    def get_mail(self):
        """
            to get mail details from mailbox
        """
        file_logger.info(f"Getting the mail data")
        subject = "Your Google Ads report is ready: LS Google PPC Report Last 7 days New"
        # invitation_mail_subject = "Invitation to receive email notifications about a Google Ads account"
        mailsender = "ads-account-noreply@google.com"

        # email_message = ""
        search_string = '(SENTSINCE {from_date} HEADER FROM "{mailsender}" Subject "{subject}")'.format(
            from_date=datetime.date.today().strftime("%d-%b-%Y"), mailsender=mailsender,
            subject=subject)

        file_logger.info(f"Search string : {search_string}")
        result_status, search_result_data = self.mail.uid('search', None, search_string)

        file_logger.info(f"Mail UID : {result_status}, {search_result_data}")
        search_result_mail_unique_ids = search_result_data[0].split()
        for mail_unique_id in search_result_mail_unique_ids:
            # Fetch the email body (RFC822) for the given uid
            result, data = self.mail.uid('fetch', mail_unique_id, '(RFC822)')
            # Raw text of the whole email
            raw_email = data[0][1]
            file_logger.info(f"Raw_data : {raw_email}")
            # Create EmailMessage object from the raw email
            email_message = email.message_from_string(raw_email)
            return email_message

    @staticmethod
    def get_document_url(email_message):
        """
        Get document url from mail body where VIEW REPORT link is defined
        """
        maintype = email_message.get_content_maintype()

        if maintype == 'multipart':
            # filename = part.get_filename()
            for part in email_message.walk():
                if part.get_content_maintype() == 'multipart':
                    mail_body_html = part.get_payload()[1].get_payload(decode=True).replace('\r\n', '').replace('  ',
                                                                                                                '')
                    document_url = re.search(r'<!-- CTA OUTLOOK END--><a href=([^ ]+)\s', mail_body_html).group(1)
                    return document_url
                if part.get_content_maintype() == 'text':
                    continue
                if part.get_content_maintype() == 'application':
                    continue

    def download_adwords_report(self, document_url):
        """
        Download the report using wget
        Pls change this code to pure python
        """
        report_name = self.ppc_file_path + "LS_Google_PPC_Report_Last_7_Days_%s.csv" % (
            datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
        os.system("wget %s -O %s" % (document_url, report_name))

    def download_googleads_files(self):
        if self.google:
            ret_email_message = self.get_mail()
            document_url = self.get_document_url(ret_email_message)
            self.download_adwords_report(document_url)

    def download_discoverads_files(self):
        pass

    def upload_file_to_s3(self):
        pass

    def extractloadfile(self):
        try:
            if self.all:
                self.download_xlsx_files()
            elif self.bing:
                self.download_bingads_files()
            elif self.google:
                self.download_googleads_files()
            elif self.discover:
                self.download_discoverads_files()
            self.upload_file_to_s3()
        except Exception as file_load:
            file_except = traceback.format_exc()
            file_except_logger.error(file_except)
            file_except_logger.error(file_load)


if __name__ == "__main__":

    # noinspection PyBroadException
    try:
        file_logger.info(f"Starting the ingestion of files to data-lake")
        File = IngestFile(googleads_flag=1)
        File.extractloadfile()
        file_logger.info(f"Files loaded to the data-lake")

    except Exception as e:
        exception_occ = traceback.format_exc()
        file_except_logger.info(exception_occ)
