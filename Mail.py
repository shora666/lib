import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.application import MIMEApplication
from email.utils import formatdate
from email import encoders
from base64 import encodebytes

class Mail:

        def __init__(self , name,frm,to,cc,subject):
            self.name = name
            self.cc=cc;
            self.to=to
            self.frm=frm 
            self.subject=subject
            self.mailhost='localhost'
        def set_To (self,to):
            self.to = to
        def set_From (self, frm):
            self.frm=frm
        def set_Cc (self,cc):
            self.cc=cc
        def set_subject (self,subject):
            self.subject = subject
        def set_body (self,body):
            self.body = body
        def set_content_file (self,textfile):
            self.contentfile = textfile 
        def set_mailhost (self, mailhost):
            self.mailhost = mailhost

        def send_text_email_from_file (self):
            with open(self.contentfile) as fp:
                # Create a text/plain message
                msg = MIMEText(fp.read())

        def send_text_email (self):
            self.msg = MIMEText(self.body)
            self.msg['Subject'] = self.subject
            self.msg['From'] = self.frm
            self.msg['To'] = self.to
            if self.cc is not None:
                self.msg['Cc'] = self.cc
            s = smtplib.SMTP('localhost')
            s.send_message(self.msg)
            s.quit()

        def send_attach_email (self,files_to_attach):
            self.msg = MIMEMultipart('mixed')
            self.msg['From'] = self.frm
            self.msg['To'] = self.to
            self.msg['Date'] = formatdate(localtime = True)
            if self.cc is not None:
                self.msg['Cc'] = self.cc
            self.msg['Subject'] = self.subject
            self.msg.attach(MIMEText(self.body))
            for file_path in files_to_attach:
                file_base_name=os.path.basename(file_path)
                #part = MIMEBase('application', "octet-stream",Name=file_base_name)
                part = MIMEBase('application', "vnd.ms-excel")
                part.set_payload(open(file_path, "rb").read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', 'attachment', filename=file_base_name)
                self.msg.attach(part)

            #context = ssl.SSLContext(ssl.PROTOCOL_SSLv3)
            #SSL connection only working on Python 3+
            s = smtplib.SMTP('localhost')
            #print(self.msg)
            s.send_message(self.msg)
            s.quit()
