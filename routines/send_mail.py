import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate


def send_mail(params):

    assert isinstance(params.get('send_to'), list)

    msg = MIMEMultipart()
    msg['From'] = params.get('send_from')
    msg['To'] = COMMASPACE.join(params.get('send_to'))
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = params.get('subject')

    msg.attach(MIMEText(params.get('text')))

    for f in params.get('files') or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
            part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
            msg.attach(part)


    smtp = smtplib.SMTP(params.get('server'),25)
    smtp.login(params.get('username'),params.get('password'))
    smtp.sendmail(params.get('send_from'), params.get('send_to'), msg.as_string())
    smtp.close()