
#-------------------- Libraries for Email----------------------------------------------------- 
import smtplib
#--------------------Start code for WhatsApp----------------------------------------------------- 
from twilio.rest import Client

def send_whatsapp_notification(sender, to_number, message, account_sid, auth_token):
    client = Client(account_sid, auth_token)
    message=f'Hey there! \nYou have been contacted through IAS group 5.\nYou have received a message from {sender}. \nThe message is{message}'
    message = client.messages.create( 
        body=message,
        from_='whatsapp:+14155238886',
        to=f'whatsapp:{to_number}'
    )

    print(f"WhatsApp notification sent to {to_number}, Message SID: {message.sid}")

# If you wish to change the 'to_number', make sure you send 'join appearance-stove' to '+14155238886'
to_number = '+917506636566'
message = "The message is from Fault Tolerance module. There is a problem with node sid_vm!"
senderName='Sanya Gandhi'

# Account has been already created. So, use the following
account_sid = "ACef0a0280e62b5f9afeb8707d73b3053d"
auth_token = "66ec1a3be77bfc9446a246839b4f7ac0"


# For testing purposes
send_whatsapp_notification(senderName ,to_number, message, account_sid, auth_token)

#------------------------End code for WhatsApp-----------------

 
def send_mail(sender_email, sender_password, receiver_email, cc=None, bcc=None, sub="hi ", body="ignore me"):
    # Broadcasting message to all if it is a list
    if isinstance(receiver_email, list):
        for receiver in receiver_email:
            # Send email to each receiver
            send(sender_email,sender_password, receiver, cc, bcc,sub, body)
    else:
        # Send email to single receiver
        send(sender_email, sender_password, receiver_email, cc, bcc,sub, body)


def send(sender_email, sender_password, receiver_email, cc=None, bcc=None, sub="hi ", body="ignore me"):
    message = f"From: {sender_email}\nTo: {receiver_email}\n"
    if cc:
        if isinstance(cc, list):
            message += f"Cc: {','.join(cc)}\n"
        else:
            message += f"Cc: {cc}\n"
    if bcc:
        if isinstance(bcc, list):
            message += f"Bcc: {','.join(bcc)}\n"
        else:
            message += f"Bcc: {bcc}\n"
    message += f"Subject: {sub}\n\n{body}"
    try:
        server = smtplib.SMTP('smtp.gmail.com')
        # start TLS
        server.starttls()
        try:
            server.login(sender_email, password)
            # print('Login successful to server')
        except Exception as e:
            print(f"Error: {str(e)}")
            print("Failed to login. Please check your credentials and try again.")
            print("Generate less secure key, Check it out https://support.google.com/mail/answer/185833?hl=en")
            return

        try:
            # Send the email
            to_list = [receiver_email]
            if cc:
                if isinstance(cc, list):
                    to_list += cc
                else:
                    to_list.append(cc)
            if bcc:
                if isinstance(bcc, list):
                    to_list += bcc
                else:
                    to_list.append(bcc)
            result = server.sendmail(sender_email, to_list, message)
            if not result:
                print(f'message sent successfully to {receiver_email}')
            else:
                print(f'Failed to send email to {result.keys()}')

        except Exception as e:
            print(f"Error: {str(e)}")
            print("Failed to send email. Please check the email addresses provided and try again.")
            return
        finally:
            # close the connection
            server.quit()
    except Exception as e:
        print(f"Error {str(e)}")
        print("Failed to connect to SMTP. Please check internet connectivity")
