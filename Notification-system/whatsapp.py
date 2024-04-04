from twilio.rest import Client

def send_whatsapp_notification(to_number, message, account_sid, auth_token):
    client = Client(account_sid, auth_token)

    message = client.messages.create( 
        body=message,
        from_='whatsapp:+14155238886',
        to=f'whatsapp:{to_number}'
    )

    print(f"WhatsApp notification sent to {to_number}, Message SID: {message.sid}")

to_number = '+917021794108'
message = "Hello, this is a notification message!"

account_sid = "ACef0a0280e62b5f9afeb8707d73b3053d"
auth_token = "66ec1a3be77bfc9446a246839b4f7ac0"

send_whatsapp_notification(to_number, message, account_sid, auth_token)
