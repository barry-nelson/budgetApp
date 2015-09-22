from flask import Flask, request, redirect
import twilio.twiml
import MySQLdb
import BAfunctions as ba
import re

app = Flask(__name__)


@app.route("/", methods=['GET','POST'])
# def hello_there():
#     return 'Hello World! Friggin Idiots!'

def hello_monkey():
    body = request.values.get('Body', None)
    from_ = request.values .get('From', None)
    from_ = from_.strip('+')
    messageSID = request.values.get('MessageSid', None)

    if str(body).lower().strip('?') in ('get all balances', 'get all budget balances', 'all', 'get balances', 'get budget balances', 'how much is left in all budgets'):  #get budget balances for all budgets
        respString = ba.getBalance()
    elif ' ' not in body and str(body).lower() not in ('all'):  #single budget balance retrieval
        respString = ba.getBalance(str(body).lower())
    elif str(body).lower()[:16] == 'get balances for':
        respString = ba.getBalance('multiple', body)
    elif str(body).lower().strip('?') in ('what are the names', 'what are the budget names', 'budget names', 'budget names all', 'budget names fixed'):
        if str(body).lower().strip('?') == 'budget names all':
            respString = ba.getBudgetNames('all')
        elif str(body).lower().strip('?') == 'budget names fixed':
            respString = ba.getBudgetNames('fixed')
        else:
            respString = ba.getBudgetNames()
    elif str(body).lower() in ('help please', 'what can I say'):
        respString = 'You can enter a purchase, make an amendment (forgot to enter a purchase), or get current balances.\n' \
                     'Type help purchase or help amend or help balance or help budget names (if you forgot the exact name)'
    elif str(body).lower() == 'help purchase':
        respString = 'To enter a purchase type the name of the budget, the amount and notes (if any) related to the purchase.\n ' \
                     'Ex: food 12.36 McDonalds coke, gas 35.65.\n ' \
                     'Separate purchases with a comma, do not use a comma within the same purchase.\n'
    elif str(body).lower() in ('help budget names', 'help names'):
        respString = 'To get the names of the budgets, type "budget names" or simply ask "what are the budget names" without the quotes'

    elif re.search('(get|delete) last (""|\d{1,2}) (transactio(n|ns)|purchas(e|es)|recor(d|ds))', str(body).lower()):
        respString = ba.userLastInput(str(body).lower(), from_)

    else:
        respString = ba.inputTransaction(body, from_, messageSID)

    resp = twilio.twiml.Response()
    resp.message(respString)
    return str(resp)

if __name__ == "__main__":
    app.run()
