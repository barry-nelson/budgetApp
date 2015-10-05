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
    body = body.strip('$').strip('?')
    from_ = request.values.get('From', None)
    from_ = from_.strip('+')
    messageSID = request.values.get('MessageSid', None)
    bodyLi = ['amend','amendment']
    for x in body.strip(',').split():
        bodyLi.append(x)

    #get budget balances for all budgets
    if body in ('get all balances', 'get all budget balances', 'get balances', 'get budget balances', 'how much is left in all budgets'):
        respString = ba.getBalance()

    #single budget balance retrieval
    elif ' ' not in body:
        respString = ba.getBalance(body)

    #input purchases
    elif bodyLi[0] in ba.getBudgetNames(None,'list'):
        respString = ba.inputTransaction(body, from_, messageSID)

    #get balances for multiple budgets
    elif body[:16] == 'get balances for':
        respString = ba.getBalance('multiple', body)

    #get a list of budget names, in case you forgot
    elif body in ('what are the names', 'what are the budget names', 'budget names', 'budget names all', 'budget names fixed'):
        if body == 'budget names all':
            respString = ba.getBudgetNames('all')
        elif body == 'budget names fixed':
            respString = ba.getBudgetNames('fixed')
        else:
            respString = ba.getBudgetNames()

    #help
    elif 'help' in body:
        respString = ba.help(body)

    #get or delete recent transactions
    elif re.search('(get|delete) last (""|\d{1,2}) (transactio(n|ns)|purchas(e|es)|recor(d|ds))', body):
        respString = ba.userLastInput(body, from_)

    #transfer money from 1 budget to another
    elif 'transfer' in body:
        respString = ba.transfer(body,from_,messageSID)

    elif re.search('(allowance|total)', body):
        respString = ba.getAllowances(body)

    else:
        respString = 'Houston, we have a problem'

    resp = twilio.twiml.Response()
    resp.message(respString)
    return str(resp)

if __name__ == "__main__":
    app.run()
