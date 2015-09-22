import MySQLdb
import twilio.twiml
import datetime
import re

class cnxn(object):
    sql_purchase = """ insert into budget.purchase (date, Budget_Name, amount, type, user, notes, message_sid)
                      values ({5}, '{0}', {1}, '{6}', '{2}', "{3}", '{4}');  """
    sql_budgetTotal = """ select round(beg_balance, 2) from budget.vw_balances where budget_name = '{0}'
                                  and date = {1}  """
    sql_budgetBalance = """ select round(end_or_cur_balance, 2) from budget.vw_balances where budget_name = '{0}' and date = {1};  """

    sql_budgetSpent = """ select round(mon_pur_amount, 2) from budget.vw_balances where budget_name = '{0}' and date = {1};  """

    sql_budgetBalancePercent = """ select round((end_or_cur_balance/beg_balance)*100,0) from budget.vw_balances where budget_name = '{0}' and date = {1}; """

    sql_budgetSpentPercent = """ select round(((mon_pur_amount)/beg_balance)*100,0) from budget.vw_balances where budget_name = '{0}' and date = {1}; """

    def __init__(self):
        self.conn = MySQLdb.connect(host=host, port=port, user=user, passwd=pwd, db=db)
        self.cur = self.conn.cursor()

    def execute(self, query):
        return self.cur.execute(query)

    def fetchone(self):
        return self.cur.fetchone()

    def fetchall(self):
        return self.cur.fetchall()

    def commit(self):
        self.conn.commit()

    def getbal(self):
        li = []
        for (budgetName, budgetAmount) in self.cur:
           li.append({'budgetName': budgetName, 'budgetBalance': budgetAmount})
        return li

    def month_perc_left(self):
        self.cur.execute(""" select day(now())  """)
        cur_mo_day = self.cur.fetchone()
        cur_mo_day = ", ".join([str(x) for x in cur_mo_day])
        self.cur.execute(""" select day(now() + interval 1 month - interval day(now() + interval 1 month) day)  """)
        mo_end_day = self.fetchone()
        mo_end_day = ", ".join([str(x) for x in mo_end_day])
        perc_left = int(round(((float(mo_end_day) - float(cur_mo_day))/float(mo_end_day))*100,0))
        return perc_left

    def get_user_last_trans(self):
        li = []
        for (budgetName, budgetAmount, budgetDate, budgetNotes) in self.cur:
            li.append({'budgetName': budgetName, 'budgetAmount': budgetAmount, 'budgetDate': budgetDate, 'budgetNotes': budgetNotes})
        return li

    def __del__(self):
        self.conn.close()

def monthLookUp(mon):
    if mon == 'jan': num = '01'
    elif mon == 'feb': num = '02'
    elif mon == 'mar': num = '03'
    elif mon == 'apr': num = '04'
    elif mon == 'may': num = '05'
    elif mon == 'jun': num = '06'
    elif mon == 'jul': num = '07'
    elif mon == 'aug': num = '08'
    elif mon == 'sep': num = '09'
    elif mon == 'oct': num = '10'
    elif mon == 'nov': num = '11'
    elif mon == 'dec': num = '12'

    if int(num) > int(datetime.datetime.now().strftime('%m')):
        yr = str(int(datetime.datetime.now().strftime('%Y')) - 1)
    else: yr = datetime.datetime.now().strftime('%Y')

    dateStr = "cast(last_day('" + yr + "-" + num + "-01') as datetime)"
    return dateStr
#print monthLookUp('jan')

def inputTransaction(body, from_, messageSID):
    chopitup = body.split(',')
    resultsLi = []
    prob = None
    balDate = '(select max(date) from budget.vw_balances)'
    for x in chopitup:
        inputVar = x.strip().split()
        numOfInputVars = len(inputVar)
        notesList = []
        if inputVar[0] in ('amend', 'amendment'):
            amendNotification = '[AMEND]-'
            purType = 'amendment'
            budgetCat = inputVar[1].lower
            budgetAmt = inputVar[2]
            dateCue = inputVar[3].lower()
            if dateCue == 'default':
                transDate = 'cast(last_day(now() - interval 1 month) as datetime)'
            elif dateCue[:3] in ('jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'):
                mon_ = dateCue[:3]
                transDate = monthLookUp(mon_)
            else:
                prob = "Oops! Don't forget to include the month, or just type default if its for last month. EX: amend food 99.99 default"
                break

            if numOfInputVars > 4:
                for i in xrange(numOfInputVars):
                    if i > 3:
                        notesList.append(inputVar[i])
                notes = " ".join([str(z) for z in notesList])
            else:
                notes = None
        else:
            amendNotification = ''
            purType = 'purchase'
            budgetCat = inputVar[0]
            budgetAmt = inputVar[1]
            transDate = 'now()'
            if numOfInputVars > 2:
                for i in xrange(numOfInputVars):
                    if i > 1:
                        notesList.append(inputVar[i])
                notes = " ".join([str(z) for z in notesList])
            else:
                notes = None

        if prob != None:
            break
        else:
            cur = cnxn()
            cur.execute(cur.sql_purchase.format(str(budgetCat).lower(), float(budgetAmt), str(from_), str(notes), str(messageSID), transDate, purType))
            cur.commit()

            cur.execute(cur.sql_budgetBalance.format(str(budgetCat).lower(), balDate))
            budBalance = cur.fetchone()
            budBalance = ", ".join([str(x) for x in budBalance])

            cur.execute(cur.sql_budgetTotal.format(str(budgetCat).lower(), balDate))
            budTotal = cur.fetchone()
            budTotal = ", ".join([str(x) for x in budTotal])

            cur.execute(cur.sql_budgetBalancePercent.format(str(budgetCat).lower(), balDate))
            budBalPercent = cur.fetchone()
            budBalPercent = ", ".join([str(int(x)) + '%' for x in budBalPercent])

            cur.execute(cur.sql_budgetSpent.format(str(budgetCat).lower(), balDate))
            budSpent = cur.fetchone()
            budSpent = ", ".join([str(x) for x in budSpent])

            cur.execute(cur.sql_budgetSpentPercent.format(str(budgetCat).lower(), balDate))
            budSpentPercent = cur.fetchone()
            budSpentPercent = ", ".join([str(int(x)) + '%' for x in budSpentPercent])

            resultsLi.append({'budgetCategory': str(budgetCat).upper(), 'budgetBalance': budBalance, 'budgetTotal': int(float(budTotal)), 'budgetPercent': budBalPercent, 'budgetSpent': budSpent, 'budgetSpentPercent': budSpentPercent, 'amendNotification': amendNotification})
    if prob == None:
        respString = ''
        for i in resultsLi:
            respString = respString + '{6}{1}: Bal- ${0} ({2}), Spent- ${4} ({5}) out of ${3}\n'
            respString = respString.format(i['budgetBalance'], i['budgetCategory'], i['budgetPercent'], i['budgetTotal'], i['budgetSpent'], i['budgetSpentPercent'], i['amendNotification'])
        moInfoStr = ' for the month. Only ' + str(cur.month_perc_left()) + '% of the month remains '
        respString = respString + moInfoStr
    else: respString = prob

    #respString = 'Got it! Thanks sweetie.'
    return respString

#print inputTransaction("food 9.99 delete g's", 'testBarry', 'testAgain')

def getBalance(budgetCat='all',body=None):
    cur = cnxn()
    respString = ''
    if budgetCat == 'all':
        sql_budgetTotalAll = """ select a.budget_name, round(end_or_cur_balance, 2) from budget.vw_balances a join budget.dim_monthly_budgets b
                                   on a.budget_name = b.budget_name where b.fixed_budget_flag = 0 and a.date = (select max(date) from budget.vw_balances) """
        cur.execute(sql_budgetTotalAll)
        budgetTotalAllLi = cur.getbal()
        for x in budgetTotalAllLi:
            respString = respString + x['budgetName'] + ' - ' + '$' + str(x['budgetBalance']) + '\n'
        return respString
    elif budgetCat != 'all' and budgetCat != 'multiple':
        sql_singleBalance = """ select budget_name, round(end_or_cur_balance, 2) from budget.vw_balances where budget_name = '{0}' and date = (select max(date) from budget.vw_balances)  """
        cur.execute(sql_singleBalance.format(str(budgetCat).lower()))
        x = cur.getbal()
        respString = respString + x[0]['budgetName'] + ' - $' + str(x[0]['budgetBalance'])
        return respString
    else:
        leftNum = len(str(body).lower()) - 17
        rawList = str(body).lower()[-leftNum:]
        chopitup = rawList.split(',')
        listLen = len(chopitup)
        multiBudget = ''
        counter = 1
        for x in chopitup:
            if counter < listLen:
                multiBudget = multiBudget + "'" + x.strip() + "', "
                counter += 1
            else:
                multiBudget = multiBudget + "'" + x.strip() + "'"
        sql_multiple = """ select budget_name, round(end_or_cur_balance, 2) from budget.vw_balances where budget_name in ({0}) and date = (select max(date) from budget.vw_balances)  """
        cur.execute(sql_multiple.format(multiBudget))
        multipleLi = cur.getbal()
        for x in multipleLi:
            respString = respString + x['budgetName'] + ' - ' + '$' + str(x['budgetBalance']) + '\n'
        return respString

#print getBalance('multiple', 'food, gifts, pets')

def getBudgetNames(keyword=None):
    cur = cnxn()
    if keyword == None:
        sql = """ select budget_name from budget.dim_monthly_budgets
                    where fixed_budget_flag = 0 """
        cur.execute(sql)
        names = cur.fetchall()
        names = ", ".join([str(y) for x in names for y in x])
        return names
    elif keyword == 'all':
        sql = """ select budget_name from budget.dim_monthly_budgets  """
        cur.execute(sql)
        allNames = cur.fetchall()
        allNames = ", ".join([str(y) for x in allNames for y in x])
        return allNames
    elif keyword == 'fixed':
        sql = """ select budget_name from budget.dim_monthly_budgets
                    where fixed_budget_flag = 1 """
        cur.execute(sql)
        fixedNames = cur.fetchall()
        fixedNames = ", ".join([str(y) for x in fixedNames for y in x])
        return fixedNames
#print getBudgetNames()

def userLastInput(body, from_):
    respString = ''
    inputSplit = body.split()
    sql = """ select budget_name, amount, date, notes from budget.purchase where user = '{0}' order by id desc LIMIT {1}  """

    if body in ('get last transaction', 'get last purchase', 'delete last transaction', 'delete last purchase'):
        numOfTransactions = 1
    else:
        numOfTransactions = int(inputSplit[2])

    cur = cnxn()
    cur.execute(sql.format(str(from_), numOfTransactions))
    userLastTransLi = cur.get_user_last_trans()
    for x in userLastTransLi:
        respString = respString + x['budgetName'] + ' ' + str(x['budgetAmount']) + ' ' + str(x['budgetDate']) + ' ' + x['budgetNotes'] + '\n'

    if inputSplit[0] == 'delete':
        sql_delete = """ delete from budget.purchase where user = '{0}' order by id desc LIMIT {1}  """
        cur = cnxn()
        cur.execute(sql_delete.format(str(from_), numOfTransactions))
        cur.commit()
        respString = '[DELETED]- ' + respString

    return respString
#print userLastInput('get last 2 transactions', '13852527411')

def transfer(body, from_, messageSID, dateCue=None):   #transfer 50 from food to house aug notes go here
    sql_transfer1 = """ insert into budget.purchase (date, Budget_Name, amount, type, user, notes, message_sid)
                       values ({0}, '{1}', {2}, '{3}', '{4}', "{5}", '{6}'); """

    sql_transfer2 = """ insert into budget.purchase (date, Budget_Name, amount, type, user, notes, message_sid)
                      values ({0}, '{7}', -{2}, '{3}', '{4}', "{5}", '{6}'); """
    chopitup = body.split(',')
    numOfInputs = len(chopitup)
    transType = 'transfer'
    lastLi = []
    respString = ''
    counter = 1

    for x in chopitup:
        inputVar = x.strip().split()
        numOfInputVars = len(inputVar)
        notesList = []
        transferFrom = inputVar[3].lower()
        transferTo = inputVar[5].lower()
        transferAmount = inputVar[1]
        dateCue = inputVar[6].lower()
        budgetCatLi = []
        budgetCatLi.append(transferFrom)
        budgetCatLi.append(transferTo)
        resultsLi = []
        balDate = '(select max(date) from budget.vw_balances)'
        transferNotification = '[TRANSFERRED]- ${0} from {1} to {2}\n'

        if dateCue in ('default', 'current', 'now', 'today'):
            transDate = 'now()'
        elif dateCue[:3] in ('jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'):
            mon_ = dateCue[:3]
            transDate = monthLookUp(mon_)

        if numOfInputVars > 7:
            for i in xrange(numOfInputVars):
                if i > 6:
                    notesList.append(inputVar[i])
                    notes = " ".join([str(z) for z in notesList])
        else:
            notes = None

        cur = cnxn()
        cur.execute(sql_transfer1.format(transDate, transferFrom, float(transferAmount), transType, from_, notes, messageSID, transferTo))
        cur.commit()

        cur.execute(sql_transfer2.format(transDate, transferFrom, float(transferAmount), transType, from_, notes, messageSID, transferTo))
        cur.commit()

        for budgetCat in budgetCatLi:
            cur.execute(cur.sql_budgetBalance.format(str(budgetCat).lower(), balDate))
            budBalance = cur.fetchone()
            budBalance = ", ".join([str(x) for x in budBalance])

            cur.execute(cur.sql_budgetTotal.format(str(budgetCat).lower(), balDate))
            budTotal = cur.fetchone()
            budTotal = ", ".join([str(x) for x in budTotal])

            cur.execute(cur.sql_budgetBalancePercent.format(str(budgetCat).lower(), balDate))
            budBalPercent = cur.fetchone()
            budBalPercent = ", ".join([str(int(x)) + '%' for x in budBalPercent])

            cur.execute(cur.sql_budgetSpent.format(str(budgetCat).lower(), balDate))
            budSpent = cur.fetchone()
            budSpent = ", ".join([str(x) for x in budSpent])

            cur.execute(cur.sql_budgetSpentPercent.format(str(budgetCat).lower(), balDate))
            budSpentPercent = cur.fetchone()
            budSpentPercent = ", ".join([str(int(x)) + '%' for x in budSpentPercent])

            resultsLi.append({'budgetCategory': str(budgetCat).upper(), 'budgetBalance': budBalance, 'budgetTotal': int(float(budTotal)), 'budgetPercent': budBalPercent, 'budgetSpent': budSpent, 'budgetSpentPercent': budSpentPercent})
        chopString = ''
        for i in resultsLi:
            chopString = chopString + '{1}: Bal- ${0} ({2}), Spent- ${4} ({5}) out of ${3}\n'
            chopString = chopString.format(i['budgetBalance'], i['budgetCategory'], i['budgetPercent'], i['budgetTotal'], i['budgetSpent'], i['budgetSpentPercent'])
        chopString = transferNotification.format(str(transferAmount), transferFrom, transferTo) + chopString
        if counter < numOfInputs:
            chopString = chopString + '\n'
        lastLi.append(chopString)
        counter += 1
    for j in lastLi:
        respString = respString + j
    moInfoStr = 'for the month. Only ' + str(cur.month_perc_left()) + '% of the month remains'
    respString = respString + moInfoStr

    return respString

#print transfer('transfer 50 from food to house aug notes go here, transfer 15 from baby to gifts sep notes notes notes', 'test', 'test')
