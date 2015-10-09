import pymysql as db
import twilio.twiml
import datetime
import re
import BAsecrets as key
import pytz

class cnxn(object):
    sql_purchase = """ insert into budget.purchase (date, Budget_Name, amount, type, user, notes, message_sid)
                      values ({5}, '{0}', {1}, '{6}', '{2}', "{3}", '{4}');  """
    sql_budgetTotal = """ select round(beg_balance, 2) from budget.vw_balances where budget_name = '{0}'
                                  and date = {1}  """
    sql_budgetBalance = """ select end_or_cur_balance from budget.vw_balances where budget_name = '{0}' and date = {1};  """

    sql_budgetSpent = """ select mon_pur_amount from budget.vw_balances where budget_name = '{0}' and date = {1};  """

    sql_budgetBalancePercent = """ select round(((end_or_cur_balance*100)/(beg_balance*100))*100) from budget.vw_balances where budget_name = '{0}' and date = {1}; """

    sql_budgetSpentPercent = """ select round(((mon_pur_amount*100)/(beg_balance*100))*100) from budget.vw_balances where budget_name = '{0}' and date = {1}; """

    def __init__(self):
        self.conn = db.connect(host=key.host, port=key.port, user=key.user, passwd=key.pwd, db=key.db)
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

    def getbalTotal(self):
        li = []
        for (begBalTotal, curBalTotal, percBalTotal) in self.cur:
           li.append({'begBalTotal': begBalTotal, 'curBalTotal': curBalTotal, 'percBalTotal': percBalTotal})
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

abrevMonLi = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
fullMonLi = ['january','february','march','april','may','june','july','august','september','october','november','december']
curMonthNum = int(datetime.datetime.now(pytz.timezone('US/Mountain')).strftime('%m'))
curYear = int(datetime.datetime.now(pytz.timezone('US/Mountain')).strftime('%Y'))

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

def monthLookUp2(mon):
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
    return num

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
            elif dateCue[:3] in abrevMonLi:
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

            resultsLi.append({'budgetCategory': str(budgetCat).upper(), 'budgetBalance': budBalance, 'budgetTotal': '{0:.2f}'.format(float(budTotal)), 'budgetPercent': budBalPercent, 'budgetSpent': budSpent, 'budgetSpentPercent': budSpentPercent, 'amendNotification': amendNotification})
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

#print inputTransaction("barry .99 delete g's", 'testBarry', 'testAgain')

def getBalance(budgetCat='all',body=None):
    cur = cnxn()
    respString = ''
    if budgetCat == 'all':
        sql_budgetTotalAll = """ select a.budget_name, end_or_cur_balance from budget.vw_balances a join budget.dim_monthly_budgets b
                                   on a.budget_name = b.budget_name where b.fixed_budget_flag = 0 and a.date = (select max(date) from budget.vw_balances) """
        cur.execute(sql_budgetTotalAll)
        budgetTotalAllLi = cur.getbal()
        for x in budgetTotalAllLi:
            respString = respString + x['budgetName'] + ' - ' + '$' + str(x['budgetBalance']) + '\n'
        return respString
    if budgetCat == 'total':
        sql_budgetTotalTotal = """ select sum(beg_balance), sum(end_or_cur_balance), round(((sum(end_or_cur_balance)*100)/(sum(beg_balance)*100))*100)
                                    from budget.vw_balances a join budget.dim_monthly_budgets b on a.budget_name = b.budget_name
                                    where b.fixed_budget_flag = 0 and a.date = (select max(date) from budget.vw_balances) """
        cur.execute(sql_budgetTotalTotal)
        budgetTotalTotalLi = cur.getbalTotal()
        for x in budgetTotalTotalLi:
            respString = 'Beg balance - $' + str(x['begBalTotal']) + '\n' + "Current balance - $" + str(x['curBalTotal']) + '\n' + "Budget left - " + str(x['percBalTotal']) + '%'
        return respString
    elif budgetCat not in ('all','multiple'):
        sql_singleBalance = """ select budget_name, end_or_cur_balance from budget.vw_balances where budget_name = '{0}' and date = (select max(date) from budget.vw_balances)  """
        cur.execute(sql_singleBalance.format(str(budgetCat).lower()))
        x = cur.getbal()
        respString = respString + x[0]['budgetName'] + ' - $' + str(x[0]['budgetBalance'])
        return respString
    else:
        leftNum = len(body) - 17
        rawList = body[-leftNum:]
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
        sql_multiple = """ select budget_name, end_or_cur_balance from budget.vw_balances where budget_name in ({0}) and date = (select max(date) from budget.vw_balances)  """
        cur.execute(sql_multiple.format(multiBudget))
        multipleLi = cur.getbal()
        for x in multipleLi:
            respString = respString + x['budgetName'] + ' - ' + '$' + str(x['budgetBalance']) + '\n'
        return respString

#print getBalance('total', 'get total balance')

def getBudgetNames(keyword=None, list=None):
    cur = cnxn()
    if keyword == None:
        sql = """ select budget_name from budget.dim_monthly_budgets
                    where fixed_budget_flag = 0 """
        cur.execute(sql)
        names = cur.fetchall()
        names = ", ".join([str(y).lower() for x in names for y in x])
        if list == 'list':
            li = names.split(', ')
            return li
        else:
            return names
    elif keyword == 'all':
        sql = """ select budget_name from budget.dim_monthly_budgets  """
        cur.execute(sql)
        allNames = cur.fetchall()
        allNames = ", ".join([str(y).lower() for x in allNames for y in x])
        if list == 'list':
            li = allNames.split(', ')
            return li
        else:
            return allNames
    elif keyword == 'fixed':
        sql = """ select budget_name from budget.dim_monthly_budgets
                    where fixed_budget_flag = 1 """
        cur.execute(sql)
        fixedNames = cur.fetchall()
        fixedNames = ", ".join([str(y).lower() for x in fixedNames for y in x])
        return fixedNames

#print getBudgetNames('all','list')

def whereDateIn(monList):
    sqlInStr = ''
    liLen = len(monList)
    cntr = 1
    for x in monList:
        if cntr != liLen:
            sqlInStr = sqlInStr + "'{0}-{1}-01 00:00:00',".format(str(curYear), x)
        else:
            sqlInStr = sqlInStr + "'{0}-{1}-01 00:00:00'".format(str(curYear), x)
        cntr += 1
    return sqlInStr

def whereBudgetNameIn(budgetNameLi):
    sqlInStr = ''
    liLen = len(budgetNameLi)
    cntr = 1
    for x in budgetNameLi:
        if cntr != liLen:
            sqlInStr = sqlInStr + "'{0}',".format(x)
        else:
            sqlInStr = sqlInStr + "'{0}'".format(x)
        cntr += 1
    sqlInStr = 'and a.budget_name in (' + sqlInStr + ')'
    return sqlInStr
#print whereBudgetNameIn(['food','naomi','house'])

def monthExtract(body):
    monLi = []
    for x in abrevMonLi:
        if x in body:
            monLi.append(x)
    return monLi
#print monthExtract('something mar, may and feb blah for aurg')

def budgetNameExtract(body):
    bodyWordLi = body.replace(',','').lower().split()
    budgetNames = list(set(bodyWordLi).intersection(getBudgetNames('all','list')))
    return budgetNames
#print budgetNameExtract('blah something food, medical, poop')

def getAllowances(body):  #### For more than 1 budget, only 1 month. For 1 budget, max 6 months.
    cur = cnxn()
    monNumLi = []
    sql = """ select {3}, amount from budget.monthly_budgets_history a
                             left join budget.dim_monthly_budgets b on a.budget_name = b.budget_name
                             where date in ({0}) and b.fixed_budget_flag = {1} {2} order by {4}; """

#---Date range  ---------------------------------------------------------------------------------------------
    if re.search('this month', body):
        monNumLi.append(str(curMonthNum).zfill(2))
        monNameLi = ['this month']
    elif re.search('last month', body):
        monNumLi.append(str(curMonthNum-1).zfill(2))
        monNameLi = ['last month']
    elif len(monthExtract(body)) == 1:
        monNameLi = monthExtract(body)
        monNumLi.append(monthLookUp2(monNameLi[0]))
    elif len(monthExtract(body)) > 1:
        monNameLi = monthExtract(body)
        [monNumLi.append(monthLookUp2(x)) for x in monNameLi]
    else:
        monNumLi.append(str(curMonthNum).zfill(2))
        monNameLi = ['this month']

#---Is it a fixed budget?  ------------------------------------------------------------------------------------
    if 'fixed' in body:
        fixedFlag = 1
    else:
        fixedFlag = 0

#---Which budgets?  -------------------------------------------------------------------------------------------
    if re.search('(get |)(all | |)(budget total|allowance)', body) and len(monthExtract(body)) < 2:
        whichBudgets = ''
        respLi = monNameLi
        field = 'a.budget_name'
        orderBy = field
    elif len(budgetNameExtract(body)) > 1 and len(monthExtract(body)) < 2:
        whichBudgets = whereBudgetNameIn(budgetNameExtract(body))
        respLi = monNameLi
        field = 'a.budget_name'
        orderBy = field
    elif len(budgetNameExtract(body)) == 1 and len(monthExtract(body)) < 13:
        whichBudgets = whereBudgetNameIn(budgetNameExtract(body))
        respLi = budgetNameExtract(body)
        field = "date_format(date, '%b')"
        orderBy = 'date desc'
    else:
        respString = "Sorry, can't do it, try again. Only 1 budget for multiple months and 1 month for multiple budgets."
        return respString

    dateVar = whereDateIn(monNumLi)
    cur.execute(sql.format(dateVar, fixedFlag, whichBudgets, field, orderBy))
    results = cur.fetchall()
    for x in respLi:
        respString = 'Budget totals for {0}\n'.format(x.capitalize()) + '\n'.join(budgetName + ' - $' + str(amount) for budgetName, amount in results)

    return respString

#print getAllowances('get food and barry totals for oct, sept and aug')
    #this is where i left off   --need to add logic for other months besides this and last month including whether that mon is this year or last (think Jan)
    #--also need to add support for specific budgets only
    #--also, multi-month support for single budgets

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
        sql_delete_write = """ insert into budget.delete select * from budget.purchase where user = '{0}' order by id desc LIMIT {1}  """
        sql_delete = """ delete from budget.purchase where user = '{0}' order by id desc LIMIT {1}  """
        cur = cnxn()
        cur.execute(sql_delete_write.format(str(from_), numOfTransactions))
        cur.execute(sql_delete.format(str(from_), numOfTransactions))
        cur.commit()

        respString = '[DELETED]- ' + respString

    return respString
#inputTransaction('barry .98 delete, barry .97 delete','test','delete')
#userLastInput('delete last 2 transactions', 'test')

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
        elif dateCue[:3] in abrevMonLi:
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

def help(body):
    if body in ('help please','what can i say','what can i ask'):
        respString = 'You can enter a purchase, make an amendment (forgot to enter a purchase), get current balances, delete purchases, get prev purchases, or transfer $ to another budget.\n' \
                     'Type help followed by 1 of the following keywords: purchase, amend, balance, delete, last purchases, budget names, or transfer. Ex: help balance'
    elif re.search('help purchas(e|es)',body):
        respString = 'To enter a purchase type the name of the budget, the amount and notes (if any) related to the purchase.\n ' \
                     'Ex: food 12.36 McDonalds coke, gas 35.65.\n ' \
                     'Separate purchases with a comma, do not use a comma within the same purchase.\n'
    elif re.search('help(| budget) nam(e|es)', body):
        respString = 'To get the names of the budgets, type "budget names" or simply ask "what are the budget names" without the quotes'
    elif re.search('help amen(d|dment)',body):
        respString = 'To amend (or add a purchase after the fact) type amend, budget name, amount, month (abbrev is fine)(type default for last month) and any notes.\n' \
                     'Ex: amend misc 50 default I forgot to add this Dr Appt  -This will add a purchase of 50 to the misc budget applied to last month with the notes at the end.' \
                     'You could also type amend misc 50 aug I forgot blah blah blah. Assuming august was last month, this will do the same thing.'
    elif re.search('help(| get) balanc(e|es)', body):
        respString = 'For all balances, type get all balances. To get a single balance simply type the name of the budget. To get multiple balances, type get balances for' \
                     ' (list budget names here with commas).  Ex: get balances for food, barry, misc, house'
    elif re.search('help(| get) (pre(v|vious)|last) (transactio(n|ns)|purchas(e|es)|recor(d|ds))',body):
        respString = 'To get a list of the prev purchases you input, type get last (number) purchases.\n' \
                     'Ex: get last 1 purchase or get last 5 transactions.'
    elif re.search('help delete',body):
        respString = 'To delete prev purchases you input, type delete last (number) purchases.\n' \
                     'Ex: delete last 1 purchase or delete last 5 transactions.'
    elif re.search('help transfe(r|rs)',body):
        respString = 'To transfer $ from 1 budget to another follow this ex: transfer 25 from misc to house default needed more mulch.\n' \
                     ' You can also use now, current or today instead of default to indicate the current month. Type out month name for previous months.'
    elif re.search('help (allowance|budget total)', body):
        respString = 'To get the amounts allotted for budgets, include allowance or budget total in your text, including which budgets and which months.\n' \
                     'Ex: get all budget totals for this month or get food, house and misc for last month or get food budget totals for oct and sept.'
    else:
        respString = "Not sure what you're asking. Try typing help please"
    
    return respString
