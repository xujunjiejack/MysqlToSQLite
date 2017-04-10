from datetime import datetime

def calc_age(tdate,fdate, format_string='%m/%d/%Y'):
    '''
        calculates the difference in months betweet two dates given either 
        as datetime objects of as a string of the form 
        format_string using standard satetime conventions.

        throws we.date_empty_err
        or we.date_wrong_format_err

        you should probably replace fdate and tdate with the column names 
        they come from. this function isn't aware of that.
    '''
    print(tdate)
    print(fdate)
    

    # make sure the dates are datetimeobjects
    try:
        if type(tdate) is not datetime:
            tdate = datetime.strptime(tdate, format_string)
    except ValueError:
        # if the data is in string format then we check for %m/%d/%Y. If the date is not in that format then we check for another date format : '%Y-%m-%d %H:%M:%S'
        # This has to be extended as a seperate case as the code has been modified and not in the initial design.
        try:
            if type(tdate) is not datetime:
                tdate = datetime.strptime(tdate, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            raise we.date_wrong_format_err('tdate(%s) '%tdate           \
                + 'is not in the correct format(%s)'%format_string)
    
    try:
        if type(fdate) is not datetime:
            fdate = datetime.strptime(fdate, format_string)
    except ValueError:
        try:
            if type(tdate) is not datetime:
                tdate = datetime.strptime(tdate, '%Y-%m-%d %H:%M:%S')
                print(tdate.strftime("%m/%d/%Y"))
        except ValueError:
            raise we.date_wrong_format_err('fdate(%s)'%tdate            \
                + ' is not in the correct format(%s)'%format_string)
    
    return (tdate.year - fdate.year)*12 + tdate.month - fdate.month

#ans = calc_age("01/31/1990","1995-02-01 00:00:00")

#print(ans)

mydate = "1995-02-01 00:00:00"
mydate = datetime.strptime(mydate, '%Y-%m-%d %H:%M:%S')
print(mydate.year)