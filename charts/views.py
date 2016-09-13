from django.shortcuts import render
from django.utils.datastructures import MultiValueDictKeyError
import datetime as dt
from django.contrib.auth.decorators import login_required
# Create your views here.





@login_required
def orders(request,marketname):

    start, end = _get_dates(request)
    dbug = _get_debug_value(request)

    return render(request,"charts/orders.html",{"marketname":marketname,"start":start.isoformat(),"end":end.isoformat(),
                                                   "dbug": dbug})

@login_required
def users(request,marketname):

    start, end = _get_dates(request)
    dbug = _get_debug_value(request)

    return render(request,"charts/users.html",{"marketname":marketname,"start":start.isoformat(),"end":end.isoformat(),
                                               "dbug": dbug})

@login_required
def events(request,marketname):

    start, end = _get_dates(request)
    dbug = _get_debug_value(request)

    return render(request,"charts/events.html",{"marketname":marketname,"start":start.isoformat(),"end":end.isoformat(),
                                               "dbug": dbug})

def _get_dates(request):
    end = dt.datetime.now()
    try:
        days = int(request.GET["days"])
    except ValueError:
        raise BaseException("argument 'days' is invalid. It must be of type 'int'")                                     # TODO: renvoyer une page d erreur plutot qu'une exception
    except MultiValueDictKeyError:
        days = 30
    start = end - dt.timedelta(days,0,0)
    return start, end

def _get_debug_value(request):
    try:
        dbug = request.GET["dbug"]
    except:
        return False
    return dbug
