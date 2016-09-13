from django.shortcuts import render

# Create your views here.

from django.shortcuts import render
from django.utils.datastructures import MultiValueDictKeyError
import datetime as dt
from django.contrib.auth.decorators import login_required
# Create your views here.


def frequencies(request,marketname):

    start, end = _get_dates(request)
    dbug = _get_debug_value(request)

    try:
        ruleset_id = request.GET["ruleset_id"]
    except:
        ruleset_id = None

    return render(request,"reco/home.html",{"marketname":marketname,"start":start.isoformat(),"end":end.isoformat(),
                                               "ruleset_id":ruleset_id, "dbug": dbug})


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
