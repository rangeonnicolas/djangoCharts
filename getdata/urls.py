__author__ = 'developpeur'

from django.conf.urls import patterns, url

urlpatterns = patterns('getdata.views',
    url(r'^(?P<marketname>[^/]+)/(?P<filename>\S+)$', 'getdata'),
)

