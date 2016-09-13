from django.conf.urls import patterns, url

urlpatterns = patterns('charts.views',
    url(r'^(?P<marketname>[^/]+)/orders$', 'orders'),
    url(r'^(?P<marketname>[^/]+)/users$', 'users'),
    url(r'^(?P<marketname>[^/]+)/events$', 'events')
)
