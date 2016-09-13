from django.conf.urls import patterns, url

urlpatterns = patterns('reco.views',
    url(r'^(?P<marketname>[^/]+)/$', 'frequencies')
)
