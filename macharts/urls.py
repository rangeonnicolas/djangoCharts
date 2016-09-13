from django.conf.urls import patterns, include, url
from django.contrib import admin

urlpatterns = patterns('',
    url(r'^admin/', include(admin.site.urls)),
    url(r'^charts/',include('charts.urls')),
    url(r'^reco/',include('reco.urls')),
    url(r'^api/'   ,include('getdata.urls')),
    url(r'^connexion/','auth.views.connexion',name = 'connexion'),
    url(r'^deconnexion/', 'auth.views.deconnexion', name='deconnexion')
)
