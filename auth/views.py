# -*- coding: utf-8 -*-

from django.shortcuts import render,render_to_response, redirect
from django.contrib.auth import authenticate, login
from auth import forms as fo
from django.contrib.auth import logout
from django.shortcuts import render
from django.core.urlresolvers import reverse


def connexion(request):

    error = False


    if request.method == "POST":

        form = fo.ConnexionForm(request.POST)

        if form.is_valid():

            username = form.cleaned_data["username"]

            password = form.cleaned_data["password"]

            user = authenticate(username=username, password=password)  # Nous vérifions si les données sont correctes

            if user:  # Si l'objet renvoyé n'est pas None

                login(request, user)  # nous connectons l'utilisateur

            else: # sinon une erreur sera affichée

                error = True

    else:

        form = fo.ConnexionForm()


    return render(request, 'auth/login.html', locals())



def deconnexion(request):
    logout(request)
    return redirect(reverse(connexion))